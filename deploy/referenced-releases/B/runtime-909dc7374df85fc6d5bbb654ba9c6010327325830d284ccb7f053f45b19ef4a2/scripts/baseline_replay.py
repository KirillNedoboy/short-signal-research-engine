"""Staged offline Phase 4 BASELINE replay workflow.

The command intentionally has no implicit network/database access.  Fetch
stages only copy caller-supplied archives; replay stages consume normalized
files and the broad stage is hard-gated on a sealed control PASS report.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import yaml

from app.config import AppConfig
from app.infra.disk_capacity import derive_reserve_bytes, guarded_copy, read_capacity
from app.replay.market import CandleObservationKind, normalize_bybit_klines
from app.research.baseline_replay import (
    BaselineReplayDriver,
    ControlExpectation,
    ReplayOpportunity,
)
from app.research.replay import build_bundle


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m scripts.baseline_replay")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("fetch-controls", "normalize-controls"):
        command = commands.add_parser(name)
        command.add_argument("--input", required=True, type=Path)
        command.add_argument("--output", required=True, type=Path)
    replay = commands.add_parser("replay-controls")
    replay.add_argument("--spec", required=True, type=Path)
    replay.add_argument("--report", required=True, type=Path)
    verify = commands.add_parser("verify-controls")
    verify.add_argument("--report", required=True, type=Path)
    fetch_broad = commands.add_parser("fetch-broad")
    fetch_broad.add_argument("--controls-report", required=True, type=Path)
    fetch_broad.add_argument("--input", required=True, type=Path)
    fetch_broad.add_argument("--output", required=True, type=Path)
    broad = commands.add_parser("replay-broad")
    broad.add_argument("--controls-report", required=True, type=Path)
    broad.add_argument("--spec", required=True, type=Path)
    broad.add_argument("--report", required=True, type=Path)
    bundle = commands.add_parser("build-bundle")
    bundle.add_argument("--spec", required=True, type=Path)
    bundle.add_argument("--config", required=True, type=Path)
    bundle.add_argument("--output-root", required=True, type=Path)
    bundle.add_argument("--run-id")
    bundle.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()

    if args.command == "fetch-controls":
        _copy_input(args.input, args.output)
        return 0
    if args.command == "normalize-controls":
        _normalize_file(args.input, args.output)
        return 0
    if args.command == "replay-controls":
        report = _run_spec(args.spec)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
        )
        return 0 if report["passed"] else 1
    if args.command == "verify-controls":
        report = _read_gate(args.report)
        print(json.dumps(report, sort_keys=True))
        return (
            0
            if report["passed"] and report["classification"] == "CONTROL_REPLAY_PASS"
            else 1
        )
    if args.command == "fetch-broad":
        _require_pass(args.controls_report)
        _copy_input(args.input, args.output)
        return 0
    if args.command == "replay-broad":
        _require_pass(args.controls_report)
        report = _run_spec(args.spec)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
        )
        return 0
    config = AppConfig.model_validate(
        yaml.safe_load(args.config.read_text(encoding="utf-8"))
    )
    path = build_bundle(
        args.spec, config, args.output_root, args.run_id, args.allow_dirty
    )
    print(path)
    return 0


def _copy_input(source: Path, target: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    snapshot = read_capacity(source)
    reserve = derive_reserve_bytes(
        snapshot,
        log_wal_margin_bytes=AppConfig().disk_log_wal_margin_bytes,
        minimum_reserve_bytes=AppConfig().disk_min_safety_reserve_bytes,
    )
    guarded_copy(source, target, reserve_bytes=reserve, snapshot=snapshot)


def _normalize_file(source: Path, target: Path) -> None:
    raw = json.loads(source.read_text(encoding="utf-8"))
    pages = raw.get("pages", raw) if isinstance(raw, dict) else raw
    symbol = raw.get("symbol", "UNKNOWN") if isinstance(raw, dict) else "UNKNOWN"
    frame, report = normalize_bybit_klines(pages, symbol=symbol)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {"rows": frame.to_dict(orient="records"), "report": asdict(report)},
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )


def _run_spec(path: Path) -> dict[str, object]:
    spec = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    driver = BaselineReplayDriver(AppConfig())
    opportunities: list[ReplayOpportunity] = []
    expectations: list[ControlExpectation] = []
    for row in spec.get("opportunities", []):
        frame = pd.DataFrame(row["candles"])
        kind = CandleObservationKind(
            str(row.get("kind", CandleObservationKind.CLOSED_CANDLE))
        )
        opportunities.append(
            ReplayOpportunity(
                row["symbol"],
                _parse_utc(row["decision_time_utc"]),
                frame,
                kind,
                sequence=int(row.get("sequence", 0)),
                observability_flags=tuple(row.get("observability_flags", ())),
            )
        )
        if row.get("expected"):
            expected = row["expected"]
            expectations.append(
                ControlExpectation(
                    str(expected["control_id"]),
                    row["symbol"],
                    _parse_utc(row["decision_time_utc"]),
                    int(expected["score"]),
                    str(expected["grade"]),
                    str(expected["signal_type"]),
                )
            )
    _, control_report = driver.replay_controls(opportunities, expectations)
    return {
        "passed": control_report.passed,
        "classification": control_report.classification,
        "mismatches": list(control_report.mismatches),
    }


def _read_gate(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, dict)
        or "passed" not in payload
        or "classification" not in payload
    ):
        raise ValueError("invalid control report seal")
    return payload


def _require_pass(path: Path) -> None:
    report = _read_gate(path)
    if not report["passed"] or report["classification"] != "CONTROL_REPLAY_PASS":
        raise RuntimeError("broad replay is sealed until both controls pass")


def _parse_utc(value: str):
    from datetime import datetime, timezone

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


if __name__ == "__main__":
    raise SystemExit(main())
