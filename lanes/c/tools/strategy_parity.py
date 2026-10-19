"""Replay actionable cases through frozen C and candidate admission paths."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


RELEASE = "sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a"


def _run_worker(worker: Path, source_root: Path, fixture: Path, implementation: str) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            str(worker),
            "--source-root",
            str(source_root),
            "--fixture",
            str(fixture),
            "--implementation",
            implementation,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{implementation} strategy replay failed for {fixture.name}: "
            f"{completed.stderr.strip()[-1600:]}"
        )
    return json.loads(completed.stdout)


def _eval_projection(result: dict[str, Any]) -> list[dict[str, Any]]:
    fields = ("strategy_type", "strategy_subtype", "model_version", "actionable", "score", "grade", "vetoes", "blockers")
    return [{field: item.get(field) for field in fields} for item in result.get("evaluations", [])]


def _decision_projection(result: dict[str, Any]) -> dict[str, Any] | None:
    decision = result.get("decision")
    if not decision:
        return None
    fields = (
        "symbol", "event_id", "signal_type", "grade", "score", "market_price",
        "short_zone_low", "short_zone_high", "actionable", "strategy_type",
        "strategy_subtype", "model_version", "lifecycle_state", "reasons",
        "risk_flags", "blockers",
    )
    return {field: decision.get(field) for field in fields}


def _state_projection(result: dict[str, Any]) -> dict[str, Any] | None:
    state = result.get("state")
    if not state:
        return None
    # The frozen C method records lifecycle bookkeeping in its repository. The
    # candidate keeps the same bounded bookkeeping in the event snapshot, so
    # compare the identity/state fields here and report snapshot differences
    # separately instead of hiding them.
    fields = ("symbol", "event_id", "state", "event_start_time", "event_high", "event_high_time", "zone_low", "zone_high", "signal_id")
    return {field: state.get(field) for field in fields}


def run(*, cases: Path, old_root: Path, candidate_root: Path) -> dict[str, Any]:
    payload = json.loads(cases.read_text(encoding="utf-8"))
    worker = Path(__file__).with_name("strategy_parity_worker.py")
    reports: list[dict[str, Any]] = []
    for case in payload.get("cases", []):
        fixture = cases.with_name(f".strategy-case-{case['name']}.json")
        fixture.write_text(json.dumps(case, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")
        try:
            old = _run_worker(worker, old_root, fixture, "old")
            candidate = _run_worker(worker, candidate_root, fixture, "candidate")
        finally:
            fixture.unlink(missing_ok=True)
        old_snapshot = (old.get("state") or {}).get("event_features_snapshot") or {}
        candidate_snapshot = (candidate.get("state") or {}).get("event_features_snapshot") or {}
        comparison = {
            "features": old.get("features") == candidate.get("features"),
            "evaluations": _eval_projection(old) == _eval_projection(candidate),
            "decision": _decision_projection(old) == _decision_projection(candidate),
            "identity": old.get("identity") == candidate.get("identity"),
            "state_core": _state_projection(old) == _state_projection(candidate),
        }
        expected = {
            "strategy_type": case.get("expected_strategy_type"),
            "strategy_subtype": case.get("expected_strategy_subtype"),
        }
        identity = candidate.get("identity") or {}
        strategy_gate = (
            bool(candidate.get("actionable"))
            and identity.get("strategy_type") == expected["strategy_type"]
            and identity.get("strategy_subtype") == expected["strategy_subtype"]
        )
        reports.append(
            {
                "case": case["name"],
                "expected": expected,
                "old_actionable": bool(old.get("actionable")),
                "candidate_actionable": bool(candidate.get("actionable")),
                "strategy_gate": strategy_gate,
                "comparisons": comparison,
                "all_comparisons_pass": all(comparison.values()),
                "registry_size": len(candidate.get("evaluations", [])),
                "lifecycle_snapshot_keys_old": sorted(old_snapshot),
                "lifecycle_snapshot_keys_candidate": sorted(candidate_snapshot),
                "lifecycle_snapshot_preserved": old_snapshot == candidate_snapshot,
            }
        )
    passed = bool(reports) and all(
        item["strategy_gate"] and item["all_comparisons_pass"] and item["registry_size"] == 5
        for item in reports
    )
    return {
        "verification": "PASS" if passed else "FAIL",
        "fixture_sha256": hashlib.sha256(cases.read_bytes()).hexdigest(),
        "cases": reports,
        "production_24h_claim": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cases",
        type=Path,
        default=Path(__file__).parents[1] / "tests" / "fixtures" / "strategy_cases.json",
    )
    parser.add_argument(
        "--old-root",
        type=Path,
        default=Path(__file__).parents[1] / "release" / RELEASE,
    )
    parser.add_argument(
        "--candidate-root",
        type=Path,
        default=Path(__file__).parents[1] / "src",
    )
    args = parser.parse_args()
    report = run(
        cases=args.cases.resolve(),
        old_root=args.old_root.resolve(),
        candidate_root=args.candidate_root.resolve(),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["verification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
