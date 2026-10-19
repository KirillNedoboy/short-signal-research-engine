"""Offline replay of bounded Lane C shadow captures.

Each captured event is fed to the frozen C package and the candidate package in
separate subprocesses.  The report contains only bounded projections and exact
comparison booleans; it never reads a production database or invokes a live
service.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


RELEASE = "sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a"
WORKER = Path(__file__).with_name("shadow_replay_worker.py")
DEFAULT_MAX_RECORDS = 4096
DEFAULT_MAX_INPUT_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_SECONDS = 300.0


def _comparison_module():
    # The CLI itself needs the candidate helper only for comparison.  Workers
    # receive an explicit source root and never inherit this import path.
    candidate_src = Path(__file__).parents[1] / "src"
    if str(candidate_src) not in sys.path:
        sys.path.insert(0, str(candidate_src))
    from app.runtime.shadow_comparison import compare_results

    return compare_results


def _source_root(value: str | Path) -> Path:
    path = Path(value).expanduser().resolve()
    if path.name == "app" and path.is_dir():
        path = path.parent
    if not path.is_dir() or not (path / "app").is_dir():
        raise ValueError(f"source root must contain an app package: {path}")
    return path


def _capture_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    preferred = root / "raw" / "shadow-inputs.jsonl"
    if preferred.exists():
        return [preferred]
    return sorted(
        path
        for path in root.rglob("*.jsonl")
        if path.name != "shadow-aggregates.jsonl"
    )


def _load_captures_bounded(
    root: str | Path,
    *,
    max_records: int | None = None,
    max_bytes: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
    """Load capture records with explicit input bounds.

    Once a bound is reached, the remaining input is represented by one
    synthetic UNKNOWN aggregate in the report.  This keeps the CLI bounded
    without turning an unprocessed tail into PASS.
    """

    captures: list[dict[str, Any]] = []
    bytes_read = 0
    malformed = 0
    truncated = False
    for path in _capture_files(Path(root).expanduser().resolve()):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line_bytes = len(line.encode("utf-8"))
                    if max_bytes is not None and bytes_read + line_bytes > max_bytes:
                        truncated = True
                        return captures, {
                            "bytes_read": bytes_read,
                            "malformed": malformed,
                            "truncated": truncated,
                        }
                    bytes_read += line_bytes
                    if not line.strip():
                        continue
                    try:
                        value = json.loads(line)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        malformed += 1
                        continue
                    if isinstance(value, dict) and value.get("kind") == "shadow_capture":
                        if max_records is not None and len(captures) >= max_records:
                            truncated = True
                            return captures, {
                                "bytes_read": bytes_read,
                                "malformed": malformed,
                                "truncated": truncated,
                            }
                        captures.append(value)
        except (OSError, UnicodeDecodeError):
            # A malformed optional capture is represented in the report rather
            # than preventing other bounded records from being replayed.
            malformed += 1
            continue
    return captures, {
        "bytes_read": bytes_read,
        "malformed": malformed,
        "truncated": truncated,
    }


def load_captures(root: str | Path) -> list[dict[str, Any]]:
    """Load all capture records for callers that do not need replay bounds."""

    captures, _ = _load_captures_bounded(root)
    return captures


def _safe_environment() -> dict[str, str]:
    blocked = (
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "CREDENTIAL",
        "CHAT_ID",
        "DATABASE_URL",
        "DB_URL",
        "DSN",
        "API_KEY",
        "AUTHORIZATION",
    )
    environment = {
        key: value
        for key, value in os.environ.items()
        if not any(part in key.upper() for part in blocked)
    }
    environment.update({"PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1"})
    return environment


def _run_worker(
    source_root: Path,
    record_file: Path,
    implementation: str,
    *,
    worker: Path = WORKER,
) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            str(worker),
            "--source-root",
            str(source_root),
            "--record",
            str(record_file),
            "--implementation",
            implementation,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_safe_environment(),
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"{implementation} shadow replay failed ({completed.returncode}): {detail[-1600:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"{implementation} shadow replay emitted no JSON")
    value = json.loads(lines[-1])
    if not isinstance(value, dict):
        raise RuntimeError(f"{implementation} shadow replay emitted non-object JSON")
    return value


def _run_worker_batch(
    source_root: Path,
    records: list[tuple[int, Mapping[str, Any]]],
    implementation: str,
    *,
    timeout: float,
    worker: Path = WORKER,
) -> dict[int, dict[str, Any]]:
    """Replay a bounded batch through one isolated worker process.

    The stdin JSONL protocol keeps the old and candidate package interpreters
    isolated while avoiding one subprocess per captured frame.
    """

    if not records:
        return {}
    payload = "".join(
        json.dumps(
            {"index": index, "record": record},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for index, record in records
    )
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(worker),
                "--source-root",
                str(source_root),
                "--implementation",
                implementation,
                "--stdin-jsonl",
            ],
            input=payload,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, float(timeout)),
            env=_safe_environment(),
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"{implementation} replay exceeded time cap") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"{implementation} shadow replay batch failed ({completed.returncode}): {detail[-1600:]}"
        )
    outputs: dict[int, dict[str, Any]] = {}
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
            index = int(value["index"])
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"{implementation} shadow replay emitted malformed batch JSON") from exc
        if not isinstance(value, dict):
            raise RuntimeError(f"{implementation} shadow replay emitted non-object batch JSON")
        if value.get("ok"):
            result = value.get("result")
            if not isinstance(result, dict):
                raise RuntimeError(f"{implementation} shadow replay emitted non-object result")
            outputs[index] = {"result": result}
        else:
            outputs[index] = {"error": str(value.get("error") or "worker_error")[-1600:]}
    return outputs


def _record_key(record: Mapping[str, Any], index: int) -> str:
    symbol = str(record.get("symbol") or "UNKNOWN")
    stamp = str(record.get("captured_at_utc") or index)
    return f"{symbol}:{stamp}:{index}"


def _replay_one(
    record: Mapping[str, Any],
    *,
    old_root: Path,
    candidate_root: Path,
    record_file: Path,
    compare_results: Any,
) -> dict[str, Any]:
    input_status = str(record.get("input_status") or "unknown")
    final_required = bool(record.get("final_recheck_required"))
    final_captured = bool(record.get("final_recheck_captured"))
    if input_status != "captured":
        comparison = compare_results(
            None,
            None,
            input_status=input_status,
            final_recheck_required=final_required,
            final_recheck_captured=final_captured,
        )
        return {
            "key": _record_key(record, 0),
            "symbol": record.get("symbol"),
            "input_status": input_status,
            "comparison": comparison.as_dict(),
            "replayed": False,
        }
    old_result: dict[str, Any] | None = None
    candidate_result: dict[str, Any] | None = None
    errors: list[str] = []
    try:
        old_result = _run_worker(old_root, record_file, "old")
    except Exception as exc:  # noqa: BLE001 - classify one capture as UNKNOWN
        errors.append(f"old:{type(exc).__name__}:{exc}")
    try:
        candidate_result = _run_worker(candidate_root, record_file, "candidate")
    except Exception as exc:  # noqa: BLE001 - classify one capture as UNKNOWN
        errors.append(f"candidate:{type(exc).__name__}:{exc}")
    comparison = compare_results(
        old_result,
        candidate_result,
        input_status=input_status,
        final_recheck_required=final_required,
        final_recheck_captured=final_captured,
        error="; ".join(errors) if errors else None,
    )
    return {
        "key": _record_key(record, 0),
        "symbol": record.get("symbol"),
        "input_status": input_status,
        "comparison": comparison.as_dict(),
        "replayed": not errors,
        "errors": errors,
        "old_identity": (old_result or {}).get("identity"),
        "candidate_identity": (candidate_result or {}).get("identity"),
    }


def run(
    *,
    capture_root: str | Path,
    old_root: str | Path,
    candidate_root: str | Path,
    output: str | Path | None = None,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_input_bytes: int = DEFAULT_MAX_INPUT_BYTES,
    max_seconds: float = DEFAULT_MAX_SECONDS,
) -> dict[str, Any]:
    if max_records < 1 or max_input_bytes < 1 or max_seconds <= 0:
        raise ValueError("replay bounds must be positive")
    captures, load_stats = _load_captures_bounded(
        capture_root,
        max_records=max_records,
        max_bytes=max_input_bytes,
    )
    old_source = _source_root(old_root)
    candidate_source = _source_root(candidate_root)
    compare_results = _comparison_module()
    reports: list[dict[str, Any]] = []
    worker_errors: dict[str, str] = {}
    captured_records = [
        (index, record)
        for index, record in enumerate(captures)
        if str(record.get("input_status") or "unknown") == "captured"
    ]
    old_outputs: dict[int, dict[str, Any]] = {}
    candidate_outputs: dict[int, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="lane-c-shadow-replay-") as temp_name:
        temp_root = Path(temp_name)
        # Keep the temporary root in scope for compatibility with single
        # record debugging helpers; the bounded batch path streams JSONL over
        # stdin and does not write one file per capture.
        _ = temp_root
        deadline = time.monotonic() + float(max_seconds)
        if captured_records:
            try:
                old_outputs = _run_worker_batch(
                    old_source,
                    captured_records,
                    "old",
                    timeout=max(0.01, deadline - time.monotonic()),
                )
            except Exception as exc:  # noqa: BLE001 - classify every missing result as UNKNOWN
                worker_errors["old"] = f"{type(exc).__name__}:{exc}"
            if time.monotonic() < deadline:
                try:
                    candidate_outputs = _run_worker_batch(
                        candidate_source,
                        captured_records,
                        "candidate",
                        timeout=max(0.01, deadline - time.monotonic()),
                    )
                except Exception as exc:  # noqa: BLE001 - classify every missing result as UNKNOWN
                    worker_errors["candidate"] = f"{type(exc).__name__}:{exc}"
            else:
                worker_errors["candidate"] = "replay_time_cap:overall replay time cap exhausted"

        for index, record in enumerate(captures):
            input_status = str(record.get("input_status") or "unknown")
            final_required = bool(record.get("final_recheck_required"))
            final_captured = bool(record.get("final_recheck_captured"))
            errors: list[str] = []
            old_result: dict[str, Any] | None = None
            candidate_result: dict[str, Any] | None = None
            if input_status == "captured":
                old_entry = old_outputs.get(index)
                candidate_entry = candidate_outputs.get(index)
                if old_entry is None:
                    errors.append(worker_errors.get("old", "old:missing batch result"))
                elif old_entry.get("error"):
                    errors.append(f"old:{old_entry['error']}")
                else:
                    old_result = old_entry.get("result")
                if candidate_entry is None:
                    errors.append(worker_errors.get("candidate", "candidate:missing batch result"))
                elif candidate_entry.get("error"):
                    errors.append(f"candidate:{candidate_entry['error']}")
                else:
                    candidate_result = candidate_entry.get("result")
            comparison = compare_results(
                old_result,
                candidate_result,
                input_status=input_status,
                final_recheck_required=final_required,
                final_recheck_captured=final_captured,
                error="; ".join(errors) if errors else None,
            )
            reports.append(
                {
                    "key": _record_key(record, index),
                    "symbol": record.get("symbol"),
                    "input_status": input_status,
                    "comparison": comparison.as_dict(),
                    "replayed": not errors and input_status == "captured",
                    "errors": errors,
                    "old_identity": (old_result or {}).get("identity"),
                    "candidate_identity": (candidate_result or {}).get("identity"),
                }
            )
    counts = Counter(item["comparison"]["status"] for item in reports)
    malformed_unknown = int(load_stats["malformed"])
    truncated_unknown = 1 if bool(load_stats["truncated"]) else 0
    incomplete_unknown = malformed_unknown + truncated_unknown
    if incomplete_unknown:
        counts["UNKNOWN"] += incomplete_unknown
    if not reports:
        verification = "UNKNOWN" if incomplete_unknown else "EMPTY"
    elif counts.get("MISMATCH", 0):
        verification = "MISMATCH"
    elif counts.get("UNKNOWN", 0):
        verification = "UNKNOWN"
    else:
        verification = "PASS"
    report = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "verification": verification,
        "capture_root": str(Path(capture_root).expanduser().resolve().name),
        "old_source": str(old_source.name),
        "candidate_source": str(candidate_source.name),
        "reference_scope": {
            "old": "frozen.app.main.ShortSignalBot._process_symbol",
            "old_fast_monitor": (
                "frozen.app.main.ShortSignalBot._evaluate_and_send_climax"
            ),
            "candidate": "candidate.runtime.pipeline.LaneCStrategyPipeline.evaluate",
            "candidate_fast_monitor": (
                "candidate.runtime.pipeline.LaneCStrategyPipeline.evaluate_climax_only"
            ),
            "comparison_boundary": "pre_persistence",
            "old_repository_context": (
                "repository-dependent admission is UNKNOWN when captured owner context is absent"
            ),
        },
        "records_seen": len(captures) + incomplete_unknown,
        "records_replayed": sum(item["replayed"] for item in reports),
        "counts": {
            "PASS": counts.get("PASS", 0),
            "MISMATCH": counts.get("MISMATCH", 0),
            "UNKNOWN": counts.get("UNKNOWN", 0),
        },
        "records": reports,
        "incomplete_aggregates": [
            {
                "status": "UNKNOWN",
                "count": incomplete_unknown,
                "reason": (
                    "replay_input_bound_exhausted"
                    if truncated_unknown
                    else "malformed_capture_input"
                ),
            }
        ]
        if incomplete_unknown
        else [],
        "limits": {
            "max_records": max_records,
            "max_input_bytes": max_input_bytes,
            "max_seconds": max_seconds,
            "input_bytes_read": int(load_stats["bytes_read"]),
        },
        "worker_processes": {"old": 1 if captured_records else 0, "candidate": 1 if captured_records else 0},
        "worker_errors": worker_errors,
        "schedule_comparison": {
            "status": "UNSUPPORTED",
            "reason": "same-input replay does not prove historical production scheduling",
        },
        "historical_telegram_comparison": {
            "status": "UNSUPPORTED",
            "reason": "Telegram timing/history is outside the captured semantic input",
        },
        "production_24h_claim": False,
        "capturable_data": [
            "immutable raw frame_1m rows and frame metadata",
            "derivatives and liquidity mappings present in the decision snapshot",
            "captured/as-of timestamps and availability bound",
            "deep-copied BEFORE EventState",
            "fast-monitor enabled/poll sequence",
            "candidate PipelineResult and explicit fresh final-recheck snapshot when supplied",
            "strategy configuration fingerprint and explicit decision allowlist",
        ],
        "limitations": [
            "Sampling, byte-budget drops, missing inputs, and missing final rechecks are UNKNOWN.",
            "Each implementation is replayed through one bounded app-package subprocess per report.",
            "The frozen side executes ShortSignalBot._process_symbol normally and _evaluate_and_send_climax for fast-monitor records at the pre-persistence boundary; missing repository-owner context is UNKNOWN.",
            "Fast-monitor candidate records require LaneCStrategyPipeline.evaluate_climax_only; a missing seam is UNKNOWN and never falls back to the full pipeline.",
            "Input record/time bounds can leave an UNKNOWN incomplete aggregate.",
            "The report does not claim a completed 24-hour shadow window.",
        ],
    }
    if output is not None:
        output_path = Path(output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-root", required=True, type=Path)
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
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--max-input-bytes", type=int, default=DEFAULT_MAX_INPUT_BYTES)
    parser.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    args = parser.parse_args()
    report = run(
        capture_root=args.capture_root,
        old_root=args.old_root,
        candidate_root=args.candidate_root,
        output=args.output,
        max_records=args.max_records,
        max_input_bytes=args.max_input_bytes,
        max_seconds=args.max_seconds,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["verification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

