"""Compare the frozen C semantic path with the candidate in separate processes.

The replay intentionally covers the production feature builder, event
lifecycle, canonical registry and signal projection.  It is a deterministic
semantic gate, not a claim that a 24-hour production run has been reproduced.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


RELEASE = "sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a"


def _walk_diff(left: Any, right: Any, path: str = "$") -> list[str]:
    if type(left) is not type(right):
        return [f"{path}: type {type(left).__name__} != {type(right).__name__}"]
    if isinstance(left, dict):
        differences: list[str] = []
        for key in sorted(set(left) | set(right)):
            if key not in left:
                differences.append(f"{path}.{key}: missing in old")
            elif key not in right:
                differences.append(f"{path}.{key}: missing in candidate")
            else:
                differences.extend(_walk_diff(left[key], right[key], f"{path}.{key}"))
        return differences
    if isinstance(left, list):
        differences = []
        if len(left) != len(right):
            differences.append(f"{path}: length {len(left)} != {len(right)}")
        for index, (old_item, candidate_item) in enumerate(zip(left, right)):
            differences.extend(_walk_diff(old_item, candidate_item, f"{path}[{index}]"))
        return differences
    if left != right:
        return [f"{path}: {left!r} != {right!r}"]
    return []


def _run_worker(worker: Path, source_root: Path, fixture: Path, implementation: str) -> dict[str, Any]:
    command = [
        sys.executable,
        str(worker),
        "--source-root",
        str(source_root),
        "--fixture",
        str(fixture),
        "--implementation",
        implementation,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            f"{implementation} replay failed with exit {completed.returncode}: "
            f"{completed.stderr.strip()[-1200:]}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"{implementation} replay did not return JSON: {completed.stdout[-1200:]}"
        ) from exc


def run(*, fixture: Path, old_root: Path, candidate_root: Path) -> dict[str, Any]:
    worker = Path(__file__).with_name("parity_worker.py")
    old_result = _run_worker(worker, old_root, fixture, "old")
    candidate_result = _run_worker(worker, candidate_root, fixture, "candidate")
    old_steps = old_result.get("steps", [])
    candidate_steps = candidate_result.get("steps", [])
    differences = _walk_diff(old_result, candidate_result)
    step_names = ("features", "state", "decision", "evaluations")
    comparisons = {
        name: old_steps == candidate_steps
        if name == "all"
        else all(
            old_step.get(name) == candidate_step.get(name)
            for old_step, candidate_step in zip(old_steps, candidate_steps)
        )
        for name in (*step_names, "all")
    }
    actionable = bool(old_result.get("actionable")) and bool(candidate_result.get("actionable"))
    report = {
        "verification": "PASS" if not differences and actionable else "FAIL",
        "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
        "old_source": str(old_root),
        "candidate_source": str(candidate_root),
        "steps_compared": len(old_steps),
        "actionable_case_present": actionable,
        "signal_identity_match": old_result.get("signal_identity") == candidate_result.get("signal_identity"),
        "comparisons": comparisons,
        "difference_count": len(differences),
        "differences": differences[:20],
        "production_24h_claim": False,
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).parents[1] / "tests" / "fixtures" / "parity_fixture.json",
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
        fixture=args.fixture.resolve(),
        old_root=args.old_root.resolve(),
        candidate_root=args.candidate_root.resolve(),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["verification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
