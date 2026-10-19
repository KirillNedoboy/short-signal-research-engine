from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_enabled_strategy_actionable_replays_match_frozen_c() -> None:
    root = Path(__file__).parents[1]
    completed = subprocess.run(
        [sys.executable, str(root / "tools" / "strategy_parity.py")],
        cwd=root.parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    report = json.loads(completed.stdout)
    assert report["verification"] == "PASS"
    assert len(report["cases"]) == 4
    assert all(case["strategy_gate"] for case in report["cases"])
    assert all(case["all_comparisons_pass"] for case in report["cases"])
    assert all(case["lifecycle_snapshot_preserved"] for case in report["cases"])
