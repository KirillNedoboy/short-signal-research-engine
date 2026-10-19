from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "tools"))

from parity_replay import RELEASE, run


def test_frozen_c_and_candidate_match_on_actionable_fixture() -> None:
    lane_root = Path(__file__).parents[1]
    report = run(
        fixture=lane_root / "tests" / "fixtures" / "parity_fixture.json",
        old_root=lane_root / "release" / RELEASE,
        candidate_root=lane_root / "src",
    )
    assert report["verification"] == "PASS"
    assert report["actionable_case_present"] is True
    assert report["signal_identity_match"] is True
    assert report["comparisons"]["features"] is True
    assert report["comparisons"]["state"] is True
    assert report["comparisons"]["decision"] is True
