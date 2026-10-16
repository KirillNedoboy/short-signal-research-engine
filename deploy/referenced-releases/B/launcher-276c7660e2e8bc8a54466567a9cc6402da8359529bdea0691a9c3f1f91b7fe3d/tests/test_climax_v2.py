from datetime import UTC, datetime, timedelta

import pandas as pd

from app.signals.climax_v2 import (
    ClimaxV2Input,
    evaluate_low_volume_extension_failure_v2,
    evaluate_volume_climax_v2,
)
from app.signals.trapped_longs_v2 import OiObservation

T = datetime(2026, 1, 1, tzinfo=UTC)


def frame(volumes=(10, 20, 40, 400, 10), closes=(99, 100, 105, 109, 99)):
    rows = []
    for index, (volume, close) in enumerate(zip(volumes, closes, strict=True)):
        start = T + timedelta(minutes=index * 5)
        high = 110 if index == 3 else max(close, 100)
        for minute in range(5):
            rows.append((start + timedelta(minutes=minute), close, high, 98, close, volume, volume * close))
    return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])


def input(**overrides):
    values = dict(
        symbol="X",
        event_id="e",
        event_base=100,
        event_high=110,
        event_high_at=T + timedelta(minutes=15),
        decision_at=T + timedelta(minutes=25),
        frame_1m=frame(),
        oi=(
            OiObservation(T + timedelta(minutes=19), 100),
            OiObservation(T + timedelta(minutes=24), 98),
        ),
        config={
            "max_age_minutes": 20,
            "pre_leg_candles": 2,
            "post_leg_candles": 1,
            "robust_history_candles": 3,
            "min_volume_z": 1,
            "oi_horizon_minutes": 5,
            "oi_sample_tolerance_minutes": 2,
            "max_oi_change_pct": -1,
            "min_extension_pct": 5,
            "max_contraction_ratio": 0.7,
        },
        frozen_breakout_reference=100,
    )
    values.update(overrides)
    return ClimaxV2Input(**values)


def test_fixed_anchor_windows_use_configured_complete_counts():
    result = evaluate_volume_climax_v2(input())
    legs = result.explanation["event_high_anchored_legs"].measurement
    assert legs["pre_count"] == 2
    assert legs["post_count"] == 1
    assert legs["robust_history_count"] == 3
    assert legs["pre_end"] < legs["high_close"] == legs["post_start"]


def test_forming_and_incomplete_bins_are_excluded():
    forming = evaluate_volume_climax_v2(
        input(config={**input().config, "post_leg_candles": 2}, decision_at=T + timedelta(minutes=24))
    )
    incomplete = evaluate_volume_climax_v2(input(frame_1m=frame().drop(index=[15])))
    assert "bounded_candle_history_missing" in forming.blockers
    assert "event_high_bin_missing" in incomplete.blockers


def test_selected_zero_volume_fails_closed():
    result = evaluate_volume_climax_v2(input(frame_1m=frame(volumes=(10, 20, 40, 0, 10))))
    assert "volume_missing_or_invalid" in result.blockers


def test_volume_branch_requires_nonzero_robust_mad():
    result = evaluate_volume_climax_v2(input(frame_1m=frame(volumes=(10, 10, 10, 400, 10))))
    assert "volume_mad_zero" in result.blockers


def test_volume_branch_enforces_minimum_robust_zscore():
    result = evaluate_volume_climax_v2(input(frame_1m=frame(volumes=(10, 20, 40, 45, 10))))
    assert "volume_not_climactic" in result.blockers
    assert not result.explanation["robust_volume_climax"].passed


def test_stale_event_fails_age_gate():
    result = evaluate_volume_climax_v2(input(decision_at=T + timedelta(minutes=40)))
    assert "event_age_invalid" in result.blockers


def test_unsorted_oi_uses_exact_asof_horizon_samples():
    observations = (
        OiObservation(T + timedelta(minutes=24), 98),
        OiObservation(T + timedelta(minutes=18), 101),
        OiObservation(T + timedelta(minutes=19), 100),
    )
    result = evaluate_volume_climax_v2(input(oi=observations))
    oi = result.explanation["oi_horizon"].measurement
    assert oi["baseline_at"] == (T + timedelta(minutes=19)).isoformat()
    assert oi["decision_at"] == (T + timedelta(minutes=24)).isoformat()
    assert oi["change_pct"] == -2


def test_missing_oi_blocks_volume_but_is_neutral_for_low_volume():
    volume = evaluate_volume_climax_v2(input(oi=()))
    low = evaluate_low_volume_extension_failure_v2(input(oi=()))
    assert "oi_predicate_not_met" in volume.blockers
    assert "oi_predicate_not_met" in low.blockers
    assert low.explanation["oi_horizon"].measurement == "UNKNOWN"


def test_low_volume_extension_is_hard_gate():
    result = evaluate_low_volume_extension_failure_v2(input(event_base=109))
    assert "extension_missing_or_below_threshold" in result.blockers


def test_low_volume_factors_are_unique_and_inputs_are_not_mutated():
    candidate = input()
    before = candidate.frame_1m.copy(deep=True)
    result = evaluate_low_volume_extension_failure_v2(candidate)
    assert list(result.explanation).count("volume_contraction") == 1
    assert list(result.explanation).count("latest_close_below_frozen_reference") == 1
    pd.testing.assert_frame_equal(candidate.frame_1m, before)
