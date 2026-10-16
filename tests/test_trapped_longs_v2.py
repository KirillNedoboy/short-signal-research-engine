from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from app.signals.trapped_longs_v2 import (
    FrozenBreakout,
    OiObservation,
    evaluate_trapped_longs_reversal_v2,
)


T0 = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)


def _frame(rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    minute_rows = []
    for timestamp, high, low, close, volume in rows:
        start = datetime.fromisoformat(timestamp.replace(chr(90), chr(43)+chr(48)+chr(48)+chr(58)+chr(48)+chr(48)))
        for offset in range(5):
            minute_rows.append((start + timedelta(minutes=offset), close, high, low, close, volume, volume * close))
    return pd.DataFrame(
        minute_rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"]
    )

def _candidate_frame() -> pd.DataFrame:
    return _frame(
        [
            ("2026-09-10T09:55:00Z", 99.0, 98.0, 98.5, 10.0),
            ("2026-09-10T10:00:00Z", 101.0, 99.0, 100.5, 12.0),
            ("2026-09-10T10:05:00Z", 99.0, 97.0, 98.0, 11.0),
            ("2026-09-10T10:10:00Z", 99.0, 97.0, 98.0, 9.0),
            ("2026-09-10T10:15:00Z", 100.0, 96.0, 97.0, 8.0),
        ]
    )


def _breakout() -> FrozenBreakout:
    return FrozenBreakout("root-7", T0, 100.0, 3)


def _oi() -> tuple[OiObservation, ...]:
    return (
        OiObservation(datetime(2026, 9, 10, 10, 0, tzinfo=UTC), 100.0),
        OiObservation(datetime(2026, 9, 10, 10, 5, tzinfo=UTC), 105.0),
        OiObservation(datetime(2026, 9, 10, 10, 10, tzinfo=UTC), 103.0),
        OiObservation(datetime(2026, 9, 10, 10, 20, tzinfo=UTC), 102.0),
    )


def test_excludes_forming_five_minute_bin_at_exact_boundary() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(), datetime(2026, 9, 10, 10, 15, tzinfo=UTC)
    )

    assert result.explanation["closed_5m_candles"].measurement == 4
    assert "failed_retest_missing" in result.blockers


def test_requires_caller_frozen_reference_without_synthetic_fallback() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(),
        FrozenBreakout("root-7", T0, None, 3),
        _oi(),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
    )

    assert "frozen_breakout_reference_missing" in result.blockers
    assert all(".995" not in str(factor.measurement) for factor in result.explanation.values())


def test_accepts_breakout_in_first_later_closed_bin() -> None:
    frame = _frame(
        [
            ("2026-09-10T09:55:00Z", 99.0, 98.0, 98.5, 10.0),
            ("2026-09-10T10:00:00Z", 100.5, 99.0, 99.5, 12.0),
            ("2026-09-10T10:05:00Z", 101.0, 99.0, 100.5, 12.0),
            ("2026-09-10T10:10:00Z", 99.0, 97.0, 98.0, 11.0),
            ("2026-09-10T10:15:00Z", 100.0, 96.0, 97.0, 8.0),
            ("2026-09-10T10:20:00Z", 100.0, 96.0, 97.0, 8.0),
        ]
    )
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), _oi(), datetime(2026, 9, 10, 10, 25, tzinfo=UTC)
    )

    assert "frozen_breakout_not_observed" not in result.blockers


def test_rejects_failure_before_breakout_and_requires_reference_tied_retest() -> None:
    failure_before_breakout = _frame(
        [
            ("2026-09-10T09:55:00Z", 99.0, 97.0, 98.0, 10.0),
            ("2026-09-10T10:00:00Z", 101.0, 99.0, 100.5, 10.0),
            ("2026-09-10T10:05:00Z", 101.0, 99.5, 100.5, 10.0),
            ("2026-09-10T10:10:00Z", 101.0, 99.5, 100.5, 10.0),
        ]
    )
    result = evaluate_trapped_longs_reversal_v2(
        failure_before_breakout, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert "failure_after_breakout_missing" in result.blockers
    assert "failed_retest_missing" in result.blockers


def test_accepts_complete_chronological_retest_and_oi_sequence() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert result.actionable
    assert result.blockers == ()
    assert result.explanation["failed_retest"].passed
    assert result.explanation["oi_sequence"].passed


def test_identity_includes_symbol_and_contract_material() -> None:
    decision_at = datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    first = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), FrozenBreakout("root-7", T0, 100.0, 3, "AAAUSDT"), (), decision_at
    )
    second = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), FrozenBreakout("root-7", T0, 100.0, 3, "BBBUSDT"), (), decision_at
    )
    assert first.identity != second.identity


def test_missing_oi_fails_closed_and_identity_is_deterministic_by_revision() -> None:
    decision_at = datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    first = evaluate_trapped_longs_reversal_v2(_candidate_frame(), _breakout(), (), decision_at)
    second = evaluate_trapped_longs_reversal_v2(_candidate_frame(), _breakout(), (), decision_at)
    next_revision = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), FrozenBreakout("root-7", T0, 100.0, 4), (), decision_at
    )

    assert "oi_sequence_unknown" in first.blockers
    assert first.identity == second.identity
    assert first.identity != next_revision.identity
    assert first.revision == 3


def test_selects_four_chronological_oi_points_from_asof_history() -> None:
    history = (
        OiObservation(datetime(2026, 9, 10, 9, 45, tzinfo=UTC), 98.0),
        *_oi(),
        OiObservation(datetime(2026, 9, 10, 10, 25, tzinfo=UTC), 101.0),
    )
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), history, datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert result.actionable
    assert result.explanation["oi_sequence"].passed


def test_does_not_mutate_frame_or_contract_inputs() -> None:
    frame = _candidate_frame()
    before = frame.copy(deep=True)
    breakout = _breakout()
    oi = _oi()

    evaluate_trapped_longs_reversal_v2(
        frame, breakout, oi, datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    pd.testing.assert_frame_equal(frame, before)
    assert breakout == _breakout()
    assert oi == _oi()


def test_rejects_incomplete_bin_and_high_touch_without_close_above_reference() -> None:
    incomplete = _candidate_frame().drop(index=[5])
    missing = evaluate_trapped_longs_reversal_v2(
        incomplete, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )
    high_touch = _candidate_frame().copy()
    high_touch.loc[(high_touch["timestamp"] >= T0) & (high_touch["timestamp"] < T0 + timedelta(minutes=5)), "close"] = 99.0
    rejected = evaluate_trapped_longs_reversal_v2(
        high_touch, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert "frozen_breakout_not_observed" in missing.blockers
    assert "frozen_breakout_not_observed" in rejected.blockers


def test_requires_strict_oi_retention_after_failure() -> None:
    oi = _oi()
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), (*oi[:3], OiObservation(oi[3].observed_at, 99.0)),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
    )
    assert "oi_sequence_not_retained" in result.blockers


def test_vetoes_price_and_oi_continuation_acceleration() -> None:
    frame = _frame([
        ("2026-09-10T09:55:00Z", 99.0, 98.0, 98.5, 10.0),
        ("2026-09-10T10:00:00Z", 101.0, 99.0, 100.5, 12.0),
        ("2026-09-10T10:05:00Z", 99.0, 97.0, 98.0, 11.0),
        ("2026-09-10T10:10:00Z", 99.0, 97.0, 98.0, 9.0),
        ("2026-09-10T10:15:00Z", 100.5, 96.0, 99.5, 8.0),
    ])
    oi = (*_oi()[:3], OiObservation(datetime(2026, 9, 10, 10, 20, tzinfo=UTC), 110.0))
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), oi, datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
        price_acceleration_veto_pct=0.5, oi_acceleration_veto_pct=2.0,
    )
    assert "price_oi_continuation_acceleration" in result.blockers


def test_vetoes_new_high_after_failure() -> None:
    frame = _frame([
        ("2026-09-10T09:55:00Z", 99.0, 98.0, 98.5, 10.0),
        ("2026-09-10T10:00:00Z", 101.0, 99.0, 100.5, 12.0),
        ("2026-09-10T10:05:00Z", 99.0, 97.0, 98.0, 11.0),
        ("2026-09-10T10:10:00Z", 101.0, 97.0, 99.0, 9.0),
        ("2026-09-10T10:15:00Z", 100.0, 96.0, 97.0, 8.0),
    ])
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
    )
    assert "new_high_before_delivery" in result.blockers


def test_requires_two_closed_candles_after_breakout() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(), datetime(2026, 9, 10, 10, 10, tzinfo=UTC),
        min_closed_5m_candles_after_breakout=2,
    )
    assert "insufficient_closed_5m_candles" in result.blockers


def test_rejects_oi_timestamp_chronology_and_phase_boundaries() -> None:
    out_of_order = (
        OiObservation(datetime(2026, 9, 10, 10, 5, tzinfo=UTC), 105.0),
        OiObservation(datetime(2026, 9, 10, 10, 0, tzinfo=UTC), 100.0),
        *_oi()[2:],
    )
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), out_of_order,
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
    )
    assert "oi_sequence_chronology_invalid" in result.blockers


def test_v2_result_is_shadow_contract_without_delivery_side_effects() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
    )
    assert result.identity.startswith("trapped-longs-v2:")
    assert not hasattr(result, "signal_id")


def test_rejects_non_finite_oi_values() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), (*_oi()[:3], OiObservation(_oi()[3].observed_at, float("inf"))),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
    )
    assert "oi_sequence_invalid" in result.blockers


def test_rejects_stale_oi_using_configured_age() -> None:
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
        config=type("Config", (), {"trapped_longs_max_oi_age_minutes": 10})(),
    )
    assert "oi_sequence_stale" in result.blockers


def test_config_flags_control_sequence_requirements() -> None:
    without_sequence = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), (),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
        require_oi_sequence=False,
    )
    assert without_sequence.actionable

    fewer_points = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
        require_oi_sequence_points=5,
    )
    assert not fewer_points.actionable
    assert "oi_sequence_points_invalid" in fewer_points.blockers


def test_v2_config_object_with_invalid_sequence_points_is_never_actionable() -> None:
    config = type("Config", (), {
        "trapped_longs_require_oi_sequence_points": 3,
    })()
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(),
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
        config=config,
    )
    assert not result.actionable
    assert result.blockers[0] == "oi_sequence_points_invalid"


def test_v2_selects_exactly_four_phase_points_for_the_four_point_contract() -> None:
    history = (
        OiObservation(datetime(2026, 9, 10, 9, 45, tzinfo=UTC), 98.0),
        *_oi(),
        OiObservation(datetime(2026, 9, 10, 10, 25, tzinfo=UTC), 101.0),
    )
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), history,
        datetime(2026, 9, 10, 10, 20, tzinfo=UTC),
        require_oi_sequence_points=4,
    )
    assert result.actionable
    assert len(result.explanation["oi_sequence"].measurement["values"]) == 4


def test_new_high_before_failure_is_vetoed() -> None:
    frame = _candidate_frame()
    mask = (frame["timestamp"] >= T0 + timedelta(minutes=5)) & (frame["timestamp"] < T0 + timedelta(minutes=10))
    frame.loc[mask, "high"] = 101.0
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )
    assert "new_high_before_delivery" in result.blockers


def test_malformed_breakout_contract_fails_closed_deterministically() -> None:
    malformed = FrozenBreakout("", "not-a-datetime", float("nan"), -1)
    first = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), malformed, _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )
    second = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), malformed, _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert not first.actionable
    assert first.identity == second.identity
    assert first.blockers == second.blockers
    assert {
        "frozen_breakout_reference_missing",
        "breakout_occurred_at_invalid",
        "breakout_identity_missing",
        "breakout_revision_invalid",
    } <= set(first.blockers)


def test_missing_candle_columns_fail_closed_without_key_error() -> None:
    frame = _candidate_frame().drop(columns=["volume"])
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert not result.actionable
    assert "candle_frame_invalid" in result.blockers


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_candle_values_fail_closed(bad_value: float) -> None:
    frame = _candidate_frame()
    frame.loc[frame.index[0], "close"] = bad_value
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert not result.actionable
    assert "candle_frame_invalid" in result.blockers


def test_negative_candle_volume_fails_closed() -> None:
    frame = _candidate_frame()
    frame.loc[frame.index[0], "volume"] = -1.0
    result = evaluate_trapped_longs_reversal_v2(
        frame, _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC)
    )

    assert not result.actionable
    assert "candle_frame_invalid" in result.blockers


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("trapped_longs_max_oi_age_minutes", float("nan")),
        ("trapped_longs_max_oi_age_minutes", -1.0),
        ("trapped_longs_v2_min_oi_retained_increase_pct", float("inf")),
        ("trapped_longs_v2_min_oi_retained_increase_pct", -1.0),
        ("trapped_longs_price_acceleration_veto_pct", float("nan")),
        ("trapped_longs_price_acceleration_veto_pct", -1.0),
        ("trapped_longs_oi_acceleration_veto_pct", float("inf")),
        ("trapped_longs_oi_acceleration_veto_pct", -1.0),
        ("trapped_longs_new_high_tolerance_pct", float("nan")),
        ("trapped_longs_new_high_tolerance_pct", -1.0),
        ("trapped_longs_min_closed_5m_candles_after_breakout", 1.5),
        ("trapped_longs_min_closed_5m_candles_after_breakout", -1),
        ("trapped_longs_require_oi_sequence_points", 3),
        ("trapped_longs_require_oi_sequence_points", float("nan")),
    ],
)
def test_invalid_direct_config_threshold_fails_closed(name: str, value: object) -> None:
    config = type("Config", (), {name: value})()
    result = evaluate_trapped_longs_reversal_v2(
        _candidate_frame(), _breakout(), _oi(), datetime(2026, 9, 10, 10, 20, tzinfo=UTC), config=config
    )

    assert not result.actionable
    assert "config_invalid" in result.blockers
