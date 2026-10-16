from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

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
