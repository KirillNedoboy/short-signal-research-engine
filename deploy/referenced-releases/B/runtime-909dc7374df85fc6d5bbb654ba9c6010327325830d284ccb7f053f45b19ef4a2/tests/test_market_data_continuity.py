from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.market_data.continuity import GapState, GapTracker
from app.market_data.models import (
    BYBIT_PUBLIC_WS_SOURCE,
    REST_BACKFILL_SOURCE,
    ConnectionEpoch,
    MarketCandle,
)

BASE = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
EPOCH = ConnectionEpoch(0, 1)


def candle(
    minute: int,
    *,
    close: str = "100",
    confirmed: bool = True,
    source: str = BYBIT_PUBLIC_WS_SOURCE,
) -> MarketCandle:
    opened = BASE + timedelta(minutes=minute)
    return MarketCandle(
        symbol="BTCUSDT",
        open_time=opened,
        close_time=opened + timedelta(milliseconds=59_999),
        interval="1",
        open=Decimal(100),
        high=Decimal(110),
        low=Decimal(90),
        close=Decimal(close),
        volume=Decimal(7),
        turnover=Decimal(700),
        confirmed=confirmed,
        exchange_timestamp=opened + timedelta(seconds=59),
        received_at=opened + timedelta(minutes=1),
        connection_epoch=EPOCH,
        source=source,
    )


def test_startup_anchor_does_not_create_historical_gap() -> None:
    tracker = GapTracker()
    tracker.observe_closed_candle(candle(12))

    assert tracker.get_symbol_state("BTCUSDT") is GapState.TRACKING
    assert tracker.active_gaps == ()


def test_missing_interval_is_detected_from_confirmed_continuity() -> None:
    tracker = GapTracker()
    tracker.observe_closed_candle(candle(0))
    tracker.observe_closed_candle(candle(1))
    tracker.observe_closed_candle(candle(3))

    gap = tracker.active_gaps[0]
    assert gap.expected_start == BASE + timedelta(minutes=2)
    assert gap.observed_next_start == BASE + timedelta(minutes=3)
    assert gap.missing_count == 1
    assert gap.connection_epoch == EPOCH
    assert tracker.get_health().gaps_detected == 1


def test_multi_candle_gap_and_cross_epoch_continuity_are_exact() -> None:
    tracker = GapTracker()
    tracker.observe_closed_candle(candle(0))
    next_epoch = replace(candle(1), connection_epoch=ConnectionEpoch(0, 2))
    tracker.observe_closed_candle(next_epoch)
    assert tracker.active_gaps == ()

    later = replace(candle(5), connection_epoch=ConnectionEpoch(0, 3))
    tracker.observe_closed_candle(later)
    assert tracker.active_gaps[0].missing_count == 3
    assert tracker.active_gaps[0].connection_epoch == ConnectionEpoch(0, 3)


def test_forming_duplicate_and_late_old_candles_are_idempotent() -> None:
    tracker = GapTracker()
    tracker.observe_closed_candle(candle(0, confirmed=False))
    tracker.observe_closed_candle(candle(1))
    tracker.observe_closed_candle(candle(1))
    tracker.observe_closed_candle(candle(0))

    assert tracker.get_health().duplicates == 1
    assert tracker.get_health().late_candles == 1
    assert tracker.active_gaps == ()


def test_rest_backfill_repairs_order_and_matching_ws_confirms_source() -> None:
    tracker = GapTracker()
    for minute in (0, 1, 3):
        tracker.observe_closed_candle(candle(minute))

    tracker.observe_closed_candle(candle(2, source=REST_BACKFILL_SOURCE))
    tracker.observe_closed_candle(candle(2))

    assert [item.open_time for item in tracker.get_candles("BTCUSDT")] == [
        BASE + timedelta(minutes=value) for value in (0, 1, 2, 3)
    ]
    assert tracker.get_symbol_state("BTCUSDT") is GapState.CONTIGUOUS
    assert tracker.active_gaps == ()
    assert tracker.get_health().gaps_backfilled == 1
    assert tracker.get_health().source_confirmations == 1


def test_conflicting_ws_after_backfill_records_conflict_without_overwrite() -> None:
    tracker = GapTracker()
    for minute in (0, 1, 3):
        tracker.observe_closed_candle(candle(minute))
    tracker.observe_closed_candle(candle(2, source=REST_BACKFILL_SOURCE))

    tracker.observe_closed_candle(candle(2, close="101"))

    assert tracker.get_candles("BTCUSDT")[2].source == REST_BACKFILL_SOURCE
    assert tracker.conflicts[0].field_differences == ("close",)
    assert tracker.get_health().parity_conflicts == 1


def test_tracker_keeps_bounded_recent_candle_identities() -> None:
    tracker = GapTracker(max_candles_per_symbol=4)
    for minute in range(8):
        tracker.observe_closed_candle(candle(minute))

    assert [item.open_time for item in tracker.get_candles("BTCUSDT")] == [
        BASE + timedelta(minutes=value) for value in range(4, 8)
    ]


def test_bounded_eviction_never_silently_forgets_an_unrepaired_gap() -> None:
    tracker = GapTracker(max_candles_per_symbol=4)
    for minute in (0, 2, 3, 4, 5, 6):
        tracker.observe_closed_candle(candle(minute))

    assert len(tracker.get_candles("BTCUSDT")) == 4
    assert tracker.active_gaps[0].expected_start == BASE + timedelta(minutes=1)
    assert tracker.get_symbol_state("BTCUSDT") is GapState.GAP_DETECTED


def test_partial_late_fill_does_not_split_or_duplicate_active_gap() -> None:
    tracker = GapTracker()
    tracker.observe_closed_candle(candle(0))
    tracker.observe_closed_candle(candle(4))

    original = tracker.active_gaps[0]
    tracker.observe_closed_candle(candle(1))

    assert tracker.active_gaps == (original,)
    assert tracker.get_health().gaps_detected == 1
