"""Offline decision-time Store admission regressions against the baseline."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.features.builder import FeatureBuilder
from app.market_data.models import ConnectionEpoch, MarketCandle, MarketTickerSnapshot
from app.market_data.observers import ConnectionOpened
from app.market_data.store import (
    STALE_CONTINUITY,
    STALE_EPOCH_INVALIDATED,
    CanonicalMarketStore,
)
from app.infra.request_scheduler import RequestPriority


START = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
SYMBOL = "TESTUSDT"
EPOCH = ConnectionEpoch(0, 1)
MINUTE = timedelta(minutes=1)
MISSING_DECISION_CANDLE = "MISSING_DECISION_CANDLE"


def candle(minute: int, *, confirmed: bool = True) -> MarketCandle:
    opened = START + minute * MINUTE
    return MarketCandle(
        symbol=SYMBOL,
        open_time=opened,
        close_time=opened + MINUTE,
        interval="1",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1000"),
        turnover=Decimal("100000"),
        confirmed=confirmed,
        exchange_timestamp=opened,
        received_at=opened + (
            timedelta(seconds=5) if not confirmed else MINUTE + timedelta(seconds=1)
        ),
        connection_epoch=EPOCH,
    )


def ticker(at: datetime) -> MarketTickerSnapshot:
    return MarketTickerSnapshot(
        symbol=SYMBOL,
        connection_epoch=EPOCH,
        received_at=at,
        last_price=Decimal("100"),
        mark_price=Decimal("100"),
        index_price=Decimal("100"),
        funding_rate=Decimal("0.0001"),
        open_interest=Decimal("5000"),
        volume_24h=Decimal("1"),
        turnover_24h=Decimal("1"),
        price_24h_pcnt=Decimal("0"),
        high_price_24h=Decimal("1"),
        low_price_24h=Decimal("1"),
        prev_price_24h=Decimal("1"),
        bid1_price=Decimal("100"),
        bid1_size=Decimal("1"),
        ask1_price=Decimal("100"),
        ask1_size=Decimal("1"),
    )


def seeded_ring(*, hub=None, minutes=range(1, 301), boundary_minute=302):
    boundary = START + boundary_minute * MINUTE
    # Seed after the final candle's receive time; historical decision instants
    # are supplied independently to snapshot/peek_snapshot below.
    now = {"at": boundary + timedelta(seconds=1)}
    store = CanonicalMarketStore(
        ring_size=300, stale_after_sec=90, clock=lambda: now["at"], hub=hub
    )
    store.update_symbols([SYMBOL])
    for minute in minutes:
        store.observe_closed_candle(candle(minute))
    return store, now, boundary


def test_closed_candle_received_after_decision_is_not_causal_evidence() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = boundary + timedelta(seconds=31)
    store.observe_closed_candle(replace(candle(301), received_at=decision + timedelta(seconds=30)))
    store.observe_ticker(ticker(decision))
    frame = store.peek_snapshot(SYMBOL, decision)
    assert frame is not None and not frame.actionable


def test_missing_decision_minute_stays_repairable_after_stale_grace() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=45)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.peek_snapshot(SYMBOL, decision)
    assert frame is not None and not frame.actionable
    assert frame.stale_reason == "CANDLE_STALE"


def test_missing_penultimate_minute_with_current_decision_candle_is_not_actionable() -> None:
    store, now, _ = seeded_ring(minutes=(*range(0, 299), 300), boundary_minute=301)
    decision = START + 301 * MINUTE + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.peek_snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == STALE_CONTINUITY


def test_preboundary_full_ring_remains_actionable() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary - timedelta(seconds=1)
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable, frame.stale_reason if frame else None
    assert len(frame.frame_1m) == 300
    assert frame.frame_1m["timestamp"].iloc[-1] == START + 300 * MINUTE


@pytest.mark.parametrize("seconds_after_boundary", [0, 1])
def test_missing_decision_minute_after_boundary_is_explicitly_stale(
    seconds_after_boundary: int,
) -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=seconds_after_boundary)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert len(frame.frame_1m) == 300
    assert frame.last_closed_at_utc == boundary - MINUTE
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE
    assert store.counters["frames_missing_decision_candle"] == 1


def test_peek_missing_decision_minute_is_counted_without_snapshot_side_effects() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.peek_snapshot(SYMBOL, decision)
    assert frame is not None and frame.stale_reason == MISSING_DECISION_CANDLE
    assert store.counters["peeks_missing_decision_candle"] == 1
    assert store.counters["frames_stale"] == 0


def test_forming_mandatory_minute_cannot_restore_admission() -> None:
    class Hub:
        def get_forming_candle(self, symbol):
            assert symbol == SYMBOL
            return candle(301, confirmed=False)

        def is_current_connection_epoch(self, epoch):
            return epoch == EPOCH

    store, now, boundary = seeded_ring(hub=Hub())
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_peek_snapshot_rechecks_missing_minute_at_each_decision() -> None:
    store, now, boundary = seeded_ring()
    before = boundary - timedelta(seconds=1)
    store.observe_ticker(ticker(before))
    prior = store.peek_snapshot(SYMBOL, before)
    assert prior is not None and prior.actionable
    now["at"] = boundary
    store.observe_ticker(ticker(boundary))
    frame = store.peek_snapshot(SYMBOL, boundary)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_direct_unconfirmed_closed_candle_cannot_restore_admission() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_closed_candle(replace(
        candle(301, confirmed=False), received_at=decision,
    ))
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_confirmed_mandatory_minute_restores_admission() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_closed_candle(candle(301))
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable, frame.stale_reason if frame else None
    assert len(frame.frame_1m) == 300
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary - MINUTE


@pytest.mark.parametrize("invalid", ["old_epoch", "future_minute", "future_receipt"])
def test_invalid_forming_is_not_used_as_actionable_price(invalid: str) -> None:
    class Hub:
        def get_forming_candle(self, symbol):
            forming = replace(
                candle(302 if invalid != "future_minute" else 303, confirmed=False),
                close=Decimal("800"), received_at=decision,
            )
            if invalid == "old_epoch":
                forming = replace(forming, connection_epoch=ConnectionEpoch(0, 0))
            if invalid == "future_receipt":
                forming = replace(forming, received_at=decision + timedelta(seconds=1))
            return forming

        def is_current_connection_epoch(self, epoch):
            return epoch == EPOCH

    store, now, boundary = seeded_ring(hub=Hub())
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_closed_candle(candle(301))
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable
    assert len(frame.frame_1m) == 300
    assert FeatureBuilder().build(SYMBOL, frame.frame_1m, market_asof=decision).price == 100.0


def test_actionable_frame_preserves_current_forming_row_for_evaluation() -> None:
    class Hub:
        def get_forming_candle(self, symbol):
            assert symbol == SYMBOL
            return replace(
                candle(302, confirmed=False), high=Decimal("140"),
                close=Decimal("120"), volume=Decimal("9000"),
                turnover=Decimal("1080000"), received_at=decision,
            )

        def is_current_connection_epoch(self, epoch):
            return epoch == EPOCH

    store, now, boundary = seeded_ring(hub=Hub())
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_closed_candle(candle(301))
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable, frame.stale_reason if frame else None
    assert len(frame.frame_1m) == 301
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary
    assert frame.frame_1m[["high", "close", "volume"]].iloc[-1].tolist() == [140, 120, 9000]
    features = FeatureBuilder().build(SYMBOL, frame.frame_1m, market_asof=decision)
    assert features.price == 120.0
    assert features.ret_5m == pytest.approx(20.0)
    assert features.vwap > 100.0


def test_future_confirmed_mandatory_minute_cannot_restore_admission() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    # Within the existing five-second insertion tolerance, but not yet closed
    # at the actual decision instant. It must never be proof of slot M.
    premature = replace(
        candle(301), close_time=decision + timedelta(seconds=2), received_at=decision
    )
    store.observe_closed_candle(premature)
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_full_count_ring_with_interior_gap_is_not_actionable() -> None:
    minutes = (minute for minute in range(1, 302) if minute != 150)
    store, now, boundary = seeded_ring(minutes=minutes)
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert len(frame.frame_1m) == 300  # Count alone cannot establish continuity.
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary - MINUTE
    assert not frame.actionable
    assert frame.stale_reason == STALE_CONTINUITY


def test_gap_outside_retained_ring_does_not_block_current_window() -> None:
    minutes = (minute for minute in range(1, 303) if minute != 2)
    store, now, boundary = seeded_ring(minutes=minutes, boundary_minute=303)
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable, frame.stale_reason if frame else None
    assert len(frame.frame_1m) == 300
    assert frame.frame_1m["timestamp"].iloc[0] == START + 3 * MINUTE
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary - MINUTE


def test_restored_rest_decision_minute_restores_admission() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision

    class Client:
        calls = 0

        async def fetch_klines(
            self, symbol, interval, *, limit, start_ms, end_ms,
            priority, deadline_monotonic,
        ):
            self.calls += 1
            assert symbol == SYMBOL and interval == "1"
            assert limit == 300 and priority is RequestPriority.RECOVERY
            assert start_ms <= int((boundary - MINUTE).timestamp() * 1000)
            assert end_ms == int((boundary - MINUTE).timestamp() * 1000)
            return [[str(end_ms), "100", "101", "99", "100", "1000", "100000"]]

    store.observe_ticker(ticker(decision))
    before = store.snapshot(SYMBOL, decision)
    assert before is not None
    assert before.last_closed_at_utc == boundary - MINUTE
    client = Client()
    assert asyncio.run(store.backfill_missing(
        client, SYMBOL, priority=RequestPriority.RECOVERY,
    ))
    assert client.calls == 1
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable, frame.stale_reason if frame else None
    assert len(frame.frame_1m) == 300
    assert frame.source == "MIXED"
    assert frame.connection_epoch is None
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary - MINUTE


def test_off_grid_raw_rest_row_cannot_prove_decision_minute() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))

    class Client:
        async def fetch_klines(
            self, symbol, interval, *, limit, start_ms, end_ms,
            priority, deadline_monotonic,
        ):
            assert symbol == SYMBOL and interval == "1"
            return [[str(end_ms + 3000), "100", "101", "99", "100", "1000", "100000"]]

    restored = asyncio.run(store.backfill_missing(
        Client(), SYMBOL, priority=RequestPriority.RECOVERY,
    ))
    assert restored is False
    frame = store.peek_snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_rest_completion_crossing_next_minute_rechecks_new_slot() -> None:
    store, now, boundary = seeded_ring()
    now["at"] = boundary + timedelta(seconds=1)
    store.observe_ticker(ticker(now["at"]))

    class Client:
        async def fetch_klines(
            self, symbol, interval, *, limit, start_ms, end_ms,
            priority, deadline_monotonic,
        ):
            assert symbol == SYMBOL and interval == "1"
            assert end_ms == int((boundary - MINUTE).timestamp() * 1000)
            now["at"] = boundary + MINUTE + timedelta(seconds=1)
            return [[str(end_ms), "100", "101", "99", "100", "1000", "100000"]]

    assert not asyncio.run(store.backfill_missing(
        Client(), SYMBOL, priority=RequestPriority.RECOVERY,
    ))
    store.observe_ticker(ticker(now["at"]))
    frame = store.peek_snapshot(SYMBOL, now["at"])
    assert frame is not None
    assert frame.last_closed_at_utc == boundary
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_malformed_close_timestamp_is_not_decision_minute_evidence() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    # Already in the past, but thirty seconds short of the slot's real close.
    malformed = replace(
        candle(301), close_time=boundary - timedelta(seconds=30), received_at=decision
    )
    store.observe_closed_candle(malformed)
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_malformed_close_inside_retained_ring_is_not_actionable() -> None:
    store, now, boundary = seeded_ring(minutes=(
        minute for minute in range(1, 302) if minute != 150
    ))
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_closed_candle(replace(
        candle(150), close_time=START + 150 * MINUTE + timedelta(seconds=30),
        received_at=decision,
    ))
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == STALE_CONTINUITY


def test_non_minute_aligned_open_cannot_prove_decision_slot() -> None:
    store, now, boundary = seeded_ring()
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_closed_candle(replace(
        candle(301), open_time=boundary - MINUTE + timedelta(seconds=3),
        received_at=decision,
    ))
    store.observe_ticker(ticker(decision))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == MISSING_DECISION_CANDLE


def test_retired_epoch_decision_candle_cannot_restore_admission() -> None:
    current_epoch = ConnectionEpoch(0, 2)

    class Hub:
        def is_current_connection_epoch(self, epoch):
            return epoch == current_epoch

    store, now, boundary = seeded_ring(hub=Hub())
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_connection(ConnectionOpened(
        current_epoch, (f"kline.1.{SYMBOL}",), decision
    ))
    store.observe_closed_candle(candle(301))  # Previous connection epoch.
    store.observe_ticker(replace(ticker(decision), connection_epoch=current_epoch))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None
    assert frame.last_closed_at_utc == boundary
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary - MINUTE
    assert not frame.actionable
    assert frame.stale_reason == STALE_EPOCH_INVALIDATED


def test_current_epoch_decision_candle_restores_admission() -> None:
    current_epoch = ConnectionEpoch(0, 2)

    class Hub:
        def is_current_connection_epoch(self, epoch):
            return epoch == current_epoch

    store, now, boundary = seeded_ring(hub=Hub())
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_connection(ConnectionOpened(
        current_epoch, (f"kline.1.{SYMBOL}",), decision
    ))
    store.observe_closed_candle(replace(candle(301), connection_epoch=current_epoch))
    store.observe_ticker(replace(ticker(decision), connection_epoch=current_epoch))
    frame = store.snapshot(SYMBOL, decision)
    assert frame is not None and frame.actionable, frame.stale_reason if frame else None
    assert frame.connection_epoch == current_epoch
    assert frame.frame_1m["timestamp"].iloc[-1] == boundary - MINUTE


@pytest.mark.parametrize("failed_shard", [0, 1])
def test_split_shard_observer_failure_and_missing_minute_both_veto(failed_shard: int) -> None:
    epochs = (EPOCH, ConnectionEpoch(1, 1))
    available = {0, 1}

    class Hub:
        def is_current_connection_epoch(self, epoch):
            return epoch.shard_id in available and epoch == epochs[epoch.shard_id]

    store, now, boundary = seeded_ring(hub=Hub())
    before = boundary - timedelta(seconds=1)
    store.observe_ticker(replace(ticker(before), connection_epoch=epochs[1]))
    assert store.peek_snapshot(SYMBOL, before).actionable

    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(replace(ticker(decision), connection_epoch=epochs[1]))
    assert store.peek_snapshot(SYMBOL, decision).stale_reason == MISSING_DECISION_CANDLE

    # An observer callback fails before invalidating Store state.
    def broken_observer(_event):
        raise RuntimeError("observer unavailable")

    available.remove(failed_shard)
    with pytest.raises(RuntimeError, match="observer unavailable"):
        broken_observer(None)
    store.observe_closed_candle(candle(301))
    frame = store.peek_snapshot(SYMBOL, decision)
    assert frame is not None and frame.stale_reason == STALE_EPOCH_INVALIDATED


def test_planned_rotation_interrupted_after_old_close_requires_actual_minute() -> None:
    current = {0: EPOCH}

    class Hub:
        def is_current_connection_epoch(self, epoch):
            return current.get(epoch.shard_id) == epoch

    store, now, boundary = seeded_ring(hub=Hub())
    decision = boundary + timedelta(seconds=1)
    now["at"] = decision
    store.observe_ticker(ticker(decision))
    assert store.peek_snapshot(SYMBOL, decision).stale_reason == MISSING_DECISION_CANDLE
    current[0] = ConnectionEpoch(0, 2)
    # A lagging old generation emits the required slot; invalidation failed.
    store.observe_closed_candle(candle(301))
    assert store.peek_snapshot(SYMBOL, decision).stale_reason == STALE_EPOCH_INVALIDATED
