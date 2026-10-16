from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app.market.bybit_client import RestMarketResponse
from app.market_data.backfill import BackfillStatus, ClosedCandleBackfiller
from app.market_data.continuity import GapEvent, GapTracker
from app.market_data.models import REST_BACKFILL_SOURCE, ConnectionEpoch

BASE = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
EPOCH = ConnectionEpoch(0, 2)


def gap(*, missing: int = 1) -> GapEvent:
    return GapEvent(
        symbol="BTCUSDT",
        expected_start=BASE + timedelta(minutes=2),
        observed_next_start=BASE + timedelta(minutes=2 + missing),
        missing_count=missing,
        detected_at=BASE + timedelta(minutes=2 + missing),
        connection_epoch=EPOCH,
    )


def row(minute: int) -> list[str]:
    return [
        str(int((BASE + timedelta(minutes=minute)).timestamp() * 1000)),
        "100", "110", "90", "100", "7", "700",
    ]


class FakeClient:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = []

    async def fetch_klines_with_metadata(self, symbol, interval, **kwargs):
        self.calls.append((symbol, interval, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return RestMarketResponse(response, BASE + timedelta(minutes=4))


def test_backfill_fetches_exact_closed_range_and_inserts_in_order() -> None:
    tracker = GapTracker()
    client = FakeClient([[row(2)]])
    service = ClosedCandleBackfiller(client=client, tracker=tracker)

    result = asyncio.run(service.repair(gap(), decision_time=BASE + timedelta(minutes=4)))

    assert result.status is BackfillStatus.SUCCESS
    assert result.attempts == 1
    assert result.candles[0].source == REST_BACKFILL_SOURCE
    assert result.candles[0].connection_epoch == EPOCH
    assert client.calls == [("BTCUSDT", "1", {
        "limit": 1,
        "start_ms": int((BASE + timedelta(minutes=2)).timestamp() * 1000),
        "end_ms": int((BASE + timedelta(minutes=3)).timestamp() * 1000) - 1,
    })]


def test_rest_visibility_retry_is_bounded_and_uses_safe_delays() -> None:
    delays = []

    async def sleeper(value: float) -> None:
        delays.append(value)

    client = FakeClient([[], [], [row(2)]])
    service = ClosedCandleBackfiller(client=client, tracker=GapTracker(), sleeper=sleeper)
    result = asyncio.run(service.repair(gap(), decision_time=BASE + timedelta(minutes=4)))

    assert result.status is BackfillStatus.SUCCESS
    assert result.attempts == 3
    assert delays == [1.0, 2.0]


def test_range_exceeded_never_calls_rest() -> None:
    client = FakeClient([])
    result = asyncio.run(
        ClosedCandleBackfiller(client=client, tracker=GapTracker()).repair(
            gap(missing=16), decision_time=BASE + timedelta(hours=1)
        )
    )
    assert result.status is BackfillStatus.RANGE_EXCEEDED
    assert client.calls == []


def test_forming_or_future_candle_is_rejected_without_tracker_insertion() -> None:
    tracker = GapTracker()
    result = asyncio.run(
        ClosedCandleBackfiller(client=FakeClient([[row(2)]]), tracker=tracker).repair(
            gap(), decision_time=BASE + timedelta(minutes=2, seconds=30)
        )
    )
    assert result.status is BackfillStatus.FUTURE_LEAKAGE
    assert tracker.get_candles("BTCUSDT") == ()


def test_duplicate_or_unexpected_rest_rows_fail_closed() -> None:
    duplicate = asyncio.run(
        ClosedCandleBackfiller(client=FakeClient([[row(2), row(2)]]), tracker=GapTracker()).repair(
            gap(), decision_time=BASE + timedelta(minutes=4)
        )
    )
    unexpected = asyncio.run(
        ClosedCandleBackfiller(client=FakeClient([[row(2), row(1)]]), tracker=GapTracker()).repair(
            gap(), decision_time=BASE + timedelta(minutes=4)
        )
    )
    assert duplicate.status is BackfillStatus.DATA_GAP
    assert unexpected.status is BackfillStatus.DATA_GAP


def test_missing_and_rest_failure_are_explicit() -> None:
    missing_tracker = GapTracker()
    missing = asyncio.run(
        ClosedCandleBackfiller(client=FakeClient([[], [], []]), tracker=missing_tracker).repair(
            gap(), decision_time=BASE + timedelta(minutes=4)
        )
    )
    assert missing.status is BackfillStatus.REST_MISSING
    assert missing_tracker.get_health().backfill_failures == 1

    failed = asyncio.run(
        ClosedCandleBackfiller(client=FakeClient([RuntimeError("10006")]), tracker=GapTracker()).repair(
            gap(), decision_time=BASE + timedelta(minutes=4)
        )
    )
    assert failed.status is BackfillStatus.REST_ERROR


@pytest.mark.asyncio
async def test_cancellation_during_visibility_wait_propagates_cleanly() -> None:
    gate = asyncio.Event()

    async def sleeper(_value: float) -> None:
        await gate.wait()

    service = ClosedCandleBackfiller(client=FakeClient([[], [row(2)]]), tracker=GapTracker(), sleeper=sleeper)
    task = asyncio.create_task(service.repair(gap(), decision_time=BASE + timedelta(minutes=4)))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
