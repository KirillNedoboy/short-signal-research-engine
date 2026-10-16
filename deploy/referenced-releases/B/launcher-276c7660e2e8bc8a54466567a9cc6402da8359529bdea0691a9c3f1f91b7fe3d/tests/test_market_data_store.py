from __future__ import annotations

import asyncio
import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.infra.request_scheduler import RequestPriority
from app.market_data.continuity import GapState, GapTracker
from app.market_data.models import (
    BYBIT_PUBLIC_WS_SOURCE,
    ConnectionEpoch,
    MarketCandle,
    MarketTickerSnapshot,
)
from app.market_data.observers import ConnectionOpened
from app.market_data.store import (
    STALE_CANDLE_STALE,
    STALE_EPOCH_INVALIDATED,
    STALE_WAITING_FOR_WS,
    STALE_CONTINUITY,
    STALE_INCOMPLETE_RING,
    STALE_NO_TICKER,
    STALE_OI_HISTORY,
    STALE_TICKER_STALE,
    STALE_WARMING,
    CanonicalMarketStore,
    ContinuityStatus,
    DecisionMarketFrame,
)

_START = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


def _holder_clock(now: datetime) -> tuple[dict, object]:
    holder = {"now": now}
    return holder, lambda: holder["now"]


def _clock(now: datetime):
    return lambda: now


def _candle(
    symbol: str,
    minute: int,
    *,
    confirmed: bool = True,
    source: str = BYBIT_PUBLIC_WS_SOURCE,
    epoch: ConnectionEpoch | None = ConnectionEpoch(0, 1),
    base: float = 100.0,
) -> MarketCandle:
    open_time = _START + timedelta(minutes=minute)
    value = Decimal(str(base + minute))
    return MarketCandle(
        symbol=symbol,
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        interval="1",
        open=value,
        high=value + Decimal("1"),
        low=value - Decimal("1"),
        close=value,
        volume=Decimal("1000"),
        turnover=Decimal("100000"),
        confirmed=confirmed,
        exchange_timestamp=open_time,
        received_at=open_time + timedelta(seconds=5),
        connection_epoch=epoch,
        source=source,
    )


def _ticker(symbol: str, now: datetime, **overrides) -> MarketTickerSnapshot:
    values = {
        "last_price": Decimal("100"),
        "mark_price": Decimal("100"),
        "index_price": Decimal("100"),
        "funding_rate": Decimal("0.0001"),
        "open_interest": Decimal("5000"),
        "volume_24h": Decimal("1"),
        "turnover_24h": Decimal("1"),
        "price_24h_pcnt": Decimal("0"),
        "high_price_24h": Decimal("1"),
        "low_price_24h": Decimal("1"),
        "prev_price_24h": Decimal("1"),
        "bid1_price": Decimal("100"),
        "bid1_size": Decimal("1"),
        "ask1_price": Decimal("100"),
        "ask1_size": Decimal("1"),
    }
    values.update(overrides)
    return MarketTickerSnapshot(
        symbol=symbol,
        connection_epoch=ConnectionEpoch(0, 1),
        received_at=now,
        **values,
    )


def _warm_store(
    ring_size: int = 5,
    *,
    continuity: GapTracker | None = None,
    now: datetime | None = None,
    hub: object | None = None,
) -> CanonicalMarketStore:
    now = now or _START + timedelta(minutes=ring_size + 1)
    store = CanonicalMarketStore(
        ring_size=ring_size,
        stale_after_sec=90,
        continuity=continuity,
        clock=_clock(now),
        hub=hub,
    )
    store.update_symbols(["TESTUSDT"])
    for minute in range(1, ring_size + 1):
        store.observe_closed_candle(_candle("TESTUSDT", minute))
    store.observe_ticker(_ticker("TESTUSDT", now))
    return store


def test_warm_300_candles_ring_is_bounded_and_complete() -> None:
    holder, clock = _holder_clock(_START + timedelta(minutes=301))
    store = CanonicalMarketStore(ring_size=300, clock=clock)
    store.update_symbols(["TESTUSDT"])
    for minute in range(1, 301):
        store.observe_closed_candle(_candle("TESTUSDT", minute))
    store.observe_ticker(_ticker("TESTUSDT", _START + timedelta(minutes=301)))
    ring = store.peek_snapshot("TESTUSDT", _START + timedelta(minutes=301))
    assert ring is not None
    assert ring.actionable, ring.stale_reason
    assert len(ring.frame_1m) == 300
    assert ring.frame_1m["timestamp"].iloc[0] == _START + timedelta(minutes=1)
    # Advancing time and appending another closed candle keeps the ring at
    # exactly 300 entries and evicts the oldest.
    holder["now"] = _START + timedelta(minutes=302)
    store.observe_closed_candle(_candle("TESTUSDT", 301))
    store.observe_ticker(_ticker("TESTUSDT", _START + timedelta(minutes=302)))
    ring = store.peek_snapshot("TESTUSDT", _START + timedelta(minutes=302))
    assert ring is not None and len(ring.frame_1m) == 300
    assert ring.frame_1m["timestamp"].iloc[-1] == _START + timedelta(minutes=301)
    assert ring.frame_1m["timestamp"].iloc[0] == _START + timedelta(minutes=2)
    assert ring.actionable, ring.stale_reason


def test_duplicate_closed_candle_replaces_slot_idempotently() -> None:
    store = _warm_store()
    now = _START + timedelta(minutes=6)
    original = store.peek_snapshot("TESTUSDT", now)
    replacement = _candle("TESTUSDT", 5, base=250.0)
    store.observe_closed_candle(replacement)
    store.observe_closed_candle(replacement)
    after = store.peek_snapshot("TESTUSDT", now)
    assert len(after.frame_1m) == len(original.frame_1m)  # no growth
    assert float(after.frame_1m["close"].iloc[-1]) == 255.0
    assert store.counters["candle_duplicates_replaced"] == 2


def test_out_of_order_candle_only_accepted_for_known_gap() -> None:
    continuity = GapTracker()
    store = CanonicalMarketStore(
        ring_size=5,
        continuity=continuity,
        clock=_clock(_START + timedelta(minutes=10)),
    )
    store.update_symbols(["TESTUSDT"])
    for minute in (1, 2, 3, 5, 6):  # minute 4 missing -> gap detected
        candle = _candle("TESTUSDT", minute)
        store.observe_closed_candle(candle)
        continuity.observe_closed_candle(candle)
    store.observe_ticker(_ticker("TESTUSDT", _START + timedelta(minutes=10)))
    assert continuity.get_symbol_state("TESTUSDT") is GapState.GAP_DETECTED
    late = _candle("TESTUSDT", 4)
    store.observe_closed_candle(late)
    continuity.observe_closed_candle(late)
    assert store.counters["candle_gap_fill"] == 1
    ring_times = [
        candle.open_time for candle in store._ordered_ring(store._states["TESTUSDT"])
    ]
    assert _START + timedelta(minutes=4) in ring_times
    assert continuity.get_symbol_state("TESTUSDT") is GapState.CONTIGUOUS
    # With a contiguous ring, an old out-of-order candle is rejected.
    store.observe_closed_candle(_candle("TESTUSDT", -40))
    assert _START - timedelta(minutes=40) not in store._states["TESTUSDT"].candles
    assert (
        store.counters["candle_out_of_order_rejected"]
        + store.counters["candle_out_of_window_rejected"]
    ) >= 1


def test_stale_and_incomplete_rings_are_rejected() -> None:
    store = _warm_store(now=_START + timedelta(minutes=6))
    # Fresh 60 seconds after the last close, with a re-sampled ticker.
    store.observe_ticker(_ticker("TESTUSDT", _START + timedelta(minutes=7)))
    fresh = store.snapshot("TESTUSDT", _START + timedelta(minutes=7))
    assert fresh is not None and fresh.actionable
    # 120 seconds after the last close exceeds the 90-second gate.
    stale = store.snapshot("TESTUSDT", _START + timedelta(minutes=8))
    assert stale is not None and stale.stale_reason == STALE_CANDLE_STALE
    assert not stale.actionable
    # Incomplete ring: two closed candles out of a five-slot ring.
    store.update_symbols(["TESTUSDT", "NEWUSDT"])
    store.observe_closed_candle(_candle("NEWUSDT", 1))
    store.observe_closed_candle(_candle("NEWUSDT", 2))
    store.observe_ticker(_ticker("NEWUSDT", _START + timedelta(minutes=7)))
    incomplete = store.snapshot("NEWUSDT", _START + timedelta(minutes=7))
    assert incomplete is not None
    assert incomplete.stale_reason == STALE_INCOMPLETE_RING
    assert incomplete.continuity_status is ContinuityStatus.WARMING
    # A symbol with no candles at all reports WARMING.
    store.update_symbols(["TESTUSDT", "NEWUSDT", "FRESHUSDT"])
    empty = store.snapshot("FRESHUSDT", _START + timedelta(minutes=7))
    assert empty is not None
    assert empty.stale_reason == STALE_WARMING


def test_stale_ticker_cannot_make_a_frame_actionable() -> None:
    store = _warm_store(now=_START + timedelta(minutes=6))
    old = store.snapshot("TESTUSDT", _START + timedelta(minutes=6, seconds=40))
    assert old.actionable
    # Decision 46+ seconds after the last ticker snapshot: the ticker fails
    # freshness, so the frame is explicitly stale and not actionable.
    late = store.snapshot("TESTUSDT", _START + timedelta(minutes=6, seconds=46))
    assert late is not None
    assert not late.actionable
    assert late.stale_reason == STALE_TICKER_STALE


def test_reconnect_epoch_invalidates_until_continuity_reestablished() -> None:
    hub_epoch = {"current": ConnectionEpoch(0, 1)}

    class _Hub:
        def get_forming_candle(self, symbol):  # noqa: ANN001
            return None

        def is_current_connection_epoch(self, epoch):  # noqa: ANN001
            return epoch == hub_epoch["current"]

    holder, clock = _holder_clock(_START + timedelta(minutes=6))
    store = CanonicalMarketStore(
        ring_size=5,
        clock=clock,
        hub=_Hub(),
        continuity=GapTracker(),
    )
    store.update_symbols(["TESTUSDT"])
    for minute in range(1, 6):
        store.observe_closed_candle(_candle("TESTUSDT", minute))
    store.observe_ticker(_ticker("TESTUSDT", holder["now"]))
    assert store.snapshot("TESTUSDT", holder["now"]).actionable

    now = holder["now"]
    epoch2 = ConnectionEpoch(0, 2)
    hub_epoch["current"] = epoch2
    store.observe_connection(
        ConnectionOpened(epoch2, ("tickers.TESTUSDT", "kline.1.TESTUSDT"), now)
    )
    invalidated = store.snapshot("TESTUSDT", now)
    assert invalidated.stale_reason == STALE_EPOCH_INVALIDATED
    assert invalidated.continuity_status is ContinuityStatus.RECONNECTING
    # A candle from the previous generation cannot restore actionability.
    holder["now"] = _START + timedelta(minutes=7)
    store.observe_closed_candle(_candle("TESTUSDT", 6, epoch=ConnectionEpoch(0, 1)))
    store.observe_ticker(_ticker("TESTUSDT", holder["now"]))
    still = store.snapshot("TESTUSDT", holder["now"])
    assert still.stale_reason == STALE_EPOCH_INVALIDATED
    # The first confirmed candle from the new epoch restores it.
    store.observe_closed_candle(_candle("TESTUSDT", 6, epoch=epoch2))
    store.observe_ticker(
        replace(_ticker("TESTUSDT", holder["now"]), connection_epoch=epoch2)
    )
    restored = store.snapshot("TESTUSDT", holder["now"])
    assert restored.actionable, restored.stale_reason
    assert store.counters["epoch_invalidations"] == 1



def test_current_ticker_from_different_current_ws_shard_restores_readiness() -> None:
    kline_epoch = ConnectionEpoch(0, 2)
    ticker_epoch = ConnectionEpoch(1, 1)
    current_epochs = {kline_epoch, ticker_epoch}

    class _Hub:
        def get_forming_candle(self, symbol):  # noqa: ANN001
            return None

        def is_current_connection_epoch(self, epoch):  # noqa: ANN001
            return epoch in current_epochs

    now = _START + timedelta(minutes=7)
    store = CanonicalMarketStore(
        ring_size=5,
        clock=_clock(now),
        hub=_Hub(),
        continuity=GapTracker(),
    )
    store.update_symbols(["TESTUSDT"])
    for minute in range(1, 6):
        candle = _candle("TESTUSDT", minute, epoch=ConnectionEpoch(0, 1))
        store.observe_closed_candle(candle)
    store.observe_connection(
        ConnectionOpened(kline_epoch, ("kline.1.TESTUSDT",), now)
    )
    store.observe_closed_candle(_candle("TESTUSDT", 6, epoch=kline_epoch))
    store.observe_ticker(
        replace(_ticker("TESTUSDT", now), connection_epoch=ticker_epoch)
    )

    frame = store.snapshot("TESTUSDT", now)
    assert frame is not None and frame.actionable, frame.stale_reason


def test_cross_shard_ticker_from_retired_generation_is_rejected() -> None:
    kline_epoch = ConnectionEpoch(0, 2)
    current_ticker_epoch = ConnectionEpoch(1, 2)
    retired_ticker_epoch = ConnectionEpoch(1, 1)
    current_epochs = {kline_epoch, current_ticker_epoch}

    class _Hub:
        def get_forming_candle(self, symbol):  # noqa: ANN001
            return None

        def is_current_connection_epoch(self, epoch):  # noqa: ANN001
            return epoch in current_epochs

    now = _START + timedelta(minutes=7)
    store = CanonicalMarketStore(
        ring_size=5,
        clock=_clock(now),
        hub=_Hub(),
        continuity=GapTracker(),
    )
    store.update_symbols(["TESTUSDT"])
    for minute in range(1, 6):
        store.observe_closed_candle(
            _candle("TESTUSDT", minute, epoch=ConnectionEpoch(0, 1))
        )
    store.observe_connection(
        ConnectionOpened(kline_epoch, ("kline.1.TESTUSDT",), now)
    )
    store.observe_closed_candle(_candle("TESTUSDT", 6, epoch=kline_epoch))
    store.observe_ticker(
        replace(_ticker("TESTUSDT", now), connection_epoch=retired_ticker_epoch)
    )

    frame = store.snapshot("TESTUSDT", now)
    assert frame is not None
    assert frame.stale_reason == STALE_NO_TICKER

def test_oi_15m_1h_sampled_from_ticker_stream() -> None:
    now = _START + timedelta(minutes=70)
    store = CanonicalMarketStore(ring_size=5, clock=_clock(now))
    store.update_symbols(["TESTUSDT"])
    for minute in range(1, 69):  # ring keeps minutes 64..68; last close +69min
        store.observe_closed_candle(_candle("TESTUSDT", minute))
    base = now - timedelta(minutes=1)
    for offset, value in (
        (60, "1000"),
        (45, "1100"),
        (30, "1200"),
        (15, "1300"),
        (0, "1500"),  # newest last: the retained ticker snapshot is fresh
    ):
        store.observe_ticker(
            _ticker(
                "TESTUSDT",
                base - timedelta(minutes=offset) + timedelta(seconds=20),
                open_interest=Decimal(value),
            )
        )
    frame = store.snapshot("TESTUSDT", now)
    assert frame.actionable, frame.stale_reason
    assert abs(frame.oi_change_15m - (1500 / 1300 - 1) * 100) < 1e-4
    assert abs(frame.oi_change_1h - 50.0) < 1e-6


def test_minute_sampling_keeps_one_sample_per_minute() -> None:
    now = _START + timedelta(minutes=70)
    store = CanonicalMarketStore(ring_size=5, clock=_clock(now))
    store.update_symbols(["TESTUSDT"])
    base = now - timedelta(minutes=1)
    store.observe_ticker(
        _ticker("TESTUSDT", base + timedelta(seconds=5), open_interest=Decimal("1"))
    )
    store.observe_ticker(
        _ticker("TESTUSDT", base + timedelta(seconds=40), open_interest=Decimal("2"))
    )
    samples = store._states["TESTUSDT"].oi_samples
    assert len(samples) == 1
    assert samples[-1][1] == Decimal("2")  # newest within the minute wins
    payload = store.derivatives_payload("TESTUSDT")
    assert payload["funding"] == [{"fundingRate": "0.0001"}]


def test_fallback_backfills_missing_ring_via_p1_single_request() -> None:
    class _Client:
        def __init__(self) -> None:
            self.calls = 0
            self.priorities: list[object] = []

        async def fetch_klines(
            self,
            symbol,  # noqa: ANN001
            interval,  # noqa: ANN001
            *,
            limit,  # noqa: ANN001
            priority=None,  # noqa: ANN001
            deadline_monotonic=None,  # noqa: ANN001
            **kwargs,  # noqa: ANN003
        ):
            self.calls += 1
            self.priorities.append(priority)
            rows = []
            for minute in range(limit, 0, -1):
                open_time = _START + timedelta(minutes=minute)
                rows.append(
                    [
                        str(int(open_time.timestamp() * 1000)),
                        "1",
                        "2",
                        "0.5",
                        "1.5",
                        "1000",
                        "100000",
                    ]
                )
            return rows

    client = _Client()
    store = CanonicalMarketStore(
        ring_size=5, clock=_clock(_START + timedelta(minutes=6))
    )
    store.update_symbols(["TESTUSDT"])
    store.observe_ticker(_ticker("TESTUSDT", _START + timedelta(minutes=6)))
    before = store.snapshot("TESTUSDT", _START + timedelta(minutes=6))
    assert before is not None and not before.actionable

    async def _fallback() -> bool:
        return await store.backfill_missing(
            client, "TESTUSDT", priority=RequestPriority.P1
        )

    assert asyncio.run(_fallback())
    assert client.calls == 1
    frame = store.snapshot("TESTUSDT", _START + timedelta(minutes=6))
    assert frame.actionable, frame.stale_reason
    assert client.priorities[0] is RequestPriority.P1


def test_fallback_failure_keeps_explicit_stale_status() -> None:
    class _Client:
        async def fetch_klines(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("deadline")

    store = CanonicalMarketStore(
        ring_size=5, clock=_clock(_START + timedelta(minutes=6))
    )
    store.update_symbols(["TESTUSDT"])

    async def _fallback() -> bool:
        return await store.backfill_missing(
            _Client(), "TESTUSDT", priority=RequestPriority.P1
        )

    assert not asyncio.run(_fallback())
    frame = store.snapshot("TESTUSDT", _START + timedelta(minutes=6))
    assert frame.stale_reason is not None


def test_warmup_failure_keeps_symbol_warming() -> None:
    class _FailingClient:
        async def fetch_klines(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("boom")

    store = CanonicalMarketStore(ring_size=5, clock=_clock(_START))
    store.update_symbols(["FAILUSDT"])

    async def run() -> dict:
        return await store.warm_symbols(
            _FailingClient(), ["FAILUSDT"], priority=RequestPriority.P1
        )

    result = asyncio.run(run())
    assert result["seeded"] == 0
    assert result["failed"] == 1
    assert result["skipped"] == 0
    frame = store.snapshot("FAILUSDT", _START)
    assert frame.continuity_status is ContinuityStatus.WARMING


def test_warmup_skips_symbols_with_full_rings() -> None:
    """A restored/complete ring must not trigger REST re-seed (READY gate)."""

    class _CountingClient:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_klines(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.calls += 1
            return []

        async def fetch_open_interest(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return []

    store = _warm_store(ring_size=5, now=_START + timedelta(minutes=6))
    client = _CountingClient()

    async def run() -> dict:
        return await store.warm_symbols(
            client, ["TESTUSDT", "NEWUSDT"], priority=RequestPriority.P1
        )

    result = asyncio.run(run())
    assert result["skipped"] == 1
    assert result["seeded"] == 1  # only the incomplete symbol hits REST
    assert client.calls == 1


def test_rest_seed_waits_for_ws_then_late_ws_restores_actionability_without_refetch() -> None:
    class _Client:
        def __init__(self) -> None:
            self.kline_calls = 0
            self.oi_calls = 0

        async def fetch_klines(self, symbol, interval, *, limit, priority=None, **kwargs):  # noqa: ANN001, ANN002, ANN003
            self.kline_calls += 1
            return [
                [
                    str(int((_START + timedelta(minutes=minute)).timestamp() * 1000)),
                    "100",
                    "101",
                    "99",
                    "100",
                    "1000",
                    "100000",
                ]
                for minute in range(limit, 0, -1)
            ]

        async def fetch_open_interest(self, symbol, interval="15min", *, limit, priority=None, **kwargs):  # noqa: ANN001, ANN002, ANN003
            self.oi_calls += 1
            return [
                {"openInterest": str(1000 + index), "timestamp": str(index)}
                for index in range(limit)
            ]

    now = _START + timedelta(minutes=6)
    store = CanonicalMarketStore(ring_size=5, clock=_clock(now))
    store.update_symbols(["TESTUSDT"])
    client = _Client()

    first = asyncio.run(
        store.warm_symbols(client, ["TESTUSDT"], priority=(RequestPriority.LIVE_SCAN if hasattr(RequestPriority, "LIVE_SCAN") else RequestPriority.P1))
    )
    assert first["seeded"] == 1
    waiting = store.snapshot("TESTUSDT", now)
    assert waiting is not None
    assert waiting.stale_reason == "WAITING_FOR_WS"

    second = asyncio.run(
        store.warm_symbols(client, ["TESTUSDT"], priority=(RequestPriority.LIVE_SCAN if hasattr(RequestPriority, "LIVE_SCAN") else RequestPriority.P1))
    )
    assert second["skipped"] == 1
    assert client.kline_calls == 1
    assert client.oi_calls == 1

    epoch = ConnectionEpoch(0, 1)
    store.observe_connection(
        ConnectionOpened(epoch, ("kline.1.TESTUSDT",), now)
    )
    store.observe_closed_candle(_candle("TESTUSDT", 5, epoch=epoch))
    store.observe_ticker(replace(_ticker("TESTUSDT", now), connection_epoch=epoch))
    ready = store.snapshot("TESTUSDT", now)
    assert ready is not None and ready.actionable, ready.stale_reason


def test_warmup_dedupes_symbols_and_bounds_request_timeout_and_budget() -> None:
    store = CanonicalMarketStore(
        ring_size=5,
        clock=_clock(_START + timedelta(minutes=6)),
    )
    store.update_symbols(["AAUSDT", "BBUSDT"])

    class _SlowClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def fetch_klines(self, symbol, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            self.calls.append(symbol)
            await asyncio.sleep(0.05)
            return []

        async def fetch_open_interest(self, symbol, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            self.calls.append(f"oi:{symbol}")
            return []

    client = _SlowClient()

    async def run() -> dict:
        return await store.warm_symbols(
            client,
            ["AAUSDT", "AAUSDT", "BBUSDT"],
            priority=RequestPriority.P1,
            timeout_sec=0.001,
            backoff_sec=0,
            max_requests=1,
        )

    result = asyncio.run(run())
    assert result["failed"] == 1
    assert result["requests"] == 1
    assert result["timed_out"] == 1
    assert client.calls == ["AAUSDT"]


def test_checkpoint_and_restore_roundtrip(tmp_path) -> None:  # noqa: ANN001
    store = _warm_store(ring_size=5, now=_START + timedelta(minutes=6))
    path = tmp_path / "frames.json.gz"
    written = store.checkpoint(path)
    assert written == 1

    restored_clock_now = _START + timedelta(minutes=6)
    store2 = CanonicalMarketStore(ring_size=5, clock=_clock(restored_clock_now))
    result = store2.restore(path, max_age_sec=600)
    assert result["restored"] == 1
    frame = store2.snapshot("TESTUSDT", restored_clock_now)
    # Restored rings wait for the current WS epoch before becoming actionable.
    assert frame.stale_reason == STALE_WAITING_FOR_WS

    # Expired snapshots are rejected per-symbol by written_at age.
    old = CanonicalMarketStore(
        ring_size=5, clock=_clock(restored_clock_now + timedelta(hours=2))
    )
    late = old.restore(path, max_age_sec=600)
    assert late["restored"] == 0
    assert late["rejected"] == 1


def test_incomplete_ring_is_not_checkpointed(tmp_path) -> None:  # noqa: ANN001
    store = CanonicalMarketStore(ring_size=5, clock=_clock(_START))
    store.update_symbols(["TESTUSDT"])
    store.observe_closed_candle(_candle("TESTUSDT", 1))
    path = tmp_path / "frames.json.gz"
    assert store.checkpoint(path) == 0
    assert not path.exists()


def test_recent_interior_hole_blocks_until_continuity_repaired() -> None:
    # Ring length only gates the retained history bound; recent missing
    # minutes are caught by the shared GapTracker continuity gate.
    continuity = GapTracker()
    store = CanonicalMarketStore(
        ring_size=3,
        continuity=continuity,
        clock=_clock(_START + timedelta(minutes=6)),
    )
    store.update_symbols(["TESTUSDT"])
    for minute in (1, 3, 4):  # minute 2 missing inside the window
        candle = _candle("TESTUSDT", minute)
        store.observe_closed_candle(candle)
        continuity.observe_closed_candle(candle)
    store.observe_ticker(_ticker("TESTUSDT", _START + timedelta(minutes=6)))
    frame = store.snapshot("TESTUSDT", _START + timedelta(minutes=6))
    assert frame.stale_reason == STALE_CONTINUITY
    assert frame.continuity_status is ContinuityStatus.GAP
    # Repairing the interior slot restores actionability.
    repair = _candle("TESTUSDT", 2)
    store.observe_closed_candle(repair)
    continuity.observe_closed_candle(repair)
    frame = store.snapshot("TESTUSDT", _START + timedelta(minutes=6))
    assert frame.actionable, frame.stale_reason


def test_rest_repair_clears_continuity_gap_gate() -> None:
    # A detected interval gap blocks actionability until continuity sees the
    # repair; store-inserted REST candles must be forwarded to the tracker.
    continuity = GapTracker()
    now = _START + timedelta(minutes=10)
    store = CanonicalMarketStore(
        ring_size=3, continuity=continuity, clock=_clock(now)
    )
    store.update_symbols(["TESTUSDT"])
    for minute in (7, 9):  # minute 8 missing -> real interval gap
        candle = _candle("TESTUSDT", minute)
        store.observe_closed_candle(candle)
        continuity.observe_closed_candle(candle)
    store.observe_ticker(_ticker("TESTUSDT", now))
    assert continuity.get_symbol_state("TESTUSDT") is GapState.GAP_DETECTED
    assert store.snapshot("TESTUSDT", now).stale_reason == STALE_INCOMPLETE_RING

    class _Client:
        async def fetch_klines(self, symbol, interval, *, limit, **kwargs):  # noqa: ANN001, ANN002
            return [
                [
                    str(int((_START + timedelta(minutes=k)).timestamp() * 1000)),
                    "1", "2", "0.5", "1.5", "1000", "100000",
                ]
                for k in (9, 8, 7)
            ]

    async def run() -> bool:
        return await store.backfill_missing(
            _Client(), "TESTUSDT", priority=RequestPriority.P1
        )

    assert asyncio.run(run())
    assert continuity.get_symbol_state("TESTUSDT") is GapState.CONTIGUOUS
    frame = store.snapshot("TESTUSDT", now)
    assert frame.actionable, frame.stale_reason


def test_frame_fields_are_exact() -> None:
    store = _warm_store()
    frame = store.snapshot("TESTUSDT", _START + timedelta(minutes=6))
    assert set(DecisionMarketFrame.__dataclass_fields__) == {
        "symbol",
        "frame_1m",
        "ticker",
        "oi_change_15m",
        "oi_change_1h",
        "funding_rate",
        "captured_at_utc",
        "last_closed_at_utc",
        "connection_epoch",
        "continuity_status",
        "source",
        "stale_reason",
    }
    assert frame.source == BYBIT_PUBLIC_WS_SOURCE


def test_checkpoint_rotates_bounded_history_and_records_generation(tmp_path) -> None:
    store = _warm_store(ring_size=5, now=_START + timedelta(minutes=6))
    path = tmp_path / "frames.json.gz"

    for generation in range(1, 5):
        assert store.checkpoint(
            path,
            generation=generation,
            metadata={"lane": "test"},
        ) == 1

    assert path.exists()
    assert path.with_name("frames.json.gz.prev").exists()
    assert path.with_name("frames.json.gz.prev.1").exists()
    assert path.with_name("frames.json.gz.prev.2").exists()
    assert not path.with_name("frames.json.gz.prev.3").exists()
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["schema_version"] == 1
    assert payload["generation"] == 4
    assert payload["metadata"] == {"lane": "test"}
    assert payload["checksums"]["TESTUSDT"]


def test_restore_recovers_good_symbols_when_one_checksum_is_corrupt(tmp_path) -> None:
    store = _warm_store(ring_size=5, now=_START + timedelta(minutes=6))
    path = tmp_path / "frames.json.gz"
    assert store.checkpoint(path, generation=7) == 1

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["symbols"]["BROKENUSDT"] = dict(payload["symbols"]["TESTUSDT"])
    payload["checksums"]["BROKENUSDT"] = "0" * 64
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)

    restored = CanonicalMarketStore(
        ring_size=5,
        clock=_clock(_START + timedelta(minutes=6)),
    )
    result = restored.restore(path, max_age_sec=600)
    assert result["restored"] == 1
    assert result["corrupt"] == 1
    assert restored.snapshot("TESTUSDT", _START + timedelta(minutes=6)) is not None
    assert restored.snapshot("BROKENUSDT", _START + timedelta(minutes=6)) is None


def test_restore_accepts_legacy_checkpoint_shape_without_optional_metadata(tmp_path) -> None:
    store = _warm_store(ring_size=5, now=_START + timedelta(minutes=6))
    path = tmp_path / "frames.json.gz"
    assert store.checkpoint(path) == 1
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload.pop("generation", None)
    payload.pop("metadata", None)
    payload.pop("checksums", None)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)

    restored = CanonicalMarketStore(
        ring_size=5,
        clock=_clock(_START + timedelta(minutes=6)),
    )
    result = restored.restore(path, max_age_sec=600)
    assert result["restored"] == 1
    assert result.get("corrupt", 0) == 0


def test_restored_seed_waits_for_current_epoch_ticker_and_oi(tmp_path) -> None:
    source_now = _START + timedelta(minutes=6)
    source = _warm_store(ring_size=5, now=source_now)
    path = tmp_path / "frames.json.gz"
    assert source.checkpoint(path, generation=1) == 1

    epochs = {"current": ConnectionEpoch(0, 2)}

    class _Hub:
        def get_forming_candle(self, symbol):  # noqa: ANN001
            return None

        def is_current_connection_epoch(self, epoch):  # noqa: ANN001
            return epoch == epochs["current"]

    now = _START + timedelta(minutes=7)
    restored = CanonicalMarketStore(
        ring_size=5,
        clock=_clock(now),
        hub=_Hub(),
    )
    assert restored.restore(path, max_age_sec=600)["restored"] == 1
    restored.observe_connection(
        ConnectionOpened(
            epochs["current"],
            ("tickers.TESTUSDT", "kline.1.TESTUSDT"),
            now,
        )
    )
    # A restored ring and an old OI sample wait for current WS data.
    assert restored.snapshot("TESTUSDT", now).stale_reason == STALE_WAITING_FOR_WS

    restored.observe_closed_candle(
        _candle("TESTUSDT", 6, epoch=epochs["current"])
    )
    assert restored.snapshot("TESTUSDT", now).stale_reason == STALE_NO_TICKER

    current_without_oi = replace(
        _ticker("TESTUSDT", now),
        connection_epoch=epochs["current"],
        open_interest=None,
    )
    restored.observe_ticker(current_without_oi)
    assert restored.snapshot("TESTUSDT", now).stale_reason == STALE_OI_HISTORY

    restored.observe_ticker(
        replace(current_without_oi, open_interest=Decimal("5000"))
    )
    frame = restored.snapshot("TESTUSDT", now)
    assert frame is not None and frame.actionable, frame.stale_reason
