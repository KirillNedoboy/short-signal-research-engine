"""Regression gates for shard-local reconnect failure boundaries."""
import asyncio
from datetime import UTC, datetime
from dataclasses import replace

import pytest

from app.config import AppConfig
from app.market_data.continuity import GapState
from app.market_data.models import ConnectionEpoch, HealthStatus
from app.market_data.provider import CanonicalMarketDataProvider
from app.market_data.hub import MarketDataHub
from app.market_data.observers import ConnectionOpened
from app.market_data.store import CanonicalMarketStore
from test_hub_shard_local_reconnect import Clock, Socket, payload, ticker, two_shard_plan, until
from test_market_data_provider import _Continuity, _Hub, _Scanner, _hub_snapshot


def test_provider_reconnecting_known_shard_preserves_healthy_ticker_not_failed_or_unknown():
    hub = _Hub(_hub_snapshot())
    hub.health_state = HealthStatus.RECONNECTING
    failed_snapshot = replace(_hub_snapshot(), symbol="BBBUSDT", connection_epoch=ConnectionEpoch(1, 1))
    hub.get_snapshot = lambda symbol: hub.snapshot if symbol == "AAAUSDT" else failed_snapshot
    hub.get_subscription_state = lambda: type("State", (), {
        "acknowledged_topics": frozenset({"tickers.AAAUSDT", "tickers.BBBUSDT"}),
        "failed_topics": frozenset({"tickers.BBBUSDT"}),
        "pending_topics": frozenset(),
        "expected_topics": frozenset({"tickers.AAAUSDT", "tickers.BBBUSDT"}),
    })()
    hub.is_shard_available = lambda shard_id: shard_id == 0
    provider = CanonicalMarketDataProvider(scanner=_Scanner(), hub=hub,
        continuity=_Continuity(GapState.CONTIGUOUS),
        config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"))
    now = datetime.now(UTC)
    assert provider._eligible_hub_snapshot("AAAUSDT", now)[1] == "HUB_PREFERRED"
    assert provider._eligible_hub_snapshot("BBBUSDT", now)[0] is None
    hub.is_shard_available = lambda shard_id: False
    assert provider._eligible_hub_snapshot("AAAUSDT", now)[0] is None
    del hub.is_shard_available
    assert provider._eligible_hub_snapshot("AAAUSDT", now)[0] is None


def test_global_unknown_failure_vetoes_even_mapped_shard():
    hub = MarketDataHub(["AAAUSDT"])
    socket = Socket()
    hub._connections.add(socket)
    hub._shard_connections[0] = socket
    hub._active_topic_shards["tickers.AAAUSDT"] = 0
    assert hub.is_shard_available(0)
    hub._global_failure = True
    assert not hub.is_shard_available(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("negative_ack", [True, False])
async def test_replacement_subscribe_negative_or_missing_ack_retries_only_failed_shard(monkeypatch, negative_ack):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kw: two_shard_plan(symbols))
    first, healthy, replacement, retry = (Socket() for _ in range(4))
    sockets = iter((first, healthy, replacement, retry))
    calls = []
    async def connector(**kw):
        socket = next(sockets)
        calls.append(socket)
        return socket
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        ping_interval=100, pong_deadline=0.03)
    await hub.start()
    try:
        await until(lambda: first.sent and healthy.sent)
        await healthy.incoming.put({"op": "subscribe", "req_id": healthy.sent[0]["req_id"], "success": True})
        await first.incoming.put(ConnectionError("lost"))
        await until(lambda: replacement.sent)
        if negative_ack:
            await replacement.incoming.put({"op": "subscribe", "req_id": replacement.sent[0]["req_id"], "success": False})
        await asyncio.wait_for(until(lambda: retry.sent), 2)
        assert not healthy.closed
        assert replacement.closed
        assert hub.is_shard_available(1)
        assert not hub.is_shard_available(0)
    finally:
        await hub.stop()


@pytest.mark.asyncio
async def test_stop_during_blocking_retired_close_awaits_recv_and_closes_socket(monkeypatch):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kw: two_shard_plan(symbols))
    entered = asyncio.Event()
    class BlockingSocket(Socket):
        def __init__(self):
            super().__init__()
            self.close_calls = 0
        async def close(self):
            self.close_calls += 1
            entered.set()
            if self.close_calls == 1:
                await asyncio.Event().wait()
            self.closed = True
    first, healthy = BlockingSocket(), Socket()
    sockets = iter((first, healthy))
    async def connector(**kw):
        return next(sockets)
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        close_timeout=0.03, ping_interval=100)
    await hub.start()
    await until(lambda: first.sent and healthy.sent)
    await first.incoming.put(ConnectionError("lost"))
    await entered.wait()
    await asyncio.wait_for(hub.stop(), 1)
    assert first.closed
    assert not any(task.get_name().startswith("market-data-hub") and not task.done()
                   for task in asyncio.all_tasks())


@pytest.mark.asyncio
async def test_observer_failure_on_invalidation_cannot_leave_store_actionable(monkeypatch):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kw: two_shard_plan(symbols))
    clock = Clock()
    first, healthy, replacement = Socket(), Socket(), Socket()
    sockets = iter((first, healthy, replacement))
    async def connector(**kw):
        return next(sockets)
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        clock=clock, ping_interval=100)
    store = CanonicalMarketStore(ring_size=2, clock=clock, hub=hub)
    store.update_symbols(["AAAUSDT", "BBBUSDT"])
    hub.add_observer(store)
    await hub.start()
    try:
        await until(lambda: first.sent and healthy.sent)
        for sock, sym in ((first, "AAAUSDT"), (healthy, "BBBUSDT")):
            await sock.incoming.put({"op": "subscribe", "req_id": sock.sent[0]["req_id"], "success": True})
            for minute in (1, 2):
                await sock.incoming.put(payload(sym, minute))
            await sock.incoming.put(ticker(sym))
        await until(lambda: store.peek_snapshot("AAAUSDT", clock.value).actionable and store.peek_snapshot("BBBUSDT", clock.value).actionable)
        original = store.observe_connection
        def broken(event):
            if event.connection_epoch.generation > 1:
                raise RuntimeError("observer failed")
            original(event)
        store.observe_connection = broken
        await first.incoming.put(ConnectionError("lost"))
        await until(lambda: first.closed)
        assert not store.peek_snapshot("AAAUSDT", clock.value).actionable
        assert store.peek_snapshot("BBBUSDT", clock.value).actionable
    finally:
        await hub.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_topic", ["kline.1.AAAUSDT", "tickers.AAAUSDT"])
async def test_store_split_shard_observer_failure_vetoes_retired_component(failed_topic):
    clock = Clock()
    hub = MarketDataHub(["AAAUSDT"], clock=clock)
    store = CanonicalMarketStore(ring_size=2, clock=clock, hub=hub)
    store.update_symbols(["AAAUSDT"])
    hub.add_observer(store)
    kline_epoch = hub._next_connection_epoch(0)
    ticker_epoch = hub._next_connection_epoch(1)
    store.observe_connection(ConnectionOpened(kline_epoch, ("kline.1.AAAUSDT",), clock.value))
    for minute in (0, 1):
        hub._handle_message(payload("AAAUSDT", minute), connection_epoch=kline_epoch)
    hub._handle_message(ticker("AAAUSDT"), connection_epoch=ticker_epoch)
    # A full old ring cannot waive the missing current decision slot.
    assert store.peek_snapshot("AAAUSDT", clock.value).stale_reason == "MISSING_DECISION_CANDLE"
    hub._handle_message(payload("AAAUSDT", 2), connection_epoch=kline_epoch)
    assert store.peek_snapshot("AAAUSDT", clock.value).actionable
    def broken(event):
        raise RuntimeError("connection observer failed")
    store.observe_connection = broken
    failed_epoch = kline_epoch if failed_topic.startswith("kline") else ticker_epoch
    replacement_epoch = hub._next_connection_epoch(failed_epoch.shard_id)
    hub._notify("observe_connection", ConnectionOpened(replacement_epoch, (failed_topic,), clock.value))
    assert not store.peek_snapshot("AAAUSDT", clock.value).actionable


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["open", "send"])
async def test_second_shard_initial_failure_keeps_first_open(monkeypatch, failure):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kw: two_shard_plan(symbols))
    first = Socket()
    class FailedSend(Socket):
        async def send(self, message):
            raise ConnectionError("subscribe failed")
    calls = 0
    async def connector(**kw):
        nonlocal calls
        calls += 1
        if calls == 1:
            return first
        if calls == 2:
            if failure == "open":
                raise ConnectionError("open failed")
            return FailedSend()
        return Socket()
    delays = []
    async def sleeper(delay):
        delays.append(delay)
        await asyncio.Event().wait()
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        sleeper=sleeper, ping_interval=100)
    await hub.start()
    try:
        await until(lambda: calls >= 3)
        await asyncio.sleep(0)
        assert not first.closed
        assert first in hub._connections
        assert hub.is_shard_available(0)
    finally:
        await hub.stop()
