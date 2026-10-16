"""Isolated two-shard transport failures retain the other Store evidence."""
import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.market_data.hub import MarketDataHub
from app.market_data.models import HealthStatus
from app.market_data.store import CanonicalMarketStore


class Clock:
    def __init__(self):
        self.value = datetime(2026, 9, 5, 12, 3, tzinfo=UTC)

    def __call__(self):
        return self.value


class Socket:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.sent = []
        self.closed = False

    async def send(self, message):
        self.sent.append(json.loads(message))

    async def recv(self):
        message = await self.incoming.get()
        if isinstance(message, BaseException):
            raise message
        return message

    async def close(self):
        self.closed = True


async def until(predicate):
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0)


def payload(symbol, minute):
    start = int(datetime(2026, 9, 5, 12, minute, tzinfo=UTC).timestamp() * 1000)
    return {"topic": f"kline.1.{symbol}", "ts": start + 59999,
            "data": [{"interval": "1", "start": start, "end": start + 59999,
                      "open": "1", "high": "2", "low": "1", "close": "2",
                      "volume": "1", "turnover": "2", "confirm": True}]}


def ticker(symbol):
    return {"topic": f"tickers.{symbol}", "type": "snapshot",
            "data": {"symbol": symbol, "lastPrice": "2", "openInterest": "10"}}


def two_shard_plan(symbols):
    plans = []
    topics = []
    for symbol in symbols:
        owned = (f"kline.1.{symbol}", f"tickers.{symbol}")
        topics.extend(owned)
        plans.append(SimpleNamespace(topics=owned, batches=(SimpleNamespace(topics=owned),)))
    return SimpleNamespace(topics=tuple(topics), connections=tuple(plans))


@pytest.mark.asyncio
async def test_recv_failure_replaces_only_failed_shard_preserving_healthy_store(monkeypatch):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: two_shard_plan(symbols))
    clock = Clock()
    first, healthy, replacement = Socket(), Socket(), Socket()
    sockets = [first, healthy, replacement]
    calls = []
    async def connector(**kwargs):
        socket = sockets.pop(0)
        calls.append(socket)
        return socket
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        clock=clock, ping_interval=100)
    store = CanonicalMarketStore(ring_size=2, clock=clock, hub=hub)
    store.update_symbols(["AAAUSDT", "BBBUSDT"])
    hub.add_observer(store)
    await hub.start()
    try:
        await until(lambda: len(calls) == 2 and all(s.sent for s in calls))
        for socket, symbol in ((first, "AAAUSDT"), (healthy, "BBBUSDT")):
            await socket.incoming.put({"op": "subscribe", "req_id": socket.sent[0]["req_id"], "success": True})
            for minute in (1, 2):
                await socket.incoming.put(payload(symbol, minute))
            await socket.incoming.put(ticker(symbol))
        await until(lambda: all(store.peek_snapshot(s, clock.value).actionable for s in ("AAAUSDT", "BBBUSDT")))
        healthy_epoch = store._states["BBBUSDT"].required_epoch
        failed_epoch = store._states["AAAUSDT"].required_epoch
        await first.incoming.put(ConnectionError("lost"))
        await until(lambda: first.closed)
        assert not healthy.closed
        assert store._states["BBBUSDT"].required_epoch == healthy_epoch
        assert hub.is_current_connection_epoch(healthy_epoch)
        assert not hub.is_current_connection_epoch(failed_epoch)
        assert store.peek_snapshot("BBBUSDT", clock.value).actionable
        assert not store.peek_snapshot("AAAUSDT", clock.value).actionable
        await healthy.incoming.put(payload("BBBUSDT", 2))
        await until(lambda: store._states["BBBUSDT"].last_closed_epoch == healthy_epoch
                    and store._states["BBBUSDT"].candles.get(datetime(2026, 9, 5, 12, 2, tzinfo=UTC)) is not None)
        assert store.peek_snapshot("BBBUSDT", clock.value).actionable
        await until(lambda: replacement.sent)
        assert calls == [first, healthy, replacement]
        assert hub.get_health().state is not HealthStatus.HEALTHY
    finally:
        await hub.stop()


@pytest.mark.asyncio
async def test_connector_retry_keeps_other_shard_and_pending_ack_isolated(monkeypatch):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: two_shard_plan(symbols))
    first, healthy, replacement = Socket(), Socket(), Socket()
    calls = 0
    retry_gate = asyncio.Event()
    delays = []

    async def connector(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ConnectionError("replacement unavailable")
        return (first, healthy, replacement)[0 if calls == 1 else 1 if calls == 2 else 2]

    async def sleeper(delay):
        delays.append(delay)
        await retry_gate.wait()

    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        sleeper=sleeper, ping_interval=100)
    await hub.start()
    try:
        await until(lambda: healthy.sent and first.sent)
        healthy_ack = healthy.sent[0]["req_id"]
        await first.incoming.put(ConnectionError("lost"))
        await until(lambda: delays == [1])
        assert calls == 3 and not healthy.closed
        assert hub.get_health().state is HealthStatus.RECONNECTING
        state = hub.get_subscription_state()
        assert set(two_shard_plan(["AAAUSDT"]).topics) <= state.failed_topics
        assert set(two_shard_plan(["BBBUSDT"]).topics) <= state.pending_topics
        # A wrong-shard ACK cannot steal the other shard's pending request.
        await healthy.incoming.put({"op": "subscribe", "req_id": first.sent[0]["req_id"], "success": True})
        await until(lambda: hub.get_health().anomalies == 1)
        assert set(two_shard_plan(["BBBUSDT"]).topics) <= hub.get_subscription_state().pending_topics
        await healthy.incoming.put({"op": "subscribe", "req_id": healthy_ack, "success": True})
        await until(lambda: not hub.get_subscription_state().pending_topics)
        assert hub.get_health().state is HealthStatus.RECONNECTING
        retry_gate.set()
        await until(lambda: replacement.sent)
        assert calls == 4 and not healthy.closed
        assert hub.get_health().state is HealthStatus.RECONNECTING
        await replacement.incoming.put({"op": "subscribe", "req_id": replacement.sent[0]["req_id"], "success": True})
        await until(lambda: hub.get_health().acknowledgements == 2)
        assert not hub.get_subscription_state().failed_topics
    finally:
        retry_gate.set()
        await hub.stop()


@pytest.mark.asyncio
async def test_pong_deadline_replaces_only_owning_shard(monkeypatch):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: two_shard_plan(symbols))
    clock = Clock()
    first, healthy, replacement = Socket(), Socket(), Socket()
    sockets = iter((first, healthy, replacement))
    async def connector(**kwargs):
        return next(sockets)
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector, clock=clock,
                        ping_interval=20, pong_deadline=10)
    await hub.start()
    try:
        await until(lambda: first.sent and healthy.sent)
        clock.value += timedelta(seconds=21)
        await first.incoming.put({"op": "subscribe", "req_id": first.sent[0]["req_id"], "success": True})
        await until(lambda: any(m["op"] == "ping" for m in healthy.sent))
        healthy_ping = next(m for m in healthy.sent if m["op"] == "ping")
        await healthy.incoming.put({"op": "pong", "req_id": healthy_ping["req_id"]})
        await until(lambda: hub.get_health().pong_messages == 1)
        clock.value += timedelta(seconds=11)
        await healthy.incoming.put(ticker("BBBUSDT"))
        await until(lambda: first.closed)
        assert not healthy.closed
        await until(lambda: replacement.sent and len(hub._connections) == 2)
    finally:
        await hub.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("rotate", [False, True])
async def test_pending_reconnect_is_cancelled_by_shutdown_or_universe_change(monkeypatch, rotate):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: two_shard_plan(symbols))
    first, healthy = Socket(), Socket()
    entered = asyncio.Event()
    gate = asyncio.Event()
    replacement = Socket()
    calls = 0
    async def connector(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            entered.set()
            await gate.wait()
        return (first, healthy, replacement, Socket(), Socket())[calls - 1]
    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector, ping_interval=100)
    await hub.start()
    try:
        await until(lambda: first.sent and healthy.sent)
        await first.incoming.put(ConnectionError("lost"))
        await entered.wait()
        if rotate:
            hub.update_universe(["BBBUSDT", "CCCUSDT"])
            await until(lambda: calls >= 5)
        else:
            await asyncio.wait_for(hub.stop(), 1)
            assert not replacement.sent and not hub._connections
        assert not replacement.sent
    finally:
        gate.set()
        await hub.stop()


@pytest.mark.asyncio
async def test_replacement_waits_for_all_positive_batches_before_restoring_shard(monkeypatch):
    plan = two_shard_plan(["AAAUSDT", "BBBUSDT"])
    first_plan = plan.connections[0]
    plan.connections = (
        SimpleNamespace(topics=first_plan.topics, batches=tuple(
            SimpleNamespace(topics=(topic,)) for topic in first_plan.topics
        )), plan.connections[1]
    )
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: plan)
    first, healthy, replacement = Socket(), Socket(), Socket()
    sockets = iter((first, healthy, replacement))
    calls = []

    async def connector(**kwargs):
        socket = next(sockets)
        calls.append(socket)
        return socket

    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector, ping_interval=100)
    await hub.start()
    try:
        await until(lambda: len(first.sent) == 2 and healthy.sent)
        await first.incoming.put(ConnectionError("lost"))
        await until(lambda: len(replacement.sent) == 2)
        await replacement.incoming.put({"op": "subscribe", "req_id": replacement.sent[0]["req_id"], "success": True})
        await until(lambda: hub.get_health().acknowledgements == 1)
        assert calls == [first, healthy, replacement]
        assert not replacement.closed
        assert set(replacement.sent[1]["args"]) <= hub.get_subscription_state().pending_topics
        await replacement.incoming.put({"op": "subscribe", "req_id": replacement.sent[1]["req_id"], "success": True})
        await until(lambda: hub.is_shard_available(0))
        assert hub.get_subscription_state().failed_topics == frozenset()
        assert hub.get_health().state is HealthStatus.HEALTHY
        assert not healthy.closed
    finally:
        await hub.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("rotate", [False, True])
async def test_failed_replacement_is_tracked_through_close_timeout_and_cleanup(monkeypatch, rotate):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: two_shard_plan(symbols))

    class FailedReplacement(Socket):
        def __init__(self):
            super().__init__()
            self.close_started = asyncio.Event()
            self.close_calls = 0

        async def send(self, message):
            raise ConnectionError("subscribe failed")

        async def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                self.close_started.set()
                await asyncio.Event().wait()
            self.closed = True

    first, healthy, replacement = Socket(), Socket(), FailedReplacement()
    sockets = iter((first, healthy, replacement, Socket(), Socket()))
    retry_gate = asyncio.Event()
    delays = []

    async def connector(**kwargs):
        return next(sockets)

    async def sleeper(delay):
        delays.append(delay)
        await retry_gate.wait()

    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        sleeper=sleeper, ping_interval=100, close_timeout=0.01)
    await hub.start()
    try:
        await until(lambda: first.sent and healthy.sent)
        await first.incoming.put(ConnectionError("lost"))
        await replacement.close_started.wait()
        assert replacement in hub._connections
        await until(lambda: delays == [1])
        assert replacement in hub._connections and not replacement.closed
        if rotate:
            hub.update_universe(["AAAUSDT", "BBBUSDT"])
            await until(lambda: replacement.closed)
        else:
            await hub.stop()
            assert replacement.closed
        assert replacement.close_calls >= 2
    finally:
        retry_gate.set()
        await hub.stop()


@pytest.mark.asyncio
async def test_first_shard_initial_open_failure_still_opens_healthy_second(monkeypatch):
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: two_shard_plan(symbols))
    healthy, replacement = Socket(), Socket()
    retry_gate = asyncio.Event()
    delays = []
    calls = 0

    async def connector(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("first shard unavailable")
        if calls == 3:
            await retry_gate.wait()
        return (healthy, replacement)[calls - 2]

    async def sleeper(delay):
        delays.append(delay)
        await retry_gate.wait()

    hub = MarketDataHub(["AAAUSDT", "BBBUSDT"], connector=connector,
                        sleeper=sleeper, ping_interval=100)
    await hub.start()
    try:
        await until(lambda: healthy.sent and calls == 3)
        assert calls == 3
        assert delays == []
        assert hub.get_health().state is HealthStatus.RECONNECTING
        assert not healthy.closed
        assert hub.is_shard_available(1)
        assert not hub.is_shard_available(0)
        await healthy.incoming.put({"op": "subscribe", "req_id": healthy.sent[0]["req_id"], "success": True})
        await until(lambda: hub.get_health().acknowledgements == 1)
        assert not healthy.closed
        retry_gate.set()
        await until(lambda: replacement.sent)
        assert calls == 3
    finally:
        retry_gate.set()
        await hub.stop()


@pytest.mark.asyncio
async def test_single_shard_initial_open_failure_keeps_global_fail_closed(monkeypatch):
    plan = two_shard_plan(["AAAUSDT"])
    monkeypatch.setattr("app.market_data.hub.plan_topics", lambda symbols, **kwargs: plan)
    retry_gate = asyncio.Event()
    calls = 0
    delays = []

    async def connector(**kwargs):
        nonlocal calls
        calls += 1
        raise ConnectionError("initial open failed")

    async def sleeper(delay):
        delays.append(delay)
        await retry_gate.wait()

    hub = MarketDataHub(["AAAUSDT"], connector=connector, sleeper=sleeper)
    await hub.start()
    try:
        await until(lambda: delays == [1])
        assert calls == 1
        assert hub._global_failure
        assert not hub.is_shard_available(0)
        assert hub.get_health().state is HealthStatus.RECONNECTING
    finally:
        await hub.stop()
