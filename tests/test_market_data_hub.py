"""Behavioural tests for the isolated public-market WebSocket shadow core."""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.market_data.hub import MarketDataHub
from app.market_data.models import HealthStatus
from app.market_data.planning import plan_topics


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value


def ticker_payload(
    *, snapshot: bool, data: dict[str, str], ts: int = 1_788_609_600_000
) -> dict:
    return {
        "topic": "tickers.BTCUSDT",
        "type": "snapshot" if snapshot else "delta",
        "cs": 12,
        "ts": ts,
        "data": {"symbol": "BTCUSDT", **data},
    }


def kline_payload(*, confirm: bool, start: int, close: str) -> dict:
    return {
        "topic": "kline.1.BTCUSDT",
        "ts": start + 10,
        "data": [
            {
                "symbol": "BTCUSDT",
                "interval": "1",
                "start": start,
                "end": start + 59_999,
                "open": "100",
                "high": "110",
                "low": "90",
                "close": close,
                "volume": "7",
                "turnover": "700",
                "confirm": confirm,
            }
        ],
    }


def test_topic_planning_is_sorted_sequential_and_bounded() -> None:
    plan = plan_topics(
        ["ETHUSDT", "BTCUSDT", "BTCUSDT"], connection_budget=35, request_budget=24
    )
    assert plan.topics == (
        "kline.1.BTCUSDT",
        "kline.1.ETHUSDT",
        "tickers.BTCUSDT",
        "tickers.ETHUSDT",
    )
    assert all(
        sum(map(len, connection.topics)) <= 35 for connection in plan.connections
    )
    assert all(
        sum(map(len, batch.topics)) <= 24
        for connection in plan.connections
        for batch in connection.batches
    )


def test_ticker_snapshot_delta_merge_and_parse_errors() -> None:
    clock = Clock()
    hub = MarketDataHub(["btcusdt"], clock=clock)
    hub._handle_message(
        ticker_payload(
            snapshot=True,
            data={
                "lastPrice": "100.5",
                "volume24h": "3",
                "bid1Price": "100.4",
                "bid1Size": "4",
                "ask1Price": "100.6",
                "ask1Size": "5",
                "openInterestValue": "900",
                "nextFundingTime": "1788609600000",
            },
        )
    )
    clock.value += timedelta(seconds=1)
    hub._handle_message(
        ticker_payload(snapshot=False, data={"lastPrice": "101.5", "markPrice": "bad"})
    )

    snapshot = hub.get_snapshot("BTCUSDT")
    assert snapshot is not None
    assert snapshot.last_price == Decimal("101.5")
    assert snapshot.volume_24h == Decimal(3)
    assert snapshot.bid1_price == Decimal("100.4")
    assert snapshot.bid1_size == Decimal(4)
    assert snapshot.ask1_price == Decimal("100.6")
    assert snapshot.ask1_size == Decimal(5)
    assert snapshot.open_interest_value == Decimal(900)
    assert snapshot.next_funding_time == datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    assert snapshot.mark_price is None
    assert snapshot.received_at == clock.value
    assert hub.get_health().parse_errors == 1
    with pytest.raises(FrozenInstanceError):
        snapshot.last_price = Decimal(2)  # type: ignore[misc]


def test_delta_before_snapshot_is_ignored_and_regressions_are_observable() -> None:
    clock = Clock()
    hub = MarketDataHub(["BTCUSDT"], clock=clock)
    hub._handle_message(ticker_payload(snapshot=False, data={"lastPrice": "99"}))
    assert hub.get_snapshot("BTCUSDT") is None
    hub._handle_message(
        ticker_payload(snapshot=True, data={"lastPrice": "100"}, ts=2000)
    )
    hub._handle_message(
        ticker_payload(snapshot=False, data={"lastPrice": "101"}, ts=1000)
    )
    assert hub.get_snapshot("BTCUSDT").last_price == Decimal(101)  # latest arrival wins
    assert hub.get_health().anomalies == 2


def test_health_becomes_stale_from_the_injected_clock() -> None:
    clock = Clock()
    hub = MarketDataHub(["BTCUSDT"], clock=clock, stale_after=45)
    hub._handle_message(ticker_payload(snapshot=True, data={"lastPrice": "100"}))
    clock.value += timedelta(seconds=46)
    health = hub.get_health()
    assert health.state == HealthStatus.STALE
    assert health.stale_symbols == 1
    assert health.stale_ticker_symbols == frozenset({"BTCUSDT"})


def test_kline_keeps_forming_and_latest_closed_candle_only() -> None:
    clock = Clock()
    hub = MarketDataHub(["BTCUSDT"], clock=clock)
    hub._handle_message(kline_payload(confirm=False, start=60_000, close="101"))
    assert hub.get_forming_candle("BTCUSDT").close == Decimal(101)
    hub._handle_message(kline_payload(confirm=True, start=60_000, close="102"))
    assert hub.get_forming_candle("BTCUSDT") is None
    assert hub.get_last_closed_candle("BTCUSDT").close == Decimal(102)
    hub._handle_message(kline_payload(confirm=True, start=0, close="90"))
    assert hub.get_last_closed_candle("BTCUSDT").close == Decimal(102)


def test_kline_symbol_is_derived_from_topic_when_bybit_omits_it() -> None:
    hub = MarketDataHub(["BTCUSDT"])
    payload = kline_payload(confirm=False, start=60_000, close="101")
    payload["data"][0].pop("symbol")
    hub._handle_message(payload)
    candle = hub.get_forming_candle("BTCUSDT")
    assert candle is not None
    assert candle.symbol == "BTCUSDT"
    assert hub.get_health().parse_errors == 0


def test_ack_state_maps_exact_topics_and_negative_ack_is_visible() -> None:
    hub = MarketDataHub(["BTCUSDT"])
    hub._register_subscribe("subscribe-1", ("tickers.BTCUSDT", "kline.1.BTCUSDT"))
    hub._handle_message({"op": "subscribe", "req_id": "subscribe-1", "success": True})
    assert hub.get_subscription_state().acknowledged_topics == frozenset(
        {"tickers.BTCUSDT", "kline.1.BTCUSDT"}
    )
    hub._register_subscribe("subscribe-2", ("tickers.ETHUSDT",))
    hub._handle_message(
        {"op": "subscribe", "req_id": "subscribe-2", "success": False, "ret_msg": "bad"}
    )
    assert hub.get_subscription_state().failed_topics == frozenset({"tickers.ETHUSDT"})


def test_bybit_ping_response_with_op_ping_is_counted_and_matched() -> None:
    hub = MarketDataHub(["BTCUSDT"])
    pending: dict[str, datetime] = {}
    hub._handle_message({"op": "ping", "req_id": "ping-1", "ret_msg": "pong"}, pending)
    assert hub.get_health().pong_messages == 1


@pytest.mark.asyncio
async def test_native_transport_uses_the_exact_endpoint_and_disabled_auto_ping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict]] = []

    async def connect(url: str, **kwargs: object) -> FakeConnection:
        calls.append((url, kwargs))
        return FakeConnection()

    monkeypatch.setitem(
        sys.modules, "websockets.asyncio.client", types.SimpleNamespace(connect=connect)
    )
    from app.market_data.bybit_ws import (
        BYBIT_PUBLIC_LINEAR_ENDPOINT,
        connect_public_linear,
    )

    await connect_public_linear(
        url=BYBIT_PUBLIC_LINEAR_ENDPOINT,
        max_queue=7,
        ping_interval=None,
        ping_timeout=None,
    )
    assert calls == [
        (
            BYBIT_PUBLIC_LINEAR_ENDPOINT,
            {"max_queue": 7, "ping_interval": None, "ping_timeout": None},
        )
    ]


@pytest.mark.asyncio
async def test_lifecycle_subscribes_processes_messages_and_stops() -> None:
    clock = Clock()
    connection = FakeConnection()

    async def connector(**_: object) -> FakeConnection:
        return connection

    hub = MarketDataHub(
        ["BTCUSDT"], connector=connector, clock=clock, ping_interval=100
    )
    await hub.start()
    await eventually(lambda: bool(connection.sent))
    subscribe = connection.sent[0]
    await connection.incoming.put(
        {"op": "subscribe", "req_id": subscribe["req_id"], "success": True}
    )
    await connection.incoming.put(
        ticker_payload(snapshot=True, data={"lastPrice": "123"})
    )
    await eventually(lambda: hub.get_snapshot("BTCUSDT") is not None)
    assert hub.get_health().state == HealthStatus.HEALTHY
    await hub.stop()
    assert connection.closed
    assert hub.get_health().state == HealthStatus.STOPPED


@pytest.mark.asyncio
async def test_heartbeat_sends_ping_and_records_matching_pong() -> None:
    clock = Clock()
    connection = FakeConnection()

    async def connector(**_: object) -> FakeConnection:
        return connection

    hub = MarketDataHub(
        ["BTCUSDT"],
        connector=connector,
        clock=clock,
        ping_interval=20,
        pong_deadline=10,
    )
    await hub.start()
    try:
        await eventually(lambda: bool(connection.sent))
        clock.value += timedelta(seconds=21)
        await connection.incoming.put(
            {"op": "subscribe", "req_id": connection.sent[0]["req_id"], "success": True}
        )
        await eventually(
            lambda: any(message["op"] == "ping" for message in connection.sent)
        )
        ping = next(message for message in connection.sent if message["op"] == "ping")
        await connection.incoming.put({"op": "pong", "req_id": ping["req_id"]})
        await eventually(lambda: hub.get_health().pong_messages == 1)
    finally:
        await hub.stop()


@pytest.mark.asyncio
async def test_disconnect_marks_missing_ack_and_reconnects_with_backoff() -> None:
    first = FakeConnection()
    await first.incoming.put(ConnectionError("network lost"))
    second = FakeConnection()
    connections = [first, second]
    delays: list[float] = []
    gate = asyncio.Event()

    async def connector(**_: object) -> FakeConnection:
        return connections.pop(0)

    async def sleeper(delay: float) -> None:
        delays.append(delay)
        await gate.wait()

    hub = MarketDataHub(
        ["BTCUSDT"], connector=connector, sleeper=sleeper, ping_interval=100
    )
    try:
        await hub.start()
        await eventually(lambda: delays == [1])
        assert hub.get_subscription_state().failed_topics == frozenset(
            {"tickers.BTCUSDT", "kline.1.BTCUSDT"}
        )
        gate.set()
        await eventually(lambda: bool(second.sent))
        assert hub.get_health().reconnects == 1
        assert hub.get_health().disconnects == 1
    finally:
        await hub.stop()


@pytest.mark.asyncio
async def test_universe_change_bounds_state_and_resubscribes() -> None:
    first = FakeConnection()
    second = FakeConnection()
    connections = [first, second]

    async def connector(**_: object) -> FakeConnection:
        return connections.pop(0)

    hub = MarketDataHub(["BTCUSDT"], connector=connector, ping_interval=100)
    hub._handle_message(ticker_payload(snapshot=True, data={"lastPrice": "1"}))
    await hub.start()
    await eventually(lambda: bool(first.sent))
    hub.update_universe(["ETHUSDT"])
    assert hub.get_snapshot("BTCUSDT") is None
    await eventually(lambda: bool(second.sent))
    assert second.sent[0]["args"] == ["kline.1.ETHUSDT", "tickers.ETHUSDT"]
    await hub.stop()


@pytest.mark.asyncio
async def test_universe_change_is_consumed_once_not_an_unbounded_reconnect_loop() -> (
    None
):
    calls: list[FakeConnection] = []

    async def connector(**_: object) -> FakeConnection:
        connection = FakeConnection()
        calls.append(connection)
        return connection

    hub = MarketDataHub(["BTCUSDT"], connector=connector, ping_interval=100)
    try:
        await hub.start()
        await eventually(lambda: len(calls) == 1 and bool(calls[0].sent))
        hub.update_universe(["ETHUSDT"])
        await eventually(lambda: len(calls) == 2 and bool(calls[1].sent))
        await asyncio.sleep(0.02)
        assert len(calls) == 2
    finally:
        await hub.stop()


@pytest.mark.asyncio
async def test_shutdown_has_a_bounded_socket_close_timeout() -> None:
    close_gate = asyncio.Event()
    connection = HangingCloseConnection(close_gate)

    async def connector(**_: object) -> HangingCloseConnection:
        return connection

    hub = MarketDataHub(
        ["BTCUSDT"], connector=connector, ping_interval=100, close_timeout=0.01
    )
    try:
        await hub.start()
        await eventually(lambda: bool(connection.sent))
        await asyncio.wait_for(hub.stop(), timeout=0.2)
        assert hub.get_health().state == HealthStatus.STOPPED
    finally:
        close_gate.set()
        await hub.stop()


class FakeConnection:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[object] = asyncio.Queue(maxsize=8)
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, message: str) -> None:
        import json

        self.sent.append(json.loads(message))

    async def recv(self) -> object:
        value = await self.incoming.get()
        if isinstance(value, BaseException):
            raise value
        return value

    async def close(self) -> None:
        self.closed = True


class HangingCloseConnection(FakeConnection):
    def __init__(self, close_gate: asyncio.Event) -> None:
        super().__init__()
        self._close_gate = close_gate

    async def close(self) -> None:
        await self._close_gate.wait()
        self.closed = True


async def eventually(predicate: object) -> None:
    for _ in range(50):
        if predicate():  # type: ignore[operator]
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not met")
