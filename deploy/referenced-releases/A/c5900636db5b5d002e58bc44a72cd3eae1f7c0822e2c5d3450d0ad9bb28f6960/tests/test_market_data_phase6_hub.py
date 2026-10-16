from __future__ import annotations

import asyncio

import pytest

from app.market_data.hub import MarketDataHub
from app.market_data.models import ConnectionEpoch
from app.market_data.planning import plan_topics


def _ticker(*, snapshot: bool, price: str = "1") -> dict:
    return {
        "topic": "tickers.BTCUSDT",
        "type": "snapshot" if snapshot else "delta",
        "ts": 1_788_609_600_000,
        "cs": 1,
        "data": {"symbol": "BTCUSDT", "lastPrice": price},
    }


class RecordingObserver:
    def __init__(self) -> None:
        self.connections = []
        self.tickers = []
        self.candles = []

    def observe_connection(self, event) -> None:
        self.connections.append(event)

    def observe_ticker(self, snapshot) -> None:
        self.tickers.append(snapshot)

    def observe_closed_candle(self, candle) -> None:
        self.candles.append(candle)


def test_connection_epoch_distinguishes_shards_and_generations() -> None:
    hub = MarketDataHub(["BTCUSDT"])

    first_shard = hub._next_connection_epoch(0)
    second_shard = hub._next_connection_epoch(1)
    reconnected_first = hub._next_connection_epoch(0)

    assert first_shard == ConnectionEpoch(shard_id=0, generation=1)
    assert second_shard == ConnectionEpoch(shard_id=1, generation=1)
    assert reconnected_first == ConnectionEpoch(shard_id=0, generation=2)


def test_ticker_event_retains_socket_epoch_and_publishes_merged_state() -> None:
    observer = RecordingObserver()
    hub = MarketDataHub(["BTCUSDT"], observers=(observer,))
    epoch = ConnectionEpoch(2, 4)

    hub._handle_message(_ticker(snapshot=True), connection_epoch=epoch)
    hub._handle_message(_ticker(snapshot=False, price="2"), connection_epoch=epoch)

    assert observer.tickers[-1].last_price == 2
    assert observer.tickers[-1].connection_epoch == epoch


def test_new_epoch_invalidates_old_ticker_and_requires_snapshot() -> None:
    hub = MarketDataHub(["BTCUSDT"])
    old_epoch = ConnectionEpoch(0, 1)
    new_epoch = ConnectionEpoch(0, 2)
    hub._handle_message(_ticker(snapshot=True), connection_epoch=old_epoch)

    hub._invalidate_tickers_for_topics(("tickers.BTCUSDT",))
    hub._handle_message(_ticker(snapshot=False, price="2"), connection_epoch=new_epoch)

    assert hub.get_snapshot("BTCUSDT") is None


def test_observer_failure_is_bounded_and_does_not_block_state() -> None:
    class BrokenObserver(RecordingObserver):
        def observe_ticker(self, snapshot) -> None:
            raise RuntimeError("observer failed")

    hub = MarketDataHub(["BTCUSDT"], observers=(BrokenObserver(),))
    hub._handle_message(
        _ticker(snapshot=True), connection_epoch=ConnectionEpoch(0, 1)
    )

    assert hub.get_snapshot("BTCUSDT") is not None
    assert hub.get_health().observer_errors == 1


class FakeConnection:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[object] = asyncio.Queue()
        self.sent = []

    async def send(self, value: str) -> None:
        self.sent.append(value)

    async def recv(self) -> object:
        value = await self.incoming.get()
        if isinstance(value, BaseException):
            raise value
        return value

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_simultaneous_shards_reconnect_with_independent_epochs(monkeypatch) -> None:
    observer = RecordingObserver()
    connections = [FakeConnection() for _ in range(4)]

    async def connector(**_kwargs):
        return connections.pop(0)

    async def no_delay(_seconds: float) -> None:
        return None

    two_shards = plan_topics(
        ["AAAUSDT", "BBBUSDT"], connection_budget=35, request_budget=100
    )
    monkeypatch.setattr(
        "app.market_data.hub.plan_topics",
        lambda _symbols, *, max_connections: two_shards,
    )
    hub = MarketDataHub(
        ["AAAUSDT", "BBBUSDT"],
        connector=connector,
        sleeper=no_delay,
        ping_interval=100,
        observers=(observer,),
    )
    await hub.start()
    try:
        await _eventually(lambda: len(observer.connections) == 2)
        assert {event.connection_epoch for event in observer.connections} == {
            ConnectionEpoch(0, 1), ConnectionEpoch(1, 1)
        }
        first_live = tuple(hub._connections)
        await first_live[0].incoming.put(ConnectionError("controlled close"))
        await _eventually(lambda: len(observer.connections) == 4)
        assert {event.connection_epoch for event in observer.connections[-2:]} == {
            ConnectionEpoch(0, 2), ConnectionEpoch(1, 2)
        }
    finally:
        await hub.stop()


async def _eventually(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not met")
