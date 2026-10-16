"""Planned-rotation state-keep tests for MarketDataHub (HUB-1 rotation wave).

A PLANNED universe rotation (``_UniverseChanged``) must reuse the shard's
connection epoch and must NOT invalidate ticker snapshots for symbols that
stay subscribed, while a genuine reconnect must keep invalidating everything
exactly as before — regardless of the kill-switch flag.

The FIRST subscription cycle legitimately notifies/invalidates (there is
nothing to keep yet); these tests baseline on that and assert only that the
PLANNED rotation cycle adds no invalidation and no re-notification.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.market_data.hub import (
    KEEP_STATE_ON_PLANNED_ROTATION_CONFIG_KEY,
    KEEP_STATE_ON_PLANNED_ROTATION_ENV,
    MarketDataHub,
)
from app.market_data.models import HealthStatus


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value


async def eventually(predicate: object) -> None:
    for _ in range(300):
        if predicate():  # type: ignore[operator]
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not met")


class FakeConnection:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[object] = asyncio.Queue(maxsize=8)
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> object:
        value = await self.incoming.get()
        if isinstance(value, BaseException):
            raise value
        return value

    async def close(self) -> None:
        self.closed = True


class SpyObserver:
    """Consumer-side view of the STALE_DATA wave: every ConnectionOpened for a
    topic means downstream state for that symbol is invalidated again."""

    def __init__(self) -> None:
        self.opened: list = []

    def observe_connection(self, event) -> None:
        self.opened.append(event)

    def observe_ticker(self, snapshot) -> None:
        pass

    def observe_closed_candle(self, candle) -> None:
        pass


def ticker_message(
    symbol: str, *, kind: str, last_price: str, cs: int, ts: int = 1_788_609_600_000
) -> dict:
    return {
        "topic": f"tickers.{symbol}",
        "type": kind,
        "cs": cs,
        "ts": ts,
        "data": {"symbol": symbol, "lastPrice": last_price},
    }


async def noop_sleeper(_delay: float) -> None:
    await asyncio.sleep(0)


def connection_factory():
    connections: list[FakeConnection] = []

    async def connector(**_: object) -> FakeConnection:
        connection = FakeConnection()
        connections.append(connection)
        return connection

    return connections, connector


async def nth_connection(
    connections: list[FakeConnection], index: int
) -> FakeConnection:
    await eventually(lambda: len(connections) > index)
    return connections[index]


async def ack_subscriptions(hub: MarketDataHub, connection: FakeConnection) -> None:
    """Acknowledge every subscribe batch the hub sends on this connection."""
    acked: set[str] = set()
    for _ in range(50):
        for message in list(connection.sent):
            req_id = message.get("req_id")
            if message.get("op") == "subscribe" and req_id not in acked:
                acked.add(str(req_id))
                await connection.incoming.put(
                    {"op": "subscribe", "req_id": req_id, "success": True}
                )
        if hub.get_subscription_state().pending_topics == frozenset() and any(
            message.get("op") == "subscribe" for message in connection.sent
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("subscriptions were never acknowledged")


async def seed_snapshot(
    hub: MarketDataHub, connection: FakeConnection, symbols: list[str]
) -> None:
    await eventually(lambda: bool(connection.sent))
    await ack_subscriptions(hub, connection)
    for index, symbol in enumerate(symbols):
        await connection.incoming.put(
            ticker_message(
                symbol, kind="snapshot", last_price=f"10{index}", cs=10 + index
            )
        )
    await eventually(
        lambda: all(hub.get_snapshot(symbol) is not None for symbol in symbols)
    )


def subscribed_topics(connection: FakeConnection) -> set[str]:
    return {
        topic
        for message in connection.sent
        if message.get("op") == "subscribe"
        for topic in message.get("args", [])
    }


def notified_topics(spy: SpyObserver) -> list[tuple[int, tuple[str, ...]]]:
    return [(event.connection_epoch.generation, event.topics) for event in spy.opened]


# ---------------------------------------------------------------------------
# (a) planned rotation over the same universe -> kept symbols hold state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_planned_rotation_keeps_state_for_retained_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KEEP_STATE_ON_PLANNED_ROTATION_ENV, raising=False)
    connections, connector = connection_factory()
    spy = SpyObserver()
    hub = MarketDataHub(
        ["BTCUSDT"],
        connector=connector,
        clock=Clock(),
        ping_interval=100,
        observers=(spy,),
    )
    try:
        await hub.start()
        first = await nth_connection(connections, 0)
        await seed_snapshot(hub, first, ["BTCUSDT"])
        before = hub.get_snapshot("BTCUSDT")
        assert before is not None
        # Baseline: the FIRST cycle legitimately notifies once, at gen 1.
        assert notified_topics(spy) == [(1, ("kline.1.BTCUSDT", "tickers.BTCUSDT"))]
        assert before.connection_epoch is not None
        assert before.connection_epoch.generation == 1
        anomalies_before = hub.get_health().anomalies

        # Planned rotation of the same universe.
        hub.update_universe(["BTCUSDT"])
        await eventually(lambda: len(connections) == 2 and bool(connections[1].sent))
        second = connections[1]
        assert first.closed
        # The transport really rotated and re-subscribed...
        assert subscribed_topics(second) == {"tickers.BTCUSDT", "kline.1.BTCUSDT"}
        await ack_subscriptions(hub, second)
        await asyncio.sleep(0.02)

        # ...but the kept symbol's snapshot survives with the SAME epoch.
        kept = hub.get_snapshot("BTCUSDT")
        assert kept is not None
        assert kept.last_price == before.last_price
        assert kept.connection_epoch == before.connection_epoch
        # No re-notification for already-active topics -> no STALE_DATA wave.
        assert notified_topics(spy) == [(1, ("kline.1.BTCUSDT", "tickers.BTCUSDT"))]
        assert hub.get_health().reconnects == 0
        assert hub.get_health().state == HealthStatus.HEALTHY

        # Proof the kept state is actionable on the new socket: a bare delta
        # merges instead of being dropped as "delta before snapshot".
        await second.incoming.put(
            ticker_message(
                "BTCUSDT",
                kind="delta",
                last_price="999",
                cs=99,
                ts=1_788_609_660_000,
            )
        )
        await eventually(lambda: hub.get_snapshot("BTCUSDT").last_price != kept.last_price)  # type: ignore[union-attr]
        merged = hub.get_snapshot("BTCUSDT")
        assert merged is not None
        assert str(merged.last_price) == "999"
        assert merged.connection_epoch == before.connection_epoch
        assert hub.get_health().anomalies == anomalies_before
    finally:
        await hub.stop()


# ---------------------------------------------------------------------------
# (b) genuine reconnect -> invalidation unchanged even with the flag ON and a
#     rotation pending (the real-error branch wins and resets the marker)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_genuine_reconnect_still_invalidates_with_flag_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KEEP_STATE_ON_PLANNED_ROTATION_ENV, raising=False)
    connections, connector = connection_factory()
    spy = SpyObserver()
    hub = MarketDataHub(
        ["BTCUSDT"],
        connector=connector,
        clock=Clock(),
        sleeper=noop_sleeper,
        ping_interval=100,
        observers=(spy,),
    )
    try:
        await hub.start()
        first = await nth_connection(connections, 0)
        await seed_snapshot(hub, first, ["BTCUSDT"])
        before = hub.get_snapshot("BTCUSDT")
        assert before is not None and before.connection_epoch is not None
        assert before.connection_epoch.generation == 1

        # Transport failure racing a requested rotation.
        hub._rotation_pending = True
        await first.incoming.put(ConnectionError("network lost"))
        await eventually(lambda: len(connections) == 2 and bool(connections[1].sent))
        second = connections[1]
        await asyncio.sleep(0.02)

        # Pre-existing behaviour preserved: the shard epoch bumps, the ticker
        # snapshot is invalidated, and every topic is re-notified.
        assert hub.get_snapshot("BTCUSDT") is None
        assert hub._rotation_pending is False
        assert [gen for gen, _ in notified_topics(spy)] == [1, 2]
        assert "tickers.BTCUSDT" in notified_topics(spy)[1][1]
        assert hub.get_health().reconnects == 1
        assert hub.get_health().disconnects == 1

        # Fresh data re-attaches under the new generation.
        await seed_snapshot(hub, second, ["BTCUSDT"])
        fresh = hub.get_snapshot("BTCUSDT")
        assert fresh is not None and fresh.connection_epoch is not None
        assert fresh.connection_epoch.generation == 2
    finally:
        await hub.stop()


# ---------------------------------------------------------------------------
# (c) rotation where a symbol leaves -> only that symbol's state is dropped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rotation_drops_only_symbols_that_leave_the_universe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KEEP_STATE_ON_PLANNED_ROTATION_ENV, raising=False)
    connections, connector = connection_factory()
    spy = SpyObserver()
    hub = MarketDataHub(
        ["BTCUSDT", "ETHUSDT"],
        connector=connector,
        clock=Clock(),
        ping_interval=100,
        observers=(spy,),
    )
    try:
        await hub.start()
        first = await nth_connection(connections, 0)
        await seed_snapshot(hub, first, ["BTCUSDT", "ETHUSDT"])
        btc = hub.get_snapshot("BTCUSDT")
        assert btc is not None and btc.connection_epoch is not None
        assert hub.get_snapshot("ETHUSDT") is not None

        # Planned rotation: ETHUSDT leaves the universe, BTCUSDT stays.
        hub.update_universe(["BTCUSDT"])
        await eventually(lambda: len(connections) == 2 and bool(connections[1].sent))
        second = connections[1]
        await ack_subscriptions(hub, second)
        await asyncio.sleep(0.02)

        # Only the departing symbol loses state (update_universe prunes it)...
        assert hub.get_snapshot("ETHUSDT") is None
        assert "kline.1.ETHUSDT" not in subscribed_topics(second)
        # ...while the retained symbol keeps snapshot and epoch.
        kept = hub.get_snapshot("BTCUSDT")
        assert kept is not None
        assert kept.connection_epoch == btc.connection_epoch
        # No ConnectionOpened at all for this rotation.
        assert [gen for gen, _ in notified_topics(spy)] == [1]
        assert hub.get_health().reconnects == 0
    finally:
        await hub.stop()


# ---------------------------------------------------------------------------
# (d) kill switch OFF (env override, or config key) -> old behaviour returns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["env", "config"])
@pytest.mark.asyncio
async def test_kill_switch_restores_invalidation_on_planned_rotation(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.delenv(KEEP_STATE_ON_PLANNED_ROTATION_ENV, raising=False)
    config = None
    if mode == "env":
        monkeypatch.setenv(KEEP_STATE_ON_PLANNED_ROTATION_ENV, "0")
    else:
        config = SimpleNamespace(**{KEEP_STATE_ON_PLANNED_ROTATION_CONFIG_KEY: False})
    connections, connector = connection_factory()
    spy = SpyObserver()
    hub = MarketDataHub(
        ["BTCUSDT"],
        connector=connector,
        clock=Clock(),
        ping_interval=100,
        observers=(spy,),
        config=config,
    )
    try:
        await hub.start()
        await seed_snapshot(
            hub, await nth_connection(connections, 0), ["BTCUSDT"]
        )
        assert hub.get_snapshot("BTCUSDT") is not None

        hub.update_universe(["BTCUSDT"])
        await eventually(lambda: len(connections) == 2 and bool(connections[1].sent))
        await asyncio.sleep(0.02)

        # Pre-fix behaviour: the rotation bumps the epoch and invalidates.
        assert hub.get_snapshot("BTCUSDT") is None
        assert [gen for gen, _ in notified_topics(spy)] == [1, 2]
        assert "tickers.BTCUSDT" in notified_topics(spy)[1][1]
    finally:
        await hub.stop()
