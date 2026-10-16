"""Supervisor diagnostics are safe and leave reconnection semantics unchanged."""
import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta

import pytest

from app.market_data.hub import MarketDataHub
from app.market_data.models import HealthStatus
from test_market_data_hub import FakeConnection, eventually, ticker_payload


class Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 11, tzinfo=UTC)

    def __call__(self):
        return self.value


def test_safe_close_code_is_numeric_only():
    from types import SimpleNamespace
    from app.market_data.hub import _safe_close_code

    error = ConnectionError("wss://host/?token=SECRET")
    error.rcvd = SimpleNamespace(code=1006, reason="token=SECRET")
    assert _safe_close_code(error) == 1006
    error.rcvd.code = "1006;token=SECRET"
    assert _safe_close_code(error) == "unknown"


def test_pong_from_other_shard_cannot_clear_its_deadline():
    from app.market_data.models import ConnectionEpoch

    hub = MarketDataHub(["BTCUSDT"])
    deadline = datetime(2026, 10, 11, tzinfo=UTC)
    pongs = {"request": (deadline, 1)}
    hub._handle_message({"op": "pong", "req_id": "request"}, pongs,
                        connection_epoch=ConnectionEpoch(0, 1))
    assert pongs == {"request": (deadline, 1)}
    hub._handle_message({"op": "pong", "req_id": "request"}, pongs,
                        connection_epoch=ConnectionEpoch(1, 1))
    assert pongs == {}


@pytest.mark.asyncio
async def test_pong_deadline_reports_second_shard_not_first(caplog, monkeypatch):
    from types import SimpleNamespace
    from app.market_data import hub as hub_module

    clock = Clock()
    connections = [FakeConnection(), FakeConnection()]
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connections.pop(0)

    async def sleeper(delay):
        await gate.wait()

    plan = SimpleNamespace(topics=("tickers.BTCUSDT", "tickers.ETHUSDT"), connections=(
        SimpleNamespace(topics=("tickers.BTCUSDT",), batches=()),
        SimpleNamespace(topics=("tickers.ETHUSDT",), batches=()),
    ))
    monkeypatch.setattr(hub_module, "plan_topics", lambda *args, **kwargs: plan)
    hub = MarketDataHub(["BTCUSDT", "ETHUSDT"], connector=connector, clock=clock,
                        sleeper=sleeper, ping_interval=0.01, pong_deadline=2)
    # Keep references despite the connector consuming its list.
    first_conn, second_conn = connections
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: hub.get_health().active_connections == 2)
            clock.value += timedelta(seconds=1)
            await eventually(lambda: any(m["op"] == "ping" for m in second_conn.sent))
            first_ping = next(m for m in first_conn.sent if m["op"] == "ping")
            await first_conn.incoming.put({"op": "pong", "req_id": first_ping["req_id"]})
            await eventually(lambda: hub.get_health().pong_messages == 1)
            clock.value += timedelta(seconds=3)
            await eventually(lambda: hub.get_health().reconnects == 1)
        finally:
            gate.set()
            await hub.stop()
    assert "shard_id=1" in caplog.text
    assert "reason=pong_deadline" in caplog.text


@pytest.mark.asyncio
async def test_pong_deadline_names_owning_shard_and_hub_source(caplog):
    clock = Clock()
    connection = FakeConnection()
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connection

    async def sleeper(delay):
        await gate.wait()

    hub = MarketDataHub(["BTCUSDT"], connector=connector, clock=clock,
                        sleeper=sleeper, ping_interval=0.01, pong_deadline=2)
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: bool(connection.sent))
            clock.value += timedelta(seconds=1)
            await eventually(lambda: any(message["op"] == "ping" for message in connection.sent))
            clock.value += timedelta(seconds=3)
            await eventually(lambda: hub.get_health().reconnects == 1)
        finally:
            gate.set()
            await hub.stop()
    assert "shard_id=0" in caplog.text
    assert "timeout_source=hub_pong_deadline" in caplog.text
    assert "reason=pong_deadline" in caplog.text
    lag = re.search(r"loop_lag_ms=(\d+)", caplog.text)
    assert lag is not None and 0 <= int(lag.group(1)) <= 60_000
    assert "queue_depth=unknown" in caplog.text


@pytest.mark.asyncio
async def test_receive_failure_does_not_report_previous_wait_lag_as_current(caplog):
    connection = FakeConnection()
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connection

    async def sleeper(delay):
        await gate.wait()

    hub = MarketDataHub(["BTCUSDT"], connector=connector, sleeper=sleeper, ping_interval=100)
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: hub.get_health().active_connections == 1)
            hub._last_loop_lag_ms = 42  # a previous timed-out wait, not this receive
            await connection.incoming.put(ConnectionError("lost"))
            await eventually(lambda: hub.get_health().reconnects == 1)
        finally:
            gate.set()
            await hub.stop()
    assert "reason=connection_error" in caplog.text
    assert "loop_lag_ms=unknown" in caplog.text


@pytest.mark.asyncio
async def test_ping_send_failure_does_not_report_previous_wait_lag(caplog):
    clock = Clock()
    connection = FakeConnection()
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connection

    async def sleeper(delay):
        await gate.wait()

    async def fail_ping(message):
        raise ConnectionError("lost")

    hub = MarketDataHub(["BTCUSDT"], connector=connector, clock=clock,
                        sleeper=sleeper, ping_interval=0.01)
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: hub.get_health().active_connections == 1)
            hub._last_loop_lag_ms = 42
            connection.send = fail_ping
            clock.value += timedelta(seconds=1)
            await eventually(lambda: hub.get_health().reconnects == 1)
        finally:
            gate.set()
            await hub.stop()
    assert "reason=connection_error" in caplog.text
    assert "loop_lag_ms=unknown" in caplog.text


@pytest.mark.asyncio
async def test_transport_timeout_is_not_mislabeled_as_hub_pong_deadline(caplog):
    connection = FakeConnection()
    await connection.incoming.put(TimeoutError("wss://host/?token=SECRET"))
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connection

    async def sleeper(delay):
        await gate.wait()

    hub = MarketDataHub(["BTCUSDT"], connector=connector, sleeper=sleeper, ping_interval=100)
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: hub.get_health().reconnects == 1)
        finally:
            gate.set()
            await hub.stop()
    assert "timeout_source=transport" in caplog.text
    assert "reason=timeout" in caplog.text
    assert "shard_id=0" in caplog.text
    assert "SECRET" not in caplog.text


@pytest.mark.asyncio
async def test_failed_planned_cycle_reports_attempt_without_preserving_epoch(caplog):
    first, failed, recovery = FakeConnection(), FakeConnection(), FakeConnection()
    connections = [first, failed, recovery]
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connections.pop(0)

    async def sleeper(delay):
        await gate.wait()

    hub = MarketDataHub(["BTCUSDT"], connector=connector, sleeper=sleeper, ping_interval=100)
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: bool(first.sent))
            original = hub._generation_by_shard[0]
            # The planned socket fails at subscribe before entering receive.
            async def fail_send(message):
                raise ConnectionError("wss://host/?token=SECRET")
            failed.send = fail_send
            hub.update_universe(["BTCUSDT"])
            await eventually(lambda: hub.get_health().reconnects == 1)
            assert hub._rotation_pending is False
            gate.set()
            await eventually(lambda: bool(recovery.sent))
            assert hub._generation_by_shard[0] == original + 1
        finally:
            gate.set()
            await hub.stop()
    assert "rotation_attempt=planned" in caplog.text
    assert "rotation=fresh" in caplog.text
    assert "shard_id=0" in caplog.text
    assert "SECRET" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_class", "reason"),
    [
        (asyncio.TimeoutError("wss://example.invalid/?token=SECRET"), "TimeoutError", "timeout"),
        (ConnectionError("wss://example.invalid/?token=SECRET"), "ConnectionError", "connection_error"),
    ],
)
async def test_fresh_reconnect_logs_safe_class_reason_and_shard_context(
    failure, expected_class, reason, caplog
):
    first, second = FakeConnection(), FakeConnection()
    await first.incoming.put(failure)
    connections = [first, second]
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connections.pop(0)

    async def sleeper(delay):
        await gate.wait()

    hub = MarketDataHub(["BTCUSDT"], connector=connector, sleeper=sleeper, ping_interval=100)
    with caplog.at_level(logging.WARNING):
        try:
            await hub.start()
            await eventually(lambda: hub.get_health().reconnects == 1)
            gate.set()
            await eventually(lambda: bool(second.sent))
            assert hub.get_health().disconnects == 1
            assert hub.get_health().state in (HealthStatus.RECONNECTING, HealthStatus.CONNECTING)
        finally:
            gate.set()
            await hub.stop()
    log = caplog.text
    assert f"exception_class={expected_class}" in log
    assert f"reason={reason}" in log
    assert "rotation=fresh" in log
    assert "shards=1" in log
    assert "shard_id=0" in log
    assert "cycle=1" in log
    assert "reconnects=1" in log
    assert "SECRET" not in log
    assert "example.invalid" not in log


@pytest.mark.asyncio
async def test_reconnect_invalidates_ticker_from_previous_generation():
    first, second = FakeConnection(), FakeConnection()
    connections = [first, second]
    gate = asyncio.Event()

    async def connector(**kwargs):
        return connections.pop(0)

    async def sleeper(delay):
        await gate.wait()

    hub = MarketDataHub(["BTCUSDT"], connector=connector, sleeper=sleeper, ping_interval=100)
    try:
        await hub.start()
        await eventually(lambda: bool(first.sent))
        await first.incoming.put(ticker_payload(snapshot=True, data={"lastPrice": "123"}))
        await eventually(lambda: hub.get_snapshot("BTCUSDT") is not None)
        old = hub.get_snapshot("BTCUSDT")
        await first.incoming.put(ConnectionError("lost"))
        await eventually(lambda: hub.get_health().reconnects == 1)
        gate.set()
        await eventually(lambda: bool(second.sent))
        assert hub.get_snapshot("BTCUSDT") is None
        assert not hub.is_current_ticker_snapshot(old)
    finally:
        gate.set()
        await hub.stop()
