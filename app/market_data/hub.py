"""Bounded in-memory shadow state for Bybit public linear WebSocket feeds."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .bybit_ws import (
    BYBIT_PUBLIC_LINEAR_ENDPOINT,
    DEFAULT_MAX_QUEUE,
    connect_public_linear,
)
from .models import (
    BYBIT_PUBLIC_WS_SOURCE,
    ConnectionEpoch,
    HealthStatus,
    MarketCandle,
    MarketDataHealth,
    MarketTickerSnapshot,
    SubscriptionState,
    TICKER_DECIMAL_FIELDS,
)
from .observers import ConnectionOpened, MarketDataObserver
from .planning import DEFAULT_MAX_CONNECTIONS, normalize_symbols, plan_topics

LOGGER = logging.getLogger(__name__)
_BACKOFF_SECONDS = (1, 2, 4, 8, 16, 30)
# A PLANNED WebSocket/subscription rotation (universe refresh every ~900s) must
# not discard the ticker/kline state of symbols that REMAIN subscribed: doing so
# produced a STALE_DATA wave (24-29 symbols/window) plus a REST-backfill
# avalanche that pushed the tail of a scan window past p1_scan_deadline_sec. A
# genuine reconnect still invalidates everything (correctness first). The
# behaviour is configurable via env MARKET_DATA_KEEP_STATE_ON_PLANNED_ROTATION=0
# or config.yaml key market_data_keep_state_on_planned_rotation.
KEEP_STATE_ON_PLANNED_ROTATION_ENV = "MARKET_DATA_KEEP_STATE_ON_PLANNED_ROTATION"
KEEP_STATE_ON_PLANNED_ROTATION_CONFIG_KEY = (
    "market_data_keep_state_on_planned_rotation"
)


class _UniverseChanged(Exception):
    pass


class _PongDeadlineExceeded(TimeoutError):
    def __init__(self, shard_id: int):
        super().__init__()
        self.shard_id = shard_id


def _safe_close_code(exc: Exception) -> int | str:
    """Read only a numeric WebSocket close code, never a provider reason string."""
    received = getattr(exc, "rcvd", None)
    code = getattr(received, "code", None) if received is not None else getattr(exc, "code", None)
    return code if type(code) is int and 1000 <= code <= 4999 else "unknown"


def _safe_transport_failure(exc: Exception) -> tuple[str, str]:
    """Classify without ever formatting provider-supplied exception arguments."""
    name = type(exc).__name__
    safe_class = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", name) else "UnknownException"
    if isinstance(exc, _PongDeadlineExceeded):
        reason = "pong_deadline"
    elif isinstance(exc, TimeoutError):
        reason = "timeout"
    elif isinstance(exc, ConnectionError):
        reason = "connection_error"
    elif name in {"ConnectionClosed", "ConnectionClosedError", "ConnectionClosedOK"}:
        reason = "connection_closed"
    elif isinstance(exc, OSError):
        reason = "os_error"
    else:
        reason = "transport_error_unresolved"
    return safe_class, reason


class MarketDataHub:
    """Owns only a current-universe, read-only normalized shadow of feed state."""

    def __init__(
        self,
        symbols: Iterable[str] = (),
        *,
        connector: Callable[..., Awaitable[Any]] | None = None,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
        ping_interval: float = 20,
        pong_deadline: float = 10,
        stale_after: float = 45,
        closed_candle_stale_after: float = 150,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        close_timeout: float = 5,
        observers: Iterable[MarketDataObserver] = (),
        config: Any | None = None,
    ) -> None:
        self._symbols = normalize_symbols(symbols)
        self._connector = connector or connect_public_linear
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper or asyncio.sleep
        self._ping_interval = ping_interval
        self._pong_deadline = pong_deadline
        self._stale_after = stale_after
        self._closed_candle_stale_after = closed_candle_stale_after
        self._max_connections = max_connections
        self._close_timeout = close_timeout
        self._observers = tuple(observers)
        now = self._now()
        self._health = MarketDataHealth(HealthStatus.STOPPED, now)
        self._snapshots: dict[str, MarketTickerSnapshot] = {}
        self._forming: dict[str, MarketCandle] = {}
        self._closed: dict[str, MarketCandle] = {}
        self._pending_batches: dict[str, tuple[str, ...]] = {}
        self._pending_batch_shards: dict[str, int] = {}
        self._subscribe_deadlines: dict[str, float] = {}
        self._unavailable_shards: set[int] = set()
        self._global_failure = False
        self._expected: set[str] = set()
        self._acknowledged: set[str] = set()
        self._failed: set[str] = set()
        self._connections: set[Any] = set()
        self._shard_connections: dict[int, Any] = {}
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._universe_changed = asyncio.Event()
        self._request_counter = 0
        self._generation_by_shard: dict[int, int] = {}
        self._last_stale_signature: tuple[frozenset[str], frozenset[str]] | None = None
        self._config = config
        self._rotation_pending = False
        self._active_topics: frozenset[str] = frozenset()
        self._active_topic_shards: dict[str, int] = {}
        self._cycle = 0
        self._failed_shard_id: int | None = None
        self._last_loop_lag_ms: int | None = None

    def add_observer(self, observer: MarketDataObserver) -> None:
        """Attach a bounded normalized-data observer before the feed starts."""
        if observer not in self._observers:
            self._observers = (*self._observers, observer)

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopping = False
        self._universe_changed.clear()
        self._task = asyncio.create_task(self._supervise(), name="market-data-hub")

    async def stop(self) -> None:
        LOGGER.info("MarketDataHub shutdown requested")
        self._stopping = True
        self._universe_changed.set()
        task = self._task
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._task = None
        await self._close_all()
        self._set_health(HealthStatus.STOPPED, active_connections=0)
        LOGGER.info("MarketDataHub shutdown complete")

    def update_universe(self, symbols: Iterable[str]) -> None:
        # Plan the complete replacement before touching any live state.  A
        # capacity rejection must leave the current working universe intact.
        normalized = normalize_symbols(symbols)
        plan = plan_topics(normalized, max_connections=self._max_connections)
        self._symbols = normalized
        allowed = set(self._symbols)
        self._snapshots = {
            key: value for key, value in self._snapshots.items() if key in allowed
        }
        self._forming = {
            key: value for key, value in self._forming.items() if key in allowed
        }
        self._closed = {
            key: value for key, value in self._closed.items() if key in allowed
        }
        self._expected = set(plan.topics)
        self._acknowledged.clear()
        self._failed.clear()
        self._pending_batches.clear()
        self._pending_batch_shards.clear()
        self._subscribe_deadlines.clear()
        self._rotation_pending = True
        self._universe_changed.set()

    def get_snapshot(self, symbol: str) -> MarketTickerSnapshot | None:
        return self._snapshots.get(symbol.upper())

    def is_current_ticker_snapshot(self, snapshot: MarketTickerSnapshot) -> bool:
        """Return whether a ticker belongs to the currently active socket generation."""
        return self.is_current_connection_epoch(snapshot.connection_epoch)

    def is_current_connection_epoch(self, epoch: ConnectionEpoch | None) -> bool:
        """Return whether an observed feed item belongs to its current socket generation."""
        return (
            epoch is not None
            and not self._global_failure
            and epoch.shard_id not in self._unavailable_shards
            and self._generation_by_shard.get(epoch.shard_id) == epoch.generation
        )

    def is_shard_available(self, shard_id: int) -> bool:
        """Only a known, opened, non-retired shard is locally admissible."""
        return (not self._global_failure
                and shard_id in self._active_topic_shards.values()
                and shard_id not in self._unavailable_shards
                and self._shard_connections.get(shard_id) in self._connections)

    def get_forming_candle(self, symbol: str) -> MarketCandle | None:
        return self._forming.get(symbol.upper())

    def get_last_closed_candle(self, symbol: str) -> MarketCandle | None:
        return self._closed.get(symbol.upper())

    def get_health(self) -> MarketDataHealth:
        now = self._now()
        state = self._health.state
        ticker_stale = frozenset(
            symbol
            for symbol, snapshot in self._snapshots.items()
            if snapshot.received_at
            and now - snapshot.received_at > timedelta(seconds=self._stale_after)
        )
        closed_stale = frozenset(
            symbol
            for symbol, candle in self._closed.items()
            if now - candle.received_at
            > timedelta(seconds=self._closed_candle_stale_after)
        )
        if state is HealthStatus.HEALTHY and (
            ticker_stale
            or closed_stale
            or (
                self._health.last_message_at
                and now - self._health.last_message_at
                > timedelta(seconds=self._stale_after)
            )
        ):
            state = HealthStatus.STALE
        stale_signature = (
            (ticker_stale, closed_stale) if state is HealthStatus.STALE else None
        )
        if (
            stale_signature is not None
            and stale_signature != self._last_stale_signature
        ):
            LOGGER.warning(
                "MarketDataHub stale | ticker_symbols=%s closed_candle_symbols=%s",
                len(ticker_stale),
                len(closed_stale),
            )
        self._last_stale_signature = stale_signature
        return replace(
            self._health,
            state=state,
            stale_ticker_symbols=ticker_stale,
            stale_closed_candle_symbols=closed_stale,
            stale_symbols=len(ticker_stale | closed_stale),
        )

    def get_subscription_state(self) -> SubscriptionState:
        pending = frozenset(
            topic for batch in self._pending_batches.values() for topic in batch
        )
        return SubscriptionState(
            expected_topics=frozenset(self._expected),
            acknowledged_topics=frozenset(self._acknowledged),
            failed_topics=frozenset(self._failed),
            pending_topics=pending,
        )

    async def _supervise(self) -> None:
        attempt = 0
        while not self._stopping:
            if not self._symbols:
                self._set_health(HealthStatus.DISCONNECTED, active_connections=0)
                await self._universe_changed.wait()
                self._universe_changed.clear()
                continue
            plan = None
            self._failed_shard_id = None
            self._last_loop_lag_ms = None
            rotation_attempt = "fresh"
            self._cycle += 1
            try:
                self._set_health(
                    HealthStatus.CONNECTING
                    if attempt == 0
                    else HealthStatus.RECONNECTING
                )
                plan = plan_topics(self._symbols, max_connections=self._max_connections)
                self._expected = set(plan.topics)
                self._acknowledged.clear()
                self._failed.clear()
                self._pending_batches.clear()
                self._pending_batch_shards.clear()
                self._subscribe_deadlines.clear()
                self._unavailable_shards.clear()
                rotation_attempt = (
                    "planned" if self._rotation_pending and self._keep_state_on_planned_rotation()
                    else "fresh"
                )
                await self._run_cycle(plan)
                attempt = 0
            except _UniverseChanged:
                self._universe_changed.clear()
                attempt = 0
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor must reconnect on any transport failure
                self._global_failure = True
                self._rotation_pending = False
                self._fail_pending()
                if self._stopping:
                    break
                exception_class, reason = _safe_transport_failure(exc)
                timeout_source = (
                    "hub_pong_deadline" if isinstance(exc, _PongDeadlineExceeded)
                    else "transport" if isinstance(exc, TimeoutError) else "none"
                )
                LOGGER.warning(
                    "MarketDataHub reconnect attempt | exception_class=%s reason=%s timeout_source=%s close_code=%s rotation_attempt=%s rotation=fresh shards=%s shard_id=%s cycle=%s reconnects=%s backoff_index=%s queue_depth=unknown loop_lag_ms=%s",
                    exception_class,
                    reason,
                    timeout_source,
                    _safe_close_code(exc),
                    rotation_attempt,
                    len(plan.connections) if plan is not None else 0,
                    exc.shard_id if isinstance(exc, _PongDeadlineExceeded)
                    else self._failed_shard_id if self._failed_shard_id is not None else "unknown",
                    self._cycle,
                    self._health.reconnects + 1,
                    attempt,
                    self._last_loop_lag_ms if self._last_loop_lag_ms is not None else "unknown",
                )
                self._set_health(
                    HealthStatus.RECONNECTING,
                    reconnects=self._health.reconnects + 1,
                    disconnects=self._health.disconnects + 1,
                    active_connections=0,
                )
                await self._sleeper(
                    _BACKOFF_SECONDS[min(attempt, len(_BACKOFF_SECONDS) - 1)]
                )
                attempt += 1
            finally:
                await self._close_all()
        if not self._stopping:
            self._set_health(HealthStatus.DISCONNECTED, active_connections=0)

    async def _run_cycle(self, plan: Any) -> None:
        keep_state = self._keep_state_on_planned_rotation() and self._rotation_pending
        previous_topic_shards = self._active_topic_shards
        self._rotation_pending = False
        connections = []
        initial_failures: dict[int, tuple[Any | None, Exception]] = {}
        for shard_id, connection_plan in enumerate(plan.connections):
            try:
                connection = await self._open_connection()
            except Exception as exc:
                self._failed_shard_id = shard_id
                if len(plan.connections) < 2:
                    raise
                initial_failures[shard_id] = (None, exc)
                epoch = self._next_connection_epoch(shard_id)
                self._invalidate_tickers_for_topics(connection_plan.topics)
                self._notify("observe_connection", ConnectionOpened(epoch, tuple(connection_plan.topics), self._now()))
                connections.append((None, connection_plan, epoch))
                continue
            if keep_state:
                # Planned rotation: symbols that stay subscribed keep their
                # ticker/kline state and their current connection generation.
                epoch = self._current_connection_epoch(shard_id)
            else:
                epoch = self._next_connection_epoch(shard_id)
                self._invalidate_tickers_for_topics(connection_plan.topics)
            self._connections.add(connection)
            self._shard_connections[shard_id] = connection
            connections.append((connection, connection_plan, epoch))
            if keep_state:
                notify_topics = tuple(
                    topic
                    for topic in connection_plan.topics
                    if previous_topic_shards.get(topic) != shard_id
                )
            else:
                notify_topics = tuple(connection_plan.topics)
            if notify_topics:
                self._notify(
                    "observe_connection",
                    ConnectionOpened(epoch, notify_topics, self._now()),
                )
            LOGGER.info(
                "MarketDataHub connected | topics=%s rotation=%s",
                len(connection_plan.topics),
                "planned" if keep_state else "fresh",
            )
        self._active_topics = frozenset(plan.topics)
        self._active_topic_shards = {
            topic: shard_id
            for shard_id, connection_plan in enumerate(plan.connections)
            for topic in connection_plan.topics
        }
        self._set_health(self._health.state, active_connections=len(connections))
        for connection, connection_plan, epoch in connections:
            if connection is None:
                continue
            for batch in connection_plan.batches:
                req_id = self._next_request_id("subscribe")
                self._register_subscribe(req_id, batch.topics, shard_id=epoch.shard_id)
                try:
                    await connection.send(
                        json.dumps(
                            {
                                "op": "subscribe",
                                "args": list(batch.topics),
                                "req_id": req_id,
                            }
                        )
                    )
                except Exception as exc:
                    self._failed_shard_id = epoch.shard_id
                    if len(connections) < 2:
                        raise
                    initial_failures[epoch.shard_id] = (connection, exc)
                    break

        receive_tasks = {
            asyncio.create_task(connection.recv()): (connection, epoch)
            for connection, _plan, epoch in connections
            if connection is not None and epoch.shard_id not in initial_failures
        }
        event_task = asyncio.create_task(self._universe_changed.wait())
        last_ping = self._now()
        pongs: dict[str, tuple[datetime, int]] = {}
        reconnect_tasks: dict[asyncio.Task[Any], int] = {}
        retired_receive_tasks: set[asyncio.Task[Any]] = set()

        async def replace_shard(shard_id: int, old_connection: Any | None) -> tuple[Any, ConnectionEpoch]:
            if old_connection is not None:
                try:
                    await asyncio.wait_for(self._close(old_connection), timeout=self._close_timeout)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass  # retain it for the final bounded close during stop/rotation
                else:
                    self._connections.discard(old_connection)
            attempt = 0
            while True:
                try:
                    connection = await self._open_connection()
                    self._connections.add(connection)
                    try:
                        for batch in plan.connections[shard_id].batches:
                            req_id = self._next_request_id("subscribe")
                            self._register_subscribe(req_id, batch.topics, shard_id=shard_id)
                            await connection.send(json.dumps({
                                "op": "subscribe", "args": list(batch.topics), "req_id": req_id
                            }))
                    except BaseException:
                        try:
                            await asyncio.wait_for(self._close(connection), timeout=self._close_timeout)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass  # keep an unconfirmed socket for rotation/shutdown
                        else:
                            self._connections.discard(connection)
                        raise
                    return connection, self._current_connection_epoch(shard_id)
                except Exception as exc:
                    for req_id, owner in tuple(self._pending_batch_shards.items()):
                        if owner == shard_id:
                            self._failed.update(self._pending_batches.pop(req_id, ()))
                            self._pending_batch_shards.pop(req_id, None)
                            self._subscribe_deadlines.pop(req_id, None)
                    exception_class, reason = _safe_transport_failure(exc)
                    LOGGER.warning("MarketDataHub shard retry | shard_id=%s exception_class=%s reason=%s",
                                   shard_id, exception_class, reason)
                    await self._sleeper(_BACKOFF_SECONDS[min(attempt, len(_BACKOFF_SECONDS) - 1)])
                    attempt += 1

        def retire(shard_id: int, exc: Exception) -> None:
            connection, connection_plan, _epoch = connections[shard_id]
            self._unavailable_shards.add(shard_id)
            epoch = self._next_connection_epoch(shard_id)
            self._invalidate_tickers_for_topics(connection_plan.topics)
            self._notify("observe_connection", ConnectionOpened(epoch, tuple(connection_plan.topics), self._now()))
            for req_id, owner in tuple(self._pending_batch_shards.items()):
                if owner == shard_id:
                    self._failed.update(self._pending_batches.pop(req_id, ()))
                    self._pending_batch_shards.pop(req_id, None)
                    self._subscribe_deadlines.pop(req_id, None)
            self._acknowledged.difference_update(connection_plan.topics)
            self._failed.update(connection_plan.topics)
            for req_id, (_deadline, owner) in tuple(pongs.items()):
                if owner == shard_id:
                    pongs.pop(req_id)
            for receive_task, (_socket, task_epoch) in tuple(receive_tasks.items()):
                if task_epoch.shard_id == shard_id:
                    receive_task.cancel()
                    receive_tasks.pop(receive_task)
                    retired_receive_tasks.add(receive_task)
            self._shard_connections.pop(shard_id, None)
            self._set_health(HealthStatus.RECONNECTING,
                             reconnects=self._health.reconnects + 1,
                             disconnects=self._health.disconnects + 1,
                             active_connections=len(self._shard_connections))
            exception_class, reason = _safe_transport_failure(exc)
            LOGGER.warning("MarketDataHub shard reconnect | shard_id=%s exception_class=%s reason=%s",
                           shard_id, exception_class, reason)
            reconnect_tasks[asyncio.create_task(replace_shard(shard_id, connection))] = shard_id
        for shard_id, (_connection, exc) in initial_failures.items():
            retire(shard_id, exc)
        self._global_failure = False
        try:
            while True:
                now = self._now()
                if now - last_ping >= timedelta(seconds=self._ping_interval):
                    for connection, _plan, epoch in connections:
                        if epoch.shard_id in self._unavailable_shards:
                            continue
                        req_id = self._next_request_id("ping")
                        try:
                            await connection.send(
                                json.dumps({"op": "ping", "req_id": req_id})
                            )
                        except Exception as exc:
                            self._last_loop_lag_ms = None
                            self._failed_shard_id = epoch.shard_id
                            if len(connections) < 2:
                                raise
                            retire(epoch.shard_id, exc)
                            continue
                        pongs[req_id] = (now + timedelta(seconds=self._pong_deadline), epoch.shard_id)
                    last_ping = now
                for deadline, shard_id in tuple(pongs.values()):
                    if now > deadline:
                        if len(connections) < 2:
                            raise _PongDeadlineExceeded(shard_id)
                        if shard_id not in self._unavailable_shards:
                            retire(shard_id, _PongDeadlineExceeded(shard_id))
                        break
                timeout = self._wait_timeout(now, last_ping, pongs)
                loop = asyncio.get_running_loop()
                if self._subscribe_deadlines:
                    timeout = min(timeout, max(0, min(self._subscribe_deadlines.values()) - loop.time()))
                for req_id, deadline in tuple(self._subscribe_deadlines.items()):
                    if loop.time() < deadline:
                        continue
                    shard_id = self._pending_batch_shards.get(req_id)
                    if shard_id is not None:
                        if len(connections) < 2:
                            raise TimeoutError("subscribe acknowledgement deadline")
                        retire(shard_id, TimeoutError("subscribe acknowledgement deadline"))
                        break
                wait_started = loop.time()
                done, _ = await asyncio.wait(
                    [*receive_tasks, *reconnect_tasks, event_task],
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done and timeout > 0:
                    self._last_loop_lag_ms = min(
                        60_000, max(0, int((loop.time() - wait_started - timeout) * 1000))
                    )
                else:
                    # A completed receive (including a failed one) is not a
                    # timeout sample; never attribute a prior wait to it.
                    self._last_loop_lag_ms = None
                if event_task in done:
                    raise _UniverseChanged
                for task in done:
                    if task in reconnect_tasks:
                        shard_id = reconnect_tasks.pop(task)
                        connection, epoch = task.result()
                        connections[shard_id] = (connection, plan.connections[shard_id], epoch)
                        self._connections.add(connection)
                        self._shard_connections[shard_id] = connection
                        self._set_health(HealthStatus.RECONNECTING,
                                         active_connections=len(self._shard_connections))
                        receive_tasks[asyncio.create_task(connection.recv())] = (connection, epoch)
                        continue
                    connection, epoch = receive_tasks.pop(task)
                    try:
                        message = task.result()
                    except Exception as exc:
                        self._failed_shard_id = epoch.shard_id
                        if len(connections) < 2:
                            raise
                        retire(epoch.shard_id, exc)
                        continue
                    rejected = self._handle_message(message, pongs, connection_epoch=epoch)
                    if rejected and connection in self._connections:
                        retire(epoch.shard_id, ConnectionError("subscription rejected"))
                        continue
                    receive_tasks[asyncio.create_task(connection.recv())] = (
                        connection,
                        epoch,
                    )
        finally:
            event_task.cancel()
            for task in receive_tasks:
                task.cancel()
            for task in reconnect_tasks:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await event_task
            await asyncio.gather(*receive_tasks, *retired_receive_tasks, return_exceptions=True)
            await asyncio.gather(*reconnect_tasks, return_exceptions=True)
            # Every opened replacement, including one completed alongside
            # rotation, is already owned by _connections for bounded cleanup.

    async def _open_connection(self) -> Any:
        kwargs = {
            "url": BYBIT_PUBLIC_LINEAR_ENDPOINT,
            "max_queue": DEFAULT_MAX_QUEUE,
            "ping_interval": None,
            "ping_timeout": None,
        }
        try:
            result = self._connector(**kwargs)
        except TypeError:
            result = self._connector(
                BYBIT_PUBLIC_LINEAR_ENDPOINT,
                max_queue=DEFAULT_MAX_QUEUE,
                ping_interval=None,
                ping_timeout=None,
            )
        connection = await result if inspect.isawaitable(result) else result
        if hasattr(connection, "__aenter__"):
            connection = await connection.__aenter__()
        return connection

    async def _close_all(self) -> None:
        connections = tuple(self._connections)
        self._shard_connections.clear()
        results = await asyncio.gather(
            *(
                asyncio.wait_for(self._close(connection), timeout=self._close_timeout)
                for connection in connections
            ),
            return_exceptions=True,
        )
        for connection, result in zip(connections, results):
            if result is None:
                self._connections.discard(connection)

    @staticmethod
    async def _close(connection: Any) -> None:
        close = getattr(connection, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    def _wait_timeout(
        self, now: datetime, last_ping: datetime, pongs: dict[str, tuple[datetime, int]]
    ) -> float:
        deadlines = [last_ping + timedelta(seconds=self._ping_interval)]
        deadlines.extend(deadline for deadline, _shard_id in pongs.values())
        if self._health.last_message_at:
            deadlines.append(
                self._health.last_message_at + timedelta(seconds=self._stale_after)
            )
        return max(0.0, min((deadline - now).total_seconds() for deadline in deadlines))

    def _handle_message(
        self,
        raw: object,
        pongs: dict[str, tuple[datetime, int]] | None = None,
        *,
        connection_epoch: ConnectionEpoch | None = None,
    ) -> bool:
        epoch = connection_epoch or ConnectionEpoch(0, 0)
        try:
            payload = (
                json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            )
            if not isinstance(payload, dict):
                raise TypeError("WebSocket message must be an object")
        except (ValueError, TypeError, json.JSONDecodeError):
            self._increment(parse_errors=1)
            return False
        self._message_received()
        op = payload.get("op")
        if op == "subscribe":
            return self._handle_ack(payload, connection_epoch=epoch)
        elif op == "pong" or (op == "ping" and payload.get("ret_msg") == "pong"):
            self._increment(pong_messages=1)
            if pongs is not None:
                pending = pongs.get(str(payload.get("req_id", "")))
                if pending is not None and pending[1] == epoch.shard_id:
                    pongs.pop(str(payload.get("req_id", "")), None)
        elif str(payload.get("topic", "")).startswith("tickers."):
            self._handle_ticker(payload, connection_epoch=epoch)
        elif str(payload.get("topic", "")).startswith("kline.1."):
            topic = str(payload.get("topic", ""))
            self._handle_kline(
                payload,
                topic_symbol=topic.removeprefix("kline.1."),
                connection_epoch=epoch,
            )
        return False

    def _handle_ack(self, payload: dict[str, Any], *, connection_epoch: ConnectionEpoch | None = None) -> bool:
        req_id = str(payload.get("req_id", ""))
        owner = self._pending_batch_shards.get(req_id)
        if owner is not None and connection_epoch is not None and owner != connection_epoch.shard_id:
            self._increment(anomalies=1)
            return False
        topics = self._pending_batches.pop(req_id, None)
        self._subscribe_deadlines.pop(req_id, None)
        self._pending_batch_shards.pop(req_id, None)
        if topics is None:
            self._increment(anomalies=1)
            return False
        if payload.get("success") is True:
            self._acknowledged.update(topics)
            self._failed.difference_update(topics)
            if owner is not None and not any(shard == owner for shard in self._pending_batch_shards.values()) and not any(
                topic in self._failed for topic in self._active_topics if self._active_topic_shards.get(topic) == owner
            ):
                self._unavailable_shards.discard(owner)
                if (
                    self._health.state is HealthStatus.RECONNECTING
                    and not self._unavailable_shards
                    and not self._global_failure
                    and len(self._shard_connections) == len(set(self._active_topic_shards.values()))
                ):
                    self._set_health(HealthStatus.HEALTHY,
                                     active_connections=len(self._shard_connections))
            LOGGER.info(
                "MarketDataHub subscriptions acknowledged | topics=%s", len(topics)
            )
        else:
            self._failed.update(topics)
            LOGGER.error(
                "MarketDataHub subscriptions rejected | topics=%s", len(topics)
            )
        self._increment(acknowledgements=1)
        return payload.get("success") is not True

    def _handle_ticker(
        self, payload: dict[str, Any], *, connection_epoch: ConnectionEpoch
    ) -> None:
        data = payload.get("data")
        if not isinstance(data, dict):
            self._increment(parse_errors=1)
            return
        symbol = data.get("symbol")
        if not isinstance(symbol, str) or symbol.upper() not in self._symbols:
            self._increment(anomalies=1)
            return
        symbol = symbol.upper()
        snapshot_type = payload.get("type") == "snapshot"
        old = self._snapshots.get(symbol)
        if not snapshot_type and old is None:
            self._increment(anomalies=1)
            return
        exchange_timestamp = self._timestamp(payload.get("ts"))
        cs = self._integer(payload.get("cs"))
        if old is not None:
            if cs is not None and old.cs is not None and cs < old.cs:
                self._increment(anomalies=1)
                LOGGER.warning("Bybit ticker sequence regression for %s", symbol)
            if (
                exchange_timestamp
                and old.exchange_timestamp
                and exchange_timestamp < old.exchange_timestamp
            ):
                self._increment(anomalies=1)
                LOGGER.warning("Bybit ticker timestamp regression for %s", symbol)
        values: dict[str, Any] = {}
        for external, internal in TICKER_DECIMAL_FIELDS.items():
            if external not in data:
                continue
            decimal = self._decimal(data[external])
            if decimal is not None:
                values[internal] = decimal
        if "nextFundingTime" in data:
            funding_time = self._timestamp(data["nextFundingTime"])
            if funding_time is not None:
                values["next_funding_time"] = funding_time
        now = self._now()
        if snapshot_type:
            self._snapshots[symbol] = MarketTickerSnapshot(
                symbol=symbol,
                connection_epoch=connection_epoch,
                cs=cs,
                exchange_timestamp=exchange_timestamp,
                received_at=now,
                source=BYBIT_PUBLIC_WS_SOURCE,
                **values,
            )
        else:
            self._snapshots[symbol] = replace(
                old,
                **values,
                cs=cs if cs is not None else old.cs,
                exchange_timestamp=exchange_timestamp
                if exchange_timestamp is not None
                else old.exchange_timestamp,
                received_at=now,
                connection_epoch=connection_epoch,
            )
        self._health = replace(self._health, last_ticker_at=now)
        self._increment(ticker_updates=1)
        self._notify("observe_ticker", self._snapshots[symbol])

    def _handle_kline(
        self,
        payload: dict[str, Any],
        *,
        topic_symbol: str | None = None,
        connection_epoch: ConnectionEpoch,
    ) -> None:
        entries = payload.get("data")
        if not isinstance(entries, list):
            self._increment(parse_errors=1)
            return
        for entry in entries:
            candle = self._candle(
                entry,
                payload.get("ts"),
                topic_symbol=topic_symbol,
                connection_epoch=connection_epoch,
            )
            if candle is None or candle.symbol not in self._symbols:
                continue
            if candle.confirmed:
                current = self._closed.get(candle.symbol)
                if current is None or candle.open_time > current.open_time:
                    self._closed[candle.symbol] = candle
                    self._health = replace(
                        self._health, last_closed_candle_at=candle.received_at
                    )
                forming = self._forming.get(candle.symbol)
                if forming and forming.open_time == candle.open_time:
                    self._forming.pop(candle.symbol, None)
                self._notify("observe_closed_candle", candle)
            else:
                self._forming[candle.symbol] = candle
            self._increment(kline_updates=1)

    def _candle(
        self,
        entry: object,
        message_ts: object,
        *,
        topic_symbol: str | None = None,
        connection_epoch: ConnectionEpoch,
    ) -> MarketCandle | None:
        symbol = entry.get("symbol") if isinstance(entry, dict) else None
        if symbol is None:
            symbol = topic_symbol
        if (
            not isinstance(entry, dict)
            or entry.get("interval") != "1"
            or not isinstance(symbol, str)
            or not isinstance(entry.get("confirm"), bool)
        ):
            self._increment(parse_errors=1)
            return None
        open_time = self._timestamp(entry.get("start"))
        close_time = self._timestamp(entry.get("end"))
        decimals = {
            name: self._decimal(entry.get(name))
            for name in ("open", "high", "low", "close", "volume", "turnover")
        }
        if (
            open_time is None
            or close_time is None
            or any(value is None for value in decimals.values())
        ):
            return None
        return MarketCandle(
            symbol=symbol.upper(),
            open_time=open_time,
            close_time=close_time,
            interval="1",
            open=decimals["open"],
            high=decimals["high"],
            low=decimals["low"],
            close=decimals["close"],
            volume=decimals["volume"],
            turnover=decimals["turnover"],
            confirmed=entry.get("confirm") is True,
            exchange_timestamp=self._timestamp(message_ts),
            received_at=self._now(),
            connection_epoch=connection_epoch,
            source=BYBIT_PUBLIC_WS_SOURCE,
        )

    def _register_subscribe(self, req_id: str, topics: tuple[str, ...], *, shard_id: int | None = None) -> None:
        self._pending_batches[req_id] = topics
        if shard_id is not None:
            self._subscribe_deadlines[req_id] = asyncio.get_running_loop().time() + self._pong_deadline
            self._pending_batch_shards[req_id] = shard_id
        self._expected.update(topics)

    def _fail_pending(self) -> None:
        for topics in self._pending_batches.values():
            self._failed.update(topics)
        self._pending_batches.clear()
        self._pending_batch_shards.clear()
        self._subscribe_deadlines.clear()

    def _next_request_id(self, operation: str) -> str:
        self._request_counter += 1
        return f"market-data-{operation}-{self._request_counter}"

    def _message_received(self) -> None:
        now = self._now()
        target = HealthStatus.RECONNECTING if self._unavailable_shards else HealthStatus.HEALTHY
        if self._health.state is not target:
            LOGGER.info(
                "MarketDataHub health transition | from=%s to=%s",
                self._health.state,
                target,
            )
        self._health = replace(
            self._health,
            state=target,
            updated_at=now,
            last_message_at=now,
            messages_received=self._health.messages_received + 1,
        )

    def _set_health(self, state: HealthStatus, **changes: Any) -> None:
        if self._health.state is not state:
            LOGGER.info(
                "MarketDataHub health transition | from=%s to=%s",
                self._health.state,
                state,
            )
        self._health = replace(
            self._health, state=state, updated_at=self._now(), **changes
        )

    def _increment(
        self,
        *,
        parse_errors: int = 0,
        anomalies: int = 0,
        ticker_updates: int = 0,
        kline_updates: int = 0,
        acknowledgements: int = 0,
        pong_messages: int = 0,
        observer_errors: int = 0,
    ) -> None:
        self._health = replace(
            self._health,
            updated_at=self._now(),
            parse_errors=self._health.parse_errors + parse_errors,
            anomalies=self._health.anomalies + anomalies,
            ticker_updates=self._health.ticker_updates + ticker_updates,
            kline_updates=self._health.kline_updates + kline_updates,
            acknowledgements=self._health.acknowledgements + acknowledgements,
            pong_messages=self._health.pong_messages + pong_messages,
            observer_errors=self._health.observer_errors + observer_errors,
        )

    def _keep_state_on_planned_rotation(self) -> bool:
        """Whether a planned rotation may preserve state for symbols that remain
        subscribed. Overridable without a code change via env
        MARKET_DATA_KEEP_STATE_ON_PLANNED_ROTATION=0 or config key
        market_data_keep_state_on_planned_rotation=False (getattr default True)."""
        raw = os.getenv(KEEP_STATE_ON_PLANNED_ROTATION_ENV)
        if raw is not None:
            return raw.strip().lower() not in {"0", "false", "no", "off"}
        return bool(
            getattr(self._config, KEEP_STATE_ON_PLANNED_ROTATION_CONFIG_KEY, True)
        )

    def _next_connection_epoch(self, shard_id: int) -> ConnectionEpoch:
        generation = self._generation_by_shard.get(shard_id, 0) + 1
        self._generation_by_shard[shard_id] = generation
        return ConnectionEpoch(shard_id=shard_id, generation=generation)

    def _current_connection_epoch(self, shard_id: int) -> ConnectionEpoch:
        """Reuse a shard's live generation across a planned rotation so kept
        snapshots/candles stay current; mint gen 1 only for a brand-new shard."""
        generation = self._generation_by_shard.get(shard_id)
        if generation is None:
            return self._next_connection_epoch(shard_id)
        return ConnectionEpoch(shard_id=shard_id, generation=generation)

    def _invalidate_tickers_for_topics(self, topics: Iterable[str]) -> None:
        for topic in topics:
            if topic.startswith("tickers."):
                self._snapshots.pop(topic.removeprefix("tickers."), None)

    def _notify(self, method: str, value: object) -> None:
        for observer in self._observers:
            callback = getattr(observer, method, None)
            if callback is None:
                continue
            try:
                callback(value)
            except Exception:
                self._increment(observer_errors=1)
                LOGGER.exception("MarketDataHub observer failed | method=%s", method)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("market-data clock must return a timezone-aware timestamp")
        return value.astimezone(UTC)

    def _decimal(self, value: object) -> Decimal | None:
        if value is None or isinstance(value, bool):
            self._increment(parse_errors=1)
            return None
        try:
            decimal = Decimal(str(value))
        except (InvalidOperation, ValueError):
            self._increment(parse_errors=1)
            return None
        if not decimal.is_finite():
            self._increment(parse_errors=1)
            return None
        return decimal

    def _integer(self, value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(str(value))
        except (TypeError, ValueError):
            self._increment(parse_errors=1)
            return None

    def _timestamp(self, value: object) -> datetime | None:
        if value is None:
            return None
        try:
            milliseconds = int(str(value))
            return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
        except (TypeError, ValueError, OSError, OverflowError):
            self._increment(parse_errors=1)
            return None
