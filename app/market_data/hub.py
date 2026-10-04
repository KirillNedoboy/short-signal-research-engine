"""Bounded in-memory shadow state for Bybit public linear WebSocket feeds."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
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
)
from .observers import ConnectionOpened, MarketDataObserver
from .planning import DEFAULT_MAX_CONNECTIONS, normalize_symbols, plan_topics

LOGGER = logging.getLogger(__name__)
_BACKOFF_SECONDS = (1, 2, 4, 8, 16, 30)
_TICKER_FIELDS = {
    "lastPrice": "last_price",
    "markPrice": "mark_price",
    "indexPrice": "index_price",
    "fundingRate": "funding_rate",
    "openInterest": "open_interest",
    "volume24h": "volume_24h",
    "turnover24h": "turnover_24h",
    "price24hPcnt": "price_24h_pcnt",
    "highPrice24h": "high_price_24h",
    "lowPrice24h": "low_price_24h",
    "prevPrice24h": "prev_price_24h",
    "bid1Price": "bid1_price",
    "bid1Size": "bid1_size",
    "ask1Price": "ask1_price",
    "ask1Size": "ask1_size",
    "openInterestValue": "open_interest_value",
}


class _UniverseChanged(Exception):
    pass


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
        self._expected: set[str] = set()
        self._acknowledged: set[str] = set()
        self._failed: set[str] = set()
        self._connections: set[Any] = set()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._universe_changed = asyncio.Event()
        self._request_counter = 0
        self._generation_by_shard: dict[int, int] = {}
        self._last_stale_signature: tuple[frozenset[str], frozenset[str]] | None = None

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
        self._symbols = normalize_symbols(symbols)
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
        plan = plan_topics(self._symbols, max_connections=self._max_connections)
        self._expected = set(plan.topics)
        self._acknowledged.clear()
        self._failed.clear()
        self._pending_batches.clear()
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
            and self._generation_by_shard.get(epoch.shard_id) == epoch.generation
        )

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
                await self._run_cycle(plan)
                attempt = 0
            except _UniverseChanged:
                self._universe_changed.clear()
                attempt = 0
                continue
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - supervisor must reconnect on any transport failure
                self._fail_pending()
                if self._stopping:
                    break
                LOGGER.warning(
                    "MarketDataHub reconnect attempt | backoff_index=%s", attempt
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
        connections = []
        for shard_id, connection_plan in enumerate(plan.connections):
            connection = await self._open_connection()
            epoch = self._next_connection_epoch(shard_id)
            self._invalidate_tickers_for_topics(connection_plan.topics)
            self._connections.add(connection)
            connections.append((connection, connection_plan, epoch))
            self._notify(
                "observe_connection",
                ConnectionOpened(epoch, connection_plan.topics, self._now()),
            )
            LOGGER.info(
                "MarketDataHub connected | topics=%s", len(connection_plan.topics)
            )
        self._set_health(self._health.state, active_connections=len(connections))
        for connection, connection_plan, _epoch in connections:
            for batch in connection_plan.batches:
                req_id = self._next_request_id("subscribe")
                self._register_subscribe(req_id, batch.topics)
                await connection.send(
                    json.dumps(
                        {
                            "op": "subscribe",
                            "args": list(batch.topics),
                            "req_id": req_id,
                        }
                    )
                )

        receive_tasks = {
            asyncio.create_task(connection.recv()): (connection, epoch)
            for connection, _plan, epoch in connections
        }
        event_task = asyncio.create_task(self._universe_changed.wait())
        last_ping = self._now()
        pongs: dict[str, datetime] = {}
        try:
            while True:
                now = self._now()
                if now - last_ping >= timedelta(seconds=self._ping_interval):
                    for connection, _plan, _epoch in connections:
                        req_id = self._next_request_id("ping")
                        await connection.send(
                            json.dumps({"op": "ping", "req_id": req_id})
                        )
                        pongs[req_id] = now + timedelta(seconds=self._pong_deadline)
                    last_ping = now
                if any(now > deadline for deadline in pongs.values()):
                    raise TimeoutError("Bybit public WebSocket pong deadline exceeded")
                timeout = self._wait_timeout(now, last_ping, pongs)
                done, _ = await asyncio.wait(
                    [*receive_tasks, event_task],
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if event_task in done:
                    raise _UniverseChanged
                for task in done:
                    connection, epoch = receive_tasks.pop(task)
                    message = task.result()
                    self._handle_message(message, pongs, connection_epoch=epoch)
                    receive_tasks[asyncio.create_task(connection.recv())] = (
                        connection,
                        epoch,
                    )
        finally:
            event_task.cancel()
            for task in receive_tasks:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await event_task
            await asyncio.gather(*receive_tasks, return_exceptions=True)

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
        connections, self._connections = self._connections, set()
        await asyncio.gather(
            *(
                asyncio.wait_for(self._close(connection), timeout=self._close_timeout)
                for connection in connections
            ),
            return_exceptions=True,
        )

    @staticmethod
    async def _close(connection: Any) -> None:
        close = getattr(connection, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    def _wait_timeout(
        self, now: datetime, last_ping: datetime, pongs: dict[str, datetime]
    ) -> float:
        deadlines = [last_ping + timedelta(seconds=self._ping_interval)]
        deadlines.extend(pongs.values())
        if self._health.last_message_at:
            deadlines.append(
                self._health.last_message_at + timedelta(seconds=self._stale_after)
            )
        return max(0.0, min((deadline - now).total_seconds() for deadline in deadlines))

    def _handle_message(
        self,
        raw: object,
        pongs: dict[str, datetime] | None = None,
        *,
        connection_epoch: ConnectionEpoch | None = None,
    ) -> None:
        epoch = connection_epoch or ConnectionEpoch(0, 0)
        try:
            payload = (
                json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            )
            if not isinstance(payload, dict):
                raise TypeError("WebSocket message must be an object")
        except (ValueError, TypeError, json.JSONDecodeError):
            self._increment(parse_errors=1)
            return
        self._message_received()
        op = payload.get("op")
        if op == "subscribe":
            self._handle_ack(payload)
        elif op == "pong" or (op == "ping" and payload.get("ret_msg") == "pong"):
            self._increment(pong_messages=1)
            if pongs is not None:
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

    def _handle_ack(self, payload: dict[str, Any]) -> None:
        req_id = str(payload.get("req_id", ""))
        topics = self._pending_batches.pop(req_id, None)
        if topics is None:
            self._increment(anomalies=1)
            return
        if payload.get("success") is True:
            self._acknowledged.update(topics)
            self._failed.difference_update(topics)
            LOGGER.info(
                "MarketDataHub subscriptions acknowledged | topics=%s", len(topics)
            )
        else:
            self._failed.update(topics)
            LOGGER.error(
                "MarketDataHub subscriptions rejected | topics=%s", len(topics)
            )
        self._increment(acknowledgements=1)

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
        for external, internal in _TICKER_FIELDS.items():
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

    def _register_subscribe(self, req_id: str, topics: tuple[str, ...]) -> None:
        self._pending_batches[req_id] = topics
        self._expected.update(topics)

    def _fail_pending(self) -> None:
        for topics in self._pending_batches.values():
            self._failed.update(topics)
        self._pending_batches.clear()

    def _next_request_id(self, operation: str) -> str:
        self._request_counter += 1
        return f"market-data-{operation}-{self._request_counter}"

    def _message_received(self) -> None:
        now = self._now()
        if self._health.state is not HealthStatus.HEALTHY:
            LOGGER.info(
                "MarketDataHub health transition | from=%s to=%s",
                self._health.state,
                HealthStatus.HEALTHY,
            )
        self._health = replace(
            self._health,
            state=HealthStatus.HEALTHY,
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

    def _next_connection_epoch(self, shard_id: int) -> ConnectionEpoch:
        generation = self._generation_by_shard.get(shard_id, 0) + 1
        self._generation_by_shard[shard_id] = generation
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
