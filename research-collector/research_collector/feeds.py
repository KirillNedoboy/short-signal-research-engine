"""Public market-data adapters and bounded in-memory feature state.

The adapters intentionally fail closed.  A transport reconnect or sequence
anomaly invalidates the affected book and makes derived fields UNKNOWN until a
new snapshot is received.  Trade and book freshness are tracked separately.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
import json
import logging
import time
from typing import Any, Awaitable, Callable, Iterable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import BinanceConfig, BybitConfig
from .db import ResearchDB
from .models import BookUpdate, FeatureSnapshot, Trade
from .orderbook import BookState

try:  # Optional at import time so offline schema tools still work.
    import websockets
except ImportError:  # pragma: no cover - exercised on an unprovisioned host
    websockets = None  # type: ignore[assignment]


LOG = logging.getLogger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


def _decimal(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite() or result < 0:
        raise InvalidOperation
    return result


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _json_object(payload: str | bytes) -> Mapping[str, Any] | None:
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, Mapping) else None


def _levels(raw: Any) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(raw, list):
        return ()
    result: list[tuple[Decimal, Decimal]] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            result.append((_decimal(item[0]), _decimal(item[1])))
        except (InvalidOperation, ValueError):
            continue
    return tuple(result)


def parse_bybit_trade(message: Mapping[str, Any], provider: str, market: str, received_ms: int) -> list[Trade]:
    topic = str(message.get("topic") or "")
    if not topic.startswith("publicTrade."):
        return []
    rows = message.get("data")
    if not isinstance(rows, list):
        return []
    result: list[Trade] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        try:
            side = str(row["S"]).upper()
            price = _decimal(row["p"])
            quantity = _decimal(row["v"])
            exchange_ms = int(row["T"])
        except (KeyError, TypeError, ValueError, InvalidOperation):
            continue
        if side not in {"BUY", "SELL"} or price <= 0 or quantity <= 0:
            continue
        result.append(
            Trade(provider, market, str(row.get("s") or topic.rsplit(".", 1)[-1]).upper(), str(row.get("i")) if row.get("i") is not None else None, side, price, quantity, exchange_ms, received_ms)
        )
    return result


def parse_bybit_book(message: Mapping[str, Any], provider: str, market: str, received_ms: int) -> BookUpdate | None:
    topic = str(message.get("topic") or "")
    if not topic.startswith("orderbook."):
        return None
    parts = topic.split(".")
    data = message.get("data")
    if len(parts) != 3 or not isinstance(data, Mapping):
        return None
    try:
        update_id = int(data["u"])
        exchange_ms = int(message.get("ts") or data.get("ts") or received_ms)
    except (KeyError, TypeError, ValueError):
        return None
    seq = data.get("seq")
    try:
        cross = int(seq) if seq is not None else None
    except (TypeError, ValueError):
        cross = None
    return BookUpdate(
        provider=provider,
        market=market,
        symbol=str(data.get("s") or parts[2]).upper(),
        update_type="snapshot" if message.get("type") == "snapshot" else "delta",
        update_id=update_id,
        cross_seq=cross,
        exchange_ts_ms=exchange_ms,
        bids=_levels(data.get("b")),
        asks=_levels(data.get("a")),
    )


def parse_binance(message: Mapping[str, Any], received_ms: int) -> tuple[Trade | None, BookUpdate | None]:
    data = message.get("data") if message.get("stream") else message
    if not isinstance(data, Mapping):
        return None, None
    event = data.get("e")
    symbol = str(data.get("s") or "").upper()
    if event == "trade":
        price, quantity = _float(data.get("p")), _float(data.get("q"))
        exchange_ms = data.get("T")
        if price is None or quantity is None or price <= 0 or quantity <= 0 or exchange_ms is None:
            return None, None
        # m=True means the buyer was maker, so the taker side was SELL.
        side = "SELL" if bool(data.get("m")) else "BUY"
        trade = Trade("binance", "spot", symbol, str(data.get("t")) if data.get("t") is not None else None, side, Decimal(str(price)), Decimal(str(quantity)), int(exchange_ms), received_ms)
        return trade, None
    if event == "depthUpdate":
        try:
            update_id = int(data["u"])
            first_id = int(data["U"])
            previous_id = int(data["pu"]) if data.get("pu") is not None else None
            exchange_ms = int(data.get("E") or received_ms)
        except (KeyError, TypeError, ValueError):
            return None, None
        return None, BookUpdate(
            provider="binance",
            market="spot",
            symbol=symbol,
            update_type="delta",
            update_id=update_id,
            cross_seq=None,
            exchange_ts_ms=exchange_ms,
            bids=_levels(data.get("b")),
            asks=_levels(data.get("a")),
            first_update_id=first_id,
            previous_update_id=previous_id,
        )
    return None, None


class TradeWindow:
    def __init__(self, *, max_trades: int = 30_000, retention_ms: int = 120_000) -> None:
        self.max_trades = max_trades
        self.retention_ms = retention_ms
        self.rows: deque[Trade] = deque(maxlen=max_trades)
        self.ids: set[str] = set()
        self.id_order: deque[str] = deque(maxlen=max_trades)
        self.last_received_ms: int | None = None

    def add(self, trade: Trade) -> bool:
        if trade.trade_id is not None and trade.trade_id in self.ids:
            return False
        if len(self.rows) == self.max_trades:
            old = self.rows[0]
            if old.trade_id:
                self.ids.discard(old.trade_id)
        self.rows.append(trade)
        if trade.trade_id:
            if len(self.id_order) == self.id_order.maxlen:
                self.ids.discard(self.id_order.popleft())
            self.ids.add(trade.trade_id)
            self.id_order.append(trade.trade_id)
        self.last_received_ms = trade.received_ts_ms
        self.prune(trade.received_ts_ms)
        return True

    def prune(self, now: int) -> None:
        cutoff = now - self.retention_ms
        while self.rows and self.rows[0].received_ts_ms < cutoff:
            old = self.rows.popleft()
            if old.trade_id:
                self.ids.discard(old.trade_id)

    def imbalance(self, now: int, window_ms: int) -> float | None:
        self.prune(now)
        cutoff = now - window_ms
        buy = sell = 0.0
        for trade in self.rows:
            if trade.received_ts_ms < cutoff:
                continue
            value = float(trade.notional)
            if trade.side == "BUY":
                buy += value
            else:
                sell += value
        total = buy + sell
        return (buy - sell) / total if total > 0 else None


@dataclass(slots=True)
class MarketState:
    symbol: str
    bybit_depth: int = 50
    stale_after_ms: int = 5_000
    perp_book: BookState = field(init=False)
    bybit_spot_book: BookState = field(init=False)
    binance_spot_book: BookState = field(init=False)
    perp_trades: TradeWindow = field(default_factory=TradeWindow)
    bybit_spot_trades: TradeWindow = field(default_factory=TradeWindow)
    binance_spot_trades: TradeWindow = field(default_factory=TradeWindow)
    perp_price: float | None = None
    bybit_spot_price: float | None = None
    binance_spot_price: float | None = None
    funding_rate: float | None = None
    open_interest: float | None = None
    bybit_spot_available: bool | None = None
    binance_spot_available: bool | None = None
    last_perp_ms: int | None = None
    last_bybit_spot_ms: int | None = None
    last_binance_spot_ms: int | None = None
    last_perp_oi_ms: int | None = None
    connection_state: dict[str, str] = field(default_factory=dict)
    last_reason: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.perp_book = BookState(self.bybit_depth, self.stale_after_ms)
        self.bybit_spot_book = BookState(self.bybit_depth, self.stale_after_ms)
        self.binance_spot_book = BookState(20, self.stale_after_ms)

    def feed_status(self, market: str, now: int, last_ms: int | None) -> str:
        if self.connection_state.get(market) == "DISCONNECTED":
            return "UNKNOWN"
        if last_ms is None or now - last_ms > self.stale_after_ms:
            return "UNKNOWN"
        return "KNOWN"

    def _spot_source(self, now: int) -> tuple[str | None, float | None, BookState | None, TradeWindow | None, str]:
        if self.bybit_spot_available is True and self.feed_status("bybit_spot", now, self.last_bybit_spot_ms) == "KNOWN":
            return "bybit", self.bybit_spot_price, self.bybit_spot_book, self.bybit_spot_trades, "SPOT_PERP"
        if self.binance_spot_available is True and self.feed_status("binance_spot", now, self.last_binance_spot_ms) == "KNOWN":
            return "binance", self.binance_spot_price, self.binance_spot_book, self.binance_spot_trades, "SPOT_PERP"
        if self.bybit_spot_available is False and self.binance_spot_available is False:
            return None, None, None, None, "PERP_ONLY"
        return None, None, None, None, "SPOT_UNKNOWN"

    def snapshot(self, observed_ms: int) -> FeatureSnapshot:
        provider, spot_price, spot_book, spot_trades, profile = self._spot_source(observed_ms)
        perp_book_known = self.perp_book.is_fresh(observed_ms)
        perp_price_known = self.feed_status("bybit_linear", observed_ms, self.last_perp_ms) == "KNOWN" and self.perp_price is not None
        features: dict[str, Any] = {
            "trade_imbalance_5s": self._obs(self.perp_trades.imbalance(observed_ms, 5_000), "no_perp_trades"),
            "trade_imbalance_30s": self._obs(self.perp_trades.imbalance(observed_ms, 30_000), "no_perp_trades"),
            "trade_imbalance_120s": self._obs(self.perp_trades.imbalance(observed_ms, 120_000), "no_perp_trades"),
            "bid_ask_imbalance_l10": self._book_imbalance(self.perp_book, observed_ms),
            "bid_depth_replenishment": self._obs(self.perp_book.replenishment("bid"), "insufficient_book_history"),
            "ask_depth_replenishment": self._obs(self.perp_book.replenishment("ask"), "insufficient_book_history"),
            "perp_price": self._obs(self.perp_price if perp_price_known else None, "perp_price_stale"),
            "funding_rate": self._obs(self.funding_rate if self.feed_status("bybit_linear", observed_ms, self.last_perp_ms) == "KNOWN" else None, "funding_stale"),
            "open_interest": self._obs(self.open_interest if self.last_perp_oi_ms and observed_ms - self.last_perp_oi_ms <= 180_000 else None, "oi_stale"),
            "spot_price": self._obs(spot_price, "spot_unavailable_or_stale"),
            "spot_perp_divergence_bps": self._divergence(self.perp_price if perp_price_known else None, spot_price),
            "spot_bid_ask_imbalance_l10": self._book_imbalance(spot_book, observed_ms) if spot_book else self._unknown("spot_book_missing"),
            "perp_feed": self.feed_status("bybit_linear", observed_ms, self.last_perp_ms),
            "spot_feed": self.feed_status("bybit_spot" if provider == "bybit" else "binance_spot", observed_ms, self.last_bybit_spot_ms if provider == "bybit" else self.last_binance_spot_ms) if provider else "UNKNOWN",
            "spot_provider": provider or "UNKNOWN",
            "market_profile": profile,
        }
        return FeatureSnapshot(self.symbol, observed_ms, profile, features)

    @staticmethod
    def _unknown(reason: str) -> dict[str, Any]:
        return {"status": "UNKNOWN", "value": None, "reason": reason}

    @classmethod
    def _obs(cls, value: float | None, reason: str) -> dict[str, Any]:
        return {"status": "KNOWN", "value": value} if value is not None else cls._unknown(reason)

    @classmethod
    def _book_imbalance(cls, book: BookState | None, now: int) -> dict[str, Any]:
        if book is None or not book.is_fresh(now):
            return cls._unknown("book_stale_or_invalid")
        bid, ask = book.depth_notional("bid", 10), book.depth_notional("ask", 10)
        if bid is None or ask is None or bid + ask <= 0:
            return cls._unknown("one_sided_book")
        return {"status": "KNOWN", "value": float((bid - ask) / (bid + ask))}

    @classmethod
    def _divergence(cls, perp: float | None, spot: float | None) -> dict[str, Any]:
        if perp is None or spot is None or perp <= 0 or spot <= 0:
            return cls._unknown("spot_or_perp_price_missing")
        return {"status": "KNOWN", "value": (perp / spot - 1.0) * 10_000.0}

    def current_price(self, now: int) -> tuple[float | None, str | None, str]:
        if self.feed_status("bybit_linear", now, self.last_perp_ms) == "KNOWN" and self.perp_price is not None:
            return self.perp_price, "bybit_linear", "KNOWN"
        return None, None, "UNKNOWN"


async def http_json(url: str, params: Mapping[str, Any] | None = None, *, timeout: float = 8.0) -> Mapping[str, Any] | None:
    query = f"?{urlencode(params)}" if params else ""
    request = Request(f"{url}{query}", headers={"User-Agent": "short-bot-research-collector/0.1"})
    def fetch() -> Mapping[str, Any] | None:
        try:
            with urlopen(request, timeout=timeout) as response:
                parsed = json.loads(response.read())
                return parsed if isinstance(parsed, Mapping) else None
        except Exception as exc:  # network failures are an UNKNOWN data point
            LOG.debug("HTTP source failure %s: %s", url, exc)
            return None
    return await asyncio.to_thread(fetch)


class FeedManager:
    """Owns active symbols and source clients; never emits signals."""

    def __init__(self, db: ResearchDB, bybit: BybitConfig, binance: BinanceConfig, *, reconnect_initial: float = 1.0, reconnect_max: float = 30.0) -> None:
        self.db = db
        self.bybit_config = bybit
        self.binance_config = binance
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max
        self.states: dict[str, MarketState] = {}
        self._active: set[str] = set()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []

    async def prepare_symbol(self, symbol: str) -> MarketState:
        symbol = symbol.upper()
        state = self.states.setdefault(symbol, MarketState(symbol, self.bybit_config.orderbook_depth, int(self.bybit_config.stale_after_sec * 1000)))
        bybit_result = await http_json(f"{self.bybit_config.rest_url}/v5/market/tickers", {"category": "spot", "symbol": symbol})
        bybit_rows = ((bybit_result or {}).get("result") or {}).get("list") or []
        state.bybit_spot_available = bool((bybit_result or {}).get("retCode") == 0 and bybit_rows)
        binance_result = await http_json(f"{self.binance_config.rest_url}/api/v3/exchangeInfo", {"symbol": symbol})
        binance_rows = (binance_result or {}).get("symbols") or []
        state.binance_spot_available = any(
            isinstance(row, Mapping) and row.get("status") == "TRADING" and row.get("isSpotTradingAllowed", True)
            for row in binance_rows
        )
        if state.binance_spot_available:
            await self._refresh_binance_snapshot(state)
        return state

    async def _fetch_binance_snapshot(self, state: MarketState) -> BookUpdate | None:
        response = await http_json(f"{self.binance_config.rest_url}/api/v3/depth", {"symbol": state.symbol, "limit": self.binance_config.depth_levels})
        if not response or "lastUpdateId" not in response:
            return None
        try:
            return BookUpdate("binance", "spot", state.symbol, "snapshot", int(response["lastUpdateId"]), None, now_ms(), _levels(response.get("bids")), _levels(response.get("asks")))
        except (TypeError, ValueError):
            return None

    async def _refresh_binance_snapshot(self, state: MarketState) -> None:
        update = await self._fetch_binance_snapshot(state)
        if update is None:
            state.binance_spot_book.reset("spot-snapshot-unknown")
            return
        state.binance_spot_book.apply(update, now_ms())

    def activate(self, symbol: str) -> None:
        self._active.add(symbol.upper())

    def deactivate(self, symbol: str) -> None:
        self._active.discard(symbol.upper())

    def active_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._active))

    def state(self, symbol: str) -> MarketState | None:
        return self.states.get(symbol.upper())

    def bybit_symbols(self, category: str) -> tuple[str, ...]:
        if category == "linear":
            return self.active_symbols()
        return tuple(symbol for symbol in self.active_symbols() if self.states.get(symbol) and self.states[symbol].bybit_spot_available is True)

    def binance_symbols(self) -> tuple[str, ...]:
        return tuple(symbol for symbol in self.active_symbols() if self.states.get(symbol) and self.states[symbol].binance_spot_available is True)

    async def start(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets package is required; install requirements.txt")
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(BybitClient(self, "linear").run(), name="bybit-linear"),
            asyncio.create_task(BybitClient(self, "spot").run(), name="bybit-spot"),
            asyncio.create_task(BinanceSpotClient(self).run(), name="binance-spot"),
            asyncio.create_task(self._bybit_rest_loop(), name="bybit-rest"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _bybit_rest_loop(self) -> None:
        while not self._stop.is_set():
            stamp = now_ms()
            for symbol in self.active_symbols():
                state = self.states.get(symbol)
                if not state:
                    continue
                response = await http_json(f"{self.bybit_config.rest_url}/v5/market/open-interest", {"category": "linear", "symbol": symbol, "intervalTime": "15min", "limit": 1})
                rows = ((response or {}).get("result") or {}).get("list") or []
                if rows and isinstance(rows[0], Mapping):
                    value = _float(rows[0].get("openInterest"))
                    if value is not None:
                        state.open_interest = value
                        state.last_perp_oi_ms = stamp
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.bybit_config.rest_poll_sec)
            except asyncio.TimeoutError:
                pass


class BybitClient:
    def __init__(self, manager: FeedManager, category: str) -> None:
        self.manager = manager
        self.category = category
        self.provider = "bybit"
        self.url = manager.bybit_config.linear_ws_url if category == "linear" else manager.bybit_config.spot_ws_url

    def topics(self, symbols: Iterable[str]) -> list[str]:
        result: list[str] = []
        for symbol in symbols:
            result.extend((f"publicTrade.{symbol}", f"orderbook.{self.manager.bybit_config.orderbook_depth}.{symbol}", f"tickers.{symbol}"))
            if self.category == "linear":
                result.append(f"allLiquidation.{symbol}")
        return result

    async def run(self) -> None:
        delay = self.manager.reconnect_initial
        while not self.manager._stop.is_set():
            if not self.manager.bybit_symbols(self.category):
                try:
                    await asyncio.wait_for(self.manager._stop.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=4 * 1024 * 1024) as socket:  # type: ignore[union-attr]
                    delay = self.manager.reconnect_initial
                    subscribed: set[str] = set()
                    for symbol in self.manager.bybit_symbols(self.category):
                        self.manager.states[symbol].connection_state[f"bybit_{self.category}"] = "CONNECTED"
                    while not self.manager._stop.is_set():
                        desired = set(self.topics(self.manager.bybit_symbols(self.category)))
                        if desired != subscribed:
                            if subscribed:
                                await socket.send(json.dumps({"op": "unsubscribe", "args": sorted(subscribed - desired)}))
                            if desired - subscribed:
                                await socket.send(json.dumps({"op": "subscribe", "args": sorted(desired - subscribed)}))
                            subscribed = desired
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            await socket.send(json.dumps({"op": "ping"}))
                            continue
                        message = _json_object(raw)
                        if message:
                            self.handle(message, now_ms())
                            # A Bybit book gap cannot be repaired from deltas;
                            # reconnect so the server sends a fresh snapshot.
                            # Check only after an orderbook frame: a ticker or
                            # trade can arrive before the first snapshot after
                            # reconnect while the previous book is INVALID.
                            if str(message.get("topic") or "").startswith("orderbook."):
                                current_symbols = self.manager.bybit_symbols(self.category)
                                invalid = any(
                                    (
                                        self.manager.states[symbol].perp_book
                                        if self.category == "linear"
                                        else self.manager.states[symbol].bybit_spot_book
                                    ).state == "INVALID"
                                    for symbol in current_symbols
                                    if symbol in self.manager.states
                                )
                                if invalid:
                                    await socket.close()
                                    break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("Bybit %s connection: %s", self.category, exc)
                for symbol in self.manager.bybit_symbols(self.category):
                    state = self.manager.states.get(symbol)
                    if state:
                        state.connection_state[f"bybit_{self.category}"] = "DISCONNECTED"
                        if self.category == "linear":
                            state.perp_book.reset("transport-disconnected")
                        else:
                            state.bybit_spot_book.reset("transport-disconnected")
                await asyncio.sleep(delay)
                delay = min(self.manager.reconnect_max, delay * 2)

    def handle(self, message: Mapping[str, Any], received_ms: int) -> None:
        topic = str(message.get("topic") or "")
        if topic.startswith("publicTrade."):
            for trade in parse_bybit_trade(message, "bybit", self.category, received_ms):
                state = self.manager.states.get(trade.symbol)
                if not state:
                    continue
                window = state.perp_trades if self.category == "linear" else state.bybit_spot_trades
                if window.add(trade):
                    try:
                        self.manager.db.raw_trade((trade.provider, trade.market, trade.symbol, trade.trade_id, trade.side, float(trade.price), float(trade.quantity), trade.exchange_ts_ms, trade.received_ts_ms))
                    except Exception as exc:
                        LOG.debug("raw trade write failed: %s", exc)
                if self.category == "linear":
                    state.last_perp_ms = received_ms
                else:
                    state.last_bybit_spot_ms = received_ms
            return
        if topic.startswith("orderbook."):
            update = parse_bybit_book(message, "bybit", self.category, received_ms)
            if not update:
                return
            state = self.manager.states.get(update.symbol)
            if not state:
                return
            book = state.perp_book if self.category == "linear" else state.bybit_spot_book
            book.apply(update, received_ms)
            state.connection_state[f"bybit_{self.category}"] = "CONNECTED"
            if self.category == "linear":
                state.last_perp_ms = received_ms
            else:
                state.last_bybit_spot_ms = received_ms
            try:
                self.manager.db.raw_orderbook((update.provider, update.market, update.symbol, update.update_type, update.update_id, update.cross_seq, update.exchange_ts_ms, received_ms, json.dumps([[str(p), str(q)] for p, q in update.bids], separators=(",", ":")), json.dumps([[str(p), str(q)] for p, q in update.asks], separators=(",", ":"))))
            except Exception as exc:
                LOG.debug("raw orderbook write failed: %s", exc)
            return
        if topic.startswith("tickers."):
            data = message.get("data")
            if isinstance(data, list):
                data = data[0] if data else None
            if not isinstance(data, Mapping):
                return
            symbol = str(data.get("symbol") or topic.rsplit(".", 1)[-1]).upper()
            state = self.manager.states.get(symbol)
            if not state:
                return
            price = _float(data.get("lastPrice") or data.get("markPrice"))
            if self.category == "linear":
                state.perp_price = price or state.perp_price
                state.funding_rate = _float(data.get("fundingRate")) if data.get("fundingRate") is not None else state.funding_rate
                state.last_perp_ms = received_ms
            else:
                state.bybit_spot_price = price or state.bybit_spot_price
                state.last_bybit_spot_ms = received_ms
            return
        if self.category == "linear" and topic.startswith("allLiquidation."):
            rows = message.get("data")
            if not isinstance(rows, list):
                return
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                symbol = str(row.get("s") or topic.rsplit(".", 1)[-1]).upper()
                # Bybit S is the liquidated position side. p is bankruptcy
                # price, not the execution price.
                try:
                    self.manager.db.liquidation(("bybit", symbol, str(row.get("S") or "UNKNOWN"), _float(row.get("p")), _float(row.get("v")), int(row.get("T") or received_ms), received_ms))
                except Exception as exc:
                    LOG.debug("liquidation write failed: %s", exc)


class BinanceSpotClient:
    def __init__(self, manager: FeedManager) -> None:
        self.manager = manager
        self._syncing: set[str] = set()
        self._buffers: dict[str, list[BookUpdate]] = {}

    async def run(self) -> None:
        delay = self.manager.reconnect_initial
        while not self.manager._stop.is_set():
            symbols = self.manager.binance_symbols()
            if not symbols:
                try:
                    await asyncio.wait_for(self.manager._stop.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                continue
            streams = [stream for symbol in symbols for stream in (f"{symbol.lower()}@trade", f"{symbol.lower()}@depth20@100ms")]
            url = f"{self.manager.binance_config.ws_url}?{urlencode({'streams': '/'.join(streams)})}"
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=4 * 1024 * 1024) as socket:  # type: ignore[union-attr]
                    delay = self.manager.reconnect_initial
                    self._syncing = set(symbols)
                    self._buffers = {symbol: [] for symbol in symbols}
                    snapshot_tasks = {
                        symbol: asyncio.create_task(self.manager._fetch_binance_snapshot(self.manager.states[symbol]))
                        for symbol in symbols
                        if symbol in self.manager.states
                    }
                    for symbol in symbols:
                        state = self.manager.states.get(symbol)
                        if state:
                            state.connection_state["binance_spot"] = "CONNECTED"
                    while not self.manager._stop.is_set() and set(self.manager.binance_symbols()) == set(symbols):
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            raw = None
                        message = _json_object(raw) if raw is not None else None
                        if message:
                            self.handle(message, now_ms())
                        self._finish_snapshots(snapshot_tasks)
                        if any(
                            self.manager.states[symbol].binance_spot_book.state == "INVALID"
                            for symbol in symbols
                            if symbol in self.manager.states and symbol not in self._syncing
                        ):
                            # A depth gap cannot be repaired from deltas;
                            # reconnect after a fresh REST snapshot.
                            await socket.close()
                            break
                    for task in snapshot_tasks.values():
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*snapshot_tasks.values(), return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("Binance spot connection: %s", exc)
                for symbol in symbols:
                    state = self.manager.states.get(symbol)
                    if state:
                        state.connection_state["binance_spot"] = "DISCONNECTED"
                        state.binance_spot_book.reset("transport-disconnected")
                await asyncio.sleep(delay)
                delay = min(self.manager.reconnect_max, delay * 2)

    def handle(self, message: Mapping[str, Any], received_ms: int) -> None:
        trade, book = parse_binance(message, received_ms)
        if trade:
            state = self.manager.states.get(trade.symbol)
            if state and state.binance_spot_available is True and state.binance_spot_trades.add(trade):
                state.binance_spot_price = float(trade.price)
                state.last_binance_spot_ms = received_ms
                try:
                    self.manager.db.raw_trade((trade.provider, trade.market, trade.symbol, trade.trade_id, trade.side, float(trade.price), float(trade.quantity), trade.exchange_ts_ms, trade.received_ts_ms))
                except Exception as exc:
                    LOG.debug("raw Binance trade write failed: %s", exc)
        if book:
            state = self.manager.states.get(book.symbol)
            if not state or state.binance_spot_available is not True:
                return
            if book.symbol in self._syncing:
                self._buffers.setdefault(book.symbol, []).append(book)
                try:
                    self.manager.db.raw_orderbook((book.provider, book.market, book.symbol, book.update_type, book.update_id, book.cross_seq, book.exchange_ts_ms, received_ms, json.dumps([[str(p), str(q)] for p, q in book.bids], separators=(",", ":")), json.dumps([[str(p), str(q)] for p, q in book.asks], separators=(",", ":"))))
                except Exception as exc:
                    LOG.debug("raw buffered Binance book write failed: %s", exc)
                return
            applied = state.binance_spot_book.apply(book, received_ms)
            if applied:
                state.last_binance_spot_ms = received_ms
                try:
                    self.manager.db.raw_orderbook((book.provider, book.market, book.symbol, book.update_type, book.update_id, book.cross_seq, book.exchange_ts_ms, received_ms, json.dumps([[str(p), str(q)] for p, q in book.bids], separators=(",", ":")), json.dumps([[str(p), str(q)] for p, q in book.asks], separators=(",", ":"))))
                except Exception as exc:
                    LOG.debug("raw Binance book write failed: %s", exc)

    def _finish_snapshots(self, tasks: dict[str, asyncio.Task[Any]]) -> None:
        for symbol, task in tuple(tasks.items()):
            if not task.done() or symbol not in self._syncing:
                continue
            state = self.manager.states.get(symbol)
            if not state:
                self._syncing.discard(symbol)
                continue
            try:
                snapshot = task.result()
            except Exception:
                snapshot = None
            if snapshot is None:
                state.binance_spot_book.reset("spot-snapshot-unknown")
                self._syncing.discard(symbol)
                continue
            stamp = now_ms()
            state.binance_spot_book.apply(snapshot, stamp)
            valid = True
            buffered = sorted(self._buffers.get(symbol, []), key=lambda item: item.update_id)
            for item in buffered:
                if item.update_id <= snapshot.update_id:
                    continue
                first = state.binance_spot_book.state != "CONTIGUOUS"
                if first:
                    if item.first_update_id is None or not (item.first_update_id <= snapshot.update_id + 1 <= item.update_id):
                        valid = False
                        break
                    item = replace(item, previous_update_id=None)
                if not state.binance_spot_book.apply(item, stamp):
                    valid = False
                    break
                state.last_binance_spot_ms = stamp
            if not valid:
                state.binance_spot_book.reset("spot-snapshot-stream-gap")
            self._buffers.pop(symbol, None)
            self._syncing.discard(symbol)

