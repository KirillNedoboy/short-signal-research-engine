"""Broad scan orchestration."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pandas as pd

from app.config import AppConfig
from app.domain import MarketSnapshot
from app.infra.cache import TTLCache
from app.market.bybit_client import BybitClient
from app.market.candles import klines_to_frame
from app.market.coverage import ScanUniverseTelemetry
from app.market.shortlist import build_shortlist, filter_universe

_DERIVATIVES_SUCCESS_TTL_SEC = 180
_DERIVATIVES_FAILURE_TTL_SEC = 60


class MarketScanner:
    """Universe scan and selective candle fetch orchestration."""

    def __init__(self, client: BybitClient, config: AppConfig) -> None:
        self._client = client
        self._config = config
        self._logger = logging.getLogger(self.__class__.__name__)
        self._snapshot_cache: TTLCache[MarketSnapshot] = TTLCache()
        self._derivatives_cache: TTLCache[dict[str, Any]] = TTLCache()
        self._last_universe_telemetry: ScanUniverseTelemetry | None = None

    @property
    def client(self) -> BybitClient:
        """Expose the underlying market client for shared services."""

        return self._client

    @property
    def last_universe_telemetry(self) -> ScanUniverseTelemetry | None:
        return self._last_universe_telemetry

    async def fetch_market_snapshots(self) -> list[MarketSnapshot]:
        """Fetch and normalize liquid ticker snapshots."""

        raw_tickers, raw_instruments = await asyncio.gather(
            self._client.fetch_tickers(),
            self._client.fetch_instruments(),
        )
        instrument_map = {
            instrument["symbol"]: instrument
            for instrument in raw_instruments
            if instrument.get("quoteCoin") == "USDT" and instrument.get("status") == "Trading"
        }
        exchange_symbols = sorted({str(instrument.get("symbol")) for instrument in raw_instruments if instrument.get("symbol") and instrument.get("quoteCoin") == "USDT" and instrument.get("status") == "Trading"})
        market_time = self._client.extract_market_time()

        snapshots: list[MarketSnapshot] = []
        for ticker in raw_tickers:
            symbol = ticker.get("symbol")
            if symbol not in instrument_map:
                continue
            snapshots.append(
                MarketSnapshot(
                    symbol=symbol,
                    last_price=float(ticker.get("lastPrice") or 0.0),
                    price_24h_pct=float(ticker.get("price24hPcnt") or 0.0) * 100,
                    turnover_24h=float(ticker.get("turnover24h") or 0.0),
                    volume_24h=float(ticker.get("volume24h") or 0.0),
                    mark_price=float(ticker.get("markPrice") or 0.0),
                    open_interest=float(ticker.get("openInterest") or 0.0),
                    timestamp=market_time,
                )
            )

        filtered = filter_universe(snapshots, self._config)
        eligible_symbols = [snapshot.symbol for snapshot in filtered]
        snapshot_by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
        explicit = {symbol.upper() for symbol in self._config.exclude_symbols}
        if self._config.exclude_btc_eth:
            explicit.update({"BTCUSDT", "ETHUSDT"})
        excluded: list[tuple[str, str]] = []
        for symbol in exchange_symbols:
            snapshot = snapshot_by_symbol.get(symbol)
            if snapshot is None:
                reason = "MARKET_DATA_INCOMPLETE"
            elif symbol.upper() in explicit:
                reason = "UNIVERSE_FILTER"
            elif snapshot.turnover_24h < self._config.min_24h_volume:
                reason = "LIQUIDITY_FILTER"
            else:
                continue
            excluded.append((symbol, reason))
        self._last_universe_telemetry = ScanUniverseTelemetry(
            exchange_symbols=tuple(exchange_symbols),
            eligible_symbols=tuple(sorted(set(eligible_symbols))),
            excluded=tuple(excluded),
            observed_at=market_time,
        )
        return filtered

    def shortlist(self, snapshots: list[MarketSnapshot]) -> list[MarketSnapshot]:
        """Rank candidates using the local snapshot cache."""

        previous = self._snapshot_cache.items()
        shortlist = build_shortlist(snapshots, previous, self._config.shortlist_size)
        for snapshot in snapshots:
            self._snapshot_cache.set(snapshot.symbol, snapshot, ttl_seconds=self._config.scan_interval_sec * 10)
        return shortlist

    async def fetch_symbol_frames(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """Fetch recent 1m candles for a set of symbols."""

        tasks = [
            self._client.fetch_klines(symbol, "1", limit=self._config.deep_scan_kline_limit)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        frames: dict[str, pd.DataFrame] = {}
        for symbol, result in zip(symbols, results, strict=True):
            if isinstance(result, Exception):
                continue
            frame = klines_to_frame(result)
            if not frame.empty:
                frames[symbol] = frame
        return frames

    async def fetch_optional_derivatives(self, symbol: str) -> dict[str, Any]:
        """Fetch optional derivatives inputs when enabled."""

        if not self._config.derivatives_enabled:
            return {}

        cached = self._derivatives_cache.get(symbol)
        if cached is not None:
            return cached

        open_interest_result, funding_result = await asyncio.gather(
            self._client.fetch_open_interest(symbol),
            self._client.fetch_funding(symbol),
            return_exceptions=True,
        )

        result = _normalize_derivatives_result(symbol, open_interest_result, funding_result)
        ttl = _DERIVATIVES_SUCCESS_TTL_SEC if result["derivatives_status"] == "OK" else _DERIVATIVES_FAILURE_TTL_SEC
        self._derivatives_cache.set(symbol, result, ttl_seconds=ttl)
        if result["derivatives_status"] != "OK":
            self._logger.warning(
                "Derivatives context unavailable for %s: status=%s reasons=%s",
                symbol,
                result["derivatives_status"],
                ",".join(result["derivatives_reasons"]),
            )
        return result

    async def fetch_optional_liquidity(self, symbol: str, price: float) -> dict[str, Any]:
        """Fetch and normalize orderbook liquidity context when available."""

        try:
            orderbook = await self._client.fetch_orderbook(symbol)
        except Exception as exc:
            self._logger.warning("Liquidity confirmation unavailable for %s: %s", symbol, exc)
            return {}
        return _orderbook_liquidity(orderbook, price)


def _normalize_derivatives_result(
    symbol: str,
    open_interest_result: list[dict[str, Any]] | Exception,
    funding_result: list[dict[str, Any]] | Exception,
) -> dict[str, Any]:
    reasons: list[str] = []
    data_quality_warnings: list[str] = []
    status = "OK"

    open_interest: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []

    if isinstance(open_interest_result, Exception):
        status = _error_status(open_interest_result)
        reasons.extend(_error_reasons(open_interest_result))
    else:
        open_interest = open_interest_result

    if isinstance(funding_result, Exception):
        funding_status = _error_status(funding_result)
        funding_reasons = _error_reasons(funding_result)
        if status == "OK":
            status = funding_status
        elif status != funding_status:
            status = "API_ERROR" if "API_ERROR" in {status, funding_status} else status
        reasons.extend(funding_reasons)
    else:
        funding = funding_result

    if not open_interest:
        data_quality_warnings.append("oi_missing")
    if not funding:
        data_quality_warnings.append("derivatives_missing")

    if status == "OK" and (not open_interest or not funding):
        status = "MISSING"
        if not open_interest:
            reasons.append("open_interest_missing")
        if not funding:
            reasons.append("funding_missing")

    normalized = {
        "open_interest": open_interest,
        "funding": funding,
        "derivatives_status": status,
        "derivatives_reasons": _dedupe(reasons),
        "data_quality_warnings": _dedupe(data_quality_warnings),
        "symbol": symbol,
    }
    return normalized


def _error_status(exc: Exception) -> str:
    message = str(exc).lower()
    if "10006" in message or "too many visits" in message or "rate limit" in message:
        return "RATE_LIMITED"
    if "symbol" in message and ("not found" in message or "invalid" in message or "unsupported" in message):
        return "UNSUPPORTED_SYMBOL"
    return "API_ERROR"


def _error_reasons(exc: Exception) -> list[str]:
    status = _error_status(exc)
    if status == "RATE_LIMITED":
        return ["bybit_rate_limit"]
    if status == "UNSUPPORTED_SYMBOL":
        return ["unsupported_symbol"]
    return ["derivatives_api_error"]


def _orderbook_liquidity(orderbook: dict[str, Any], price: float) -> dict[str, Any]:
    bids = sorted(_levels(orderbook.get("b") or orderbook.get("bids") or []), key=lambda item: item[0], reverse=True)
    asks = sorted(_levels(orderbook.get("a") or orderbook.get("asks") or []), key=lambda item: item[0])
    if price <= 0 or not bids or not asks:
        return {}

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    midpoint = (best_bid + best_ask) / 2
    if midpoint <= 0:
        return {}

    spread_pct = ((best_ask - best_bid) / midpoint) * 100
    depth_1pct = _bid_depth_usdt(bids, price * 0.99)
    depth_2pct = _bid_depth_usdt(bids, price * 0.98)
    slippage_pct = _sell_slippage_pct(bids, notional=10_000)

    return {
        "spread_pct": spread_pct,
        "slippage_pct": slippage_pct,
        "orderbook_depth_usdt_1pct": depth_1pct,
        "orderbook_depth_usdt_2pct": depth_2pct,
    }


def _levels(raw_levels: list[Any]) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for level in raw_levels:
        if isinstance(level, dict):
            price = float(level.get("price") or level.get("p") or 0.0)
            size = float(level.get("size") or level.get("qty") or level.get("q") or 0.0)
        else:
            price = float(level[0] or 0.0)
            size = float(level[1] or 0.0)
        if price > 0 and size > 0:
            levels.append((price, size))
    return levels


def _bid_depth_usdt(bids: list[tuple[float, float]], floor_price: float) -> float:
    return sum(price * size for price, size in bids if price >= floor_price)


def _sell_slippage_pct(bids: list[tuple[float, float]], notional: float) -> float | None:
    if not bids:
        return None
    best_bid = bids[0][0]
    remaining = notional
    base_sold = 0.0
    quote_received = 0.0
    for price, size in bids:
        quote_at_level = price * size
        quote_to_take = min(remaining, quote_at_level)
        base_to_sell = quote_to_take / price
        base_sold += base_to_sell
        quote_received += quote_to_take
        remaining -= quote_to_take
        if remaining <= 0:
            break
    if remaining > 0 or base_sold <= 0:
        return None
    average_price = quote_received / base_sold
    return ((best_bid - average_price) / best_bid) * 100


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
