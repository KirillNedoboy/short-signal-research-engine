"""Normalized historical observations used by the source-faithful replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd


class CandleObservationKind(StrEnum):
    CLOSED_CANDLE = "CLOSED_CANDLE"
    PARTIAL_ASOF_CANDLE = "PARTIAL_ASOF_CANDLE"


@dataclass(frozen=True, slots=True)
class NormalizationReport:
    rows_in: int
    rows_out: int
    duplicates: int = 0
    gaps: tuple[str, ...] = ()
    reverse_pages: int = 0


def normalize_bybit_klines(
    pages: Iterable[Sequence[Sequence[Any]]],
    *,
    symbol: str,
    exchange: str = "BYBIT",
    market_type: str = "LINEAR",
    settle_coin: str = "USDT",
    interval: str = "1m",
) -> tuple[pd.DataFrame, NormalizationReport]:
    """Normalize reverse-ordered Bybit V5 kline pages deterministically."""
    page_list = [list(page) for page in pages]
    flattened = [list(row) for page in page_list for row in page]
    keyed: dict[int, list[Any]] = {}
    duplicates = 0
    for row in flattened:
        if len(row) < 7:
            raise ValueError("Bybit kline row must contain seven fields")
        start_ms = int(float(row[0]))
        if start_ms in keyed:
            duplicates += 1
            continue
        keyed[start_ms] = row
    ordered = [keyed[key] for key in sorted(keyed)]
    records: list[dict[str, Any]] = []
    for row in ordered:
        start = datetime.fromtimestamp(int(float(row[0])) / 1000, tz=timezone.utc)
        close_time = start + timedelta(minutes=1)
        records.append({
            "exchange": exchange, "market_type": market_type, "settle_coin": settle_coin,
            "symbol": symbol, "interval": interval,
            "open_time_utc": _iso(start), "close_time_utc": _iso(close_time),
            "availability_time_utc": _iso(close_time), "available_at_utc": _iso(close_time),
            "open": float(row[1]), "high": float(row[2]), "low": float(row[3]),
            "close": float(row[4]), "volume": float(row[5]), "turnover": float(row[6]),
            "source": "bybit:v5/market/kline", "stable_row_id": f"{symbol}:{int(float(row[0]))}",
        })
    gaps: list[str] = []
    for left, right in zip(ordered, ordered[1:]):
        left_ms, right_ms = int(float(left[0])), int(float(right[0]))
        if right_ms - left_ms != 60_000:
            gaps.append(f"{left_ms}:{right_ms}")
    reverse_pages = sum(
        bool(page) and len(page) > 1 and int(float(page[0][0])) > int(float(page[-1][0]))
        for page in page_list
    )
    return pd.DataFrame(records), NormalizationReport(
        len(flattened), len(records), duplicates, tuple(gaps), reverse_pages,
    )


def build_sliding_frame(frame: pd.DataFrame, symbol: str, decision_time_utc: datetime, *, limit: int = 300) -> pd.DataFrame:
    """Return at most ``limit`` rows whose availability is as-of safe."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if frame.empty:
        return frame.copy()
    decision = pd.Timestamp(_utc(decision_time_utc))
    working = frame.copy()
    if "symbol" in working.columns:
        working = working[working["symbol"].astype(str) == symbol]
    working = working.loc[_availability(working) <= decision].copy()
    if "open_time_utc" in working.columns:
        working = working.sort_values("open_time_utc")
    elif "timestamp" in working.columns:
        working = working.sort_values("timestamp")
    return working.tail(limit).reset_index(drop=True)


def aggregate_trades_asof(trades: pd.DataFrame, decision_time_utc: datetime, *, symbol: str | None = None) -> dict[str, Any]:
    """Aggregate only trades at or before the injected decision time."""
    if trades.empty:
        raise ValueError("cannot aggregate an empty trade set")
    decision = pd.Timestamp(_utc(decision_time_utc))
    timestamp_column = "timestamp" if "timestamp" in trades.columns else "time"
    timestamps = pd.to_datetime(trades[timestamp_column], utc=True, errors="raise")
    mask = timestamps <= decision
    selected = trades.loc[mask].copy()
    selected_timestamps = timestamps.loc[mask]
    if selected.empty:
        raise ValueError("no trades available at decision time")
    prices = pd.to_numeric(selected["price"], errors="raise")
    quantity_column = "size" if "size" in selected.columns else "qty"
    sizes = pd.to_numeric(selected[quantity_column], errors="raise")
    bucket = selected_timestamps.min().floor("min")
    bucket_mask = selected_timestamps.dt.floor("min") == bucket
    prices, sizes, bucket_ts = prices.loc[bucket_mask], sizes.loc[bucket_mask], selected_timestamps.loc[bucket_mask]
    if prices.empty:
        raise ValueError("no trades in selected minute")
    available = bucket_ts.max().to_pydatetime().astimezone(timezone.utc)
    resolved_symbol = symbol or (str(selected["symbol"].iloc[0]) if "symbol" in selected.columns else "")
    row = {
        "symbol": resolved_symbol, "open_time_utc": _iso(bucket.to_pydatetime()),
        "close_time_utc": _iso((bucket + timedelta(minutes=1)).to_pydatetime()),
        "availability_time_utc": _iso(available), "available_at_utc": _iso(available),
        "open": float(prices.iloc[0]), "high": float(prices.max()), "low": float(prices.min()),
        "close": float(prices.iloc[-1]), "volume": float(sizes.sum()),
        "turnover": float((prices * sizes).sum()), "source": "bybit:public-trades-archive",
    }
    row["stable_row_id"] = f"{resolved_symbol}:{row['open_time_utc']}:partial:{row['availability_time_utc']}"
    return row


def normalize_derivative_points(
    rows: Iterable[Mapping[str, Any]],
    *,
    decision_time_utc: datetime,
    source: str,
) -> tuple[list[dict[str, Any]], NormalizationReport]:
    """Normalize historical OI/funding points without looking past decision time."""
    decision = _utc(decision_time_utc)
    materialized = list(rows)
    keyed: dict[datetime, dict[str, Any]] = {}
    duplicates = 0
    for row in materialized:
        raw_time = row.get("timestamp", row.get("timestamp_utc", row.get("fundingRateTimestamp")))
        if raw_time is None:
            raise ValueError("derivative point requires timestamp")
        stamp = _utc(raw_time)
        if stamp > decision:
            continue
        if stamp in keyed:
            duplicates += 1
            continue
        raw_value = row.get("value", row.get("openInterest", row.get("fundingRate")))
        if raw_value is None:
            raise ValueError("derivative point requires value")
        keyed[stamp] = {
            "timestamp_utc": _iso(stamp), "availability_time_utc": _iso(stamp),
            "value": float(raw_value), "source": source,
        }
    points = [keyed[key] for key in sorted(keyed)]
    return points, NormalizationReport(len(materialized), len(points), duplicates)


def liquidity_observability(evidence: Mapping[str, Any] | None) -> tuple[Mapping[str, Any] | None, tuple[str, ...]]:
    """Mark absent historical orderbook evidence as unknown, never as false."""
    if evidence is None:
        return None, ("FULL_ADMISSION_HISTORICALLY_UNOBSERVABLE",)
    return evidence, tuple(str(flag) for flag in evidence.get("observability_flags", ()))


@dataclass(frozen=True, slots=True)
class NormalizedMarketObservation:
    """A market frame whose rows are explicitly available at decision time."""

    symbol: str
    decision_time_utc: datetime
    kind: CandleObservationKind
    candles: pd.DataFrame

    def validate(self) -> None:
        decision = _utc(self.decision_time_utc)
        if not self.symbol or self.candles.empty:
            raise ValueError("market observation requires symbol and candles")
        if "symbol" in self.candles and (self.candles["symbol"].astype(str) != self.symbol).any():
            raise ValueError("market observation contains another symbol")
        availability = _availability(self.candles, partial_decision=self.decision_time_utc if self.kind is CandleObservationKind.PARTIAL_ASOF_CANDLE else None)
        if availability.isna().any():
            raise ValueError("market observation contains an invalid availability timestamp")
        if (availability > pd.Timestamp(decision)).any():
            raise ValueError("future market input is not available at decision time")

    @property
    def max_input_availability_time_utc(self) -> datetime:
        self.validate()
        availability = _availability(self.candles, partial_decision=self.decision_time_utc if self.kind is CandleObservationKind.PARTIAL_ASOF_CANDLE else None)
        return availability.max().to_pydatetime().astimezone(timezone.utc)


def _availability(frame: pd.DataFrame, partial_decision: datetime | None = None) -> pd.Series:
    for name in ("availability_time_utc", "available_at_utc", "availability_time", "available_at"):
        if name in frame:
            return pd.to_datetime(frame[name], utc=True, errors="coerce")
    if "timestamp" in frame:
        result = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce") + timedelta(minutes=1)
        if partial_decision is not None and not result.empty:
            result = result.copy()
            result.iloc[-1] = pd.Timestamp(_utc(partial_decision))
        return result
    if isinstance(frame.index, pd.DatetimeIndex):
        result = pd.Series(frame.index, index=frame.index, dtype="datetime64[ns, UTC]") + timedelta(minutes=1)
        if partial_decision is not None and not result.empty:
            result = result.copy()
            result.iloc[-1] = pd.Timestamp(_utc(partial_decision))
        return result
    return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")


def _utc(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")
