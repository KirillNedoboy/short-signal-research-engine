"""Helpers for turning Bybit klines into OHLCV data frames."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd


OHLCV_COLUMNS = [
    "start_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
]


def klines_to_frame(raw_klines: list[list[str]]) -> pd.DataFrame:
    """Convert Bybit kline payloads into an ascending OHLCV frame."""

    if not raw_klines:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    frame = pd.DataFrame(raw_klines, columns=OHLCV_COLUMNS)
    numeric_columns = [column for column in OHLCV_COLUMNS if column != "start_ms"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["start_ms"] = pd.to_numeric(frame["start_ms"], errors="coerce").astype("int64")
    frame["timestamp"] = pd.to_datetime(frame["start_ms"], unit="ms", utc=True)
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    frame = frame.set_index("timestamp", drop=False)
    return frame


def resample_ohlcv(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Aggregate 1m candles into higher timeframes."""

    if frame.empty:
        return frame.copy()

    aggregated = frame.resample(rule, label="right", closed="right").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "turnover": "sum",
        }
    )
    aggregated = aggregated.dropna(subset=["open", "high", "low", "close"])
    aggregated["timestamp"] = aggregated.index
    return aggregated


def normalize_utc(value: datetime | pd.Timestamp) -> datetime:
    """Return an aware UTC timestamp, treating legacy naïve values as UTC."""

    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone.utc)
    else:
        timestamp = timestamp.tz_convert(timezone.utc)
    return timestamp.to_pydatetime()


def closed_1m_rows(frame: pd.DataFrame, market_asof: datetime) -> pd.DataFrame:
    """Return 1m rows whose exchange interval has fully elapsed."""

    if frame.empty:
        return frame.copy()

    timestamps = _timestamps(frame)
    asof = pd.Timestamp(normalize_utc(market_asof))
    return frame.loc[(timestamps + timedelta(minutes=1)) <= asof].copy()


def complete_5m_ohlcv(frame: pd.DataFrame, market_asof: datetime) -> pd.DataFrame:
    """Aggregate only five fully closed consecutive 1m candles into each 5m row."""

    closed = closed_1m_rows(frame, market_asof)
    if closed.empty:
        return pd.DataFrame(columns=[*OHLCV_COLUMNS[1:], "timestamp"])

    timestamps = _timestamps(closed)
    working = closed.copy()
    working["_timestamp"] = timestamps
    working["_bucket"] = timestamps.dt.floor("5min")
    rows: list[dict[str, object]] = []
    for bucket, group in working.groupby("_bucket", sort=True):
        group = group.sort_values("_timestamp")
        expected = pd.date_range(bucket, periods=5, freq="1min", tz="UTC")
        if len(group) != 5 or not group["_timestamp"].reset_index(drop=True).equals(pd.Series(expected)):
            continue
        rows.append(
            {
                "open": float(group["open"].iloc[0]),
                "high": float(group["high"].max()),
                "low": float(group["low"].min()),
                "close": float(group["close"].iloc[-1]),
                "volume": float(group["volume"].sum()),
                "turnover": float(group["turnover"].sum()),
                "timestamp": bucket + timedelta(minutes=5),
            }
        )
    if not rows:
        return pd.DataFrame(columns=[*OHLCV_COLUMNS[1:], "timestamp"])
    aggregated = pd.DataFrame(rows)
    return aggregated.set_index("timestamp", drop=False)


def _timestamps(frame: pd.DataFrame) -> pd.Series:
    source = frame["timestamp"] if "timestamp" in frame else frame.index
    return pd.Series(pd.to_datetime(source, utc=True, errors="coerce"), index=frame.index)
