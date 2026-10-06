"""Outcome calculation for append-only strategy observations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pandas as pd

from app.market.candles import closed_1m_rows


HORIZONS_MINUTES = (("1m", 1), ("3m", 3), ("5m", 5), ("15m", 15))


def evaluate_strategy_observation(
    *,
    observed_at: datetime,
    entry_price: float,
    event_high: float | None,
    frame_1m: pd.DataFrame,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compute post-observation outcomes from fully closed 1m candles.

    The observation boundary is strict: the candle at ``observed_at`` is not
    evidence for its own outcome. Missing future candles remain explicit in the
    returned horizon map instead of being treated as zero movement.
    """

    if entry_price <= 0:
        raise ValueError("entry_price must be positive")

    observed = _as_utc(observed_at)
    asof = _as_utc(now) if now is not None else None
    if frame_1m.empty:
        if asof is None or asof < observation_coverage_ready_at(observed):
            return _missing_future_outcome(observed, asof or observed)
        return empty_strategy_observation_outcome(
            data_status="unknown",
            data_reason="NO_KLINES",
            retryable=True,
        )

    market_asof = asof or _frame_end(frame_1m)
    closed = closed_1m_rows(frame_1m, market_asof)
    if closed.empty:
        return _missing_future_outcome(observed, market_asof)

    timestamps = _timestamps(closed)
    future = closed.loc[(timestamps > observed) & (timestamps <= market_asof)].copy()
    if future.empty:
        return _missing_future_outcome(observed, market_asof)

    future["_timestamp"] = _timestamps(future)
    future = future.sort_values("_timestamp").reset_index(drop=True)
    horizons: dict[str, dict[str, float | None]] = {}
    for label, minutes in HORIZONS_MINUTES:
        target = observed + pd.Timedelta(minutes=minutes)
        rows = future.loc[future["_timestamp"] >= target]
        price = float(rows.iloc[0]["close"]) if not rows.empty else None
        horizons[label] = {
            "price": price,
            "price_change_pct": ((price - entry_price) / entry_price * 100) if price is not None else None,
            "short_return_pct": ((entry_price - price) / entry_price * 100) if price is not None else None,
        }

    low_row = future.loc[future["low"].idxmin()]
    high_row = future.loc[future["high"].idxmax()]
    mfe_pct = float((entry_price - float(low_row["low"])) / entry_price * 100)
    mae_pct = float((float(high_row["high"]) - entry_price) / entry_price * 100)
    new_high = bool(event_high is not None and float(future["high"].max()) > float(event_high))
    data_status = "complete" if all(item["price"] is not None for item in horizons.values()) else "incomplete"
    return {
        "data_status": data_status,
        "data_reason": None if data_status == "complete" else "AWAITING_15M_COVERAGE",
        "retryable": data_status == "incomplete",
        "horizons": horizons,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "time_to_mfe_minutes": _minutes_between(observed, low_row["_timestamp"]),
        "time_to_mae_minutes": _minutes_between(observed, high_row["_timestamp"]),
        "new_high_after_observation": new_high if event_high is not None else None,
        "observed_candles": int(len(future)),
        "coverage_end": _as_utc(future["_timestamp"].iloc[-1]).isoformat(),
    }


def empty_strategy_observation_outcome(
    *,
    data_status: str,
    data_reason: str,
    retryable: bool = False,
) -> dict[str, Any]:
    return {
        "data_status": data_status,
        "data_reason": data_reason,
        "retryable": retryable,
        "horizons": {
            label: {"price": None, "price_change_pct": None, "short_return_pct": None}
            for label, _ in HORIZONS_MINUTES
        },
        "mfe_pct": None,
        "mae_pct": None,
        "time_to_mfe_minutes": None,
        "time_to_mae_minutes": None,
        "new_high_after_observation": None,
        "observed_candles": 0,
        "coverage_end": None,
    }


def observation_coverage_ready_at(observed_at: datetime) -> datetime:
    """Return when the first candle at or after the 15m target is closed."""

    target = _as_utc(observed_at) + timedelta(minutes=15)
    minute = target.replace(second=0, microsecond=0)
    first_candle_open = minute if target == minute else minute + timedelta(minutes=1)
    return first_candle_open + timedelta(minutes=1)


def _missing_future_outcome(observed_at: datetime, market_asof: datetime) -> dict[str, Any]:
    if market_asof < observation_coverage_ready_at(observed_at):
        return empty_strategy_observation_outcome(
            data_status="incomplete",
            data_reason="AWAITING_CLOSED_FUTURE_CANDLES",
            retryable=True,
        )
    return empty_strategy_observation_outcome(
        data_status="unknown",
        data_reason="NO_CLOSED_FUTURE_CANDLES",
        retryable=True,
    )


def _timestamps(frame: pd.DataFrame) -> pd.Series:
    source = frame["timestamp"] if "timestamp" in frame else frame.index
    return pd.Series(pd.to_datetime(source, utc=True, errors="coerce"), index=frame.index)


def _as_utc(value: datetime | pd.Timestamp) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone.utc)
    else:
        timestamp = timestamp.tz_convert(timezone.utc)
    return timestamp.to_pydatetime()


def _frame_end(frame: pd.DataFrame) -> datetime:
    return _as_utc(cast(pd.Timestamp, _timestamps(frame).max()))


def _minutes_between(start: datetime, end: object) -> float:
    return round((_as_utc(cast(datetime | pd.Timestamp, end)) - start).total_seconds() / 60, 6)
