"""Outcome calculation for saved signals."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from app.domain import SignalOutcome, SignalRecord


class OutcomeEvaluator:
    """Compute short-side outcomes from post-signal candles."""

    def evaluate(
        self,
        signal: SignalRecord,
        frame_1m: pd.DataFrame,
        now: datetime | None = None,
    ) -> SignalOutcome | None:
        """Evaluate the signal against available future price action."""

        now = now or datetime.now(timezone.utc)
        if frame_1m.empty:
            return None

        future = frame_1m.loc[frame_1m["timestamp"] >= signal.signal_time]
        if future.empty:
            return None

        entry = signal.market_price
        outcome = SignalOutcome(signal_id=signal.id, updated_at=now)
        outcome.price_after_15m = _price_after(future, signal.signal_time + timedelta(minutes=15))
        outcome.price_after_1h = _price_after(future, signal.signal_time + timedelta(hours=1))
        outcome.price_after_4h = _price_after(future, signal.signal_time + timedelta(hours=4))
        outcome.mfe_pct = float((((entry - future["low"]) / entry) * 100).max())
        outcome.mae_pct = float((((future["high"] - entry) / entry) * 100).max())
        outcome.squeeze_extension_pct = outcome.mae_pct

        signal_vwap = signal.context_json.get("signal_vwap") or signal.context_json.get("vwap")
        if signal_vwap is not None:
            vwap_hits = future.loc[future["low"] <= float(signal_vwap)]
            outcome.reached_vwap = not vwap_hits.empty
            if not vwap_hits.empty:
                first_hit = vwap_hits.iloc[0]
                delta = _to_datetime(first_hit["timestamp"]) - signal.signal_time
                outcome.time_to_vwap_minutes = int(delta.total_seconds() // 60)

        tp_price = float(signal_vwap) if signal_vwap is not None else entry * 0.97
        sl_price = signal.short_zone_high if signal.short_zone_high else entry * 1.03
        outcome.tp1_hit = bool((future["low"] <= tp_price).any())
        outcome.stopped_virtual = bool((future["high"] >= sl_price).any()) if sl_price is not None else None
        outcome.risk_adjusted_status = _classify_risk_adjusted(outcome)
        outcome.is_clean_short = outcome.risk_adjusted_status == "CLEAN_TP"
        outcome.is_squeeze_before_tp = outcome.risk_adjusted_status == "SQUEEZE_BEFORE_TP"
        return outcome


def evaluate_hypothetical_short(
    *,
    entry_time: datetime,
    entry_price: float,
    frame_1m: pd.DataFrame,
) -> dict[str, object]:
    """Evaluate shadow-only short outcomes from a hypothetical entry."""
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    if frame_1m.empty:
        return {"horizons": {}, "mfe_pct": None, "mae_pct": None, "new_high_after_entry": False}

    future = frame_1m.loc[frame_1m["timestamp"] >= entry_time].copy()
    if future.empty:
        return {"horizons": {}, "mfe_pct": None, "mae_pct": None, "new_high_after_entry": False}

    horizons: dict[str, dict[str, float | None]] = {}
    for label, minutes in (("1m", 1), ("3m", 3), ("5m", 5), ("15m", 15)):
        target = entry_time + timedelta(minutes=minutes)
        row = future.loc[future["timestamp"] >= target]
        horizons[label] = {
            "price": float(row.iloc[0]["close"]) if not row.empty else None,
            "short_return_pct": (
                float((entry_price - row.iloc[0]["close"]) / entry_price * 100)
                if not row.empty
                else None
            ),
        }

    mfe_pct = float(((entry_price - future["low"]) / entry_price * 100).max())
    mae_pct = float(((future["high"] - entry_price) / entry_price * 100).max())
    return {
        "horizons": horizons,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "new_high_after_entry": bool((future["high"] > entry_price).any()),
    }


def _classify_risk_adjusted(outcome: SignalOutcome) -> str:
    if outcome.mfe_pct is None or outcome.mae_pct is None or outcome.squeeze_extension_pct is None:
        return "INVALID_OR_MISSING"
    if not outcome.tp1_hit and not outcome.stopped_virtual and outcome.mfe_pct < 0.5:
        return "NOT_ENTERED"
    if outcome.stopped_virtual and not outcome.tp1_hit:
        return "SL_OR_BAD"
    if not outcome.tp1_hit:
        return "SL_OR_BAD"
    if outcome.mae_pct <= 3 and outcome.squeeze_extension_pct <= 3:
        return "CLEAN_TP"
    if outcome.mae_pct > 10 or outcome.squeeze_extension_pct > 10:
        return "SQUEEZE_BEFORE_TP"
    if 3 < outcome.mae_pct <= 10:
        return "DIRTY_TP_HIGH_MAE"
    return "INVALID_OR_MISSING"


def _price_after(frame: pd.DataFrame, target_time: datetime) -> float | None:
    after = frame.loc[frame["timestamp"] >= target_time]
    if after.empty:
        return None
    return float(after.iloc[0]["close"])


def _to_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return pd.Timestamp(value, tz="UTC").to_pydatetime()
