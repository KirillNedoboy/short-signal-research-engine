"""Non-invasive ROOT_DETECTOR_SHADOW_V1 calculations."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

OUTCOME_METHOD_VERSION = "ROOT_SHADOW_OUTCOME_V2"
EPISODE_MAPPING_VERSION = "ROOT_EPISODE_MAP_V1"
EPISODE_GAP = timedelta(minutes=30)


@dataclass(frozen=True, slots=True)
class ShadowCandidateInput:
    symbol: str
    first_seen_at: datetime
    rotation_id: str
    price: float
    event_high: float
    high_time: datetime
    pump_5m: float
    pump_15m: float
    pump_1h: float
    pump_4h: float
    volume_ratio: float | None
    volume_z: float | None
    oi_5m: float | None = None
    oi_15m: float | None = None
    oi_1h: float | None = None
    distance_from_high: float | None = None
    conditions: dict[str, str] | None = None


def should_create_shadow_candidate(candidate: ShadowCandidateInput) -> bool:
    """Broad observer trigger; it never participates in live detector decisions."""
    return candidate.pump_5m >= 2.0 or candidate.pump_15m >= 4.0 or candidate.pump_1h >= 7.0 or candidate.pump_4h >= 10.0


def candidate_episode_key(candidate: ShadowCandidateInput) -> str:
    bucket = int(candidate.first_seen_at.timestamp() // (15 * 60))
    raw = f"ROOT_DETECTOR_SHADOW_V1|{candidate.symbol.upper()}|{bucket}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def qualifying_trigger(candidate: ShadowCandidateInput) -> bool:
    return should_create_shadow_candidate(candidate)


def closed_5m_rows_after(*, timestamp: datetime, market_asof: datetime, frame_5m: pd.DataFrame) -> pd.DataFrame:
    """Return only closed rows after an observation, using one candle contract."""
    observed = _utc(timestamp)
    asof = _utc(market_asof)
    rows = frame_5m.copy()
    if "timestamp" not in rows.columns and rows.index.name == "timestamp":
        rows = rows.reset_index()
    if rows.empty or "timestamp" not in rows.columns:
        return rows.iloc[0:0].copy()
    rows["timestamp"] = pd.to_datetime(rows["timestamp"], utc=True)
    close_column = "close_time" if "close_time" in rows.columns else "timestamp"
    rows["_close_time"] = pd.to_datetime(rows[close_column], utc=True)
    rows = rows[(rows["timestamp"] > observed) & (rows["_close_time"] <= asof)]
    return rows.sort_values("_close_time").reset_index(drop=True)


def evaluate_shadow_outcome(*, observed_at: datetime, entry_price: float, event_high: float | None, frame_5m: pd.DataFrame, market_asof: datetime) -> dict[str, Any]:
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    observed = _utc(observed_at)
    asof = _utc(market_asof)
    rows = closed_5m_rows_after(timestamp=observed, market_asof=asof, frame_5m=frame_5m)
    horizons = {}
    for label, minutes in (("15m", 15), ("30m", 30), ("1h", 60), ("4h", 240), ("12h", 720), ("24h", 1440)):
        target = observed + timedelta(minutes=minutes)
        match = rows[rows["_close_time"] >= target] if "_close_time" in rows.columns else rows.iloc[0:0]
        horizons[label] = {"price": None if match.empty else float(match.iloc[0]["close"]), "short_return_pct": None if match.empty else (entry_price - float(match.iloc[0]["close"])) / entry_price * 100}
    due = [value["price"] is not None for value in horizons.values()]
    physically_due = [asof >= observed + timedelta(minutes=minutes) for minutes in (15, 30, 60, 240, 720, 1440)]
    if not any(physically_due):
        status = "CENSORED"
    elif all(due):
        status = "MATURE"
    elif any(due):
        status = "PARTIAL" if all(not due_now or value for due_now, value in zip(physically_due, due)) else "DATA_GAP"
    else:
        status = "DATA_GAP" if any(physically_due) else "CENSORED"
    if rows.empty:
        return {"horizons": horizons, "mfe_pct": None, "mae_pct": None, "mfe_by_horizon": {}, "mae_by_horizon": {}, "new_high_after_candidate": None, "outcome_status": status, "coverage_end_at": None, "outcome_method_version": OUTCOME_METHOD_VERSION}
    mfe = (entry_price - rows["low"].min()) / entry_price * 100
    mae = (rows["high"].max() - entry_price) / entry_price * 100
    mfe_by_horizon: dict[str, float] = {}
    mae_by_horizon: dict[str, float] = {}
    for label, minutes in (("1h", 60), ("4h", 240), ("12h", 720), ("24h", 1440)):
        target_rows = rows[rows["_close_time"] <= observed + timedelta(minutes=minutes)]
        if not target_rows.empty and horizons[label]["price"] is not None:
            mfe_by_horizon[label] = float((entry_price - target_rows["low"].min()) / entry_price * 100)
            mae_by_horizon[label] = float((target_rows["high"].max() - entry_price) / entry_price * 100)
    return {"horizons": horizons, "mfe_pct": float(mfe), "mae_pct": float(mae), "mfe_by_horizon": mfe_by_horizon, "mae_by_horizon": mae_by_horizon, "new_high_after_candidate": None if event_high is None else bool(rows["high"].max() > event_high), "outcome_status": status, "coverage_end_at": rows.iloc[-1]["_close_time"].to_pydatetime(), "outcome_method_version": OUTCOME_METHOD_VERSION}


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
