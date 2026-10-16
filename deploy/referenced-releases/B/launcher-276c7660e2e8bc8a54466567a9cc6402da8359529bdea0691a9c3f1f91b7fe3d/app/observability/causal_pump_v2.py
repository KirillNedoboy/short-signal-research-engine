"""Causal pump-root V2: ordered max-drawup shadow detection."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum

import pandas as pd

from app.market.candles import closed_1m_rows

METHOD_VERSION = "CAUSAL_PUMP_V2_V1"

class V2PumpStatus(StrEnum):
    IDLE = "IDLE"
    PUMP_TRACKING = "PUMP_TRACKING"
    PULLBACK_TRACKING = "PULLBACK_TRACKING"
    SHORT_ZONE_ACTIVE = "SHORT_ZONE_ACTIVE"
    EXPIRED = "EXPIRED"

class V2ComparisonVerdict(StrEnum):
    V2_EARLIER = "V2_EARLIER"
    V1_EARLIER = "V1_EARLIER"
    SAME_TIME = "SAME_TIME"
    V1_ONLY = "V1_ONLY"
    V2_ONLY = "V2_ONLY"
    BOTH_MISSING = "BOTH_MISSING"

@dataclass(frozen=True, slots=True)
class CausalPumpV2Config:
    pump_min_ret_5m: float = 2.0
    pump_min_ret_15m: float = 4.0
    pump_min_ret_1h: float = 7.0
    pump_min_ret_4h: float = 10.0
    drawup_lookback_candles: int = 30
    strict_chronology: bool = True
    pullback_min_pct: float = 2.4
    pullback_max_pct: float = 8.0
    zone_fraction_of_range_low: float = 0.70
    zone_fraction_of_range_high: float = 0.92
    expiry_minutes: int = 90
    max_stale_seconds: int = 300

@dataclass(frozen=True, slots=True)
class CausalV2RootDecision:
    state: str
    root_id: str | None
    reason: str
    event_low_price: float | None = None
    event_low_time: datetime | None = None
    event_high_price: float | None = None
    event_high_time: datetime | None = None
    event_range_pct: float | None = None
    condition_snapshot: dict[str, str] = field(default_factory=dict)
    method_version: str = METHOD_VERSION

@dataclass(frozen=True, slots=True)
class V1V2ComparisonRow:
    symbol: str
    compared_at: datetime
    v1_event_id: str | None
    v1_state: str | None
    v1_event_high_time: datetime | None
    v1_event_high: float | None
    v1_trigger_window: str | None
    v2_root_id: str | None
    v2_state: str | None
    v2_event_low_time: datetime | None
    v2_event_low_price: float | None
    v2_event_high_time: datetime | None
    v2_event_high_price: float | None
    v2_trigger_window: str | None
    verdict: str
    price_at_comparison: float
    v2_ahead_by_seconds: float | None
    method_version: str = METHOD_VERSION

def _utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

class CausalPumpV2Detector:
    def __init__(self, config=None):
        self._config = config or CausalPumpV2Config()

    def detect(self, symbol, frame_1m, features, now):
        now = _utc(now)
        if frame_1m.empty:
            return None
        closed = self._closed_rows(frame_1m, now)
        if closed.empty:
            return None
        window = closed.tail(min(self._config.drawup_lookback_candles, len(closed)))
        if window.empty or len(window) < 2:
            return None
        running_low_price = None
        running_low_time = None
        best_low_price = None
        best_low_time = None
        best_high_price = None
        best_high_time = None
        best_drawup = -1.0
        for _, row in window.iterrows():
            candle_time = self._to_datetime(row["timestamp"])
            candle_low = float(row["low"])
            candle_high = float(row["high"])
            if running_low_price is not None and running_low_time < candle_time and running_low_price > 0:
                drawup = (candle_high / running_low_price) - 1.0
                if drawup > best_drawup:
                    best_drawup = drawup
                    best_low_price, best_low_time = running_low_price, running_low_time
                    best_high_price, best_high_time = candle_high, candle_time
            if running_low_price is None or candle_low < running_low_price:
                running_low_price, running_low_time = candle_low, candle_time
        if best_low_price is None:
            return None
        low_price, low_time = best_low_price, best_low_time
        high_price, high_time = best_high_price, best_high_time
        if self._config.strict_chronology and low_time >= high_time:
            return CausalV2RootDecision(
                state=V2PumpStatus.IDLE, root_id=None,
                reason="chronology_violation: low_not_before_high",
                event_low_price=low_price, event_low_time=low_time,
                event_high_price=high_price, event_high_time=high_time,
                condition_snapshot={"low_time": low_time.isoformat(),
                                    "high_time": high_time.isoformat(),
                                    "violation": "low >= high"},
            )
        if low_price <= 0 or high_price <= low_price:
            return None
        event_range_pct = ((high_price / low_price) - 1.0) * 100.0
        ret_5m = float(features.get("ret_5m") or 0.0)
        ret_15m = float(features.get("ret_15m") or 0.0)
        ret_1h = float(features.get("ret_1h") or 0.0)
        ret_4h = float(features.get("ret_4h") or 0.0)
        conditions = {}
        any_qualifying = False
        for label, ret_val, threshold in (
            ("5m", ret_5m, self._config.pump_min_ret_5m),
            ("15m", ret_15m, self._config.pump_min_ret_15m),
            ("1h", ret_1h, self._config.pump_min_ret_1h),
            ("4h", ret_4h, self._config.pump_min_ret_4h),
        ):
            passed = ret_val >= threshold
            conditions[f"pump_{label}"] = (
                f"PASS:{ret_val:.2f}" if passed else f"FAIL:{ret_val:.2f}<{threshold}"
            )
            if passed:
                any_qualifying = True
        conditions["chronology_low_before_high"] = "PASS"
        if not any_qualifying:
            return CausalV2RootDecision(
                state=V2PumpStatus.IDLE, root_id=None,
                reason="pump_thresholds_not_met",
                event_low_price=low_price, event_low_time=low_time,
                event_high_price=high_price, event_high_time=high_time,
                event_range_pct=event_range_pct,
                condition_snapshot=conditions,
            )
        root_id = (
            f"CAUSAL_PUMP_V2:{symbol.upper()}:"
            f"{int(low_time.timestamp())}:{int(high_time.timestamp())}:"
            f"{_canonical_price(high_price)}"
        )
        trigger_window = "none"
        for w in ("4h", "1h", "15m", "5m"):
            if conditions.get(f"pump_{w}", "FAIL").startswith("PASS"):
                trigger_window = w
                break
        return CausalV2RootDecision(
            state=V2PumpStatus.PUMP_TRACKING, root_id=root_id,
            reason="ordered_max_drawup_detected",
            event_low_price=low_price, event_low_time=low_time,
            event_high_price=high_price, event_high_time=high_time,
            event_range_pct=event_range_pct,
            condition_snapshot={**conditions, "trigger_window": trigger_window},
        )

    @staticmethod
    def _closed_rows(frame, market_asof):
        closed = closed_1m_rows(frame, market_asof)
        if "confirmed" in frame.columns:
            return closed[closed["confirmed"]].copy()
        return closed

    @staticmethod
    def _to_datetime(value):
        if isinstance(value, datetime):
            return _utc(value)
        return pd.Timestamp(value, tz="UTC").to_pydatetime()

class CausalPumpV2Tracker:
    def __init__(self, config=None):
        self._config = config or CausalPumpV2Config()

    def advance(self, current, frame_1m, features, now):
        now = _utc(now)
        if current.state in {V2PumpStatus.IDLE, V2PumpStatus.EXPIRED}:
            return current
        if current.event_high_time is not None:
            since_high = now - current.event_high_time
            if since_high.total_seconds() > self._config.expiry_minutes * 60:
                return CausalV2RootDecision(
                    state=V2PumpStatus.EXPIRED, root_id=current.root_id,
                    reason=f"expired_after_{self._config.expiry_minutes}min",
                    event_low_price=current.event_low_price,
                    event_low_time=current.event_low_time,
                    event_high_price=current.event_high_price,
                    event_high_time=current.event_high_time,
                    event_range_pct=current.event_range_pct,
                    condition_snapshot=dict(current.condition_snapshot),
                )
        new_state = current.state
        new_reason = current.reason
        new_high_price = current.event_high_price
        new_high_time = current.event_high_time
        new_range_pct = current.event_range_pct
        new_conditions = dict(current.condition_snapshot)
        if not frame_1m.empty:
            closed = CausalPumpV2Detector._closed_rows(frame_1m, now)
            if not closed.empty:
                window = closed.tail(self._config.drawup_lookback_candles)
                newer = window[window["timestamp"].map(CausalPumpV2Detector._to_datetime) > current.event_high_time]
                if newer.empty:
                    fresh_high, fresh_high_time = current.event_high_price, current.event_high_time
                else:
                    fresh_high = float(newer["high"].max())
                    fresh_high_idx = newer["high"].idxmax()
                    fresh_high_time = CausalPumpV2Detector._to_datetime(newer.loc[fresh_high_idx, "timestamp"])
                if current.event_high_price is not None and fresh_high > current.event_high_price:
                    new_high_price = fresh_high
                    new_high_time = fresh_high_time
                    if (current.event_low_price is not None and
                            current.event_low_price > 0):
                        new_range_pct = (
                            (new_high_price / current.event_low_price) - 1
                        ) * 100
                    new_conditions["high_reset"] = f"new_high:{fresh_high:.4f}"
                    new_reason = "causal_high_reset_upward"
        price = float(features.get("price") or 0.0)
        effective_high = new_high_price or current.event_high_price or price
        if effective_high > 0 and price > 0:
            pullback_pct = ((effective_high - price) / effective_high) * 100
            pb_min = self._config.pullback_min_pct
            pb_max = self._config.pullback_max_pct
            if (current.state == V2PumpStatus.PUMP_TRACKING and
                    pb_min <= pullback_pct <= pb_max):
                new_state = V2PumpStatus.PULLBACK_TRACKING
                if new_reason != "causal_high_reset_upward":
                    new_reason = "pullback_observed"
                new_conditions["transition"] = "pullback_observed"
                new_conditions["pullback_depth_pct"] = f"{pullback_pct:.2f}"
            if (current.state == V2PumpStatus.PULLBACK_TRACKING and
                    pullback_pct >= pb_min):
                if (current.event_low_price is not None and
                        effective_high > current.event_low_price):
                    event_range = effective_high - current.event_low_price
                    zl = current.event_low_price + (
                        self._config.zone_fraction_of_range_low * event_range
                    )
                    zh = current.event_low_price + (
                        self._config.zone_fraction_of_range_high * event_range
                    )
                    if zl <= price <= zh:
                        new_state = V2PumpStatus.SHORT_ZONE_ACTIVE
                        new_reason = "entered_short_zone"
                        new_conditions["zone_low"] = f"{zl:.4f}"
                        new_conditions["zone_high"] = f"{zh:.4f}"
        return CausalV2RootDecision(
            state=new_state, root_id=current.root_id, reason=new_reason,
            event_low_price=current.event_low_price,
            event_low_time=current.event_low_time,
            event_high_price=new_high_price,
            event_high_time=new_high_time,
            event_range_pct=new_range_pct,
            condition_snapshot=new_conditions,
        )

    def compute_zone(self, decision, price):
        if (decision.event_low_price is None or
                decision.event_high_price is None):
            return None
        if (decision.event_low_price <= 0 or
                decision.event_high_price <= decision.event_low_price):
            return None
        event_range = decision.event_high_price - decision.event_low_price
        zone_low = decision.event_low_price + (
            self._config.zone_fraction_of_range_low * event_range
        )
        zone_high = decision.event_low_price + (
            self._config.zone_fraction_of_range_high * event_range
        )
        return min(zone_low, zone_high), max(zone_low, zone_high)

class CausalPumpV2Comparator:
    def compare(self, symbol, now, v1_event_id, v1_state,
                v1_event_high_time, v1_event_high, v1_trigger_window,
                v2_root_id, v2_state, v2_event_low_time, v2_event_low_price,
                v2_event_high_time, v2_event_high_price, v2_trigger_window,
                price):
        now = _utc(now)
        verdict = V2ComparisonVerdict.BOTH_MISSING
        ahead = None
        if v1_event_id and v2_root_id:
            if v1_event_high_time and v2_event_high_time:
                diff = (v1_event_high_time - v2_event_high_time).total_seconds()
                if diff > 0:
                    verdict = V2ComparisonVerdict.V2_EARLIER
                    ahead = diff
                elif diff < 0:
                    verdict = V2ComparisonVerdict.V1_EARLIER
                    ahead = abs(diff)
                else:
                    verdict = V2ComparisonVerdict.SAME_TIME
                    ahead = 0.0
        elif v1_event_id:
            verdict = V2ComparisonVerdict.V1_ONLY
        elif v2_root_id:
            verdict = V2ComparisonVerdict.V2_ONLY
        return V1V2ComparisonRow(
            symbol=symbol.upper(), compared_at=now,
            v1_event_id=v1_event_id, v1_state=v1_state,
            v1_event_high_time=v1_event_high_time,
            v1_event_high=v1_event_high,
            v1_trigger_window=v1_trigger_window,
            v2_root_id=v2_root_id, v2_state=v2_state,
            v2_event_low_time=v2_event_low_time,
            v2_event_low_price=v2_event_low_price,
            v2_event_high_time=v2_event_high_time,
            v2_event_high_price=v2_event_high_price,
            v2_trigger_window=v2_trigger_window,
            verdict=verdict,
            price_at_comparison=price,
            v2_ahead_by_seconds=ahead,
        )


def _canonical_price(value: float) -> str:
    """Serialize price identity without a float scaling assumption."""

    return format(Decimal(str(value)).normalize(), "f")
