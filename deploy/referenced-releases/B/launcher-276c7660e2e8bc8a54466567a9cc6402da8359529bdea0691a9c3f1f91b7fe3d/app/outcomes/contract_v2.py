"""Outcome Contract V2: strict post-entry, first-passage chronology, configurable costs."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

import pandas as pd

METHOD_VERSION = "OUTCOME_CONTRACT_V2_V1"

class ContractV2Resolution(StrEnum):
    TP_FIRST = "TP_FIRST"
    SL_FIRST = "SL_FIRST"
    EXPIRED_NO_HIT = "EXPIRED_NO_HIT"
    AMBIGUOUS = "AMBIGUOUS"
    NO_FILL = "NO_FILL"
    DATA_GAP = "DATA_GAP"
    CENSORED = "CENSORED"
    EXECUTION_UNOBSERVABLE = "EXECUTION_UNOBSERVABLE"
    INVALID_ENTRY = "INVALID_ENTRY"

class EntryFillStatus(StrEnum):
    FILLED = "FILLED"
    NO_FILL = "NO_FILL"
    GAP_BEFORE_FILL = "GAP_BEFORE_FILL"


@dataclass(frozen=True, slots=True)
class ExecutionObservation:
    """Timestamped executable market evidence used for strict fills."""

    observed_at: datetime
    bid: float | None = None
    ask: float | None = None
    trade_price: float | None = None

@dataclass(frozen=True, slots=True)
class PaperTradeFill:
    """One deterministic paper fill, never an exchange order."""

    role: str
    side: str
    fill_time: datetime
    fill_price: float
    source: str
    reason: str
    fee_pct: float | None = None
    slippage_pct: float | None = None


@dataclass(frozen=True, slots=True)
class OutcomeContractV2Config:
    entry_fill_rule: str = "price_touches_or_crosses_below"
    spread_pct: float = 0.10
    funding_cost_annual_pct: float = 10.0
    latency_seconds: float = 5.0
    tp_mult: float = 0.97
    sl_mult: float = 1.03
    use_event_high_as_sl: bool = True
    max_evaluation_minutes: int = 1440
    min_data_minutes: int = 5
    require_execution_observation: bool = False

@dataclass(frozen=True, slots=True)
class OutcomeContractV2Result:
    signal_id: int | None
    symbol: str
    event_id: str | None
    decision_time: datetime
    entry_fill_time: datetime | None
    entry_fill_price: float | None
    entry_fill_status: EntryFillStatus
    tp_price: float | None
    sl_price: float | None
    tp_hit_time: datetime | None
    sl_hit_time: datetime | None
    first_passage: ContractV2Resolution
    resolution_detail: str
    mfe_pct: float | None
    mae_pct: float | None
    return_at_expiry_pct: float | None
    total_cost_pct: float | None
    net_return_pct: float | None
    evaluated_at: datetime
    evaluation_status: str
    horizons: dict[str, dict[str, float | None]] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    fills: tuple[PaperTradeFill, ...] = ()
    method_version: str = METHOD_VERSION
    @property
    def result_id(self) -> str:
        payload = {"method_version": self.method_version, "signal_id": self.signal_id, "event_id": self.event_id, "symbol": self.symbol.upper(), "decision_time": _utc(self.decision_time).isoformat()}
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

def _utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

def _to_datetime(value):
    if isinstance(value, datetime):
        return _utc(value)
    return pd.Timestamp(value, tz="UTC").to_pydatetime()

class OutcomeContractV2Evaluator:
    def __init__(self, config=None):
        self._config = config or OutcomeContractV2Config()

    def evaluate(self, *, signal_id, symbol, event_id, decision_time, decision_price, event_high, frame_1m, execution_observations=None, now=None):
        now = _utc(now or datetime.now(timezone.utc))
        decision_time = _utc(decision_time)
        if decision_price <= 0:
            return OutcomeContractV2Result(
                signal_id=signal_id, symbol=symbol.upper(), event_id=event_id,
                decision_time=decision_time, entry_fill_time=None,
                entry_fill_price=None, entry_fill_status=EntryFillStatus.NO_FILL,
                tp_price=None, sl_price=None, tp_hit_time=None, sl_hit_time=None,
                first_passage=ContractV2Resolution.INVALID_ENTRY,
                resolution_detail="decision_price <= 0",
                mfe_pct=None, mae_pct=None, return_at_expiry_pct=None,
                total_cost_pct=None, net_return_pct=None,
                evaluated_at=now, evaluation_status="MATURE",
                evidence={"decision_price": decision_price},
            )
        executable_start = decision_time + timedelta(seconds=self._config.latency_seconds)
        fill_source = "ohlcv"
        if self._config.require_execution_observation:
            if not execution_observations:
                return OutcomeContractV2Result(
                    signal_id=signal_id, symbol=symbol.upper(), event_id=event_id,
                    decision_time=decision_time, entry_fill_time=None,
                    entry_fill_price=None, entry_fill_status=EntryFillStatus.GAP_BEFORE_FILL,
                    tp_price=None, sl_price=None, tp_hit_time=None, sl_hit_time=None,
                    first_passage=ContractV2Resolution.EXECUTION_UNOBSERVABLE,
                    resolution_detail="execution quote/trade evidence is absent",
                    mfe_pct=None, mae_pct=None, return_at_expiry_pct=None,
                    total_cost_pct=None, net_return_pct=None, evaluated_at=now,
                    evaluation_status="UNKNOWN",
                    evidence={"execution_observations": 0, "executable_start": executable_start.isoformat()},
                )
            fill_time, fill_price, fill_source = self._find_execution_fill(
                execution_observations, executable_start, decision_price, now
            )
        else:
            fill_time, fill_price, fill_source = self._find_fill(
                frame_1m, executable_start, decision_price, now
            )
        if fill_time is None and self._has_minute_gap(frame_1m, executable_start):
            return OutcomeContractV2Result(
                signal_id=signal_id, symbol=symbol.upper(), event_id=event_id,
                decision_time=decision_time, entry_fill_time=None,
                entry_fill_price=None, entry_fill_status=EntryFillStatus.GAP_BEFORE_FILL,
                tp_price=None, sl_price=None, tp_hit_time=None, sl_hit_time=None,
                first_passage=ContractV2Resolution.DATA_GAP,
                resolution_detail="missing 1m executable-price evidence",
                mfe_pct=None, mae_pct=None, return_at_expiry_pct=None,
                total_cost_pct=None, net_return_pct=None, evaluated_at=now,
                evaluation_status="DATA_GAP", evidence={"executable_start": executable_start.isoformat()},
            )
        if fill_time is None or fill_price is None:
            return OutcomeContractV2Result(
                signal_id=signal_id, symbol=symbol.upper(), event_id=event_id,
                decision_time=decision_time, entry_fill_time=None,
                entry_fill_price=None, entry_fill_status=EntryFillStatus.NO_FILL,
                tp_price=None, sl_price=None, tp_hit_time=None, sl_hit_time=None,
                first_passage=ContractV2Resolution.NO_FILL,
                resolution_detail="no executable fill found",
                mfe_pct=None, mae_pct=None, return_at_expiry_pct=None,
                total_cost_pct=None, net_return_pct=None,
                evaluated_at=now,
                evaluation_status=self._evaluation_status(frame_1m, decision_time, now),
                evidence={"executable_start": executable_start.isoformat(), "latency_seconds": self._config.latency_seconds},
            )
        tp_price = fill_price * self._config.tp_mult
        if self._config.use_event_high_as_sl and event_high is not None:
            sl_price = max(fill_price * self._config.sl_mult, event_high)
        else:
            sl_price = fill_price * self._config.sl_mult
        future = self._rows_after(frame_1m, fill_time)
        tp_hit_time = None
        sl_hit_time = None
        for _, row in future.iterrows():
            ts = _to_datetime(row["timestamp"])
            low = float(row["low"])
            high = float(row["high"])
            if low <= tp_price and tp_hit_time is None:
                tp_hit_time = ts
            if high >= sl_price and sl_hit_time is None:
                sl_hit_time = ts
            if tp_hit_time is not None and sl_hit_time is not None:
                break
        resolution, detail = self._resolve(tp_hit_time, sl_hit_time, fill_time, now, decision_time)
        if not future.empty:
            mfe_pct = float((fill_price - future["low"].min()) / fill_price * 100)
            mae_pct = float((future["high"].max() - fill_price) / fill_price * 100)
        else:
            mfe_pct = None
            mae_pct = None
        return_at_expiry = self._return_at_expiry(future, fill_price, fill_time, now)
        exit_time = min((value for value in (tp_hit_time, sl_hit_time) if value is not None), default=min(now, decision_time + timedelta(minutes=self._config.max_evaluation_minutes)))
        total_cost_pct = self._compute_costs(fill_price, fill_time, exit_time)
        net_return_pct = None
        if resolution == ContractV2Resolution.TP_FIRST:
            net_return_pct = ((fill_price - tp_price) / fill_price) * 100 - (total_cost_pct or 0)
        elif resolution == ContractV2Resolution.SL_FIRST:
            sl_return = ((fill_price - sl_price) / fill_price) * 100
            net_return_pct = sl_return - (total_cost_pct or 0)
        horizons = self._compute_horizons(future, fill_price, fill_time)
        fills = self._build_fills(
            fill_time=fill_time,
            fill_price=fill_price,
            fill_source=fill_source,
            resolution=resolution,
            tp_price=tp_price,
            sl_price=sl_price,
            tp_hit_time=tp_hit_time,
            sl_hit_time=sl_hit_time,
            future=future,
            now=now,
        )
        return OutcomeContractV2Result(
            signal_id=signal_id, symbol=symbol.upper(), event_id=event_id,
            decision_time=decision_time, entry_fill_time=fill_time,
            entry_fill_price=fill_price, entry_fill_status=EntryFillStatus.FILLED,
            tp_price=tp_price, sl_price=sl_price,
            tp_hit_time=tp_hit_time, sl_hit_time=sl_hit_time,
            first_passage=resolution, resolution_detail=detail,
            mfe_pct=mfe_pct, mae_pct=mae_pct,
            return_at_expiry_pct=return_at_expiry,
            total_cost_pct=total_cost_pct, net_return_pct=net_return_pct,
            evaluated_at=now,
            evaluation_status=self._evaluation_status(frame_1m, decision_time, now),
            horizons=horizons,
            evidence={"spread_pct": self._config.spread_pct, "funding_cost_annual_pct": self._config.funding_cost_annual_pct, "latency_seconds": self._config.latency_seconds, "tp_mult": self._config.tp_mult, "sl_mult": self._config.sl_mult},
            fills=fills,
        )

    @staticmethod
    def _find_execution_fill(observations, executable_start, decision_price, now):
        for observation in sorted(observations, key=lambda item: _to_datetime(item.observed_at)):
            observed_at = _to_datetime(observation.observed_at)
            if observed_at < executable_start or observed_at > now:
                continue
            bid = observation.bid
            if bid is not None and bid > 0 and bid <= decision_price:
                return observed_at, float(bid), "execution_bid"
            trade_price = observation.trade_price
            if trade_price is not None and trade_price > 0 and trade_price <= decision_price:
                return observed_at, float(trade_price), "execution_trade"
        return None, None, "execution_observation"

    def _find_fill(self, frame, executable_start, decision_price, now):
        if frame.empty or "timestamp" not in frame.columns:
            return None, None, "ohlcv"
        future = self._rows_after(frame, executable_start)
        if future.empty:
            return None, None, "ohlcv"
        for _, row in future.iterrows():
            ts = _to_datetime(row["timestamp"])
            open_p = float(row["open"])
            low_p = float(row["low"])
            if low_p <= decision_price:
                return ts, decision_price * (1 - self._config.spread_pct / 100), "ohlcv_low_touch"
            if open_p <= decision_price:
                return ts, decision_price * (1 - self._config.spread_pct / 100), "ohlcv_open"
        return None, None, "ohlcv"

    @staticmethod
    def _has_minute_gap(frame, start):
        if frame.empty or "timestamp" not in frame.columns:
            return False
        timestamps = sorted({_to_datetime(value) for value in frame.loc[frame["timestamp"] >= start, "timestamp"]})
        return any((right - left).total_seconds() > 60 for left, right in zip(timestamps, timestamps[1:]))

    def _resolve(self, tp_time, sl_time, fill_time, now, decision_time):
        max_end = decision_time + timedelta(minutes=self._config.max_evaluation_minutes)
        if now >= max_end and tp_time is None and sl_time is None:
            return ContractV2Resolution.EXPIRED_NO_HIT, f"no TP/SL hit within {self._config.max_evaluation_minutes}min"
        if tp_time is not None and sl_time is not None:
            if tp_time < sl_time:
                return ContractV2Resolution.TP_FIRST, f"TP at {tp_time.isoformat()} before SL at {sl_time.isoformat()}"
            elif sl_time < tp_time:
                return ContractV2Resolution.SL_FIRST, f"SL at {sl_time.isoformat()} before TP at {tp_time.isoformat()}"
            else:
                return ContractV2Resolution.AMBIGUOUS, "TP and SL triggered in same candle"
        if tp_time is not None:
            return ContractV2Resolution.TP_FIRST, f"TP hit at {tp_time.isoformat()}, SL not hit"
        if sl_time is not None:
            return ContractV2Resolution.SL_FIRST, f"SL hit at {sl_time.isoformat()}, TP not hit"
        if now < max_end:
            return ContractV2Resolution.CENSORED, "still within evaluation window"
        return ContractV2Resolution.EXPIRED_NO_HIT, "evaluation window expired without TP/SL hit"

    @staticmethod
    def _rows_after(frame, start):
        if frame.empty or "timestamp" not in frame.columns:
            return frame.iloc[0:0].copy()
        return frame[frame["timestamp"] >= start].copy()

    def _build_fills(
        self,
        *,
        fill_time,
        fill_price,
        fill_source,
        resolution,
        tp_price,
        sl_price,
        tp_hit_time,
        sl_hit_time,
        future,
        now,
    ):
        fills = [
            PaperTradeFill(
                role="ENTRY",
                side="SELL",
                fill_time=fill_time,
                fill_price=fill_price,
                source=fill_source,
                reason="entry",
            )
        ]
        if resolution == ContractV2Resolution.TP_FIRST and tp_hit_time is not None:
            fills.append(
                PaperTradeFill(
                    role="EXIT",
                    side="BUY",
                    fill_time=tp_hit_time,
                    fill_price=tp_price,
                    source="ohlcv_tp",
                    reason="TP",
                )
            )
        elif resolution == ContractV2Resolution.SL_FIRST and sl_hit_time is not None:
            fills.append(
                PaperTradeFill(
                    role="EXIT",
                    side="BUY",
                    fill_time=sl_hit_time,
                    fill_price=sl_price,
                    source="ohlcv_sl",
                    reason="SL",
                )
            )
        elif resolution == ContractV2Resolution.EXPIRED_NO_HIT:
            max_end = min(now, fill_time + timedelta(minutes=self._config.max_evaluation_minutes))
            expiry_rows = future[future["timestamp"] <= max_end]
            if not expiry_rows.empty:
                row = expiry_rows.iloc[-1]
                fills.append(
                    PaperTradeFill(
                        role="EXIT",
                        side="BUY",
                        fill_time=_to_datetime(row["timestamp"]),
                        fill_price=float(row["close"]),
                        source="ohlcv_expiry",
                        reason="EXPIRY",
                    )
                )
        return tuple(fills)

    def _return_at_expiry(self, future, fill_price, fill_time, now):
        max_end = fill_time + timedelta(minutes=self._config.max_evaluation_minutes)
        actual_end = min(now, max_end)
        if future.empty:
            return None
        rows = future[future["timestamp"] <= actual_end]
        if rows.empty:
            return None
        last_close = float(rows.iloc[-1]["close"])
        return ((fill_price - last_close) / fill_price) * 100

    def _compute_costs(self, fill_price, fill_time, now):
        spread_cost = self._config.spread_pct
        elapsed_hours = (now - fill_time).total_seconds() / 3600.0
        funding_8h_intervals = max(0, elapsed_hours) / 8.0
        annual_rate = self._config.funding_cost_annual_pct / 100.0
        funding_cost = annual_rate * (funding_8h_intervals * 8 / (365 * 24)) * 100
        return round(spread_cost + funding_cost, 6)

    def _compute_horizons(self, future, fill_price, fill_time):
        horizons = {}
        for label, minutes in (("15m", 15), ("1h", 60), ("4h", 240)):
            target = fill_time + timedelta(minutes=minutes)
            rows = future[future["timestamp"] >= target] if not future.empty else future
            price = float(rows.iloc[0]["close"]) if not rows.empty else None
            short_return = ((fill_price - price) / fill_price * 100) if price is not None else None
            horizons[label] = {"price": price, "short_return_pct": short_return}
        return horizons

    def _evaluation_status(self, frame, decision_time, now):
        min_data_time = decision_time + timedelta(minutes=self._config.min_data_minutes)
        if now < min_data_time:
            return "CENSORED"
        if frame.empty:
            return "DATA_GAP"
        future = self._rows_after(frame, decision_time)
        if future.empty:
            return "DATA_GAP"
        max_end = decision_time + timedelta(minutes=self._config.max_evaluation_minutes)
        if now >= max_end:
            return "MATURE"
        return "PARTIAL"
