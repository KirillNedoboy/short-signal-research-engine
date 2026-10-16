"""Shared domain objects used across the bot."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class EventStatus(StrEnum):
    """Lifecycle states for a tracked market event."""

    IDLE = "idle"
    PUMP_DETECTED = "pump_detected"
    PULLBACK_OBSERVED = "pullback_observed"
    SHORT_ZONE_ACTIVE = "short_zone_active"
    SIGNAL_SENT = "signal_sent"
    EXPIRED = "expired"


class SignalType(StrEnum):
    """Supported signal classifications."""

    AGGRESSIVE = "Aggressive"
    CONFIRM = "Confirm"
    WATCH = "Watch"


@dataclass(slots=True)
class MarketSnapshot:
    """Flattened market ticker snapshot used for shortlist ranking."""

    symbol: str
    last_price: float
    price_24h_pct: float
    turnover_24h: float
    volume_24h: float
    mark_price: float | None = None
    open_interest: float | None = None
    timestamp: datetime | None = None


@dataclass(slots=True)
class ShortZone:
    """Computed zone where a mature short setup can trigger."""

    low: float
    high: float
    mode: str


@dataclass(slots=True)
class EventState:
    """Persistent event tracking state."""

    symbol: str
    event_id: str
    state: EventStatus = EventStatus.IDLE
    event_start_time: datetime | None = None
    event_high: float | None = None
    event_high_time: datetime | None = None
    event_base_price: float | None = None
    event_range_pct: float | None = None
    event_features_snapshot: dict[str, Any] = field(default_factory=dict)
    trigger_window: str | None = None
    pullback_detected_at: datetime | None = None
    pullback_depth_pct: float | None = None
    pullback_low_price: float | None = None
    zone_low: float | None = None
    zone_high: float | None = None
    signal_sent_at: datetime | None = None
    signal_id: int | None = None
    expires_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        """Return True when the event is still relevant for scanning."""

        return self.state not in {EventStatus.IDLE, EventStatus.EXPIRED}


@dataclass(slots=True)
class SymbolFeatures:
    """Live feature set derived from recent candles."""

    symbol: str
    asof: datetime
    price: float
    ret_5m: float
    ret_15m: float
    ret_1h: float
    ret_4h: float
    vwap: float
    dist_to_vwap_pct: float
    ema20: float
    dist_to_ema20_pct: float
    dist_to_ema20_atr: float
    rsi_15m: float
    upper_wick_ratio: float
    lower_wick_ratio: float
    body_pct: float
    rejection_from_high_pct: float
    close_position_in_range: float
    vol_zscore_30m: float
    vol_zscore_1h: float
    atr_14: float
    range_atr_ratio: float
    oi_change_15m: float | None = None
    oi_change_1h: float | None = None
    funding_rate: float | None = None
    open_interest: float | None = None
    oi_change_pct: float | None = None
    derivatives_status: str | None = None
    derivatives_reasons: list[str] = field(default_factory=list)
    data_quality_warnings: list[str] = field(default_factory=list)
    event_range_pct: float | None = None
    pullback_from_high_pct: float | None = None
    distance_to_event_high_pct: float | None = None
    inside_short_zone_flag: bool = False
    recent_high_breakout: bool = False
    latest_body_atr_ratio: float = 0.0
    latest_failed_retest: bool = False
    last_high: float = 0.0
    last_low: float = 0.0
    last_close: float = 0.0
    current_volume: float = 0.0
    spread_pct: float | None = None
    slippage_pct: float | None = None
    orderbook_depth_usdt_1pct: float | None = None
    orderbook_depth_usdt_2pct: float | None = None
    liquidity_available: bool = False
    market_asof: datetime | None = None
    last_high_time: datetime | None = None
    last_structural_close_time: datetime | None = None


@dataclass(slots=True)
class SqueezeGuardResult:
    """Explainable squeeze-risk assessment."""

    score: int
    level: str
    reasons: list[str]
    data_quality: list[str]
    action: str
    block_signal: bool = False
    force_watch: bool = False


@dataclass(slots=True)
class SignalDecision:
    """Final signal payload emitted by the signal engine."""

    symbol: str
    event_id: str
    signal_type: SignalType
    grade: str
    score: int
    market_price: float
    short_zone_low: float
    short_zone_high: float
    signal_time: datetime
    reasons: list[str]
    risk_flags: list[str]
    features_snapshot: dict[str, Any]
    score_breakdown: dict[str, float]
    decision_type: str = "SIGNAL"
    actionable: bool = True
    lifecycle_state: str | None = None
    blockers: list[str] = field(default_factory=list)
    squeeze_risk_score: int = 0
    squeeze_risk_level: str = "LOW"
    squeeze_risk_reasons: list[str] = field(default_factory=list)
    squeeze_guard_action: str = "NONE"
    data_quality_warnings: list[str] = field(default_factory=list)
    strategy_type: str = "BASELINE_PULLBACK"
    strategy_subtype: str | None = None
    model_version: str = "baseline-v1"
    strategy_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SignalProvenanceInput:
    """Immutable evidence attached to a newly persisted live signal."""

    strategy_family: str
    strategy_branch: str
    event_id: str
    root_event_id: str | None
    decision_evaluation_id: int | None
    admission_evaluation_id: int | None
    code_version: str
    config_hash: str
    runtime_instance_id: str
    runtime_started_at: datetime
    decision_at: datetime
    decision_entry_price: float | None = None
    decision_event_high: float | None = None
    decision_distance_from_high: float | None = None
    provenance_anomaly: str | None = None


@dataclass(slots=True)
class CandidateEvaluation:
    """Structured result of evaluating a mature candidate."""

    checked: bool
    decision: SignalDecision | None = None
    score: int = 0
    grade: str = "C"
    reject_reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    close_to_watch: bool = False
    timeframe: str = "15m"
    squeeze_risk_score: int = 0
    squeeze_risk_level: str = "LOW"
    squeeze_risk_reasons: list[str] = field(default_factory=list)
    squeeze_guard_action: str = "NONE"
    data_quality_warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SignalRecord:
    """Saved signal representation returned by the repository layer."""

    id: int
    symbol: str
    signal_time: datetime
    signal_type: str
    grade: str
    score: int
    market_price: float
    short_zone_low: float
    short_zone_high: float
    event_high: float
    event_base_price: float
    event_range_pct: float
    pullback_from_high_pct: float
    dist_to_vwap_pct: float
    upper_wick_ratio: float
    rejection_from_high_pct: float
    vol_zscore_30m: float
    dist_to_ema20_atr: float
    rsi_15m: float
    ret_1h: float
    ret_4h: float
    range_atr_ratio: float
    oi_change_15m: float | None
    oi_change_1h: float | None
    funding_rate: float | None
    context_json: dict[str, Any]
    telegram_sent: bool
    created_at: datetime


@dataclass(slots=True)
class WatchCandidateRecord:
    """Saved non-actionable watch candidate with nullable metrics."""

    id: int
    symbol: str
    timeframe: str
    signal_time: datetime
    score: int
    base_grade: str
    actionable: bool
    blockers_json: list[str]
    risk_flags_json: list[str]
    squeeze_risk_level: str | None
    squeeze_risk_score: int | None
    squeeze_risk_reasons_json: list[str]
    data_quality_warnings_json: list[str]
    context_json: dict[str, Any]
    dist_to_vwap_pct: float | None
    upper_wick_ratio: float | None
    rejection_from_high_pct: float | None
    volume_zscore_30m: float | None
    pullback_from_event_high_pct: float | None
    dist_to_ema20_atr: float | None
    rsi_15m: float | None
    spread_pct: float | None
    orderbook_depth_1pct: float | None
    telegram_sent: bool
    created_at: datetime


@dataclass(slots=True)
class SignalOutcome:
    """Outcome measurements for a stored signal."""

    signal_id: int
    price_after_15m: float | None = None
    price_after_1h: float | None = None
    price_after_4h: float | None = None
    mfe_pct: float | None = None
    mae_pct: float | None = None
    reached_vwap: bool | None = None
    time_to_vwap_minutes: int | None = None
    tp1_hit: bool | None = None
    stopped_virtual: bool | None = None
    risk_adjusted_status: str | None = None
    squeeze_extension_pct: float | None = None
    is_clean_short: bool | None = None
    is_squeeze_before_tp: bool | None = None
    updated_at: datetime | None = None
