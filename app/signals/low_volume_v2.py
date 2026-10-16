"""Independent delayed-admission evaluator for low-volume extension V2.

The state in this module is deliberately separate from EventState.  V1 callers
can continue passing and mutating their existing event state unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import isfinite
from typing import Any, Mapping

STRATEGY = "LOW_VOLUME_EXTENSION_FAILURE_V2"


@dataclass(slots=True)
class LowVolumeV2State:
    root_event_id: str
    event_revision: int
    first_event_high: float
    current_anchor_high: float
    current_anchor_high_time: datetime
    last_retest_high: float | None
    last_closed_candle_time: datetime | None
    episode_started_at: datetime
    entry_trigger_time: datetime | None = None
    reset_count: int = 0
    invalidated_reason: str | None = None
    v2_state: str = "DISCOVERED"
    emitted: bool = False


@dataclass(frozen=True, slots=True)
class LowVolumeV2Decision:
    strategy_subtype: str
    root_event_id: str
    event_revision: int
    entry_trigger_time: datetime
    current_anchor_high: float
    reset_count: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LowVolumeV2Observation:
    observed_at: datetime
    current_high: float
    retest_high: float | None = None
    closed_candles: int = 0
    last_closed_candle_time: datetime | None = None
    close: float | None = None
    breakout_reference: float | None = None
    lower_high_or_swing_break: bool = False
    weak_extension: bool = True
    extension_below_threshold: bool = False
    low_volume_continuation: bool = True
    rejection: bool = True
    failed_high: bool = True
    oi_change_pct: float | None = None
    price_acceleration_resumed: bool = False
    data_fresh: bool = True
    liquidity_valid: bool = True
    entry_distance_below_high_pct: float | None = None


def new_episode(root_event_id: str, event_revision: int, event_high: float, started_at: datetime) -> LowVolumeV2State:
    """Create an independent V2 episode from an established root event."""
    return LowVolumeV2State(
        root_event_id=root_event_id,
        event_revision=event_revision,
        first_event_high=float(event_high),
        current_anchor_high=float(event_high),
        current_anchor_high_time=_utc(started_at),
        last_retest_high=None,
        last_closed_candle_time=None,
        episode_started_at=_utc(started_at),
    )


def advance_low_volume_v2(
    state: LowVolumeV2State,
    observation: LowVolumeV2Observation | Mapping[str, Any],
    config: Any,
) -> LowVolumeV2Decision | None:
    """Advance one V2 episode; malformed/stale input fails closed."""
    if state.v2_state in {"INVALIDATED", "EXPIRED", "ACTIONABLE"} or state.emitted:
        return None
    obs = _coerce_observation(observation)
    if obs is None:
        state.invalidated_reason = "malformed_data"
        state.v2_state = "INVALIDATED"
        return None
    now = _utc(obs.observed_at)
    lifetime = float(getattr(config, "low_volume_v2_max_episode_lifetime_minutes", 45))
    if (now - _utc(state.episode_started_at)).total_seconds() > lifetime * 60:
        state.invalidated_reason = "episode_expired"
        state.v2_state = "EXPIRED"
        return None
    if not obs.data_fresh:
        state.invalidated_reason = "stale_data"
        state.v2_state = "INVALIDATED"
        return None
    tolerance = float(getattr(config, "low_volume_v2_new_high_tolerance_pct", 0.30)) / 100
    if obs.current_high > state.current_anchor_high * (1 + tolerance):
        state.current_anchor_high = obs.current_high
        state.current_anchor_high_time = now
        state.last_retest_high = None
        state.entry_trigger_time = None
        state.reset_count += 1
        state.v2_state = "NEW_HIGH_RESET"
        return None
    if obs.retest_high is not None:
        state.last_retest_high = obs.retest_high
    # Below-threshold extension is metadata; only missing final confirmation blocks.
    if not all((obs.low_volume_continuation, obs.rejection, obs.failed_high)):
        state.v2_state = "FAILED_HIGH_PENDING"
        return None
    state.v2_state = "RETEST_CONFIRMATION"
    if obs.last_closed_candle_time is not None:
        state.last_closed_candle_time = _utc(obs.last_closed_candle_time)
    minimum = int(getattr(config, "low_volume_v2_min_closed_candles_after_retest", 2))
    blockers: list[str] = []
    if obs.closed_candles < minimum:
        blockers.append("insufficient_closed_candles_after_retest")
    if getattr(config, "low_volume_v2_require_breakout_close", True) and not (
        obs.close is not None and obs.breakout_reference is not None and obs.close < obs.breakout_reference
    ):
        blockers.append("close_not_below_breakout_reference")
    if getattr(config, "low_volume_v2_require_lower_high_or_swing_break", True) and not obs.lower_high_or_swing_break:
        blockers.append("lower_high_or_swing_break_missing")
    oi_limit = float(getattr(config, "low_volume_v2_block_oi_acceleration_pct", 1.0))
    if obs.oi_change_pct is not None and obs.oi_change_pct > oi_limit:
        blockers.append("oi_acceleration")
    if getattr(config, "low_volume_v2_block_price_acceleration_resumed", True) and obs.price_acceleration_resumed:
        blockers.append("price_acceleration_resumed")
    if not obs.liquidity_valid:
        blockers.append("liquidity_invalid")
    if obs.entry_distance_below_high_pct is not None and obs.entry_distance_below_high_pct > float(getattr(config, "low_volume_v2_max_entry_distance_below_high_pct", 15.0)):
        blockers.append("entry_too_far_from_anchor")
    if blockers:
        return None
    state.entry_trigger_time = now
    state.v2_state = "ACTIONABLE"
    state.emitted = True
    return LowVolumeV2Decision(
        strategy_subtype=STRATEGY,
        root_event_id=state.root_event_id,
        event_revision=state.event_revision,
        entry_trigger_time=now,
        current_anchor_high=state.current_anchor_high,
        reset_count=state.reset_count,
        metadata={
            "v2_state": state.v2_state,
            "blockers": [],
            "entry_trigger_time": now.isoformat(),
            "extension_below_threshold": obs.extension_below_threshold,
            "counterfactual": {"extension_below_threshold": obs.extension_below_threshold},
        },
    )


def _coerce_observation(value: LowVolumeV2Observation | Mapping[str, Any]) -> LowVolumeV2Observation | None:
    try:
        if isinstance(value, LowVolumeV2Observation):
            obs = value
        else:
            obs = LowVolumeV2Observation(**dict(value))
        now = _utc(obs.observed_at)
        numeric = (obs.current_high, obs.close, obs.breakout_reference, obs.retest_high, obs.oi_change_pct, obs.entry_distance_below_high_pct)
        if not all(x is None or isfinite(float(x)) for x in numeric) or not isfinite(float(obs.current_high)):
            return None
        if obs.current_high <= 0:
            return None
        return LowVolumeV2Observation(**{**obs.__dict__, "observed_at": now}) if hasattr(obs, "__dict__") else obs
    except (TypeError, ValueError, OverflowError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
