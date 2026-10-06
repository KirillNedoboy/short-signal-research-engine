"""Pullback tracking and expiry logic."""

from __future__ import annotations

from datetime import datetime, timedelta

from app.config import AppConfig
from app.domain import EventState, EventStatus, SymbolFeatures
from app.market.candles import normalize_utc


class PullbackTracker:
    """Advance a stored event through pullback, zone, and expiry stages."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def advance(self, state: EventState, features: SymbolFeatures, now: datetime) -> EventState:
        """Update a tracked event with current market context."""

        now = normalize_utc(now)
        if state.expires_at is not None:
            state.expires_at = normalize_utc(state.expires_at)
        if state.signal_sent_at is not None:
            state.signal_sent_at = normalize_utc(state.signal_sent_at)
        state.updated_at = now
        if self._should_expire(state, features, now):
            state.state = EventStatus.EXPIRED
            state.expires_at = now
            return state

        if state.signal_sent_at and now >= state.signal_sent_at + timedelta(minutes=15):
            state.state = EventStatus.EXPIRED
            state.expires_at = now
            return state

        if state.event_high is None or state.event_base_price is None:
            return state

        if self.reset_after_confirmed_high(state, features, now):
            return state

        pullback_pct = ((state.event_high - features.price) / state.event_high) * 100 if state.event_high else 0.0
        state.pullback_depth_pct = max(state.pullback_depth_pct or 0.0, pullback_pct)
        state.pullback_low_price = (
            min(state.pullback_low_price, features.last_low)
            if state.pullback_low_price is not None
            else features.last_low
        )

        floor_price = state.event_base_price + (
            self._config.pullback_hold_range_floor_pct * (state.event_high - state.event_base_price)
        )
        if (
            state.pullback_detected_at is None
            and self._config.pullback_min_pct <= pullback_pct <= self._config.pullback_max_pct
            and features.dist_to_vwap_pct >= self._config.pullback_hold_vwap_min
            and features.price >= floor_price
        ):
            state.state = EventStatus.PULLBACK_OBSERVED
            state.pullback_detected_at = now
            state.pullback_depth_pct = pullback_pct
            state.pullback_low_price = features.last_low

        return state

    def reset_after_confirmed_high(self, state: EventState, features: SymbolFeatures, now: datetime) -> bool:
        """Reset stale baseline pullback state after a confirmed 1m event high."""

        if (
            state.signal_id is not None
            or state.event_high is None
            or state.event_base_price is None
            or features.last_high_time is None
            or features.last_high <= state.event_high
        ):
            return False
        confirmed_at = normalize_utc(features.last_high_time)
        state.event_high = features.last_high
        state.event_high_time = confirmed_at
        state.event_range_pct = ((state.event_high / state.event_base_price) - 1) * 100
        state.pullback_detected_at = None
        state.pullback_depth_pct = None
        state.pullback_low_price = None
        state.zone_low = None
        state.zone_high = None
        state.state = EventStatus.PUMP_DETECTED
        state.updated_at = normalize_utc(now)
        return True

    def mark_signal_sent(self, state: EventState, signal_id: int, when: datetime) -> EventState:
        """Mark the current event as already signaled."""

        state.state = EventStatus.SIGNAL_SENT
        state.signal_id = signal_id
        state.signal_sent_at = when
        state.updated_at = when
        return state

    def _should_expire(self, state: EventState, features: SymbolFeatures, now: datetime) -> bool:
        if state.expires_at and now >= state.expires_at:
            return True
        if state.event_high is None or state.event_base_price is None:
            return False
        kill_price = state.event_base_price + (0.35 * (state.event_high - state.event_base_price))
        return features.price < kill_price
