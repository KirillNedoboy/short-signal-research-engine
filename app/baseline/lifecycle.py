"""Side-effect-free BASELINE lifecycle shared by live and historical paths."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from app.config import AppConfig
from app.domain import EventState, EventStatus, ShortZone, SymbolFeatures
from app.events.pump_detector import PumpDetector
from app.events.pullback_tracker import PullbackTracker
from app.events.short_zone import ShortZoneBuilder
from app.features.builder import FeatureBuilder
from app.replay.contracts import BaselineStrategyInput
from app.replay.market import NormalizedMarketObservation


@dataclass(frozen=True, slots=True)
class TransitionRecord:
    symbol: str
    event_id: str
    transition_time_utc: datetime
    from_state: str
    to_state: str
    trigger: str
    source_market_time: datetime | None
    reason: str | None = None
    sequence: int = 0


@dataclass(frozen=True, slots=True)
class BaselineLifecycleResult:
    state: EventState | None
    features: SymbolFeatures | None
    short_zone: ShortZone | None
    evaluation_input: BaselineStrategyInput | None
    transitions: tuple[TransitionRecord, ...]
    max_input_availability_time_utc: datetime | None


class BaselineLifecycle:
    """Run only the production BASELINE event/state transitions."""

    def __init__(
        self,
        config: AppConfig,
        feature_builder: FeatureBuilder | None = None,
        *,
        pump_detector: PumpDetector | None = None,
        pullback_tracker: PullbackTracker | None = None,
        zone_builder: ShortZoneBuilder | None = None,
    ) -> None:
        self._config = config
        self._features = feature_builder or FeatureBuilder()
        self._pump = pump_detector or PumpDetector(config)
        self._pullback = pullback_tracker or PullbackTracker(config)
        self._zone = zone_builder or ShortZoneBuilder(config)

    def detect_event(
        self,
        observation: NormalizedMarketObservation,
        *,
        features: SymbolFeatures | None = None,
    ) -> BaselineLifecycleResult:
        observation.validate()
        now = _utc(observation.decision_time_utc)
        current = features or self._features.build(observation.symbol, observation.candles, market_asof=now)
        state = self._pump.build_event(observation.symbol, observation.candles, current, now)
        transitions: tuple[TransitionRecord, ...] = ()
        if state is not None:
            transitions = (self._transition(
                state, EventStatus.IDLE.value, "EVENT_CREATED", now,
                state.event_high_time, state.trigger_window, 0,
            ),)
        return BaselineLifecycleResult(state, current, None, None, transitions, observation.max_input_availability_time_utc)

    def mark_signal_sent(self, state: EventState, signal_id: int, when: datetime) -> EventState:
        """Delegate the terminal marker to the production pullback tracker."""

        return self._pullback.mark_signal_sent(state, signal_id=signal_id, when=when)

    def advance_active_event(
        self,
        observation: NormalizedMarketObservation,
        state: EventState,
        *,
        derivatives: Mapping[str, Any] | None = None,
        liquidity: Mapping[str, Any] | None = None,
        features: SymbolFeatures | None = None,
    ) -> BaselineLifecycleResult:
        observation.validate()
        now = _utc(observation.decision_time_utc)
        current = features or self._features.build(
            observation.symbol, observation.candles, state=state,
            derivatives=derivatives, market_asof=now,
        )
        return self._advance_active_event_core(
            observation.symbol, state, now, current,
            derivatives=derivatives, liquidity=liquidity,
            availability=observation.max_input_availability_time_utc,
            candles=observation.candles,
        )

    def advance_active_event_from_features(
        self,
        state: EventState,
        features: SymbolFeatures,
        when: datetime,
        *,
        derivatives: Mapping[str, Any] | None = None,
        liquidity: Mapping[str, Any] | None = None,
    ) -> BaselineLifecycleResult:
        """Advance using an already-built production feature snapshot.

        Live characterization tests and a few legacy adapters inject a feature
        object while deliberately passing a non-frame sentinel.  Keeping this
        compatibility entry point on the shared lifecycle avoids reproducing
        state-transition logic in ``ShortSignalBot`` while the normalized replay
        API remains strict about market observations.
        """

        now = _utc(when)
        return self._advance_active_event_core(
            features.symbol, state, now, features,
            derivatives=derivatives, liquidity=liquidity,
            availability=None, candles=None,
        )

    def _advance_active_event_core(
        self,
        symbol: str,
        state: EventState,
        now: datetime,
        current: SymbolFeatures,
        *,
        derivatives: Mapping[str, Any] | None,
        liquidity: Mapping[str, Any] | None,
        availability: datetime | None,
        candles: Any | None,
    ) -> BaselineLifecycleResult:
        working = deepcopy(state)
        before = deepcopy(working)
        transitions: list[TransitionRecord] = []
        if self._pullback.reset_after_confirmed_high(working, current, now):
            transitions.append(self._transition(
                working, before.state.value, "NEW_HIGH_RESET", now,
                current.last_high_time, "confirmed higher 1m high", 0,
            ))
            return BaselineLifecycleResult(working, current, None, None, tuple(transitions), availability)

        self._pullback.advance(working, current, now)
        if working.state != before.state:
            transitions.append(self._transition(
                working, before.state.value, "STATE_ADVANCE", now,
                current.last_structural_close_time, None, len(transitions),
            ))
        if working.state is EventStatus.EXPIRED or working.state is EventStatus.PUMP_DETECTED:
            return BaselineLifecycleResult(working, current, None, None, tuple(transitions), availability)

        zone = self._zone.build(working, current)
        if zone is None:
            return BaselineLifecycleResult(working, current, None, None, tuple(transitions), availability)
        working.zone_low = zone.low
        working.zone_high = zone.high
        final_features = current if candles is None else self._features.build(
            symbol, candles, state=working,
            derivatives=derivatives, liquidity=liquidity, market_asof=now,
        )
        if zone.low <= final_features.price <= zone.high and working.state is EventStatus.PULLBACK_OBSERVED:
            working.state = EventStatus.SHORT_ZONE_ACTIVE
            working.updated_at = now
            transitions.append(self._transition(
                working, EventStatus.PULLBACK_OBSERVED.value, "ZONE_ENTERED", now,
                final_features.last_structural_close_time, None, len(transitions),
            ))
        item = BaselineStrategyInput(working, final_features, zone, now)
        return BaselineLifecycleResult(working, final_features, zone, item, tuple(transitions), availability)

    @staticmethod
    def _transition(
        state: EventState, from_state: str, trigger: str, now: datetime,
        source_market_time: datetime | None, reason: str | None, sequence: int,
    ) -> TransitionRecord:
        return TransitionRecord(
            symbol=state.symbol, event_id=state.event_id,
            transition_time_utc=now, from_state=from_state,
            to_state=state.state.value, trigger=trigger,
            source_market_time=source_market_time, reason=reason, sequence=sequence,
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
