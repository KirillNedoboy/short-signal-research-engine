"""Pure C evaluation/admission engine used by the event-driven runtime.

The historical C runtime mixed this work with HTTP, persistence, and Telegram
delivery.  This module keeps the old ordering and formulas at one seam while
leaving those side effects to the composition root:

* climax branches are checked before the baseline lifecycle;
* the baseline lifecycle advances only after the early climax admission;
* trapped-longs is evaluated after a short zone is available;
* baseline is the final fallback;
* a selected decision passes the same delivery-policy and provenance checks
  before it becomes a deferred delivery intent.

The engine never writes a repository, sends a message, or performs network I/O.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import pandas as pd

from app.config import AppConfig
from app.domain import EventState, SignalDecision, SignalType, ShortZone, SymbolFeatures
from app.signals.climax import advance_volume_climax_lifecycle
from app.signals.delivery_policy import live_delivery_enabled
from app.signals.trapped_longs import (
    advance_trapped_longs_lifecycle,
    trapped_longs_root_event_id,
)
from app.strategies.baseline import BaselineAdapter
from app.strategies.climax import (
    ClimaxAdapter,
    LowVolumeExtensionFailureAdapter,
    VolumeClimaxUnwindAdapter,
)
from app.strategies.contracts import StrategyContext, StrategyEvaluation
from app.strategies.registry import (
    CLIMAX_EXHAUSTION,
    LOW_VOLUME_EXTENSION_FAILURE,
    TRAPPED_LONGS_REVERSAL,
    VOLUME_CLIMAX_UNWIND,
    StrategyRegistry,
)
from app.strategies.trapped_longs import TrappedLongsAdapter


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    """Result of evaluating the complete registry and final admission gate."""

    decision: SignalDecision | None
    evaluations: tuple[StrategyEvaluation, ...]
    selected_strategy: str | None = None
    selected_subtype: str | None = None
    blocked_reason: str | None = None


class LegacyCEvaluationEngine:
    """Keep C's evaluation order while exposing no side-effecting I/O."""

    def __init__(self, config: AppConfig, *, config_fingerprint: str) -> None:
        self.config = config
        self.config_fingerprint = config_fingerprint

    def evaluate(
        self,
        *,
        state: EventState,
        features: SymbolFeatures,
        frame_1m: pd.DataFrame,
        short_zone: ShortZone | None,
        decision_at: datetime,
        allow_climax: bool,
        allow_trapped: bool,
        allow_baseline: bool,
        trapped_attempt_created_at: datetime | None = None,
        trapped_confirmation_expires_at: datetime | None = None,
        fast_monitor: bool = False,
        poll_sequence: int | None = None,
    ) -> AdmissionResult:
        evaluations = self._evaluate_registry(
            state=state,
            features=features,
            frame_1m=frame_1m,
            short_zone=short_zone,
            decision_at=decision_at,
            trapped_attempt_created_at=trapped_attempt_created_at,
            trapped_confirmation_expires_at=trapped_confirmation_expires_at,
        )
        self._advance_shadow_lifecycles(
            state=state,
            features=features,
            evaluations=evaluations,
            observed_at=features.asof,
        )

        selected = self._select_evaluation(
            evaluations,
            allow_climax=allow_climax,
            allow_trapped=allow_trapped,
            allow_baseline=allow_baseline,
        )
        if selected is None:
            return AdmissionResult(
                decision=None,
                evaluations=evaluations,
                blocked_reason="NO_ACTIONABLE_STRATEGY",
            )

        decision = self._decision_from_evaluation(
            state=state,
            features=features,
            zone=short_zone,
            signal_time=features.asof,
            evaluation=selected,
            fast_monitor=fast_monitor,
            poll_sequence=poll_sequence,
        )
        blocked = self._final_admission(
            decision,
            selected,
            state=state,
            features=features,
            evaluations=evaluations,
        )
        if blocked is not None:
            return AdmissionResult(
                decision=None,
                evaluations=evaluations,
                selected_strategy=selected.strategy_type,
                selected_subtype=selected.strategy_subtype,
                blocked_reason=blocked,
            )
        return AdmissionResult(
            decision=decision,
            evaluations=evaluations,
            selected_strategy=selected.strategy_type,
            selected_subtype=selected.strategy_subtype,
        )

    def _evaluate_registry(
        self,
        *,
        state: EventState,
        features: SymbolFeatures,
        frame_1m: pd.DataFrame,
        short_zone: ShortZone | None,
        decision_at: datetime,
        trapped_attempt_created_at: datetime | None,
        trapped_confirmation_expires_at: datetime | None,
    ) -> tuple[StrategyEvaluation, ...]:
        if features.market_asof is None:
            return ()
        context = StrategyContext(
            symbol=features.symbol,
            event_id=state.event_id,
            event_state=state,
            features=features,
            short_zone=short_zone,
            decision_timestamp=_aware(decision_at),
            strategy_config_fingerprint=self.config_fingerprint,
            input_quality="VALID",
        )
        registry = StrategyRegistry(
            [
                BaselineAdapter(self.config),
                ClimaxAdapter(self.config, frame_1m),
                VolumeClimaxUnwindAdapter(self.config, frame_1m),
                LowVolumeExtensionFailureAdapter(self.config, frame_1m),
                TrappedLongsAdapter(
                    self.config,
                    frame_1m,
                    attempt_created_at=trapped_attempt_created_at,
                    confirmation_expires_at=trapped_confirmation_expires_at,
                ),
            ]
        )
        return registry.evaluate_all(context)

    @staticmethod
    def _select_evaluation(
        evaluations: tuple[StrategyEvaluation, ...],
        *,
        allow_climax: bool,
        allow_trapped: bool,
        allow_baseline: bool,
    ) -> StrategyEvaluation | None:
        """Use the old C priority: climax, trapped-longs, then baseline."""

        by_type = {item.strategy_type: item for item in evaluations}
        ordered: list[StrategyEvaluation] = []
        if allow_climax:
            climax = by_type.get(CLIMAX_EXHAUSTION)
            if climax is not None:
                ordered.append(climax)
        if allow_trapped:
            trapped = by_type.get(TRAPPED_LONGS_REVERSAL)
            if trapped is not None:
                ordered.append(trapped)
        if allow_baseline:
            baseline = by_type.get("BASELINE_PULLBACK")
            if baseline is not None:
                ordered.append(baseline)
        return next((item for item in ordered if item.actionable), None)

    def _decision_from_evaluation(
        self,
        *,
        state: EventState,
        features: SymbolFeatures,
        zone: ShortZone | None,
        signal_time: datetime,
        evaluation: StrategyEvaluation,
        fast_monitor: bool,
        poll_sequence: int | None,
    ) -> SignalDecision:
        if evaluation.strategy_type == CLIMAX_EXHAUSTION:
            high = _positive_float(
                evaluation.strategy_metadata.get("event_high")
            ) or features.price
            short_zone_low = high * 0.97
            short_zone_high = high
            reasons = [
                "Climax exhaustion confirmed",
                "Failed continuation below event high",
            ]
            risk_flags = (
                ["Повышенный риск ликвидности"]
                if evaluation.strategy_metadata.get("liquidity_warning")
                else []
            )
            lifecycle_state = "CLIMAX_SIGNAL_SENT"
            score_breakdown = {"climax_v1": float(evaluation.score)}
            metadata = {
                **evaluation.strategy_metadata,
                "veto_reasons": list(evaluation.vetoes),
            }
        elif evaluation.strategy_type == TRAPPED_LONGS_REVERSAL:
            reference = _positive_float(
                evaluation.strategy_metadata.get("breakout_reference")
            ) or features.price
            short_zone_low = reference * 0.99
            short_zone_high = reference * 1.01
            reasons = list(evaluation.reasons)
            risk_flags = list(evaluation.risk_flags)
            lifecycle_state = "TRAPPED_LONGS_ADMITTED"
            score_breakdown = {"registry_score": float(evaluation.score)}
            metadata = dict(evaluation.strategy_metadata)
        else:
            if zone is None:
                raise ValueError("baseline admission requires a short zone")
            short_zone_low = zone.low
            short_zone_high = zone.high
            reasons = list(evaluation.reasons)
            risk_flags = list(evaluation.risk_flags)
            lifecycle_state = state.state.value
            score_breakdown = {"registry_score": float(evaluation.score)}
            metadata = dict(evaluation.strategy_metadata)
        decision_event_id = state.event_id
        if evaluation.strategy_type == TRAPPED_LONGS_REVERSAL:
            # Historical C stores trapped-longs signals under the derived
            # root identity while the event-state row keeps the breakout ID.
            # Keep both identities available to atomic persistence.
            decision_event_id = trapped_longs_root_event_id(
                features.symbol, state.event_id
            )
            metadata.setdefault("root_event_id", decision_event_id)
        metadata["strategy_config_fingerprint"] = self.config_fingerprint
        if fast_monitor or poll_sequence is not None:
            metadata.update(
                {
                    "fast_monitor": bool(fast_monitor),
                    "fast_monitor_poll_sequence": poll_sequence,
                }
            )
        return SignalDecision(
            symbol=features.symbol,
            event_id=decision_event_id,
            signal_type=(
                SignalType.WATCH
                if metadata.get("signal_type") == SignalType.WATCH.value
                else SignalType.CONFIRM
            ),
            grade=evaluation.grade,
            score=evaluation.score,
            market_price=features.price,
            short_zone_low=short_zone_low,
            short_zone_high=short_zone_high,
            signal_time=_aware(signal_time),
            reasons=reasons,
            risk_flags=risk_flags,
            features_snapshot=asdict(features),
            score_breakdown=score_breakdown,
            decision_type="SIGNAL",
            actionable=True,
            lifecycle_state=lifecycle_state,
            blockers=list(evaluation.blockers),
            data_quality_warnings=[],
            strategy_type=evaluation.strategy_type,
            strategy_subtype=evaluation.strategy_subtype,
            model_version=evaluation.model_version or "baseline-v1",
            strategy_metadata=metadata,
        )

    def _final_admission(
        self,
        decision: SignalDecision,
        selected: StrategyEvaluation,
        *,
        state: EventState,
        features: SymbolFeatures,
        evaluations: tuple[StrategyEvaluation, ...],
    ) -> str | None:
        """Run the last synchronous checks before persistence/outbox enqueue."""

        if state.signal_id is not None:
            return "SIGNAL_ALREADY_PERSISTED"
        if not selected.actionable or not decision.actionable:
            return "SELECTED_EVALUATION_NOT_ACTIONABLE"
        if not live_delivery_enabled(decision, self.config):
            return "DELIVERY_DISABLED_BY_CONFIG"
        if decision.strategy_type == CLIMAX_EXHAUSTION:
            event_high = _positive_float(decision.strategy_metadata.get("event_high"))
            if event_high is None or event_high < decision.market_price:
                return "CLIMAX_EVENT_HIGH_INVALID"
            stored_distance = decision.strategy_metadata.get("entry_distance_below_high_pct")
            if stored_distance is not None:
                computed = (event_high - decision.market_price) / event_high * 100.0
                tolerance = 0.15 if decision.strategy_subtype == LOW_VOLUME_EXTENSION_FAILURE else 0.25
                try:
                    if abs(computed - float(stored_distance)) > tolerance:
                        return "EVENT_HIGH_METADATA_INCONSISTENT"
                except (TypeError, ValueError):
                    return "EVENT_HIGH_METADATA_INVALID"
        elif decision.strategy_type == TRAPPED_LONGS_REVERSAL:
            reference = _positive_float(decision.strategy_metadata.get("breakout_reference"))
            if reference is None or features.price >= reference:
                return "TRAPPED_LONGS_REFERENCE_INVALID"
        # Registry evaluation is deliberately retained in the result and is
        # checked as a complete set, so a missing branch cannot be mistaken for
        # a clean admission when the adapter contract is changed later.
        if len(evaluations) != 5:
            return "INCOMPLETE_STRATEGY_REGISTRY"
        return None

    def _advance_shadow_lifecycles(
        self,
        *,
        state: EventState,
        features: SymbolFeatures,
        evaluations: tuple[StrategyEvaluation, ...],
        observed_at: datetime,
    ) -> None:
        """Persist lifecycle context in the event snapshot, never in a DB."""

        snapshot = dict(state.event_features_snapshot or {})
        by_type = {item.strategy_type: item for item in evaluations}
        climax = by_type.get(CLIMAX_EXHAUSTION)
        if self.config.volume_climax_lifecycle_shadow_enabled and climax is not None:
            volume_metadata = climax.strategy_metadata.get("volume_climax_metadata") or {}
            if climax.strategy_metadata.get("volume_climax_observed"):
                root_created_at = _datetime_or(
                    state.event_start_time or state.event_high_time or features.asof,
                    observed_at,
                )
                latest_high = _positive_float(snapshot.get("volume_climax_latest_high")) or _positive_float(
                    volume_metadata.get("event_high")
                ) or features.price
                latest_high_at = _datetime_or(
                    snapshot.get("volume_climax_latest_high_at"),
                    state.event_high_time or root_created_at,
                )
                confirmation_started_at = _datetime_or(
                    snapshot.get("volume_climax_confirmation_started_at"),
                    latest_high_at,
                )
                try:
                    lifecycle = advance_volume_climax_lifecycle(
                        root_created_at=root_created_at,
                        latest_high=latest_high,
                        latest_high_at=latest_high_at,
                        confirmation_started_at=confirmation_started_at,
                        last_observed_at=_datetime_or(snapshot.get("volume_climax_last_observed_at"), root_created_at),
                        event_revision=int(snapshot.get("volume_climax_event_revision") or 1),
                        current_high=_positive_float(volume_metadata.get("event_high")) or features.price,
                        observed_at=_aware(observed_at),
                        closed_candles_after_high=int(volume_metadata.get("closed_candles_after_high") or 0),
                        min_closed_candles_after_high=self.config.volume_climax_min_closed_candles_after_high,
                        max_lifetime_minutes=self.config.volume_climax_max_lifetime_minutes,
                        confirmation_window_minutes=self.config.volume_climax_confirmation_window_minutes,
                        price_acceleration_resumed=features.ret_5m > 0,
                        active_short_squeeze=features.oi_change_pct is not None and features.oi_change_pct < 0 and features.ret_5m > 0,
                        oi_continuation=features.oi_change_pct is None or features.oi_change_pct >= 0,
                        rejection_ok=float(volume_metadata.get("rejection_pct") or 0.0) >= self.config.volume_climax_min_rejection_pct,
                        liquidity_ok=features.liquidity_available and not bool(volume_metadata.get("liquidity_warning")),
                        entry_distance_ok=float(volume_metadata.get("entry_distance_below_high_pct") or 999.0) <= self.config.volume_climax_max_entry_distance_below_high_pct,
                    )
                except (TypeError, ValueError, OverflowError):
                    lifecycle = None
                if lifecycle is not None:
                    snapshot.update(
                        {
                            "volume_climax_latest_high": lifecycle.latest_high,
                            "volume_climax_latest_high_at": lifecycle.latest_high_at.isoformat(),
                            "volume_climax_confirmation_started_at": lifecycle.confirmation_started_at.isoformat(),
                            "volume_climax_last_observed_at": lifecycle.last_observed_at.isoformat(),
                            "volume_climax_event_revision": lifecycle.event_revision,
                            "volume_climax_lifecycle_state": lifecycle.state,
                            "volume_climax_lifecycle_vetoes": list(lifecycle.veto_reasons),
                        }
                    )

        trapped = by_type.get(TRAPPED_LONGS_REVERSAL)
        if self.config.trapped_longs_reversal_enabled and trapped is not None:
            metadata = trapped.strategy_metadata
            root_created_at = _datetime_or(
                state.event_start_time or state.event_high_time or features.asof,
                observed_at,
            )
            breakout_at = _datetime_or(state.event_high_time, root_created_at)
            try:
                lifecycle = advance_trapped_longs_lifecycle(
                    root_created_at=root_created_at,
                    breakout_at=breakout_at,
                    observed_at=_aware(observed_at),
                    event_revision=int(snapshot.get("trapped_longs_event_revision") or 1),
                    breakout_confirmed="breakout_not_confirmed" not in trapped.vetoes,
                    oi_confirmed="oi_missing" not in trapped.vetoes and "oi_below_threshold" not in trapped.vetoes,
                    closed_candles=int(metadata.get("closed_structural_candles") or 0),
                    close_below_reference=bool(metadata.get("close_below_breakout_reference")),
                    failed_retest=bool(metadata.get("failed_retest_confirmed")),
                    no_new_high=bool(metadata.get("no_new_high")),
                    liquidity_ok=not any(reason in {"liquidity_unavailable", "liquidity_incomplete", "liquidity_block"} for reason in trapped.vetoes),
                    rejection_ok="rejection_below_threshold" not in trapped.vetoes,
                    max_lifetime_minutes=self.config.trapped_longs_max_lifetime_minutes,
                )
            except (TypeError, ValueError, OverflowError):
                lifecycle = None
            if lifecycle is not None:
                snapshot.update(
                    {
                        "trapped_longs_event_revision": lifecycle.event_revision,
                        "trapped_longs_lifecycle_state": lifecycle.state,
                        "trapped_longs_lifecycle_vetoes": list(lifecycle.veto_reasons),
                        "trapped_longs_attempt_created_at": root_created_at.isoformat(),
                        "trapped_longs_confirmation_expires_at": (
                            breakout_at
                            + timedelta(minutes=self.config.trapped_longs_max_lifetime_minutes)
                        ).isoformat(),
                    }
                )
        state.event_features_snapshot = snapshot


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _positive_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _datetime_or(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, str):
        try:
            return _aware(datetime.fromisoformat(value))
        except ValueError:
            pass
    return _aware(fallback)


__all__ = ["AdmissionResult", "LegacyCEvaluationEngine"]
