"""C strategy pipeline adapter used by the event-driven runtime.

    The feature, lifecycle, and strategy formulas are imported from the frozen
    C source tree copied into this release. Runtime scheduling is kept outside
    this module, so delivery/outcome/research work cannot extend evaluation.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import pandas as pd

from app.baseline.lifecycle import BaselineLifecycle
from app.config import AppConfig
from app.domain import EventState, EventStatus, SignalDecision, SignalType, SymbolFeatures
from app.events.pullback_tracker import PullbackTracker
from app.events.pump_detector import PumpDetector
from app.events.short_zone import ShortZoneBuilder
from app.features.builder import FeatureBuilder
from app.replay.market import CandleObservationKind, NormalizedMarketObservation
from app.strategies.baseline import BaselineAdapter
from app.strategies.climax import (
    ClimaxAdapter,
    LowVolumeExtensionFailureAdapter,
    VolumeClimaxUnwindAdapter,
)
from app.strategies.contracts import StrategyContext, StrategyEvaluation
from app.strategies.registry import StrategyRegistry
from app.signals.trapped_longs import trapped_longs_root_event_id
from app.strategies.trapped_longs import TrappedLongsAdapter
from app.runtime.legacy_engine import AdmissionResult, LegacyCEvaluationEngine


@dataclass(frozen=True, slots=True)
class PipelineResult:
    state: EventState | None
    features: SymbolFeatures | None
    decision: SignalDecision | None
    evaluations: tuple[StrategyEvaluation, ...] = ()
    blocked_reason: str | None = None
    # A provider-backed fresh recheck is kept out of admission fields but is
    # exposed to the shadow capture boundary as immutable evidence.  The
    # composition root may omit it from the candidate-result payload and store
    # it once under capture context instead.
    final_recheck: Any | None = None
    final_recheck_captured: bool = False


class LaneCStrategyPipeline:
    """Preserve C's lifecycle and formulas behind a small runtime seam."""

    def __init__(self, config: AppConfig, *, config_fingerprint: str = "unknown") -> None:
        self.config = config
        self.config_fingerprint = config_fingerprint
        self.features = FeatureBuilder()
        self.lifecycle = BaselineLifecycle(
            config,
            self.features,
            pump_detector=PumpDetector(config),
            pullback_tracker=PullbackTracker(config),
            zone_builder=ShortZoneBuilder(config),
        )
        self.engine = LegacyCEvaluationEngine(
            config,
            config_fingerprint=config_fingerprint,
        )

    def evaluate(
        self,
        symbol: str,
        frame_1m: pd.DataFrame,
        state: EventState | None,
        *,
        derivatives: Mapping[str, Any] | None = None,
        liquidity: Mapping[str, Any] | None = None,
        decision_at: datetime | None = None,
        fast_monitor: bool = False,
        poll_sequence: int | None = None,
    ) -> PipelineResult:
        if frame_1m.empty:
            return PipelineResult(state, None, None, blocked_reason="EMPTY_FRAME")
        now = decision_at or datetime.now(timezone.utc)
        features = self.features.build(
            symbol,
            frame_1m,
            state=state,
            derivatives=derivatives,
            # The frozen C path creates the root event from candle and
            # derivatives inputs first.  Liquidity is captured for the active
            # event evaluation; passing it into the initial event snapshot
            # would change persisted state/features without changing the
            # strategy decision.
            liquidity=liquidity if state is not None else None,
            market_asof=now,
        )
        if state is None or state.state in {EventStatus.IDLE, EventStatus.EXPIRED}:
            observation = NormalizedMarketObservation(
                symbol,
                features.asof,
                CandleObservationKind.PARTIAL_ASOF_CANDLE,
                frame_1m,
            )
            result = self.lifecycle.detect_event(observation, features=features)
            if result.state is None or not self.config.climax_short_enabled:
                return PipelineResult(result.state, result.features, None)
            # The legacy runtime rebuilt the root snapshot with liquidity before
            # its early climax check.  Keep that ordering while making the
            # check a pure engine call.
            root_features = self.features.build(
                symbol,
                frame_1m,
                state=result.state,
                derivatives=derivatives,
                liquidity=liquidity,
                market_asof=now,
            )
            admission = self.engine.evaluate(
                state=result.state,
                features=root_features,
                frame_1m=frame_1m,
                short_zone=None,
                decision_at=now,
                allow_climax=True,
                allow_trapped=False,
                allow_baseline=False,
                fast_monitor=fast_monitor,
                poll_sequence=poll_sequence,
            )
            return PipelineResult(
                result.state,
                root_features,
                admission.decision,
                admission.evaluations,
                admission.blocked_reason,
            )

        # C evaluates the climax contour before advancing the baseline state.
        # This is the fast-monitor/pre-admission seam; it cannot emit anything
        # itself and only returns a deferred decision to the caller.
        if self.config.climax_short_enabled:
            early = self.engine.evaluate(
                state=state,
                features=features,
                frame_1m=frame_1m,
                short_zone=None,
                decision_at=now,
                allow_climax=True,
                allow_trapped=False,
                allow_baseline=False,
                fast_monitor=fast_monitor,
                poll_sequence=poll_sequence,
            )
            if early.decision is not None:
                return PipelineResult(
                    state,
                    features,
                    early.decision,
                    early.evaluations,
                    early.blocked_reason,
                )

        observation = NormalizedMarketObservation(
            symbol,
            features.asof,
            CandleObservationKind.PARTIAL_ASOF_CANDLE,
            frame_1m,
        )
        result = self.lifecycle.advance_active_event(
            observation,
            state,
            derivatives=derivatives,
            liquidity=liquidity,
            features=features,
        )
        next_state = result.state
        final_features = result.features
        if next_state is None or final_features is None:
            return PipelineResult(next_state, final_features, None)
        if next_state.state is EventStatus.EXPIRED:
            return PipelineResult(next_state, final_features, None, blocked_reason="EXPIRED")
        if result.short_zone is None:
            return PipelineResult(next_state, final_features, None, blocked_reason="ZONE_NOT_READY")

        if self.config.trapped_longs_reversal_enabled:
            attempt_created_at, confirmation_expires_at = self._trapped_timing(
                next_state,
                now,
            )
        else:
            attempt_created_at = confirmation_expires_at = None
        admission = self.engine.evaluate(
            state=next_state,
            features=final_features,
            frame_1m=frame_1m,
            short_zone=result.short_zone,
            decision_at=now,
            allow_climax=self.config.climax_short_enabled,
            allow_trapped=self.config.trapped_longs_reversal_enabled,
            allow_baseline=True,
            trapped_attempt_created_at=attempt_created_at,
            trapped_confirmation_expires_at=confirmation_expires_at,
            fast_monitor=fast_monitor,
            poll_sequence=poll_sequence,
        )
        return PipelineResult(
            next_state,
            final_features,
            admission.decision,
            admission.evaluations,
            admission.blocked_reason,
        )

    def evaluate_climax_only(
        self,
        symbol: str,
        frame_1m: pd.DataFrame,
        state: EventState | None,
        *,
        derivatives: Mapping[str, Any] | None = None,
        liquidity: Mapping[str, Any] | None = None,
        decision_at: datetime | None = None,
        poll_sequence: int | None = None,
    ) -> PipelineResult:
        """Evaluate only the C climax contour for a fast-monitor candidate.

        The full 60-second pipeline owns lifecycle advancement and the complete
        five-strategy admission registry.  The bounded fast task is a separate
        pre-admission read of an already tracked event: it must not instantiate
        baseline or trapped-long adapters, advance lifecycle state, or perform
        persistence/delivery work.  Keeping this helper on the same pipeline
        object makes the fast callback use the same feature builder, climax
        adapter, decision construction, and config fingerprint as production.
        """

        if frame_1m.empty:
            return PipelineResult(state, None, None, blocked_reason="EMPTY_FRAME")
        if state is None or state.state in {EventStatus.IDLE, EventStatus.EXPIRED}:
            return PipelineResult(state, None, None, blocked_reason="CLIMAX_STATE_INACTIVE")

        now = decision_at or datetime.now(timezone.utc)
        features = self.features.build(
            symbol,
            frame_1m,
            state=state,
            derivatives=derivatives,
            liquidity=liquidity,
            market_asof=now,
        )
        if features.market_asof is None:
            return PipelineResult(
                state,
                features,
                None,
                blocked_reason="MARKET_ASOF_MISSING",
            )

        context = StrategyContext(
            symbol=features.symbol,
            event_id=state.event_id,
            event_state=state,
            features=features,
            short_zone=None,
            decision_timestamp=now,
            strategy_config_fingerprint=self.config_fingerprint,
            input_quality="VALID",
        )
        # ClimaxAdapter is the canonical bundle adapter used by the complete
        # registry.  Calling it directly preserves its branch selection while
        # avoiding BaselineAdapter, TrappedLongsAdapter, and their formulas in
        # the separate fast task.
        evaluation = ClimaxAdapter(self.config, frame_1m).evaluate(context)
        evaluations = (evaluation,)
        if not evaluation.actionable:
            return PipelineResult(
                state,
                features,
                None,
                evaluations,
                # The frozen fast callback returns no decision and leaves the
                # admission reason unset for a clean no-action contour.  Keep
                # this projection aligned with the replay boundary; a blocked
                # market-data input still receives an explicit reason above.
                None,
            )

        decision = self.engine._decision_from_evaluation(
            state=state,
            features=features,
            zone=None,
            signal_time=features.asof,
            evaluation=evaluation,
            fast_monitor=True,
            poll_sequence=poll_sequence,
        )
        return PipelineResult(state, features, decision, evaluations)

    def evaluate_envelope(
        self,
        *,
        state: EventState,
        features: SymbolFeatures,
        frame_1m: pd.DataFrame,
        short_zone: Any,
        decision_at: datetime,
        fast_monitor: bool = False,
        poll_sequence: int | None = None,
        trapped_attempt_created_at: datetime | None = None,
        trapped_confirmation_expires_at: datetime | None = None,
    ) -> PipelineResult:
        """Replay a validated C state/features envelope through the hot path.

        Production calls use :meth:`evaluate`, which owns feature building and
        lifecycle transitions. This explicit envelope seam is for deterministic
        parity fixtures and never bypasses registry/admission/persistence
        rules; it simply starts after the market snapshot has been captured.
        """

        admission = self.engine.evaluate(
            state=state,
            features=features,
            frame_1m=frame_1m,
            short_zone=short_zone,
            decision_at=decision_at,
            allow_climax=self.config.climax_short_enabled,
            allow_trapped=self.config.trapped_longs_reversal_enabled,
            allow_baseline=True,
            trapped_attempt_created_at=trapped_attempt_created_at,
            trapped_confirmation_expires_at=trapped_confirmation_expires_at,
            fast_monitor=fast_monitor,
            poll_sequence=poll_sequence,
        )
        return PipelineResult(
            state,
            features,
            admission.decision,
            admission.evaluations,
            admission.blocked_reason,
        )

    async def finalize_admission(
        self,
        result: PipelineResult,
        *,
        provider: Any,
        symbol: str,
        frame_1m: pd.DataFrame,
        derivatives: Mapping[str, Any] | None,
        decision_at: datetime,
        fast_monitor: bool = False,
        poll_sequence: int | None = None,
    ) -> PipelineResult:
        """Apply C's provider-backed final recheck before persistence.

        Historical C performs a fresh frame/liquidity capture for an
        actionable low-volume extension. The recheck is intentionally outside
        the pure engine so it can use provider I/O while retaining the same
        registry and veto semantics. Other strategies keep the first captured
        decision unchanged.
        """

        decision = result.decision
        if (
            decision is None
            or decision.strategy_subtype != "LOW_VOLUME_EXTENSION_FAILURE"
            or result.state is None
        ):
            return result

        attempts = max(1, int(getattr(self.config, "climax_fresh_recheck_attempts", 2)))
        delay = max(
            0.0,
            float(
                getattr(self.config, "climax_fresh_recheck_retry_delay_sec", 3.0)
            ),
        )
        last_blocked = "LOW_VOLUME_FINAL_RECHECK_DATA_MISSING"
        last_recheck: Any | None = None
        for attempt in range(1, attempts + 1):
            fresh = await provider.capture_decision_snapshot(
                symbol,
                frame_1m=None,
                include_liquidity=True,
                derivatives=derivatives,
            )
            if fresh is not None:
                last_recheck = fresh
            if fresh is None or fresh.frame_1m.frame.empty:
                last_blocked = "LOW_VOLUME_FINAL_RECHECK_DATA_MISSING"
            elif not _liquidity_snapshot_ready(fresh.liquidity):
                last_blocked = "LOW_VOLUME_FINAL_RECHECK_LIQUIDITY_MISSING"
            else:
                fresh_features = self.features.build(
                    symbol,
                    fresh.frame_1m.frame,
                    state=result.state,
                    derivatives=fresh.derivatives,
                    liquidity=fresh.liquidity,
                    market_asof=fresh.captured_at_utc,
                )
                admission = self.engine.evaluate(
                    state=result.state,
                    features=fresh_features,
                    frame_1m=fresh.frame_1m.frame,
                    short_zone=None,
                    decision_at=fresh.captured_at_utc,
                    allow_climax=True,
                    allow_trapped=False,
                    allow_baseline=False,
                    fast_monitor=fast_monitor,
                    poll_sequence=poll_sequence,
                )
                fresh_decision = admission.decision
                if (
                    fresh_decision is not None
                    and fresh_decision.strategy_subtype
                    == "LOW_VOLUME_EXTENSION_FAILURE"
                ):
                    return PipelineResult(
                        result.state,
                        fresh_features,
                        fresh_decision,
                        admission.evaluations,
                        admission.blocked_reason,
                        final_recheck=fresh,
                        final_recheck_captured=True,
                    )
                last_blocked = (
                    "LOW_VOLUME_FINAL_RECHECK_REJECTED"
                    if fresh_decision is None
                    else "LOW_VOLUME_FINAL_RECHECK_STRATEGY_CHANGED"
                )
            if attempt < attempts and delay:
                await asyncio.sleep(delay)
        return PipelineResult(
            result.state,
            result.features,
            None,
            result.evaluations,
            last_blocked,
            final_recheck=last_recheck,
            final_recheck_captured=last_recheck is not None,
        )

    @staticmethod
    def _trapped_timing(
        state: EventState,
        decision_at: datetime,
    ) -> tuple[datetime, datetime]:
        """Recover the bounded confirmation window without DB access."""

        snapshot = state.event_features_snapshot or {}
        created = _parse_datetime(snapshot.get("trapped_longs_attempt_created_at"))
        if created is None:
            created = state.event_start_time or state.event_high_time or decision_at
        expires = _parse_datetime(snapshot.get("trapped_longs_confirmation_expires_at"))
        if expires is None:
            breakout = state.event_high_time or created
            expires = breakout + timedelta(minutes=15)
        return created, expires

    @staticmethod
    def _decision_from_evaluation(
        *,
        state: EventState,
        features: SymbolFeatures,
        zone: Any,
        signal_time: datetime,
        evaluation: StrategyEvaluation,
    ) -> SignalDecision:
        return SignalDecision(
            symbol=features.symbol,
            event_id=state.event_id,
            signal_type=(
                SignalType.WATCH
                if evaluation.strategy_metadata.get("signal_type") == SignalType.WATCH.value
                else SignalType.CONFIRM
            ),
            grade=evaluation.grade,
            score=evaluation.score,
            market_price=features.price,
            short_zone_low=zone.low,
            short_zone_high=zone.high,
            signal_time=signal_time,
            reasons=list(evaluation.reasons),
            risk_flags=list(evaluation.risk_flags),
            features_snapshot=asdict(features),
            score_breakdown={"registry_score": float(evaluation.score)},
            decision_type="SIGNAL",
            actionable=evaluation.actionable,
            lifecycle_state=state.state.value,
            blockers=list(evaluation.blockers),
            strategy_type=evaluation.strategy_type,
            strategy_subtype=evaluation.strategy_subtype,
            model_version=evaluation.model_version or "baseline-v1",
            strategy_metadata=dict(evaluation.strategy_metadata),
        )

    def _evaluate_registry(
        self,
        state: EventState,
        features: SymbolFeatures,
        frame_1m: pd.DataFrame,
        zone: Any,
        decision_at: datetime,
    ) -> tuple[StrategyEvaluation, ...]:
        """Compatibility seam for tests that need the complete registry."""
        return self.engine._evaluate_registry(
            state=state,
            features=features,
            frame_1m=frame_1m,
            short_zone=zone,
            decision_at=decision_at,
            trapped_attempt_created_at=None,
            trapped_confirmation_expires_at=None,
        )


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    return None


def _liquidity_snapshot_ready(liquidity: Mapping[str, Any] | None) -> bool:
    if not liquidity:
        return False
    return all(
        liquidity.get(name) is not None
        for name in (
            "spread_pct",
            "slippage_pct",
            "orderbook_depth_usdt_1pct",
            "orderbook_depth_usdt_2pct",
        )
    )


__all__ = ["LaneCStrategyPipeline", "PipelineResult"]

