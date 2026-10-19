"""Pure adapter for the legacy BASELINE_PULLBACK evaluator."""

from __future__ import annotations

from typing import Any

from app.config import AppConfig
from app.replay.contracts import BASELINE_PULLBACK, BaselineStrategyInput
from app.replay.evaluator import evaluate_strategy_input

from .contracts import StrategyContext, StrategyEvaluation


class BaselineAdapter:
    """Map the existing baseline evaluator into the canonical contract.

    Lifecycle transitions and admission rules remain owned by the existing
    baseline lifecycle and signal engine.  This class only constructs the
    evaluator input and translates its result.
    """

    strategy_type = BASELINE_PULLBACK

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        if not isinstance(context, StrategyContext):
            raise TypeError("baseline adapter context must be a StrategyContext")

        if context.short_zone is None:
            return self._blocked_missing_input(context)

        legacy = evaluate_strategy_input(
            BaselineStrategyInput(
                state=context.event_state,
                features=context.features,
                short_zone=context.short_zone,
                decision_time_utc=context.decision_timestamp,
            ),
            self._config,
        )
        decision = legacy.decision
        metadata: dict[str, Any] = dict(decision.strategy_metadata) if decision is not None else {}
        metadata["strategy_config_fingerprint"] = context.strategy_config_fingerprint

        return StrategyEvaluation(
            strategy_type=self.strategy_type,
            strategy_subtype=(decision.strategy_subtype if decision is not None else None),
            model_version=(decision.model_version if decision is not None else "baseline-v1"),
            actionable=bool(decision is not None and decision.actionable),
            score=decision.score if decision is not None else legacy.score,
            grade=decision.grade if decision is not None else legacy.grade,
            reasons=(decision.reasons if decision is not None else legacy.reject_reasons),
            blockers=(decision.blockers if decision is not None else legacy.blockers),
            vetoes=legacy.reject_reasons,
            risk_flags=(decision.risk_flags if decision is not None else legacy.risk_flags),
            strategy_metadata=metadata,
            symbol=context.symbol,
            event_id=context.event_id,
            decision_timestamp=context.decision_timestamp,
            input_quality=context.input_quality,
        )

    def _blocked_missing_input(self, context: StrategyContext) -> StrategyEvaluation:
        reason = "short_zone_missing"
        return StrategyEvaluation(
            strategy_type=self.strategy_type,
            strategy_subtype=None,
            model_version="baseline-v1",
            actionable=False,
            score=0,
            grade="C",
            reasons=[reason],
            blockers=[reason],
            vetoes=[reason],
            risk_flags=[],
            strategy_metadata={
                "strategy_config_fingerprint": context.strategy_config_fingerprint,
            },
            symbol=context.symbol,
            event_id=context.event_id,
            decision_timestamp=context.decision_timestamp,
            input_quality=context.input_quality,
        )


__all__ = ["BaselineAdapter"]
