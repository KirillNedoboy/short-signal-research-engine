"""Pure strategy adapter protocol and fail-closed output boundary."""

from __future__ import annotations

from dataclasses import replace
import re
from typing import Protocol, runtime_checkable

from .contracts import (
    BLOCKED_INPUT_QUALITIES,
    StrategyContext,
    StrategyEvaluation,
    StrategyEvaluationError,
)

_IDENTIFIER = re.compile(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*\Z")


@runtime_checkable
class StrategyAdapter(Protocol):
    """Side-effect-free strategy evaluator at the canonical boundary."""

    strategy_type: str

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        ...


def validate_strategy_identifier(strategy_type: object) -> str:
    """Validate and return the stable identifier used by an adapter."""
    if not isinstance(strategy_type, str) or not _IDENTIFIER.fullmatch(strategy_type):
        raise StrategyEvaluationError(
            "strategy_type must be a non-empty uppercase identifier using underscores"
        )
    return strategy_type


def normalize_strategy_evaluation(
    adapter: StrategyAdapter,
    context: StrategyContext,
    evaluation: object,
) -> StrategyEvaluation:
    """Validate an adapter result and make unsafe actionability fail closed."""
    if not isinstance(context, StrategyContext):
        raise StrategyEvaluationError("adapter context must be a StrategyContext")
    if not callable(getattr(adapter, "evaluate", None)):
        raise StrategyEvaluationError("adapter must expose a callable evaluate")
    adapter_type = validate_strategy_identifier(getattr(adapter, "strategy_type", None))
    if not isinstance(adapter, StrategyAdapter):
        raise StrategyEvaluationError("adapter must implement StrategyAdapter")

    if not isinstance(evaluation, StrategyEvaluation):
        raise StrategyEvaluationError("adapter output must be a StrategyEvaluation")
    if evaluation.strategy_type != adapter_type:
        raise StrategyEvaluationError("adapter output strategy_type does not match adapter")
    if evaluation.symbol != context.symbol or evaluation.event_id != context.event_id:
        raise StrategyEvaluationError("adapter output identity does not match context")
    if evaluation.decision_timestamp != context.decision_timestamp:
        raise StrategyEvaluationError("adapter output timestamp does not match context")

    # A blocked context can never produce an actionable result, even if an
    # adapter forgot to propagate its input quality into the evaluation.
    if context.input_quality in BLOCKED_INPUT_QUALITIES and evaluation.actionable:
        return replace(evaluation, actionable=False)
    return evaluation


# Explicit alias for callers that treat this as a validation-only boundary.
validate_adapter_output = normalize_strategy_evaluation


__all__ = [
    "StrategyAdapter",
    "normalize_strategy_evaluation",
    "validate_adapter_output",
    "validate_strategy_identifier",
]
