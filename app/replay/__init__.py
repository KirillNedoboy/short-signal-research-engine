"""Replay contracts and production-evaluator adapters (no research imports)."""

from app.replay.contracts import (
    BASELINE_PULLBACK,
    LOW_VOLUME_EXTENSION_FAILURE,
    VOLUME_CLIMAX_UNWIND,
    BaselineStrategyInput,
    ClimaxStrategyInput,
    StrategyDecisionRecord,
    StrategyInput,
)
from app.replay.evaluator import evaluate_strategy_input

__all__ = [
    "BASELINE_PULLBACK", "LOW_VOLUME_EXTENSION_FAILURE", "VOLUME_CLIMAX_UNWIND",
    "BaselineStrategyInput", "ClimaxStrategyInput", "StrategyInput", "StrategyDecisionRecord", "evaluate_strategy_input",
]
