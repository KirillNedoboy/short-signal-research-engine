"""Thin offline adapters around the unchanged production decision functions."""

from __future__ import annotations

from typing import Any

import pandas as pd

from app.replay.contracts import BASELINE_PULLBACK, BaselineStrategyInput, ClimaxStrategyInput, StrategyInput
from app.signals.climax import evaluate_climax_bundle
from app.signals.engine import SignalEngine


def evaluate_strategy_input(strategy_input: StrategyInput, config: Any) -> Any:
    """Evaluate one replay input solely through the live strategy evaluators."""

    if isinstance(strategy_input, BaselineStrategyInput):
        if strategy_input.strategy != BASELINE_PULLBACK:
            raise ValueError("baseline input has an invalid strategy")
        return SignalEngine(config).analyze(
            strategy_input.state,
            strategy_input.features,
            strategy_input.short_zone,
            strategy_input.decision_time_utc,
        )
    if isinstance(strategy_input, ClimaxStrategyInput):
        frame = strategy_input.frame_ref
        if not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)
        return evaluate_climax_bundle(
            strategy_input.state,
            strategy_input.features,
            frame,
            config,
            strict_closed_candles=True,
        )
    raise TypeError(f"unsupported strategy input: {type(strategy_input).__name__}")
