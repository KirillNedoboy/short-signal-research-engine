"""Thin offline adapters around production and pure decision functions."""

from __future__ import annotations
from typing import Any
import pandas as pd
from app.replay.contracts import (
    BASELINE_PULLBACK,
    BaselineStrategyInput,
    ClimaxStrategyInput,
    StrategyInput,
    TrappedLongsV2StrategyInput,
)
from app.signals.climax import evaluate_climax_bundle
from app.signals.engine import SignalEngine
from app.signals.trapped_longs_v2 import (
    FrozenBreakout,
    OiObservation,
    evaluate_trapped_longs_reversal_v2,
)


def evaluate_strategy_input(strategy_input: StrategyInput, config: Any) -> Any:
    if isinstance(strategy_input, TrappedLongsV2StrategyInput):
        frame = pd.DataFrame(strategy_input.frame_rows)
        oi = tuple(
            OiObservation(
                pd.Timestamp(item["observed_at"]).to_pydatetime(), item["value"]
            )
            for item in strategy_input.oi_observations
        )
        result = evaluate_trapped_longs_reversal_v2(
            frame,
            FrozenBreakout(
                strategy_input.breakout_identity,
                strategy_input.breakout_at,
                strategy_input.breakout_reference,
                strategy_input.revision,
                strategy_input.symbol,
            ),
            oi,
            strategy_input.decision_time_utc,
            config=config,
        )
        return {
            "identity": result.identity,
            "revision": result.revision,
            "actionable": result.actionable,
            "blockers": result.blockers,
            "explanation": result.explanation,
        }
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
        frame = (
            strategy_input.frame_ref
            if isinstance(strategy_input.frame_ref, pd.DataFrame)
            else pd.DataFrame(strategy_input.frame_ref)
        )
        return evaluate_climax_bundle(
            strategy_input.state,
            strategy_input.features,
            frame,
            config,
            strict_closed_candles=True,
        )
    raise TypeError(f"unsupported strategy input: {type(strategy_input).__name__}")
