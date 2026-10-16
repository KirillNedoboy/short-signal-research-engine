"""Pure adapter for the canonical climax exhaustion evaluator."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pandas as pd

from app.signals.climax import evaluate_climax_bundle

from .contracts import StrategyContext, StrategyEvaluation


CLIMAX_EXHAUSTION = "CLIMAX_EXHAUSTION"
VOLUME_CLIMAX_UNWIND = "VOLUME_CLIMAX_UNWIND"
LOW_VOLUME_EXTENSION_FAILURE = "LOW_VOLUME_EXTENSION_FAILURE"
NO_CLIMAX_ADMISSION = "NO_CLIMAX_ADMISSION"


class ClimaxAdapter:
    """Map the unchanged climax bundle evaluator into the strategy contract.

    The candle frame is an explicit input to the adapter.  Evaluation itself
    only reads the frozen strategy context, frame, and configuration; it does
    not perform lifecycle, persistence, delivery, or execution work.
    """

    strategy_type = CLIMAX_EXHAUSTION

    def __init__(self, config: Any, frame: pd.DataFrame) -> None:
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("climax adapter frame must be a pandas DataFrame")
        self._config = config
        self._frame = frame.copy(deep=True)

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        if not isinstance(context, StrategyContext):
            raise TypeError("climax adapter context must be a StrategyContext")

        bundle = evaluate_climax_bundle(
            context.event_state,
            context.features,
            self._frame,
            self._config,
        )
        selected = bundle.selected
        return self._translate(selected, context)

    def _translate(self, selected: Any, context: StrategyContext) -> StrategyEvaluation:
        vetoes = tuple(selected.veto_reasons)
        metadata = deepcopy(selected.metadata)
        metadata["strategy_config_fingerprint"] = context.strategy_config_fingerprint

        return StrategyEvaluation(
            strategy_type=self.strategy_type,
            strategy_subtype=selected.subtype or NO_CLIMAX_ADMISSION,
            model_version=str(selected.metadata.get("model_version", "climax-v1")),
            actionable=selected.actionable,
            score=selected.score,
            grade=selected.grade,
            reasons=(),
            blockers=vetoes,
            vetoes=vetoes,
            risk_flags=tuple(selected.data_quality),
            strategy_metadata=metadata,
            symbol=context.symbol,
            event_id=context.event_id,
            decision_timestamp=context.decision_timestamp,
            input_quality=context.input_quality,
        )


class _ClimaxBranchAdapter(ClimaxAdapter):
    """Expose one bundle branch without reimplementing its evaluator."""

    branch_type: str

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        if not isinstance(context, StrategyContext):
            raise TypeError("climax adapter context must be a StrategyContext")
        bundle = evaluate_climax_bundle(
            context.event_state,
            context.features,
            self._frame,
            self._config,
        )
        selected = bundle.branch_evaluations.get(self.branch_type)
        if selected is None:
            from app.signals.climax import ClimaxEvaluation

            selected = ClimaxEvaluation(
                None,
                0,
                "C",
                {
                    "strategy_type": CLIMAX_EXHAUSTION,
                    "strategy_subtype": self.branch_type,
                    "model_version": "climax-v1",
                },
                ["branch_not_enabled"],
                [],
            )
        return self._translate(selected, context)


class VolumeClimaxUnwindAdapter(_ClimaxBranchAdapter):
    strategy_type = VOLUME_CLIMAX_UNWIND
    branch_type = VOLUME_CLIMAX_UNWIND


class LowVolumeExtensionFailureAdapter(_ClimaxBranchAdapter):
    strategy_type = LOW_VOLUME_EXTENSION_FAILURE
    branch_type = LOW_VOLUME_EXTENSION_FAILURE


__all__ = [
    "CLIMAX_EXHAUSTION",
    "ClimaxAdapter",
    "LOW_VOLUME_EXTENSION_FAILURE",
    "LowVolumeExtensionFailureAdapter",
    "NO_CLIMAX_ADMISSION",
    "VOLUME_CLIMAX_UNWIND",
    "VolumeClimaxUnwindAdapter",
]
