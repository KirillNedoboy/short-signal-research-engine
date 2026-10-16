"""Pure adapter for the trapped-longs reversal evaluator."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

import pandas as pd

from app.signals.trapped_longs import evaluate_trapped_longs_reversal

from .contracts import StrategyContext, StrategyEvaluation


TRAPPED_LONGS = "TRAPPED_LONGS"
TRAPPED_LONGS_REVERSAL = "TRAPPED_LONGS_REVERSAL"
NO_TRAPPED_LONGS_ADMISSION = "NO_TRAPPED_LONGS_ADMISSION"


class TrappedLongsAdapter:
    """Map the trapped-longs evaluator into the canonical strategy contract.

    The frame and optional confirmation-window timestamps are explicit adapter
    inputs. Evaluation only reads those inputs, the context, and configuration;
    it performs no lifecycle, persistence, delivery, or execution work.
    """

    # The legacy evaluator metadata remains TRAPPED_LONGS; the adapter's
    # canonical registry identity is the concrete reversal branch.
    strategy_type = TRAPPED_LONGS_REVERSAL

    def __init__(
        self,
        config: Any,
        frame: pd.DataFrame,
        *,
        attempt_created_at: datetime | None = None,
        confirmation_expires_at: datetime | None = None,
    ) -> None:
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("trapped-longs adapter frame must be a pandas DataFrame")
        self._config = config
        self._frame = frame.copy(deep=True)
        self._attempt_created_at = attempt_created_at
        self._confirmation_expires_at = confirmation_expires_at

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        if not isinstance(context, StrategyContext):
            raise TypeError("trapped-longs adapter context must be a StrategyContext")

        legacy = evaluate_trapped_longs_reversal(
            context.event_state,
            context.features,
            self._frame,
            self._config,
            attempt_created_at=self._attempt_created_at,
            confirmation_expires_at=self._confirmation_expires_at,
            decision_time=context.decision_timestamp,
        )
        vetoes = tuple(legacy.veto_reasons)
        metadata = deepcopy(legacy.metadata)
        metadata["strategy_config_fingerprint"] = context.strategy_config_fingerprint

        return StrategyEvaluation(
            strategy_type=self.strategy_type,
            strategy_subtype=legacy.subtype or NO_TRAPPED_LONGS_ADMISSION,
            model_version=str(legacy.metadata.get("model_version", "trapped-longs-v1")),
            actionable=legacy.actionable,
            score=legacy.score,
            grade=legacy.grade,
            reasons=(),
            blockers=vetoes,
            vetoes=vetoes,
            risk_flags=tuple(legacy.data_quality),
            strategy_metadata=metadata,
            symbol=context.symbol,
            event_id=context.event_id,
            decision_timestamp=context.decision_timestamp,
            input_quality=context.input_quality,
        )


__all__ = [
    "NO_TRAPPED_LONGS_ADMISSION",
    "TRAPPED_LONGS",
    "TRAPPED_LONGS_REVERSAL",
    "TrappedLongsAdapter",
]
