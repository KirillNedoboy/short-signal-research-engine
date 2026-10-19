"""C persistence boundary for a decision and its durable outbox row."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.domain import EventState, SignalDecision, SignalProvenanceInput, SignalType
from app.signals.formatter import format_signal_message


class SignalPersistence:
    """Persist only after a complete C evaluation; no network delivery here."""

    def __init__(self, repository: Any, *, config: Any, code_version: str, config_hash: str, runtime_instance_id: str, runtime_started_at: datetime) -> None:
        self.repository = repository
        self.config = config
        self.code_version = code_version
        self.config_hash = config_hash
        self.runtime_instance_id = runtime_instance_id
        self.runtime_started_at = runtime_started_at

    def save(self, decision: SignalDecision, state: EventState) -> Any | None:
        if not decision.actionable or decision.signal_type is SignalType.WATCH:
            return None
        provenance = SignalProvenanceInput(
            strategy_family=decision.strategy_type,
            strategy_branch=decision.strategy_subtype or decision.strategy_type,
            event_id=decision.event_id,
            root_event_id=(
                decision.strategy_metadata.get("root_event_id")
                or (decision.event_id if decision.strategy_type != "BASELINE_PULLBACK" else None)
            ),
            decision_evaluation_id=None,
            admission_evaluation_id=None,
            code_version=self.code_version,
            config_hash=self.config_hash,
            runtime_instance_id=self.runtime_instance_id,
            runtime_started_at=self.runtime_started_at,
            decision_at=decision.signal_time,
            decision_entry_price=decision.market_price,
            decision_event_high=state.event_high,
            decision_distance_from_high=decision.strategy_metadata.get("entry_distance_below_high_pct"),
        )
        payload = format_signal_message(decision, self.config.timezone)
        return self.repository.save_signal(
            decision,
            state,
            delivery_payload=payload,
            provenance=provenance,
            lifecycle_state="OUTBOX_ENQUEUED",
        )


__all__ = ["SignalPersistence"]

