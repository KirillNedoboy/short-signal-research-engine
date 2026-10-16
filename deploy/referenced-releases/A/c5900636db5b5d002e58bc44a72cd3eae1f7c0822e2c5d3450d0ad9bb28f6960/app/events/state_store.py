"""Repository-backed state store."""

from __future__ import annotations

from datetime import datetime, timezone

from app.domain import EventState, EventStatus
from app.storage.repository import BotRepository


class EventStateStore:
    """Small adapter around the repository for event-state access."""

    def __init__(self, repository: BotRepository) -> None:
        self._repository = repository

    def load_active(self, now: datetime | None = None) -> dict[str, EventState]:
        """Return active event states keyed by symbol."""

        return {state.symbol: state for state in self._repository.list_active_event_states(now=now)}

    def load(self, symbol: str) -> EventState | None:
        """Load a single symbol state."""

        return self._repository.get_event_state(symbol)

    def load_active_symbol(self, symbol: str, now: datetime | None = None) -> EventState | None:
        """Reload one symbol while applying the same active predicate as ``load_active``."""
        state = self.load(symbol)
        reference = now or datetime.now(timezone.utc)
        if state is None or state.state in {EventStatus.IDLE, EventStatus.EXPIRED}:
            return None
        if state.expires_at is not None and state.expires_at <= reference:
            return None
        return state

    def save(self, state: EventState) -> EventState:
        """Persist a state update."""

        return self._repository.upsert_event_state(state)

    def expire(self, symbol: str) -> EventState | None:
        """Mark a symbol state as expired."""

        return self._repository.expire_symbol(symbol)
