from __future__ import annotations

from datetime import datetime, timezone

from app.storage.db import Database
from app.storage.repository import BotRepository


def test_strategy_candidate_upsert_and_terminal_transition_are_idempotent(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'strategy-candidates.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    observed_at = datetime(2026, 9, 12, 16, 0, tzinfo=timezone.utc)

    assert repository.upsert_strategy_candidate(
        candidate_id="candidate-1",
        root_event_id="root-1",
        event_revision=2,
        strategy="TRAPPED_LONGS_REVERSAL_V2",
        symbol="TESTUSDT",
        state="CANDIDATE",
        observed_at=observed_at,
        attempt_id="candidate-1:r2:a1",
        evidence={"frozen_breakout": True},
    )
    assert repository.upsert_strategy_candidate(
        candidate_id="candidate-1",
        root_event_id="root-1",
        event_revision=2,
        strategy="TRAPPED_LONGS_REVERSAL_V2",
        symbol="TESTUSDT",
        state="CANDIDATE",
        observed_at=observed_at,
        attempt_id="candidate-1:r2:a1",
        evidence={"frozen_breakout": True},
    )

    assert repository.transition_strategy_candidate(
        candidate_id="candidate-1",
        state="COMPLETE",
        observed_at=observed_at,
        reason="OUTCOME_COMPLETE",
    )
    assert not repository.transition_strategy_candidate(
        candidate_id="candidate-1",
        state="COMPLETE",
        observed_at=observed_at,
        reason="DUPLICATE_TERMINAL",
    )
    row = repository.get_strategy_candidate("candidate-1")
    assert row is not None
    assert row["state"] == "COMPLETE"
    assert row["terminal_reason"] == "OUTCOME_COMPLETE"
