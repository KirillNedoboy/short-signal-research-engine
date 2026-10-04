from datetime import datetime, timezone
from datetime import timedelta

from app.domain import SignalOutcome
from app.storage.db import Database
from app.storage.repository import BotRepository


def test_repository_round_trip(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    db_path = tmp_path / "bot.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    repository = BotRepository(database)

    saved_state = repository.upsert_event_state(
        make_event_state(expires_at=datetime.now(timezone.utc) + timedelta(hours=1))
    )
    active = repository.list_active_event_states()
    record = repository.save_signal(make_signal_decision(), saved_state, telegram_sent=True, provenance=make_signal_provenance())
    outcome = repository.upsert_signal_outcome(
        SignalOutcome(
            signal_id=record.id,
            price_after_15m=103.0,
            price_after_1h=101.0,
            price_after_4h=99.0,
            mfe_pct=5.0,
            mae_pct=1.0,
            reached_vwap=True,
            time_to_vwap_minutes=45,
            tp1_hit=None,
            stopped_virtual=None,
            updated_at=datetime.now(timezone.utc),
        )
    )

    assert saved_state.symbol == "ONTUSDT"
    assert len(active) == 1
    assert record.id > 0
    assert repository.list_signals_missing_outcomes() == []
    assert outcome.price_after_4h == 99.0


def test_repository_reads_immutable_shadow_attempt_window(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'attempt.db'}")
    database.create_all()
    repository = BotRepository(database)
    created = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    expires = created + timedelta(minutes=15)
    assert repository.upsert_shadow_entry_attempt(
        attempt_id="trapped_longs:attempt:ABCUSDT:root:r1",
        root_event_id="trapped_longs:ABCUSDT:root",
        observed_at=created,
        local_retest_high=105.0,
        breakdown_level=100.0,
        attempt_state="RETEST_IN_PROGRESS",
        confirmation_expires_at=expires,
        event_revision=1,
        max_attempts_per_root_event=1,
    )
    row = repository.get_shadow_entry_attempt(
        attempt_id="trapped_longs:attempt:ABCUSDT:root:r1"
    )
    assert row is not None
    assert row["attempt_created_at"] == created
    assert row["confirmation_expires_at"] == expires
