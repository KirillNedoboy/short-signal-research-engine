from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import inspect, select

from app.domain import EventStatus
from app.storage.db import Database
from app.storage.migrations import SchemaValidationError, migrate_database
from app.storage.models import EventStateModel
from app.storage.repository import BotRepository


def _state(**overrides):
    from app.domain import EventState

    values = dict(
        symbol="TESTUSDT",
        event_id="event-1",
        state=EventStatus.PUMP_DETECTED,
        lifecycle_state="OUTBOX_ENQUEUED",
        updated_at=datetime(2026, 10, 5, tzinfo=timezone.utc),
    )
    values.update(overrides)
    return EventState(**values)


def test_event_state_lifecycle_state_round_trips_independently_from_status_and_notes(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'event-state.sqlite'}")
    database.create_all()
    repository = BotRepository(database)

    saved = repository.upsert_event_state(_state())
    with database.engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE event_states SET notes = 'legacy-note' WHERE symbol = 'TESTUSDT'"
        )

    loaded = repository.get_event_state("TESTUSDT")

    assert saved.lifecycle_state == "OUTBOX_ENQUEUED"
    assert loaded is not None
    assert loaded.lifecycle_state == "OUTBOX_ENQUEUED"
    assert loaded.state is EventStatus.PUMP_DETECTED


def test_lifecycle_state_migration_preserves_existing_rows(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-event-state.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        if "lifecycle_state" in {column["name"] for column in inspect(connection).get_columns("event_states")}:
            connection.exec_driver_sql("ALTER TABLE event_states DROP COLUMN lifecycle_state")
        connection.exec_driver_sql(
            "INSERT INTO event_states (symbol, event_id, state, event_features_snapshot, notes, updated_at) "
            "VALUES ('LEGACYUSDT', 'legacy-event', 'pump_detected', '{}', 'legacy-note', "
            "'2026-10-05T10:00:00Z')"
        )

    result = migrate_database(database.engine)

    assert result.changed is True
    with database.engine.connect() as connection:
        columns = {column["name"] for column in inspect(connection).get_columns("event_states")}
        row = connection.execute(
            select(EventStateModel.symbol, EventStateModel.state, EventStateModel.notes)
        ).one()
    assert "lifecycle_state" in columns
    assert row == ("LEGACYUSDT", "pump_detected", "legacy-note")


def test_lifecycle_state_migration_is_idempotent(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'idempotent-event-state.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        if "lifecycle_state" in {column["name"] for column in inspect(connection).get_columns("event_states")}:
            connection.exec_driver_sql("ALTER TABLE event_states DROP COLUMN lifecycle_state")

    first = migrate_database(database.engine)
    second = migrate_database(database.engine)

    assert first.changed is True
    assert second.changed is False
    assert [column["name"] for column in inspect(database.engine).get_columns("event_states")].count(
        "lifecycle_state"
    ) == 1


def test_lifecycle_state_migration_fails_closed_for_malformed_schema(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'malformed-event-state.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        if "lifecycle_state" in {column["name"] for column in inspect(connection).get_columns("event_states")}:
            connection.exec_driver_sql("ALTER TABLE event_states DROP COLUMN lifecycle_state")
        connection.exec_driver_sql("ALTER TABLE event_states DROP COLUMN notes")

    with pytest.raises(SchemaValidationError, match="schema contract"):
        migrate_database(database.engine)

    columns = {column["name"] for column in inspect(database.engine).get_columns("event_states")}
    assert "lifecycle_state" not in columns
    assert "notes" not in columns
