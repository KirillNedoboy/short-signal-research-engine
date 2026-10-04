from __future__ import annotations

import pytest
from sqlalchemy import event, inspect

from app.storage.db import Database
from app.storage.migrations import (
    CURRENT_VERSION,
    MigrationError,
    bootstrap_schema,
    migrate_database,
    validate_schema,
)
from app.storage.models import Base


def _make_legacy_database(tmp_path):
    path = tmp_path / "legacy.sqlite"
    database = Database(f"sqlite:///{path}")
    with database.engine.begin() as connection:
        Base.metadata.create_all(connection)
        connection.exec_driver_sql(
            "CREATE TABLE __db_heartbeat ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "checked_at TEXT NOT NULL"
            ")"
        )
        connection.exec_driver_sql("PRAGMA user_version = 0")
    return database


def test_bootstrap_creates_v1_schema_and_marker(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'fresh.sqlite'}")

    result = bootstrap_schema(database.engine)

    assert result.version == CURRENT_VERSION
    assert result.adopted_legacy is False
    assert validate_schema(database.engine).valid
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == CURRENT_VERSION
        assert "__db_heartbeat" in inspect(connection).get_table_names()


def test_v0_schema_is_adopted_only_after_structural_validation(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)

    result = migrate_database(database.engine)

    assert result.version == CURRENT_VERSION
    assert result.adopted_legacy is True
    assert validate_schema(database.engine).valid


def test_v0_schema_missing_required_index_fails_closed(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP INDEX ix_signals_symbol")

    with pytest.raises(MigrationError, match="schema contract"):
        migrate_database(database.engine)

    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 0


def test_future_schema_version_fails_closed_without_mutation(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA user_version = 2")

    with pytest.raises(MigrationError, match="unsupported schema version"):
        migrate_database(database.engine)

    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 2


def test_database_constructor_and_heartbeat_do_not_issue_ddl(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'runtime.sqlite'}")
    bootstrap_schema(database.engine)
    statements: list[str] = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement.strip().upper())

    event.listen(database.engine, "before_cursor_execute", capture)
    try:
        database.write_heartbeat()
    finally:
        event.remove(database.engine, "before_cursor_execute", capture)

    assert statements
    assert not any(
        statement.startswith(("CREATE", "ALTER", "DROP"))
        or "CREATE INDEX" in statement
        for statement in statements
    )


def test_runtime_constructor_does_not_create_schema(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'unprepared.sqlite'}")

    with database.engine.connect() as connection:
        assert inspect(connection).get_table_names() == []


def test_validate_schema_is_read_only_for_invalid_v0(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE signals")

    result = validate_schema(database.engine)

    assert result.valid is False
    assert "signals" in result.missing_tables
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 0


def test_validate_schema_rejects_partial_optional_v2_pair(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE root_detector_shadow_v2_outcomes")
        connection.exec_driver_sql("PRAGMA user_version = 1")

    result = validate_schema(database.engine, include_shadow_v2=False)

    assert result.valid is False
    assert result.partial_optional_tables == ("root_detector_shadow_v2_roots",)
