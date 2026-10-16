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


def test_fresh_schema_without_shadow_v2_uses_v1_marker(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'fresh-v1.sqlite'}")

    result = bootstrap_schema(database.engine, include_shadow_v2=False)

    assert result.version == 1
    assert validate_schema(database.engine, include_shadow_v2=False).valid
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 1
        tables = set(inspect(connection).get_table_names())
    assert not {"root_detector_shadow_v2_roots", "root_detector_shadow_v2_outcomes"} & tables
    assert not {"causal_pump_v2_roots", "causal_pump_v2_comparisons", "outcome_contract_v2_results"} & tables


def test_complete_v2_schema_is_accepted_when_shadow_flag_is_disabled(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'complete-v2-disabled.sqlite'}")

    bootstrap_schema(database.engine, include_shadow_v2=True)

    result = validate_schema(database.engine, include_shadow_v2=False)

    assert result.valid
    assert result.version == CURRENT_VERSION


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
    future_version = CURRENT_VERSION + 1
    with database.engine.begin() as connection:
        connection.exec_driver_sql(f"PRAGMA user_version = {future_version}")

    with pytest.raises(MigrationError, match="unsupported schema version"):
        migrate_database(database.engine)

    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == future_version


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
    assert result.partial_optional_tables == (
        "causal_pump_v2_comparisons",
        "causal_pump_v2_roots",
        "outcome_contract_v2_results",
        "root_detector_shadow_v2_roots",
    )




def test_v1_to_v2_repairs_partial_optional_tables(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE root_detector_shadow_v2_outcomes")
        connection.exec_driver_sql("PRAGMA user_version = 1")

    result = migrate_database(database.engine, include_shadow_v2=True)

    assert result.version == CURRENT_VERSION
    assert validate_schema(database.engine, include_shadow_v2=True).valid


def test_v1_to_v2_adds_correctness_tables_without_rebuild(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'v1-to-v2.sqlite'}")
    bootstrap_schema(database.engine, include_shadow_v2=False)
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 1
    result = migrate_database(database.engine, include_shadow_v2=True)
    assert result.previous_version == 1
    assert result.version == CURRENT_VERSION
    with database.engine.connect() as connection:
        tables = set(inspect(connection).get_table_names())
    assert {"causal_pump_v2_roots", "causal_pump_v2_comparisons", "outcome_contract_v2_results"} <= tables
