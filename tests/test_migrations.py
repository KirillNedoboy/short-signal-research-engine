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
    with database.engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA user_version = 3")

    with pytest.raises(MigrationError, match="unsupported schema version"):
        migrate_database(database.engine)

    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 3


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
    assert result.version == 2
    with database.engine.connect() as connection:
        tables = set(inspect(connection).get_table_names())
    assert {"causal_pump_v2_roots", "causal_pump_v2_comparisons", "outcome_contract_v2_results"} <= tables


def test_v2_migration_adds_outcome_retry_table_additively(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'outcome-retries.sqlite'}")
    bootstrap_schema(database.engine, include_shadow_v2=True)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE outcome_fetch_retries")
        connection.exec_driver_sql("PRAGMA user_version = 2")

    result = migrate_database(database.engine, include_shadow_v2=True)

    assert result.version == CURRENT_VERSION
    assert inspect(database.engine).has_table("outcome_fetch_retries")
    assert validate_schema(database.engine, include_shadow_v2=True).valid


def test_v2_partial_schema_is_repaired_additively_and_idempotently(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'partial-v2.sqlite'}")
    bootstrap_schema(database.engine, include_shadow_v2=True)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE low_volume_v2_episode_states")
        connection.exec_driver_sql("ALTER TABLE signal_provenance RENAME TO signal_provenance_partial")
        connection.exec_driver_sql(
            """CREATE TABLE signal_provenance (
                signal_id INTEGER NOT NULL PRIMARY KEY,
                strategy_family VARCHAR(64) NOT NULL,
                strategy_branch VARCHAR(64) NOT NULL,
                event_id VARCHAR(128) NOT NULL,
                root_event_id VARCHAR(128),
                decision_evaluation_id INTEGER,
                admission_evaluation_id INTEGER,
                code_version VARCHAR(64) NOT NULL,
                config_hash VARCHAR(64) NOT NULL,
                runtime_instance_id VARCHAR(64) NOT NULL,
                runtime_started_at DATETIME NOT NULL,
                decision_at DATETIME NOT NULL,
                signal_created_at DATETIME NOT NULL,
                decision_entry_price FLOAT,
                decision_event_high FLOAT,
                decision_distance_from_high FLOAT,
                provenance_anomaly VARCHAR(64),
                CONSTRAINT ck_signal_provenance_strategy_branch CHECK (
                    strategy_branch IN ('BASELINE_PULLBACK', 'VOLUME_CLIMAX_UNWIND',
                    'LOW_VOLUME_EXTENSION_FAILURE', 'LOW_VOLUME_EXTENSION_FAILURE_V2', 'TRAPPED_LONGS_REVERSAL')
                )
            )"""
        )
        connection.exec_driver_sql(
            """INSERT INTO signal_provenance (
                signal_id, strategy_family, strategy_branch, event_id, code_version,
                config_hash, runtime_instance_id, runtime_started_at, decision_at,
                signal_created_at
            ) VALUES (1, 'BASELINE_PULLBACK', 'BASELINE_PULLBACK', 'legacy-event',
                'legacy', 'legacy', 'legacy-runtime', '2026-01-01', '2026-01-01', '2026-01-01')"""
        )
        connection.exec_driver_sql("DROP TABLE signal_provenance_partial")
        for index_name, column_name in (
            ("ix_signal_provenance_strategy_branch", "strategy_branch"),
            ("ix_signal_provenance_event_id", "event_id"),
            ("ix_signal_provenance_root_event_id", "root_event_id"),
            ("ix_signal_provenance_decision_evaluation_id", "decision_evaluation_id"),
            ("ix_signal_provenance_admission_evaluation_id", "admission_evaluation_id"),
            ("ix_signal_provenance_config_hash", "config_hash"),
            ("ix_signal_provenance_runtime_instance_id", "runtime_instance_id"),
            ("ix_signal_provenance_decision_at", "decision_at"),
            ("ix_signal_provenance_provenance_anomaly", "provenance_anomaly"),
        ):
            connection.exec_driver_sql(
                f"CREATE INDEX {index_name} ON signal_provenance ({column_name})"
            )
        connection.exec_driver_sql("PRAGMA user_version = 2")

    result = migrate_database(database.engine, include_shadow_v2=False)
    assert result.version == CURRENT_VERSION
    assert validate_schema(database.engine).valid
    with database.engine.connect() as connection:
        assert inspect(connection).has_table("low_volume_v2_episode_states")
        columns = {column["name"] for column in inspect(connection).get_columns("signal_provenance")}
        assert {"event_revision", "entry_trigger_time"} <= columns
        indexes = {index["name"] for index in inspect(connection).get_indexes("signal_provenance")}
        assert "uq_signal_provenance_v2_identity" in indexes
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM signal_provenance").scalar_one() == 1

    migrate_database(database.engine, include_shadow_v2=False)
    assert validate_schema(database.engine).valid
