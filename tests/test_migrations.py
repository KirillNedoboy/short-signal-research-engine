from __future__ import annotations

import pytest
from sqlalchemy import event, inspect
from sqlalchemy.exc import IntegrityError
from app.storage.db import Database
from app.storage.migrations import (
    CURRENT_VERSION,
    MigrationError,
    SchemaValidationError,
    bootstrap_schema,
    migrate_database,
    validate_schema,
)
from app.storage.models import Base
from app.storage.identity import signal_identity


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


def test_current_v1_migration_is_idempotent_without_duplicate_rows_or_indexes(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'current.sqlite'}")
    first = migrate_database(database.engine)
    with database.engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO __db_heartbeat (id, checked_at) VALUES (1, '2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO signals "
            "(id, symbol, signal_time, signal_type, grade, score, market_price, short_zone_low, "
            "short_zone_high, event_id, event_high, event_base_price, event_range_pct, "
            "pullback_from_high_pct, dist_to_vwap_pct, upper_wick_ratio, rejection_from_high_pct, "
            "vol_zscore_30m, dist_to_ema20_atr, rsi_15m, ret_1h, ret_4h, range_atr_ratio, "
            "context_json, strategy_type, strategy_subtype, model_version, telegram_sent, created_at) "
            "VALUES (1, 'TESTUSDT', '2026-10-05T10:00:00Z', 'SELL', 'A', 8, 100.0, 99.0, "
            "101.0, 'event-1', 105.0, 90.0, 16.67, 4.76, 1.2, 0.8, 3.1, 2.0, -0.5, "
            "72.0, -1.0, -2.0, 1.5, '{}', 'CLIMAX', 'VOLUME_CLIMAX_UNWIND', 'v1', 0, "
            "'2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO signal_outcomes (signal_id, risk_adjusted_status, updated_at) "
            "VALUES (1, 'PENDING', '2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO signal_provenance "
            "(signal_id, strategy_family, strategy_branch, event_id, root_event_id, "
            "decision_evaluation_id, code_version, config_hash, runtime_instance_id, "
            "runtime_started_at, decision_at, signal_created_at) VALUES "
            "(1, 'BASELINE_PULLBACK', 'BASELINE_PULLBACK', 'event-1', NULL, NULL, "
            "'code-1', 'config-1', 'runtime-1', '2026-10-05T09:00:00Z', "
            "'2026-10-05T09:59:00Z', '2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO strategy_observations "
            "(observation_id, idempotency_key, run_id, runtime_instance_id, strategy_family, "
            "strategy, evaluation_phase, symbol, observed_at, live_decision, shadow_decision, "
            "score, blockers_json, warnings_json, model_version, config_hash, input_fingerprint, "
            "input_snapshot_json, outcome_json, outcome_attempt_count) VALUES "
            "('observation-1', 'observation-key-1', 'run-1', 'runtime-1', 'CLIMAX_EXHAUSTION', "
            "'VOLUME_CLIMAX_UNWIND', 'FINAL', 'TESTUSDT', '2026-10-05T10:00:00Z', "
            "'ACTIONABLE', 'ACTIONABLE', 8, '[]', '[]', 'v1', 'config-1', 'fingerprint-1', "
            "'{}', '{}', 0)"
        )
        connection.exec_driver_sql(
            "INSERT INTO telegram_delivery_outbox "
            "(id, entity_type, entity_id, channel, payload, idempotency_key, status, "
            "attempt_count, next_attempt_at, created_at) VALUES "
            "(1, 'signal', 1, 'signal_chat', '{}', 'outbox-key-1', 'PENDING', 0, "
            "'2026-10-05T10:00:00Z', '2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO event_states "
            "(symbol, event_id, state, event_features_snapshot, updated_at, signal_id) "
            "VALUES ('TESTUSDT', 'event-1', 'ACTIVE', '{}', '2026-10-05T10:00:00Z', 1)"
        )

    before_schema = _sqlite_schema_snapshot(database)
    before_rows = _sqlite_row_snapshot(database)
    second = migrate_database(database.engine)

    assert first.version == CURRENT_VERSION
    assert second.version == CURRENT_VERSION
    assert second.previous_version == CURRENT_VERSION
    assert second.changed is False
    assert validate_schema(database.engine).valid
    assert _sqlite_schema_snapshot(database) == before_schema


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

    before = _sqlite_schema_snapshot(database)
    with pytest.raises(MigrationError, match="schema contract"):
        migrate_database(database.engine)

    assert _sqlite_schema_snapshot(database) == before
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


def test_partial_v0_schema_fails_closed_without_additive_mutation(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'partial.sqlite'}")
    with database.engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE strategy_observations (observation_id TEXT PRIMARY KEY)"
        )
        connection.exec_driver_sql("PRAGMA user_version = 0")

    with pytest.raises(MigrationError, match="schema contract"):
        migrate_database(database.engine)

    with database.engine.connect() as connection:
        columns = {
            row[1]
            for row in connection.exec_driver_sql(
                "PRAGMA table_info(strategy_observations)"
            ).all()
        }
        assert columns == {"observation_id"}
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == 0


def test_minimal_nonempty_v0_schema_fails_closed_without_ddl_mutation(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'minimal.sqlite'}")
    with database.engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE signals (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql(
            "CREATE TABLE strategy_observations (observation_id TEXT PRIMARY KEY)"
        )
        connection.exec_driver_sql("PRAGMA user_version = 0")

    before = _sqlite_schema_snapshot(database)
    with pytest.raises(SchemaValidationError, match="schema contract"):
        migrate_database(database.engine)

    assert _sqlite_schema_snapshot(database) == before


def _sqlite_schema_snapshot(database) -> tuple[tuple[object, ...], ...]:
    with database.engine.connect() as connection:
        return tuple(
            connection.exec_driver_sql(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE type IN ('table', 'index', 'trigger', 'view') "
                "ORDER BY type, name"
            ).all()
        )


def _sqlite_row_snapshot(database) -> tuple[tuple[str, tuple[str, ...], tuple[tuple[object, ...], ...]], ...]:
    """Capture every current user table with stable columns and row ordering."""

    with database.engine.connect() as connection:
        tables = tuple(
            table for table in inspect(connection).get_table_names() if not table.startswith("sqlite_")
        )
        snapshots = []
        for table in sorted(tables):
            quoted = '"' + table.replace('"', '""') + '"'
            columns = tuple(
                row[1] for row in connection.exec_driver_sql(f"PRAGMA table_info({quoted})").all()
            )
            primary_key = tuple(
                row[1]
                for row in sorted(
                    connection.exec_driver_sql(f"PRAGMA table_info({quoted})").all(),
                    key=lambda row: row[5],
                )
                if row[5]
            )
            order_by = primary_key or columns
            ordering = ", ".join('"' + column.replace('"', '""') + '"' for column in order_by)
            rows = tuple(connection.exec_driver_sql(f"SELECT * FROM {quoted} ORDER BY {ordering}").all())
            snapshots.append((table, columns, rows))
        return tuple(snapshots)


def test_validate_schema_rejects_partial_optional_v2_pair(tmp_path) -> None:
    database = _make_legacy_database(tmp_path)
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE root_detector_shadow_v2_outcomes")
        connection.exec_driver_sql("PRAGMA user_version = 1")

    result = validate_schema(database.engine, include_shadow_v2=False)

    assert result.valid is False
    assert result.partial_optional_tables == ("root_detector_shadow_v2_roots",)


def test_legacy_strategy_observation_upgrade_rejects_missing_audit_shape_without_mutation(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-final-audit.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        for name in (
            "ix_strategy_observations_initial_evaluation_id",
            "ix_strategy_observations_initial_decision",
            "ix_strategy_observations_final_decision",
            "ix_strategy_observations_finalized_at",
        ):
            connection.exec_driver_sql(f"DROP INDEX IF EXISTS {name}")
        for column in (
            "initial_evaluation_id",
            "initial_decision",
            "final_decision",
            "final_reason",
            "finalized_at",
        ):
            connection.exec_driver_sql(f"ALTER TABLE strategy_observations DROP COLUMN {column}")

    before = _sqlite_schema_snapshot(database)
    with pytest.raises(MigrationError, match="schema contract"):
        migrate_database(database.engine)
    assert _sqlite_schema_snapshot(database) == before


def test_legacy_v1_migration_adds_and_backfills_signal_identity(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-v1-identity.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        connection.exec_driver_sql("UPDATE signals SET signal_identity = NULL")
        connection.exec_driver_sql("PRAGMA user_version = 1")
        connection.exec_driver_sql(
            "INSERT INTO signals (id, symbol, signal_time, signal_type, grade, score, market_price, short_zone_low, short_zone_high, event_id, event_high, event_base_price, event_range_pct, pullback_from_high_pct, dist_to_vwap_pct, upper_wick_ratio, rejection_from_high_pct, vol_zscore_30m, dist_to_ema20_atr, rsi_15m, ret_1h, ret_4h, range_atr_ratio, context_json, strategy_type, strategy_subtype, model_version, telegram_sent, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "BTCUSDT", "2026-10-05T10:00:00Z", "SELL", "A", 8, 100, 99, 101,
             "event-1", 105, 90, 16, 4, 1, 0.8, 3.1, 2, 1, 70, -1, -2, 1, "{}",
             "CLIMAX", "VOLUME_CLIMAX_UNWIND", "v1", 0, "2026-10-05T10:00:00Z"),
        )
    migrate_database(database.engine)
    expected = signal_identity(symbol="BTCUSDT", event_id="event-1", strategy_type="CLIMAX", strategy_subtype="VOLUME_CLIMAX_UNWIND", model_version="v1")
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT signal_identity FROM signals WHERE id = 1").scalar_one() == expected
        identity_indexes = {
            index["name"]: bool(index["unique"])
            for index in inspect(connection).get_indexes("signals")
            if index.get("name") == "uq_signal_identity"
        }
        identity_constraints = {
            constraint["name"]: True
            for constraint in inspect(connection).get_unique_constraints("signals")
            if constraint.get("name") == "uq_signal_identity"
        }
        assert identity_indexes or identity_constraints
    assert validate_schema(database.engine).valid


def test_signal_identity_migration_is_idempotent(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-v1-repeat.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        connection.exec_driver_sql("UPDATE signals SET signal_identity = NULL")
        connection.exec_driver_sql("PRAGMA user_version = 1")
    migrate_database(database.engine)
    before = _sqlite_schema_snapshot(database)
    second = migrate_database(database.engine)
    assert second.changed is False
    assert _sqlite_schema_snapshot(database) == before


def test_signal_identity_migration_fails_closed_on_collision(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-v1-collision.sqlite'}")
    database.create_all()
    with database.engine.begin() as connection:
        connection.exec_driver_sql("UPDATE signals SET signal_identity = NULL")
        connection.exec_driver_sql("PRAGMA user_version = 1")
        insert_sql = (
            "INSERT INTO signals (id, symbol, signal_time, signal_type, grade, score, market_price, "
            "short_zone_low, short_zone_high, event_id, event_high, event_base_price, event_range_pct, "
            "pullback_from_high_pct, dist_to_vwap_pct, upper_wick_ratio, rejection_from_high_pct, "
            "vol_zscore_30m, dist_to_ema20_atr, rsi_15m, ret_1h, ret_4h, range_atr_ratio, "
            "context_json, strategy_type, strategy_subtype, model_version, telegram_sent, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        for row_id in (1, 2):
            connection.exec_driver_sql(
                insert_sql,
                (row_id, "BTCUSDT" if row_id == 1 else "btcusdt", "2026-10-05T10:00:00Z", "SELL", "A", 8, 100, 99, 101,
                 "event-1", 105, 90, 16, 4, 1, 0.8, 3.1, 2, 1, 70, -1, -2, 1, "{}",
                 "CLIMAX", "VOLUME_CLIMAX_UNWIND", "v1", 0, "2026-10-05T10:00:00Z"),
            )
    before = _sqlite_schema_snapshot(database)
    with pytest.raises(MigrationError, match="collision"):
        migrate_database(database.engine)
    assert _sqlite_schema_snapshot(database) == before


def _make_representative_legacy_signal_chain_database(
    tmp_path, *, duplicate_identity: bool = False
) -> Database:
    database = Database(f"sqlite:///{tmp_path / ('duplicate-identity.sqlite' if duplicate_identity else 'representative-legacy.sqlite')}")
    database.create_all()
    signal_sql = (
        "INSERT INTO signals (id, symbol, signal_time, signal_type, grade, score, market_price, "
        "short_zone_low, short_zone_high, event_id, event_high, event_base_price, event_range_pct, "
        "pullback_from_high_pct, dist_to_vwap_pct, upper_wick_ratio, rejection_from_high_pct, "
        "vol_zscore_30m, dist_to_ema20_atr, rsi_15m, ret_1h, ret_4h, range_atr_ratio, "
        "context_json, strategy_type, strategy_subtype, model_version, telegram_sent, created_at) "
        "VALUES (?, ?, ?, 'SELL', 'A', 8, 100, 99, 101, ?, 105, 90, 16, 4, 1, 0.8, 3.1, "
        "2, 1, 70, -1, -2, 1, '{}', ?, ?, 'v1', 0, '2026-10-05T10:00:00Z')"
    )
    with database.engine.begin() as connection:
        # Rebuild only ``signals`` as a real pre-identity legacy table.  The
        # other tables stay at the current compatible shape so their legacy
        # rows and references remain representative and preserved.
        for name in (
            "ix_signals_signal_identity",
            "ix_signals_event_id",
            "ix_signals_symbol",
            "ix_signals_signal_time",
        ):
            connection.exec_driver_sql(f"DROP INDEX IF EXISTS {name}")
        connection.exec_driver_sql("DROP TABLE signals")
        connection.exec_driver_sql(
            """CREATE TABLE signals (
                id INTEGER NOT NULL, symbol VARCHAR(32) NOT NULL,
                signal_time DATETIME NOT NULL, signal_type VARCHAR(32) NOT NULL,
                grade VARCHAR(1) NOT NULL, score INTEGER NOT NULL,
                market_price FLOAT NOT NULL, short_zone_low FLOAT NOT NULL,
                short_zone_high FLOAT NOT NULL, event_id VARCHAR(128) NOT NULL,
                event_high FLOAT NOT NULL, event_base_price FLOAT NOT NULL,
                event_range_pct FLOAT NOT NULL, pullback_from_high_pct FLOAT NOT NULL,
                dist_to_vwap_pct FLOAT NOT NULL, upper_wick_ratio FLOAT NOT NULL,
                rejection_from_high_pct FLOAT NOT NULL, vol_zscore_30m FLOAT NOT NULL,
                dist_to_ema20_atr FLOAT NOT NULL, rsi_15m FLOAT NOT NULL,
                ret_1h FLOAT NOT NULL, ret_4h FLOAT NOT NULL, range_atr_ratio FLOAT NOT NULL,
                oi_change_15m FLOAT, oi_change_1h FLOAT, funding_rate FLOAT,
                context_json JSON NOT NULL, strategy_type VARCHAR(32),
                strategy_subtype VARCHAR(64), model_version VARCHAR(32),
                telegram_sent BOOLEAN NOT NULL, created_at DATETIME NOT NULL,
                PRIMARY KEY (id),
                CONSTRAINT uq_signal_enriched_identity
                    UNIQUE (symbol, event_id, strategy_subtype, model_version)
            )"""
        )
        for name, column in (
            ("ix_signals_event_id", "event_id"),
            ("ix_signals_symbol", "symbol"),
            ("ix_signals_signal_time", "signal_time"),
        ):
            connection.exec_driver_sql(f"CREATE INDEX {name} ON signals ({column})")
        if duplicate_identity:
            # Snapshot this nullable legacy column while still lacking identity
            # uniqueness; migration must reject the colliding backfill atomically.
            connection.exec_driver_sql("ALTER TABLE signals ADD COLUMN signal_identity TEXT")
        connection.exec_driver_sql("PRAGMA user_version = 0")
        connection.exec_driver_sql(signal_sql, (1, "GOODUSDT", "2026-10-05T10:00:00Z", "event-good", "BASELINE_PULLBACK", "BASELINE_PULLBACK"))
        if duplicate_identity:
            connection.exec_driver_sql(signal_sql, (4, "goodusdt", "2026-10-05T10:00:00Z", "event-good", "BASELINE_PULLBACK", "BASELINE_PULLBACK"))
        connection.exec_driver_sql(signal_sql, (2, "NOPROVUSDT", "2026-10-05T10:01:00Z", "event-noprov", "CLIMAX", "VOLUME_CLIMAX_UNWIND"))
        connection.exec_driver_sql(signal_sql, (3, "INVALIDUSDT", "2026-10-05T10:02:00Z", "event-invalid", "BROKEN", None))
        connection.exec_driver_sql(
            "INSERT INTO signal_provenance "
            "(signal_id, strategy_family, strategy_branch, event_id, root_event_id, decision_evaluation_id, "
            "admission_evaluation_id, code_version, config_hash, runtime_instance_id, runtime_started_at, "
            "decision_at, signal_created_at) VALUES "
            "(1, 'BASELINE_PULLBACK', 'BASELINE_PULLBACK', 'event-good', NULL, NULL, NULL, 'code-1', "
            "'config-1', 'runtime-1', '2026-10-05T09:00:00Z', '2026-10-05T09:59:00Z', "
            "'2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO signal_provenance "
            "(signal_id, strategy_family, strategy_branch, event_id, root_event_id, decision_evaluation_id, "
            "admission_evaluation_id, code_version, config_hash, runtime_instance_id, runtime_started_at, "
            "decision_at, signal_created_at, provenance_anomaly) VALUES "
            "(999, 'BASELINE_PULLBACK', 'BASELINE_PULLBACK', 'event-orphan', NULL, NULL, NULL, 'code-1', "
            "'config-1', 'runtime-1', '2026-10-05T09:00:00Z', '2026-10-05T09:59:00Z', "
            "'2026-10-05T10:00:00Z', 'legacy_orphan_signal')"
        )
        connection.exec_driver_sql(
            "INSERT INTO telegram_delivery_outbox "
            "(id, entity_type, entity_id, channel, payload, idempotency_key, status, attempt_count, "
            "next_attempt_at, created_at) VALUES "
            "(1, 'signal', 1, 'signal_chat', '{}', 'outbox-good', 'PENDING', 0, "
            "'2026-10-05T10:00:00Z', '2026-10-05T10:00:00Z'), "
            "(2, 'signal', 999, 'signal_chat', '{}', 'outbox-orphan', 'PENDING', 0, "
            "'2026-10-05T10:00:00Z', '2026-10-05T10:00:00Z')"
        )
        connection.exec_driver_sql(
            "INSERT INTO event_states "
            "(symbol, event_id, state, event_features_snapshot, updated_at, signal_id) VALUES "
            "('GOODUSDT', 'event-good', 'ACTIVE', '{}', '2026-10-05T10:00:00Z', 1), "
            "('DANGLINGUSDT', 'event-dangling', 'ACTIVE', '{}', '2026-10-05T10:00:00Z', 999)"
        )
    return database



def test_representative_legacy_signal_chain_upgrade(tmp_path) -> None:
    database = _make_representative_legacy_signal_chain_database(tmp_path)

    with database.engine.connect() as connection:
        assert "signal_identity" not in {
            column["name"] for column in inspect(connection).get_columns("signals")
        }
        before_counts = {
            table: connection.exec_driver_sql(f"SELECT COUNT(*) FROM {table}").scalar_one()
            for table in ("signals", "signal_provenance", "telegram_delivery_outbox", "event_states")
        }

    migrate_database(database.engine)

    expected = signal_identity(
        symbol="GOODUSDT", event_id="event-good", strategy_type="BASELINE_PULLBACK",
        strategy_subtype="BASELINE_PULLBACK", model_version="v1",
    )
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA integrity_check").scalar_one() == "ok"
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == CURRENT_VERSION
        assert validate_schema(database.engine).valid
        after_counts = {
            table: connection.exec_driver_sql(f"SELECT COUNT(*) FROM {table}").scalar_one()
            for table in before_counts
        }
        assert after_counts == before_counts
        assert connection.exec_driver_sql("SELECT signal_identity FROM signals WHERE id = 1").scalar_one() == expected
        assert connection.exec_driver_sql("SELECT signal_identity FROM signals WHERE id = 2").scalar_one() is not None
        # Invalid identity inputs use the documented NULL-preservation policy;
        # the legacy signal remains unlinked and has no anomaly-specific column.
        assert connection.exec_driver_sql("SELECT signal_identity FROM signals WHERE id = 3").scalar_one() is None
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM signal_provenance WHERE signal_id = 1").scalar_one() == 1
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM signal_provenance WHERE signal_id = 2").scalar_one() == 0
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM signal_provenance WHERE signal_id = 3").scalar_one() == 0
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM signal_provenance WHERE signal_id = 999").scalar_one() == 1
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM telegram_delivery_outbox WHERE entity_id = 2").scalar_one() == 0
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM telegram_delivery_outbox WHERE entity_id = 3").scalar_one() == 0
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM telegram_delivery_outbox WHERE entity_id = 999").scalar_one() == 1
        assert connection.exec_driver_sql("SELECT signal_id FROM event_states WHERE symbol = 'GOODUSDT'").scalar_one() == 1
        assert connection.exec_driver_sql("SELECT signal_id FROM event_states WHERE symbol = 'DANGLINGUSDT'").scalar_one() == 999
        assert connection.exec_driver_sql(
            "SELECT COUNT(*) FROM signals WHERE signal_identity IS NOT NULL GROUP BY signal_identity HAVING COUNT(*) > 1"
        ).scalar_one_or_none() is None
        identity_indexes = {
            index["name"]: bool(index["unique"])
            for index in inspect(connection).get_indexes("signals")
            if index.get("name") == "uq_signal_identity"
        }
        identity_constraints = {
            constraint["name"]: True
            for constraint in inspect(connection).get_unique_constraints("signals")
            if constraint.get("name") == "uq_signal_identity"
        }
        assert identity_indexes or identity_constraints
        with pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                """INSERT INTO signals (
                    id, symbol, signal_time, signal_type, grade, score, market_price,
                    short_zone_low, short_zone_high, event_id, event_high, event_base_price,
                    event_range_pct, pullback_from_high_pct, dist_to_vwap_pct, upper_wick_ratio,
                    rejection_from_high_pct, vol_zscore_30m, dist_to_ema20_atr, rsi_15m,
                    ret_1h, ret_4h, range_atr_ratio, oi_change_15m, oi_change_1h,
                    funding_rate, context_json, strategy_type, strategy_subtype, model_version,
                    signal_identity, telegram_sent, created_at
                )
                SELECT 1000, 'ENFORCEMENTUSDT', signal_time, signal_type, grade, score,
                       market_price, short_zone_low, short_zone_high, 'event-enforcement',
                       event_high, event_base_price, event_range_pct, pullback_from_high_pct,
                       dist_to_vwap_pct, upper_wick_ratio, rejection_from_high_pct,
                       vol_zscore_30m, dist_to_ema20_atr, rsi_15m, ret_1h, ret_4h,
                       range_atr_ratio, oi_change_15m, oi_change_1h, funding_rate,
                       context_json, strategy_type, strategy_subtype, model_version,
                       signal_identity, telegram_sent, created_at
                FROM signals WHERE id = 1"""
            )

    before_rerun = _sqlite_schema_snapshot(database)
    second = migrate_database(database.engine)
    assert second.changed is False
    assert _sqlite_schema_snapshot(database) == before_rerun

    duplicate_database = _make_representative_legacy_signal_chain_database(
        tmp_path, duplicate_identity=True
    )
    before_schema = _sqlite_schema_snapshot(duplicate_database)
    before_rows = _sqlite_row_snapshot(duplicate_database)
    with duplicate_database.engine.connect() as connection:
        before_version = connection.exec_driver_sql("PRAGMA user_version").scalar_one()
    with pytest.raises(MigrationError, match="collision"):
        migrate_database(duplicate_database.engine)
    assert _sqlite_schema_snapshot(duplicate_database) == before_schema
    assert _sqlite_row_snapshot(duplicate_database) == before_rows
    with duplicate_database.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA user_version").scalar_one() == before_version
