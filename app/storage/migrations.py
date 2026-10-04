"""Explicit, admin-owned database schema bootstrap and validation.

Runtime code must open an already prepared schema.  This module owns the
operations that may change schema state and is intentionally called by the
administrative migration command (or by the test-only ``Database.create_all``
compatibility helper).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Mapping

from sqlalchemy import Connection, Engine, UniqueConstraint, inspect, text

from app.storage.models import Base


CURRENT_VERSION = 1
LEGACY_VERSION = 0
SCHEMA_VERSION_PRAGMA = "user_version"
HEARTBEAT_TABLE = "__db_heartbeat"
_V2_TABLES = frozenset({
    "root_detector_shadow_v2_roots",
    "root_detector_shadow_v2_outcomes",
})


def _type_affinity(type_name: str) -> str:
    """Compare SQLite's stable type affinity rather than dialect spellings."""
    name = type_name.upper()
    if "INT" in name:
        return "INTEGER"
    if any(token in name for token in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if "BLOB" in name or not name:
        return "BLOB"
    if any(token in name for token in ("REAL", "FLOA", "DOUB")):
        return "REAL"
    return "NUMERIC"


class MigrationError(RuntimeError):
    """Base error for explicit migration and schema contract failures."""


class SchemaValidationError(MigrationError):
    """Raised when the database does not satisfy the expected structure."""


class UnsupportedSchemaVersion(MigrationError):
    """Raised when the database version is unknown or newer than this code."""


@dataclass(frozen=True, slots=True)
class IndexContract:
    """Required index or unique constraint in the schema contract."""

    name: str
    columns: tuple[str, ...]
    unique: bool = False


@dataclass(frozen=True, slots=True)
class TableContract:
    """Structural requirements for one persisted table."""

    name: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    column_types: Mapping[str, str] = field(default_factory=dict)
    required_columns: tuple[str, ...] = ()
    indexes: tuple[IndexContract, ...] = ()


@dataclass(frozen=True, slots=True)
class SchemaValidationResult:
    """Read-only schema inspection result.

    ``structure_valid`` deliberately does not include the version marker.  It
    lets the migration command distinguish a structurally valid unversioned
    legacy database (V0) from a malformed one before stamping V1.
    """

    version: int
    missing_tables: tuple[str, ...] = ()
    missing_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    missing_indexes: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    invalid_primary_keys: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    invalid_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    version_error: str | None = None
    unexpected_tables: tuple[str, ...] = ()
    partial_optional_tables: tuple[str, ...] = ()
    valid: bool = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "valid",
            self.structure_valid and self.version == CURRENT_VERSION and self.version_error is None,
        )

    @property
    def structure_valid(self) -> bool:
        return not (
            self.missing_tables
            or self.missing_columns
            or self.missing_indexes
            or self.invalid_primary_keys
            or self.invalid_columns
            or self.unexpected_tables
            or self.partial_optional_tables
        )

    @property
    def is_current(self) -> bool:
        return self.valid

    @property
    def errors(self) -> tuple[str, ...]:
        """Return deterministic human-readable contract violations."""

        errors: list[str] = []
        if self.version_error:
            errors.append(self.version_error)
        if self.missing_tables:
            errors.append("missing tables: " + ", ".join(self.missing_tables))
        if self.unexpected_tables:
            errors.append("unexpected tables: " + ", ".join(self.unexpected_tables))
        if self.partial_optional_tables:
            errors.append("partial optional V2 tables: " + ", ".join(self.partial_optional_tables))
        for table, columns in sorted(self.missing_columns.items()):
            errors.append(f"{table} missing columns: {', '.join(columns)}")
        for table, indexes in sorted(self.missing_indexes.items()):
            errors.append(f"{table} missing indexes: {', '.join(indexes)}")
        for table, columns in sorted(self.invalid_primary_keys.items()):
            errors.append(f"{table} primary key mismatch: expected {', '.join(columns)}")
        for table, columns in sorted(self.invalid_columns.items()):
            errors.append(f"{table} column type mismatch: {', '.join(columns)}")
        return tuple(errors)

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-friendly representation for CLI and diagnostics."""

        return {
            "version": self.version,
            "valid": self.valid,
            "structure_valid": self.structure_valid,
            "missing_tables": list(self.missing_tables),
            "missing_columns": {key: list(value) for key, value in self.missing_columns.items()},
            "missing_indexes": {key: list(value) for key, value in self.missing_indexes.items()},
            "invalid_primary_keys": {key: list(value) for key, value in self.invalid_primary_keys.items()},
            "invalid_columns": {key: list(value) for key, value in self.invalid_columns.items()},
            "version_error": self.version_error,
            "unexpected_tables": list(self.unexpected_tables),
            "partial_optional_tables": list(self.partial_optional_tables),
        }


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """Outcome of an explicit migration invocation."""

    version: int
    previous_version: int
    bootstrapped: bool
    adopted_legacy: bool
    validation: SchemaValidationResult

    @property
    def changed(self) -> bool:
        return self.bootstrapped or self.adopted_legacy


def schema_contract(*, include_shadow_v2: bool = True) -> dict[str, TableContract]:
    """Build the V1 structural contract from the declarative model metadata.

    Required columns are intentionally additive: an old database may retain
    historical columns, but every column and index used by the current
    repository must be present.  This permits safe adoption of a legacy V0
    database while still failing closed on missing runtime state.
    """

    excluded = _V2_TABLES if not include_shadow_v2 else frozenset()
    contract: dict[str, TableContract] = {}
    for table in Base.metadata.sorted_tables:
        if table.name in excluded:
            continue
        indexes = [
            IndexContract(
                name=index.name,
                columns=tuple(column.name for column in index.columns),
                unique=bool(index.unique),
            )
            for index in sorted(table.indexes, key=lambda item: item.name or "")
            if index.name
        ]
        for constraint in sorted(table.constraints, key=lambda item: item.name or ""):
            if constraint.name and isinstance(constraint, UniqueConstraint):
                indexes.append(
                    IndexContract(
                        name=constraint.name,
                        columns=tuple(column.name for column in constraint.columns),
                        unique=True,
                    )
                )
        primary_key = tuple(column.name for column in table.primary_key.columns)
        contract[table.name] = TableContract(
            name=table.name,
            columns=tuple(column.name for column in table.columns),
            column_types={column.name: _type_affinity(str(column.type)) for column in table.columns},
            required_columns=tuple(
                column.name for column in table.columns
                if not column.nullable and column.name not in {c.name for c in table.primary_key.columns}
            ),
            primary_key=primary_key,
            indexes=tuple(sorted(indexes, key=lambda item: item.name)),
        )

    contract[HEARTBEAT_TABLE] = TableContract(
        name=HEARTBEAT_TABLE,
        columns=("id", "checked_at"),
        primary_key=("id",),
    )
    return contract


# Public snapshot for callers that need to display or audit the contract.
SCHEMA_CONTRACT = schema_contract()


def validate_schema(
    bind: Engine | Connection,
    *,
    include_shadow_v2: bool = True,
) -> SchemaValidationResult:
    """Inspect schema and version without issuing any DDL or DML."""

    if isinstance(bind, Engine):
        with bind.connect() as connection:
            return _validate_connection(connection, include_shadow_v2=include_shadow_v2)
    return _validate_connection(bind, include_shadow_v2=include_shadow_v2)


def assert_schema_current(
    bind: Engine | Connection,
    *,
    include_shadow_v2: bool = True,
) -> SchemaValidationResult:
    """Validate the current schema or raise a descriptive migration error."""

    result = validate_schema(bind, include_shadow_v2=include_shadow_v2)
    if not result.valid:
        detail = "; ".join(result.errors) or "schema contract mismatch"
        raise SchemaValidationError(f"schema contract mismatch: {detail}")
    return result


def bootstrap_schema(
    bind: Engine | Connection,
    *,
    include_shadow_v2: bool = True,
) -> MigrationResult:
    """Bootstrap a new database or adopt a structurally valid V0 database.

    Existing non-empty V0 databases are never repaired implicitly.  They are
    adopted only after the read-only structural contract passes.  V1 is
    idempotent after validation; unknown/future versions fail closed.
    """

    return _run_migration(bind, include_shadow_v2=include_shadow_v2)


def migrate_database(
    bind: Engine | Connection,
    *,
    include_shadow_v2: bool = True,
) -> MigrationResult:
    """Run the explicit V0→V1 migration or fresh V1 bootstrap."""

    return _run_migration(bind, include_shadow_v2=include_shadow_v2)


def bootstrap_test_schema(
    bind: Engine | Connection,
    *,
    include_shadow_v2: bool = True,
) -> None:
    """Create and repair schemas for the existing unit-test ``create_all`` API.

    Production entrypoints must use ``migrate_database`` ahead of runtime.
    This compatibility path retains the historical tests that intentionally
    construct partial legacy tables and exercise additive repair behavior.
    """

    def operation(connection: Connection) -> None:
        tables = _metadata_tables(include_shadow_v2=include_shadow_v2)
        Base.metadata.create_all(connection, tables=tables)
        if connection.dialect.name == "sqlite":
            _ensure_legacy_sqlite_schema(connection)
            _set_schema_version(connection, CURRENT_VERSION)
        else:
            connection.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS __db_heartbeat ("
                    "id INTEGER PRIMARY KEY CHECK (id = 1), "
                    "checked_at TEXT NOT NULL"
                    ")"
                )
            )

    _with_transaction(bind, operation)


def _run_migration(
    bind: Engine | Connection,
    *,
    include_shadow_v2: bool,
) -> MigrationResult:
    def operation(connection: Connection) -> MigrationResult:
        _require_sqlite(connection)
        previous_version = _get_schema_version(connection)
        _ensure_supported_version(previous_version)
        user_tables = _user_tables(connection)
        _ensure_trapped_longs_provenance_constraint(connection)

        if previous_version == LEGACY_VERSION and not user_tables:
            _create_fresh_schema(connection, include_shadow_v2=include_shadow_v2)
            validation = _validate_connection(connection, include_shadow_v2=include_shadow_v2)
            if not validation.structure_valid:
                raise SchemaValidationError(
                    "schema contract mismatch after bootstrap: " + "; ".join(validation.errors)
                )
            _set_schema_version(connection, CURRENT_VERSION)
            return MigrationResult(
                version=CURRENT_VERSION,
                previous_version=previous_version,
                bootstrapped=True,
                adopted_legacy=False,
                validation=replace(validation, version=CURRENT_VERSION, version_error=None),
            )

        validation = _validate_connection(connection, include_shadow_v2=include_shadow_v2)
        if previous_version == LEGACY_VERSION:
            if not validation.structure_valid:
                raise SchemaValidationError(
                    "schema contract mismatch during V0 adoption: " + "; ".join(validation.errors)
                )
            _set_schema_version(connection, CURRENT_VERSION)
            return MigrationResult(
                version=CURRENT_VERSION,
                previous_version=previous_version,
                bootstrapped=False,
                adopted_legacy=True,
                validation=replace(validation, version=CURRENT_VERSION, version_error=None),
            )

        if not validation.valid:
            raise SchemaValidationError(
                "schema contract mismatch: " + "; ".join(validation.errors)
            )
        return MigrationResult(
            version=CURRENT_VERSION,
            previous_version=previous_version,
            bootstrapped=False,
            adopted_legacy=False,
            validation=validation,
        )

    return _with_transaction(bind, operation)


def _ensure_trapped_longs_provenance_constraint(connection: Connection) -> None:
    """Upgrade the immutable provenance CHECK constraint in-place for SQLite."""
    if not inspect(connection).has_table("signal_provenance"):
        return
    ddl = connection.execute(
        text("SELECT sql FROM sqlite_master WHERE type='table' AND name='signal_provenance'")
    ).scalar_one_or_none()
    if ddl is None or "TRAPPED_LONGS_REVERSAL" in str(ddl):
        return
    connection.exec_driver_sql("ALTER TABLE signal_provenance RENAME TO signal_provenance_legacy")
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
                'LOW_VOLUME_EXTENSION_FAILURE', 'TRAPPED_LONGS_REVERSAL')
            ),
            CONSTRAINT ck_signal_provenance_strategy_shape CHECK (
                (strategy_branch = 'BASELINE_PULLBACK' AND strategy_family = 'BASELINE_PULLBACK'
                 AND root_event_id IS NULL AND decision_evaluation_id IS NULL
                 AND admission_evaluation_id IS NULL)
                OR (strategy_branch IN ('VOLUME_CLIMAX_UNWIND', 'LOW_VOLUME_EXTENSION_FAILURE')
                 AND strategy_family = 'CLIMAX_EXHAUSTION' AND root_event_id IS NOT NULL
                 AND decision_evaluation_id IS NOT NULL)
                OR (strategy_branch = 'TRAPPED_LONGS_REVERSAL'
                 AND strategy_family = 'TRAPPED_LONGS_REVERSAL' AND root_event_id IS NOT NULL
                 AND decision_evaluation_id IS NOT NULL)
            ),
            FOREIGN KEY(signal_id) REFERENCES signals(id) ON DELETE CASCADE,
            FOREIGN KEY(decision_evaluation_id) REFERENCES climax_evaluations(id),
            FOREIGN KEY(admission_evaluation_id) REFERENCES climax_evaluations(id)
        )"""
    )
    columns = "signal_id, strategy_family, strategy_branch, event_id, root_event_id, decision_evaluation_id, admission_evaluation_id, code_version, config_hash, runtime_instance_id, runtime_started_at, decision_at, signal_created_at, decision_entry_price, decision_event_high, decision_distance_from_high, provenance_anomaly"
    connection.exec_driver_sql(
        f"INSERT INTO signal_provenance ({columns}) SELECT {columns} FROM signal_provenance_legacy"
    )
    connection.exec_driver_sql("DROP TABLE signal_provenance_legacy")
    for name, column in (
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
            f"CREATE INDEX IF NOT EXISTS {name} ON signal_provenance ({column})"
        )


def _create_fresh_schema(connection: Connection, *, include_shadow_v2: bool) -> None:
    Base.metadata.create_all(
        connection,
        tables=_metadata_tables(include_shadow_v2=include_shadow_v2),
    )
    connection.execute(
        text(
            "CREATE TABLE __db_heartbeat ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "checked_at TEXT NOT NULL"
            ")"
        )
    )


def _metadata_tables(*, include_shadow_v2: bool) -> list[object]:
    excluded = _V2_TABLES if not include_shadow_v2 else frozenset()
    return [table for table in Base.metadata.sorted_tables if table.name not in excluded]


def _validate_connection(connection: Connection, *, include_shadow_v2: bool) -> SchemaValidationResult:
    _require_sqlite(connection)
    version = _get_schema_version(connection)
    _ensure_supported_version(version, raise_error=False)
    contract = schema_contract(include_shadow_v2=include_shadow_v2)
    inspector = inspect(connection)
    all_tables = set(_user_tables(connection))
    present_v2 = all_tables & _V2_TABLES
    partial_optional_tables = tuple(sorted(present_v2)) if present_v2 and present_v2 != _V2_TABLES else ()
    actual_tables = all_tables | (_V2_TABLES if present_v2 == _V2_TABLES else set())
    missing_tables = tuple(sorted(set(contract) - actual_tables))
    missing_columns: dict[str, tuple[str, ...]] = {}
    missing_indexes: dict[str, tuple[str, ...]] = {}
    invalid_primary_keys: dict[str, tuple[str, ...]] = {}
    invalid_columns: dict[str, tuple[str, ...]] = {}

    for table_name, expected in contract.items():
        if table_name not in actual_tables:
            continue
        actual_columns = {column["name"] for column in inspector.get_columns(table_name)}
        missing = tuple(column for column in expected.columns if column not in actual_columns)
        if missing:
            missing_columns[table_name] = missing

        actual_info = {column["name"]: column for column in inspector.get_columns(table_name)}
        invalid = tuple(
            name for name, expected_affinity in expected.column_types.items()
            if name in actual_info
            and _type_affinity(str(actual_info[name].get("type"))) != expected_affinity
        )
        if invalid:
            invalid_columns[table_name] = invalid
        invalid_required = tuple(
            name for name in expected.required_columns
            if name in actual_info and bool(actual_info[name].get("nullable", True))
        )
        if invalid_required:
            invalid_columns[table_name] = tuple(sorted(set(invalid_columns.get(table_name, ())) | set(invalid_required)))

        actual_pk = tuple(inspector.get_pk_constraint(table_name).get("constrained_columns") or ())
        if actual_pk != expected.primary_key:
            invalid_primary_keys[table_name] = expected.primary_key

        actual_indexes: dict[str, tuple[tuple[str, ...], bool]] = {}
        for index in inspector.get_indexes(table_name):
            name = index.get("name")
            if name:
                actual_indexes[name] = (
                    tuple(index.get("column_names") or ()),
                    bool(index.get("unique")),
                )
        for unique in inspector.get_unique_constraints(table_name):
            name = unique.get("name")
            if name:
                actual_indexes[name] = (tuple(unique.get("column_names") or ()), True)

        missing_for_table: list[str] = []
        for index in expected.indexes:
            actual = actual_indexes.get(index.name)
            if actual is None:
                equivalent = any(
                    columns == index.columns and (not index.unique or unique)
                    for columns, unique in actual_indexes.values()
                )
                if not equivalent:
                    missing_for_table.append(index.name)
                continue
            columns, unique = actual
            if columns != index.columns or (index.unique and not unique):
                missing_for_table.append(index.name)
        if missing_for_table:
            missing_indexes[table_name] = tuple(sorted(set(missing_for_table)))

    unexpected_tables = tuple(sorted((all_tables - set(contract)) - _V2_TABLES))
    version_error = None
    if version != CURRENT_VERSION:
        version_error = f"schema version {version} is not current (expected {CURRENT_VERSION})"
    return SchemaValidationResult(
        version=version,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        missing_indexes=missing_indexes,
        invalid_primary_keys=invalid_primary_keys,
        invalid_columns=invalid_columns,
        partial_optional_tables=partial_optional_tables,
        version_error=version_error,
        unexpected_tables=unexpected_tables,
    )


def _ensure_supported_version(version: int, *, raise_error: bool = True) -> None:
    if version < LEGACY_VERSION or version > CURRENT_VERSION:
        error = UnsupportedSchemaVersion(
            f"unsupported schema version {version}; current version is {CURRENT_VERSION}"
        )
        if raise_error:
            raise error


def _get_schema_version(connection: Connection) -> int:
    value = connection.exec_driver_sql("PRAGMA user_version").scalar_one()
    try:
        return int(value)
    except (TypeError, ValueError) as exc:  # pragma: no cover - SQLite returns int
        raise UnsupportedSchemaVersion(f"invalid schema version {value!r}") from exc


def _set_schema_version(connection: Connection, version: int) -> None:
    if version < 0:
        raise ValueError("schema version must be non-negative")
    connection.exec_driver_sql(f"PRAGMA user_version = {int(version)}")


def _user_tables(connection: Connection) -> tuple[str, ...]:
    return tuple(
        sorted(
            table
            for table in inspect(connection).get_table_names()
            if not table.startswith("sqlite_")
        )
    )


def _require_sqlite(connection: Connection) -> None:
    if connection.dialect.name != "sqlite":
        raise MigrationError("explicit schema migrations currently support SQLite only")


def _with_transaction(bind: Engine | Connection, operation):
    if isinstance(bind, Engine):
        with bind.begin() as connection:
            return operation(connection)
    return operation(bind)


def _ensure_legacy_sqlite_schema(connection: Connection) -> None:
    """Retain the historical additive repair path used by unit tests."""

    inspector = inspect(connection)
    columns_by_table = {
        table: {column["name"] for column in inspector.get_columns(table)}
        for table in inspector.get_table_names()
    }
    additions: dict[str, list[tuple[str, str]]] = {
        "signals": [
            ("strategy_type", "TEXT"),
            ("strategy_subtype", "TEXT"),
            ("model_version", "TEXT"),
        ],
        "signal_outcomes": [
            ("risk_adjusted_status", "TEXT"),
            ("squeeze_extension_pct", "FLOAT"),
            ("is_clean_short", "BOOLEAN"),
            ("is_squeeze_before_tp", "BOOLEAN"),
        ],
        "strategy_observations": [
            ("runtime_started_at", "TIMESTAMP"),
            ("code_version", "TEXT"),
            ("outcome_status", "TEXT"),
            ("outcome_json", "JSON"),
            ("outcome_mfe_pct", "FLOAT"),
            ("outcome_mae_pct", "FLOAT"),
            ("outcome_time_to_mfe_minutes", "FLOAT"),
            ("outcome_time_to_mae_minutes", "FLOAT"),
            ("outcome_new_high_after_observation", "BOOLEAN"),
            ("outcome_updated_at", "TIMESTAMP"),
            ("outcome_next_attempt_at", "TIMESTAMP"),
            ("outcome_attempt_count", "INTEGER DEFAULT 0"),
        ],
        "signal_provenance": [
            ("decision_entry_price", "FLOAT"),
            ("decision_event_high", "FLOAT"),
            ("decision_distance_from_high", "FLOAT"),
            ("provenance_anomaly", "TEXT"),
        ],
        "climax_evaluations": [
            ("runtime_instance_id", "TEXT"),
            ("root_event_id", "TEXT"),
            ("event_revision", "INTEGER"),
            ("attempt_id", "TEXT"),
            ("observed_at", "TIMESTAMP"),
            ("market_asof", "TIMESTAMP"),
            ("pool_added_at", "TIMESTAMP"),
            ("event_age_sec", "FLOAT"),
            ("pool_age_sec", "FLOAT"),
            ("evaluation_completed_at", "TIMESTAMP"),
            ("live_decision", "TEXT"),
            ("live_veto_reasons_json", "TEXT"),
            ("shadow_decision", "TEXT"),
            ("shadow_veto_reasons_json", "TEXT"),
            ("decision_delta", "TEXT"),
            ("shadow_hypothetical_entry_price", "FLOAT"),
            ("shadow_hypothetical_grade", "TEXT"),
            ("shadow_hypothetical_score", "INTEGER"),
            ("shadow_removed_vetoes_json", "TEXT"),
        ],
        "climax_monitor_events": [
            ("runtime_instance_id", "TEXT"),
            ("root_event_id", "TEXT"),
            ("event_revision", "INTEGER"),
            ("attempt_id", "TEXT"),
            ("observed_at", "TIMESTAMP"),
            ("market_asof", "TIMESTAMP"),
            ("pool_added_at", "TIMESTAMP"),
            ("event_age_sec", "FLOAT"),
            ("pool_age_sec", "FLOAT"),
        ],
        "runtime_heartbeats": [
            ("runtime_instance_id", "TEXT"),
            ("model_version", "TEXT"),
            ("config_fingerprint", "TEXT"),
        ],
        "reject_stats": [
            ("derivatives_status", "TEXT"),
            ("derivatives_reasons_json", "JSON"),
            ("data_quality_warnings_json", "JSON"),
        ],
        "market_coverage_ledger": [
            ("unexpected_result_present", "BOOLEAN NOT NULL DEFAULT 0"),
        ],
    }
    for table_name, columns in additions.items():
        existing = columns_by_table.get(table_name, set())
        for column_name, ddl_type in columns:
            if column_name not in existing:
                connection.exec_driver_sql(
                    f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}"
                )

    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_root_shadow_symbol_seen "
        "ON root_detector_shadow_candidates(symbol, first_seen_at)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_root_shadow_status "
        "ON root_detector_shadow_candidates(outcome_status, live_root_created)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_market_coverage_unexpected_result "
        "ON market_coverage_ledger(unexpected_result_present)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_strategy_observations_outcome_next_attempt_at "
        "ON strategy_observations(outcome_next_attempt_at)"
    )
    if "signals" in columns_by_table:
        index_names = {index["name"] for index in inspect(connection).get_indexes("signals")}
        constraint_names = {
            constraint["name"]
            for constraint in inspect(connection).get_unique_constraints("signals")
        }
        duplicate = connection.exec_driver_sql(
            "SELECT 1 FROM signals "
            "WHERE strategy_subtype IS NOT NULL AND model_version IS NOT NULL "
            "GROUP BY symbol, event_id, strategy_subtype, model_version "
            "HAVING COUNT(*) > 1 LIMIT 1"
        ).first()
        if not duplicate and "uq_signal_enriched_identity" not in index_names and "uq_signal_enriched_identity" not in constraint_names:
            connection.exec_driver_sql(
                "CREATE UNIQUE INDEX uq_signal_enriched_identity "
                "ON signals(symbol, event_id, strategy_subtype, model_version)"
            )

    connection.exec_driver_sql(
        "CREATE TABLE IF NOT EXISTS __db_heartbeat ("
        "id INTEGER PRIMARY KEY CHECK (id = 1), "
        "checked_at TEXT NOT NULL"
        ")"
    )
