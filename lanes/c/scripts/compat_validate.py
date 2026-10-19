"""Validate a private Lane C SQLite copy without reading sensitive payloads.

The default operation is read-only. ``--migrate-copy`` opts into the C
additive migration bridge against the explicitly supplied copy; the path is
refused when it resolves into a known production lane. The report contains
schema and aggregate fingerprints only. In particular, ``strategy_observations``
is reported as present or absent and is never counted or row-scanned because
that table can be multi-gigabyte.

Examples::

    python lanes/c/scripts/compat_validate.py --db /tmp/c-reference.sqlite
    python lanes/c/scripts/compat_validate.py --db /tmp/c-reference.sqlite \
        --migrate-copy --report /tmp/c-compat.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCRIPT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = SCRIPT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

SCHEMA_VERSION = 1
CURRENT_C_SCHEMA_VERSION = 1
SUPPORTED_C_SCHEMA_VERSIONS = frozenset({0, 1, 2, 3})
STRATEGY_OBSERVATIONS = "strategy_observations"

# These are the minimum C persistence boundaries. The complete declarative C
# contract is imported below when dependencies are available; keeping this
# critical subset here lets the validator produce a useful safe report for a
# small fixture without weakening the production copy check.
REQUIRED_TABLES: dict[str, tuple[str, ...]] = {
    "event_states": ("symbol", "event_id", "state", "updated_at"),
    "signals": (
        "id",
        "symbol",
        "event_id",
        "strategy_type",
        "strategy_subtype",
        "model_version",
        "signal_identity",
    ),
    "telegram_delivery_outbox": (
        "id",
        "entity_id",
        "entity_type",
        "channel",
        "idempotency_key",
        "status",
        "attempt_count",
        "sent_at",
    ),
}

SAFE_SIGNAL_COLUMNS = (
    "id",
    "symbol",
    "event_id",
    "strategy_type",
    "strategy_subtype",
    "model_version",
    "signal_identity",
)
SAFE_OUTBOX_COLUMNS = (
    "id",
    "entity_type",
    "entity_id",
    "channel",
    "idempotency_key",
    "status",
    "attempt_count",
    "next_attempt_at",
    "last_attempt_at",
    "lease_until",
    "sent_at",
    "created_at",
)
KNOWN_PRODUCTION_PREFIXES = (
    "/opt/short-telegram-bot-lite",
    "/opt/short-telegram-bot-lite-admin",
    "/opt/short-bot-lane-c",
    "/run/short-telegram-bot-lite",
)


class CompatibilityError(RuntimeError):
    """Raised for an unsafe path or an unreadable validation copy."""


def _normal(path: Path) -> str:
    return path.resolve().as_posix().casefold().rstrip("/")


def _is_known_production_path(path: Path) -> bool:
    value = _normal(path)
    return any(value == prefix or value.startswith(prefix + "/") for prefix in KNOWN_PRODUCTION_PREFIXES)


def _reject_symlink_chain(path: Path) -> None:
    current = path
    while True:
        if current.exists() and current.is_symlink():
            raise CompatibilityError(f"database path or parent is a symlink: {path}")
        if current.parent == current:
            return
        current = current.parent


def safe_db_path(value: str | Path) -> Path:
    """Resolve an existing SQLite file and reject production or symlink paths."""

    raw = str(value)
    if "://" in raw or raw.strip() in {":memory:", ""}:
        raise CompatibilityError("--db must be an existing local SQLite copy")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    _reject_symlink_chain(path)
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise CompatibilityError(f"database copy does not exist: {path}") from exc
    if not resolved.is_file():
        raise CompatibilityError("--db must point to a regular SQLite file")
    if _is_known_production_path(resolved):
        raise CompatibilityError("refusing a known production database path")
    return resolved


def _report_path(value: Path) -> Path:
    path = value.expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if _is_known_production_path(path):
        raise CompatibilityError("refusing a report path in a known production root")
    _reject_symlink_chain(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _readonly_uri(path: Path) -> str:
    return path.resolve().as_uri() + "?mode=ro"


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "bytes": len(value)}
    return value


def _row_digest(rows: Iterable[Iterable[Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        encoded = json.dumps([_json_value(value) for value in row], ensure_ascii=False, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
        digest.update(encoded)
        digest.update(b"\n")
    return digest.hexdigest()


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    }


def _columns(connection: sqlite3.Connection, table: str) -> tuple[str, ...]:
    # sqlite_master has already supplied the name; quoting still prevents a
    # fixture table name from becoming SQL syntax.
    return tuple(str(row[1]) for row in connection.execute(f"PRAGMA table_info({_quote(table)})"))


def _safe_count(connection: sqlite3.Connection, table: str) -> int:
    if table == STRATEGY_OBSERVATIONS:
        raise CompatibilityError("strategy_observations is intentionally never scanned")
    return int(connection.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0])


def _signals_snapshot(connection: sqlite3.Connection, columns: set[str]) -> dict[str, Any]:
    missing = sorted(set(SAFE_SIGNAL_COLUMNS) - columns)
    if missing:
        return {"available": False, "missing_columns": missing}
    select = ", ".join(_quote(column) for column in SAFE_SIGNAL_COLUMNS)
    rows = connection.execute(
        f"SELECT {select} FROM {_quote('signals')} ORDER BY {_quote('id')}"
    )
    values = list(rows)
    identity_mismatches = 0
    identity_invalid = 0
    try:
        from app.storage.identity import signal_identity
    except Exception:  # pragma: no cover - report remains useful without optional runtime deps.
        signal_identity = None
    if signal_identity is not None:
        for row in values:
            try:
                expected = signal_identity(
                    symbol=row[1],
                    event_id=row[2],
                    strategy_type=row[3],
                    strategy_subtype=row[4],
                    model_version=row[5],
                )
            except Exception:
                identity_invalid += 1
            else:
                if str(row[6]) != expected:
                    identity_mismatches += 1
    return {
        "available": True,
        "rows": len(values),
        "identity_digest": _row_digest(values),
        "identity_mismatch_count": identity_mismatches,
        "identity_invalid_count": identity_invalid,
    }


def _outbox_snapshot(connection: sqlite3.Connection, columns: set[str]) -> dict[str, Any]:
    missing = sorted(set(SAFE_OUTBOX_COLUMNS) - columns)
    if missing:
        return {"available": False, "missing_columns": missing}
    select = ", ".join(_quote(column) for column in SAFE_OUTBOX_COLUMNS)
    values = list(
        connection.execute(
            f"SELECT {select} FROM {_quote('telegram_delivery_outbox')} ORDER BY {_quote('id')}"
        )
    )
    statuses = Counter(str(row[5]) for row in values)
    return {
        "available": True,
        "rows": len(values),
        "status_counts": dict(sorted(statuses.items())),
        "safe_digest": _row_digest(values),
    }


def _dynamic_contract() -> tuple[dict[str, Any] | None, str | None]:
    try:
        from app.storage.migrations import SCHEMA_CONTRACT
    except Exception as exc:  # pragma: no cover - dependency diagnostics are safe.
        return None, f"{type(exc).__name__}: runtime schema contract unavailable"
    return SCHEMA_CONTRACT, None


def _schema_report(connection: sqlite3.Connection, tables: set[str], version: int) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for table, required in REQUIRED_TABLES.items():
        if table not in tables:
            checks[table] = {"present": False, "missing": list(required)}
            continue
        columns = set(_columns(connection, table))
        checks[table] = {
            "present": True,
            "missing": sorted(set(required) - columns),
            "columns": sorted(columns),
            "column_count": len(columns),
        }
    critical_valid = (
        version in SUPPORTED_C_SCHEMA_VERSIONS
        and all(check["present"] and not check["missing"] for check in checks.values())
    )

    contract, contract_error = _dynamic_contract()
    full_missing_tables: list[str] = []
    full_missing_columns: dict[str, list[str]] = {}
    if contract is not None:
        for table, descriptor in sorted(contract.items()):
            if table == STRATEGY_OBSERVATIONS:
                # Never call PRAGMA or COUNT for the giant table. Presence is
                # already known from sqlite_master above.
                if table not in tables:
                    full_missing_tables.append(table)
                continue
            if table not in tables:
                full_missing_tables.append(table)
                continue
            expected = set(getattr(descriptor, "columns", ()))
            actual = set(_columns(connection, table))
            missing = sorted(expected - actual)
            if missing:
                full_missing_columns[table] = missing
    full_valid = contract is not None and not full_missing_tables and not full_missing_columns and version == CURRENT_C_SCHEMA_VERSION
    return {
        "user_version": version,
        "supported_version": version in SUPPORTED_C_SCHEMA_VERSIONS,
        "critical_valid": critical_valid,
        "full_contract_available": contract is not None,
        "full_contract_error": contract_error,
        "full_contract_valid": full_valid,
        "full_missing_tables": full_missing_tables,
        "full_missing_columns": full_missing_columns,
        "checks": checks,
    }


def inspect(path: Path) -> dict[str, Any]:
    """Collect a safe compatibility snapshot from a read-only connection."""

    path = safe_db_path(path)
    try:
        connection = sqlite3.connect(_readonly_uri(path), uri=True, timeout=5.0)
    except sqlite3.Error as exc:
        raise CompatibilityError(f"could not open database copy read-only: {type(exc).__name__}") from exc
    try:
        connection.execute("PRAGMA query_only = ON")
        tables = _tables(connection)
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        schema = _schema_report(connection, tables, version)
        signal_columns = set(_columns(connection, "signals")) if "signals" in tables else set()
        state_columns = set(_columns(connection, "event_states")) if "event_states" in tables else set()
        outbox_columns = set(_columns(connection, "telegram_delivery_outbox")) if "telegram_delivery_outbox" in tables else set()
        signal_report = _signals_snapshot(connection, signal_columns) if "signals" in tables else {"available": False}
        outbox_report = _outbox_snapshot(connection, outbox_columns) if "telegram_delivery_outbox" in tables else {"available": False}
        state_report = {
            "available": "event_states" in tables,
            "rows": _safe_count(connection, "event_states") if "event_states" in tables else 0,
            "column_count": len(state_columns),
        }
        strategy_report = {
            "present": STRATEGY_OBSERVATIONS in tables,
            "inspected": False,
            "rows": None,
            "reason": "intentionally not scanned; may be a large historical table",
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "db": {"path": str(path), "read_only": True},
            "tables": sorted(tables),
            "schema": schema,
            "checks": schema["checks"],
            "signals": signal_report,
            "event_states": state_report,
            "telegram_delivery_outbox": outbox_report,
            "strategy_observations": strategy_report,
        }
    finally:
        connection.close()


def _migration_result(path: Path) -> dict[str, Any]:
    """Apply only the existing C additive migration bridge to a safe copy."""

    try:
        from sqlalchemy import create_engine
        from app.storage.migrations import migrate_database
    except Exception as exc:  # pragma: no cover - lockfile installs these dependencies.
        raise CompatibilityError("C migration dependencies are unavailable") from exc
    engine = create_engine(f"sqlite:///{path.as_posix()}", future=True, connect_args={"check_same_thread": False})
    try:
        # ``migrate_database`` is the runtime's additive V0/V1 bridge. The
        # broader legacy compatibility helper can rebuild historical tables;
        # that operation is intentionally not used by this validator.
        result = migrate_database(engine, include_shadow_v2=True)
        validation = result.validation.as_dict()
        return {
            "applied": bool(result.changed),
            "previous_version": result.previous_version,
            "version": result.version,
            "bootstrapped": bool(result.bootstrapped),
            "adopted_legacy": bool(result.adopted_legacy),
            "schema_repaired": bool(result.schema_repaired),
            "validation": validation,
        }
    except Exception as exc:
        raise CompatibilityError(f"C additive migration failed: {type(exc).__name__}") from exc
    finally:
        engine.dispose()


def _additive_check(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_tables = set(before["tables"])
    after_tables = set(after["tables"])
    removed_tables = sorted(before_tables - after_tables)
    removed_columns: dict[str, list[str]] = {}
    for table in sorted(before_tables & after_tables):
        if table == STRATEGY_OBSERVATIONS:
            continue
        before_columns = set(before["schema"]["checks"].get(table, {}).get("columns", []))
        after_columns = set(after["schema"]["checks"].get(table, {}).get("columns", []))
        missing = sorted(before_columns - after_columns)
        if missing:
            removed_columns[table] = missing
    return {"additive": not removed_tables and not removed_columns, "removed_tables": removed_tables, "removed_columns": removed_columns}


def validate(
    path: Path,
    *,
    migrate_copy: bool = False,
) -> dict[str, Any]:
    before = inspect(path)
    migration: dict[str, Any] = {"requested": migrate_copy, "applied": False}
    after = before
    if migrate_copy:
        # Re-run the path guard immediately before opening a write-capable
        # connection. The guard is intentionally before any DB write.
        path = safe_db_path(path)
        migration = {"requested": True, **_migration_result(path)}
        after = inspect(path)
        migration["additive_check"] = _additive_check(before, after)
        migration["outbox_preserved"] = before["telegram_delivery_outbox"] == after["telegram_delivery_outbox"]
        migration["signals_preserved"] = before["signals"] == after["signals"]
    checks = after["schema"]["checks"]
    contract_compatible = (
        after["schema"]["full_contract_valid"]
        if after["schema"]["full_contract_available"]
        else after["schema"]["critical_valid"]
    )
    ok = bool(
        contract_compatible
        and after["signals"].get("identity_mismatch_count", 0) == 0
        and after["signals"].get("identity_invalid_count", 0) == 0
        and (not migrate_copy or migration.get("additive_check", {}).get("additive", False))
        and (not migrate_copy or migration.get("outbox_preserved", False))
        and (not migrate_copy or migration.get("signals_preserved", False))
    )
    return {
        **after,
        "ok": ok,
        "migration": migration,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    encoded = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path, help="existing private SQLite copy")
    parser.add_argument(
        "--migrate-copy",
        "--migrate",
        dest="migrate_copy",
        action="store_true",
        help="apply the C additive migration bridge to this copy",
    )
    parser.add_argument("--report", type=Path, help="write the same safe JSON report to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = validate(safe_db_path(args.db), migrate_copy=args.migrate_copy)
    except CompatibilityError as exc:
        report = {
            "schema_version": SCHEMA_VERSION,
            "ok": False,
            "error": str(exc),
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 2
    if args.report is not None:
        _write_json(_report_path(args.report), report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
