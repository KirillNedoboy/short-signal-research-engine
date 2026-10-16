"""Fail-closed, batched retention for non-operational research telemetry.

This worker deliberately does not use the live application's ORM.  It opens
one lane database at a time, serializes against a per-lane lock, and deletes
only rows selected by the explicit plans below.  Signals, provenance,
outcomes, event state, root/attempt state, outbox, watch rows, paper fills,
and unknown/new tables are never deletion targets.

The existing phase1c retention evaluator remains read-only.  This separate
runtime-batched policy is the explicit, operator-owned deletion boundary:
terminal raw scan/coverage/reject/monitor rows age out after one day,
completed strategy observations after seven days, and heartbeat history
after fourteen days.  Every lifecycle dependency is checked in SQL and missing
schema/dependencies fail closed before any batch starts.  Since 2026-09-23 the
policy additionally prunes aged-out terminal ``market_scan_symbol_results``
rows whose parent cycle row is provably absent (``cycle_id`` NULL or the cycle
row already gone): those can never satisfy the parent-EXISTS join and would
otherwise leak forever.  ``--catch-up`` lifts the per-run orphan cap for a
one-shot operator-driven purge bounded by ``--max-run-seconds``.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

BATCH_SIZE_DEFAULT = 1000
MAX_RUN_SECONDS_DEFAULT = 240.0
MIN_FREE_BYTES_DEFAULT = 5_905_580_032
BUSY_TIMEOUT_APPLY_MS = 500
BATCH_PAUSE_SEC = 0.05
POLICY_VERSION = "runtime-batched-v3-raw-1d-orphan-catchup"
RAW_RETENTION_DAYS = 1
STRATEGY_RETENTION_DAYS = 7
HEARTBEAT_RETENTION_DAYS = 14
SOURCE_POLICY_VERSION = "phase1c-v1"
TERMINAL_SCAN_STATUSES = ("EXCLUDED", "SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED")
TERMINAL_CYCLE_STATUSES = ("COMPLETED", "PARTIAL")
TERMINAL_ROTATION_STATUSES = ("COMPLETED", "INCOMPLETE", "ABORTED_RESTART", "FAILED")
TERMINAL_ATTEMPT_STATES = ("SHADOW_ACTIONABLE", "EXPIRED", "INVALIDATED", "ROOT_REPLACED")
# 2026-09-23 backlog fix: Lane A inserts ~91,700 rows/day into
# `market_scan_symbol_results` while the scheduled cadence ran with
# --batch-size 250, and ~99.8% of the existing backlog has cycle_id IS NULL
# (or a cycle row that is already gone), so the parent-EXISTS filter of the
# original plan can never match it.  The hot table gets a dedicated per-batch
# size and an explicit orphan catch-up plan (fail-closed: age + terminal
# status + parent provably absent).  --catch-up removes the per-run orphan
# cap for a one-shot operator purge bounded only by --max-run-seconds.
SYMBOL_RESULTS_BATCH_SIZE = 2000
ORPHAN_CATCHUP_BATCH_SIZE = 5000
ORPHAN_CATCHUP_MAX_ROWS_PER_RUN = 30_000


@dataclass(frozen=True)
class Lane:
    name: str
    db_path: Path
    runtime_path: Path


@dataclass(frozen=True)
class Plan:
    table: str
    identity: str
    timestamp: str
    retention_days: int
    where_sql: str
    required_tables: tuple[str, ...]
    required_columns: dict[str, tuple[str, ...]]
    # Per-table batch override; 0 means "use the CLI --batch-size".
    # Effective batch for a plan is max(batch_size, plan.batch_size), so an
    # explicit larger --batch-size always wins (2026-09-23 backlog fix).
    batch_size: int = 0
    label: str | None = None
    # Per-plan per-run deletion cap (apply mode); 0 = no cap. Under
    # --catch-up all caps are lifted and only the time budget applies.
    max_rows_per_run: int = 0


LANES = (
    Lane(
        "A",
        Path("/opt/short-telegram-bot-lite/data/bot.sqlite"),
        Path("/opt/short-telegram-bot-lite-admin/lane-a/runtime"),
    ),
    Lane(
        "B",
        Path("/opt/short-telegram-bot-lite-admin/lane-b/data/bot.sqlite"),
        Path("/opt/short-telegram-bot-lite-admin/lane-b/runtime"),
    ),
)

# All identifiers in these SQL fragments are fixed source code, not CLI input.
PLANS = (
    Plan(
        table="market_scan_symbol_results",
        identity="id",
        timestamp="completed_at",
        retention_days=RAW_RETENTION_DAYS,
        where_sql="""
            datetime(completed_at) < datetime(:cutoff)
            AND completed_at IS NOT NULL
            AND terminal_status IN ('EXCLUDED', 'SCANNED_OK', 'SCAN_FAILED', 'SCAN_SKIPPED')
            AND EXISTS (
                SELECT 1
                FROM market_scan_cycles AS c
                JOIN market_scan_rotations AS r ON r.rotation_id = c.rotation_id
                WHERE c.cycle_id = market_scan_symbol_results.cycle_id
                  AND c.cycle_completed_at IS NOT NULL
                  AND c.status IN ('COMPLETED', 'PARTIAL')
                  AND r.rotation_completed_at IS NOT NULL
                  AND r.status IN ('COMPLETED', 'INCOMPLETE', 'ABORTED_RESTART', 'FAILED')
            )
        """,
        required_tables=("market_scan_cycles", "market_scan_rotations"),
        required_columns={
            "market_scan_symbol_results": ("id", "cycle_id", "completed_at", "terminal_status"),
            "market_scan_cycles": ("cycle_id", "rotation_id", "cycle_completed_at", "status"),
            "market_scan_rotations": ("rotation_id", "rotation_completed_at", "status"),
        },
        batch_size=SYMBOL_RESULTS_BATCH_SIZE,
    ),
    Plan(
        # Orphan catch-up: terminal, aged-out rows whose parent cycle row can
        # never satisfy the primary plan's EXISTS anymore (cycle_id IS NULL,
        # or the cycle row is already gone).  Same age/terminal guards as the
        # primary plan; only the parent condition is inverted and provable.
        table="market_scan_symbol_results",
        identity="id",
        timestamp="completed_at",
        retention_days=RAW_RETENTION_DAYS,
        label="orphan-catchup",
        batch_size=ORPHAN_CATCHUP_BATCH_SIZE,
        max_rows_per_run=ORPHAN_CATCHUP_MAX_ROWS_PER_RUN,
        where_sql="""
            datetime(completed_at) < datetime(:cutoff)
            AND completed_at IS NOT NULL
            AND terminal_status IN ('EXCLUDED', 'SCANNED_OK', 'SCAN_FAILED', 'SCAN_SKIPPED')
            AND (
                cycle_id IS NULL
                OR NOT EXISTS (
                    SELECT 1
                    FROM market_scan_cycles AS c
                    WHERE c.cycle_id = market_scan_symbol_results.cycle_id
                )
            )
        """,
        required_tables=("market_scan_cycles",),
        required_columns={
            "market_scan_symbol_results": ("id", "cycle_id", "completed_at", "terminal_status"),
            "market_scan_cycles": ("cycle_id",),
        },
    ),
    Plan(
        table="market_coverage_ledger",
        identity="id",
        timestamp="observed_at",
        retention_days=RAW_RETENTION_DAYS,
        where_sql="""
            datetime(observed_at) < datetime(:cutoff)
            AND observed_at IS NOT NULL
            AND scan_status IN ('EXCLUDED', 'SCANNED_OK', 'SCAN_FAILED', 'SCAN_SKIPPED')
            AND EXISTS (
                SELECT 1
                FROM market_scan_rotations AS r
                WHERE r.rotation_id = market_coverage_ledger.rotation_id
                  AND r.rotation_completed_at IS NOT NULL
                  AND r.status IN ('COMPLETED', 'INCOMPLETE', 'ABORTED_RESTART', 'FAILED')
            )
        """,
        required_tables=("market_scan_rotations",),
        required_columns={
            "market_coverage_ledger": ("id", "rotation_id", "observed_at", "scan_status"),
            "market_scan_rotations": ("rotation_id", "rotation_completed_at", "status"),
        },
    ),
    Plan(
        table="market_scan_cycles",
        identity="cycle_id",
        timestamp="cycle_completed_at",
        retention_days=RAW_RETENTION_DAYS,
        where_sql="""
            datetime(cycle_completed_at) < datetime(:cutoff)
            AND cycle_completed_at IS NOT NULL
            AND status IN ('COMPLETED', 'PARTIAL')
            AND EXISTS (
                SELECT 1
                FROM market_scan_rotations AS r
                WHERE r.rotation_id = market_scan_cycles.rotation_id
                  AND r.rotation_completed_at IS NOT NULL
                  AND r.status IN ('COMPLETED', 'INCOMPLETE', 'ABORTED_RESTART', 'FAILED')
            )
            AND NOT EXISTS (
                SELECT 1
                FROM market_scan_symbol_results AS sr
                WHERE sr.cycle_id = market_scan_cycles.cycle_id
            )
        """,
        required_tables=("market_scan_rotations", "market_scan_symbol_results"),
        required_columns={
            "market_scan_cycles": ("cycle_id", "rotation_id", "cycle_completed_at", "status"),
            "market_scan_rotations": ("rotation_id", "rotation_completed_at", "status"),
            "market_scan_symbol_results": ("cycle_id",),
        },
    ),
    Plan(
        table="market_scan_rotations",
        identity="rotation_id",
        timestamp="rotation_completed_at",
        retention_days=RAW_RETENTION_DAYS,
        where_sql="""
            datetime(rotation_completed_at) < datetime(:cutoff)
            AND rotation_completed_at IS NOT NULL
            AND status IN ('COMPLETED', 'INCOMPLETE', 'ABORTED_RESTART', 'FAILED')
            AND NOT EXISTS (
                SELECT 1 FROM market_scan_cycles AS c
                WHERE c.rotation_id = market_scan_rotations.rotation_id
            )
            AND NOT EXISTS (
                SELECT 1 FROM market_scan_symbol_results AS sr
                WHERE sr.rotation_id = market_scan_rotations.rotation_id
            )
            AND NOT EXISTS (
                SELECT 1 FROM market_coverage_ledger AS ml
                WHERE ml.rotation_id = market_scan_rotations.rotation_id
            )
        """,
        required_tables=("market_scan_cycles", "market_scan_symbol_results", "market_coverage_ledger"),
        required_columns={
            "market_scan_rotations": ("rotation_id", "rotation_completed_at", "status"),
            "market_scan_cycles": ("rotation_id",),
            "market_scan_symbol_results": ("rotation_id",),
            "market_coverage_ledger": ("rotation_id",),
        },
    ),
    Plan(
        table="climax_monitor_events",
        identity="id",
        timestamp="created_at",
        retention_days=RAW_RETENTION_DAYS,
        where_sql="""
            datetime(created_at) < datetime(:cutoff)
            AND created_at IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM event_states AS es
                WHERE es.event_id = climax_monitor_events.event_id
                  AND es.state NOT IN ('idle', 'expired')
                  AND (es.expires_at IS NULL OR datetime(es.expires_at) > datetime(:as_of))
            )
            AND NOT EXISTS (
                SELECT 1
                FROM climax_root_events AS re
                WHERE re.root_event_id = climax_monitor_events.root_event_id
                  AND re.invalidated_at IS NULL
            )
            AND NOT EXISTS (
                SELECT 1
                FROM climax_entry_attempts AS ca
                WHERE ca.attempt_id = climax_monitor_events.attempt_id
                  AND ca.attempt_closed_at IS NULL
                  AND (ca.attempt_state IS NULL OR ca.attempt_state NOT IN ('SHADOW_ACTIONABLE', 'EXPIRED', 'INVALIDATED', 'ROOT_REPLACED'))
            )
        """,
        required_tables=("event_states", "climax_root_events", "climax_entry_attempts"),
        required_columns={
            "climax_monitor_events": ("id", "event_id", "root_event_id", "attempt_id", "created_at"),
            "event_states": ("event_id", "state", "expires_at"),
            "climax_root_events": ("root_event_id", "invalidated_at"),
            "climax_entry_attempts": ("attempt_id", "attempt_state", "attempt_closed_at"),
        },
    ),
    Plan(
        table="reject_stats",
        identity="id",
        timestamp="logged_at",
        retention_days=RAW_RETENTION_DAYS,
        where_sql="""
            datetime(logged_at) < datetime(:cutoff)
            AND logged_at IS NOT NULL
        """,
        required_tables=(),
        required_columns={"reject_stats": ("id", "logged_at")},
    ),
    Plan(
        table="strategy_observations",
        identity="observation_id",
        timestamp="observed_at",
        retention_days=STRATEGY_RETENTION_DAYS,
        where_sql="""
            datetime(observed_at) < datetime(:cutoff)
            AND observed_at IS NOT NULL
            AND outcome_status = 'complete'
            AND outcome_updated_at IS NOT NULL
            AND outcome_next_attempt_at IS NULL
            AND NOT EXISTS (
                SELECT 1
                FROM climax_root_events AS re
                WHERE re.root_event_id = strategy_observations.root_event_id
                  AND re.invalidated_at IS NULL
            )
            AND NOT EXISTS (
                SELECT 1
                FROM climax_entry_attempts AS ca
                WHERE ca.attempt_id = strategy_observations.attempt_id
                  AND ca.attempt_closed_at IS NULL
                  AND (ca.attempt_state IS NULL OR ca.attempt_state NOT IN ('SHADOW_ACTIONABLE', 'EXPIRED', 'INVALIDATED', 'ROOT_REPLACED'))
            )
            AND (
                strategy_observations.signal_id IS NULL
                OR NOT EXISTS (
                    SELECT 1
                    FROM signals AS s
                    LEFT JOIN signal_outcomes AS so ON so.signal_id = s.id
                    WHERE s.id = strategy_observations.signal_id
                      AND (so.signal_id IS NULL OR so.price_after_4h IS NULL)
                )
            )
        """,
        required_tables=("event_states", "climax_root_events", "climax_entry_attempts", "signals", "signal_outcomes"),
        required_columns={
            "strategy_observations": (
                "observation_id", "root_event_id", "attempt_id", "signal_id",
                "observed_at", "outcome_status", "outcome_updated_at", "outcome_next_attempt_at",
            ),
            "event_states": ("event_id", "state", "expires_at"),
            "climax_root_events": ("root_event_id", "invalidated_at"),
            "climax_entry_attempts": ("attempt_id", "attempt_state", "attempt_closed_at"),
            "signals": ("id",),
            "signal_outcomes": ("signal_id", "price_after_4h"),
        },
    ),
    Plan(
        table="runtime_heartbeat_history",
        identity="id",
        timestamp="created_at",
        retention_days=HEARTBEAT_RETENTION_DAYS,
        where_sql="""
            datetime(created_at) < datetime(:cutoff)
            AND created_at IS NOT NULL
        """,
        required_tables=(),
        required_columns={"runtime_heartbeat_history": ("id", "created_at")},
    ),
)

TARGET_TABLES = frozenset(plan.table for plan in PLANS)
PROTECTED_TABLES = frozenset({
    "__db_heartbeat", "event_states", "signals", "signal_provenance",
    "signal_outcomes", "telegram_delivery_outbox", "watch_candidates",
    "climax_root_events", "climax_entry_attempts", "paper_trade_fills",
    "early_drop_warnings",
})


class RetentionError(RuntimeError):
    pass


class RetentionBusy(RetentionError):
    pass


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _utc(value: datetime | str | None = None) -> datetime:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _params(as_of: datetime, cutoff: datetime) -> dict[str, str | int]:
    return {
        "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
        "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})")
    }


def _preflight(connection: sqlite3.Connection) -> None:
    tables = _tables(connection)
    missing_tables = sorted(
        table for plan in PLANS for table in (plan.table, *plan.required_tables) if table not in tables
    )
    if missing_tables:
        raise RetentionError("missing retention schema tables: " + ", ".join(sorted(set(missing_tables))))
    missing_columns: list[str] = []
    for plan in PLANS:
        for table, required in plan.required_columns.items():
            missing = sorted(set(required) - _columns(connection, table))
            if missing:
                missing_columns.append(f"{table}: {', '.join(missing)}")
    if missing_columns:
        raise RetentionError("missing retention schema columns: " + "; ".join(missing_columns))
    if not TARGET_TABLES.isdisjoint(PROTECTED_TABLES):
        raise RetentionError("internal retention target protection invariant failed")


def _count(connection: sqlite3.Connection, plan: Plan, params: dict[str, str | int]) -> int:
    sql = (
        f"SELECT COUNT(*) FROM {_quote_identifier(plan.table)} "
        f"WHERE {plan.where_sql}"
    )
    return int(connection.execute(sql, params).fetchone()[0])


def _select_batch_ids(
    connection: sqlite3.Connection,
    plan: Plan,
    params: dict[str, str | int],
    batch_size: int,
) -> list[object]:
    identity = _quote_identifier(plan.identity)
    table = _quote_identifier(plan.table)
    sql = (
        f"SELECT {identity} FROM {table} WHERE {plan.where_sql} "
        f"ORDER BY {identity} LIMIT :limit"
    )
    select_params = dict(params)
    select_params["limit"] = batch_size
    return [row[0] for row in connection.execute(sql, select_params).fetchall()]


def _delete_batch(
    connection: sqlite3.Connection,
    plan: Plan,
    params: dict[str, str | int],
    batch_size: int,
) -> int:
    candidate_ids = _select_batch_ids(connection, plan, params, batch_size)
    if not candidate_ids:
        return 0
    identity = _quote_identifier(plan.identity)
    table = _quote_identifier(plan.table)
    placeholders = ", ".join(f":candidate_{index}" for index in range(len(candidate_ids)))
    sql = (
        f"DELETE FROM {table} WHERE {identity} IN ({placeholders}) "
        f"AND ({plan.where_sql})"
    )
    delete_params = dict(params)
    delete_params.update(
        {f"candidate_{index}": value for index, value in enumerate(candidate_ids)}
    )
    try:
        connection.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            raise RetentionBusy("database busy; retry on the next scheduled run") from exc
        raise
    try:
        cursor = connection.execute(sql, delete_params)
        deleted = max(0, int(cursor.rowcount))
        connection.commit()
        return deleted
    except Exception:
        connection.rollback()
        raise


@contextmanager
def _lane_lock(path: Path) -> Iterator[bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _disk_snapshot(path: Path) -> dict[str, int]:
    usage = shutil.disk_usage(path.parent)
    wal = Path(f"{path}-wal")
    return {
        "free_bytes": int(usage.free),
        "database_bytes": int(path.stat().st_size),
        "wal_bytes": int(wal.stat().st_size) if wal.exists() else 0,
    }


def _connect(path: Path, *, read_only: bool) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    busy_timeout_ms = 30_000 if read_only else BUSY_TIMEOUT_APPLY_MS
    if read_only:
        connection = sqlite3.connect(
            f"file:{resolved}?mode=ro", uri=True, timeout=busy_timeout_ms / 1000
        )
    else:
        connection = sqlite3.connect(resolved, timeout=busy_timeout_ms / 1000)
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
    connection.execute("PRAGMA foreign_keys=ON")
    if read_only:
        connection.execute("PRAGMA query_only=ON")
    return connection


def run_lane(
    lane: Lane,
    *,
    apply: bool,
    as_of: datetime,
    batch_size: int,
    max_run_seconds: float,
    minimum_free_bytes: int,
    catch_up: bool = False,
) -> dict[str, object]:
    lock_path = lane.runtime_path / "research-retention-batch.lock"
    report: dict[str, object] = {
        "schema_version": 1,
        "policy_version": POLICY_VERSION,
        "source_policy_version": SOURCE_POLICY_VERSION,
        "lane": lane.name,
        "mode": "APPLY" if apply else "DRY_RUN",
        "as_of_utc": as_of.isoformat(),
        "batch_size": batch_size,
        "max_run_seconds": max_run_seconds,
        "minimum_free_bytes": minimum_free_bytes,
        "catch_up": catch_up,
        "tables": [],
    }
    deadline = time.monotonic() + max_run_seconds
    stopped_reason: str | None = None
    with _lane_lock(lock_path) as acquired:
        if not acquired:
            report["status"] = "SKIPPED_LOCKED"
            return report
        before_disk = _disk_snapshot(lane.db_path)
        report["disk_before"] = before_disk
        if apply and before_disk["free_bytes"] < minimum_free_bytes:
            raise RetentionError(
                f"disk guard: free={before_disk['free_bytes']} below minimum={minimum_free_bytes}"
            )
        connection = _connect(lane.db_path, read_only=not apply)
        try:
            _preflight(connection)
            reports: list[dict[str, object]] = []
            for plan in PLANS:
                plan_batch = max(batch_size, plan.batch_size)
                plan_cap = 0 if catch_up else plan.max_rows_per_run
                cutoff = as_of - timedelta(days=plan.retention_days)
                params = _params(as_of, cutoff)
                eligible_before = _count(connection, plan, params)
                deleted = 0
                capped = False
                if apply:
                    while True:
                        if time.monotonic() >= deadline:
                            stopped_reason = "time_budget"
                            break
                        if plan_cap and deleted >= plan_cap:
                            capped = True
                            break
                        disk = _disk_snapshot(lane.db_path)
                        if disk["free_bytes"] < minimum_free_bytes:
                            raise RetentionError(
                                f"disk guard during {plan.table}: free={disk['free_bytes']} below minimum={minimum_free_bytes}"
                            )
                        try:
                            count = _delete_batch(connection, plan, params, plan_batch)
                        except RetentionBusy:
                            stopped_reason = "database_busy"
                            break
                        deleted += count
                        if count == 0:
                            break
                        time.sleep(BATCH_PAUSE_SEC)
                eligible_after = (
                    max(0, eligible_before - deleted)
                    if stopped_reason
                    else _count(connection, plan, params)
                )
                reports.append(
                    {
                        "table": plan.table,
                        "label": plan.label or plan.table,
                        "timestamp": plan.timestamp,
                        "retention_days": plan.retention_days,
                        "cutoff_utc": cutoff.isoformat(),
                        "batch_size": plan_batch,
                        "max_rows_per_run": plan_cap,
                        "capped": capped,
                        "eligible_before": eligible_before,
                        "deleted": deleted,
                        "eligible_after": eligible_after,
                    }
                )
                if stopped_reason:
                    break
            report["tables"] = reports
            report["status"] = "PARTIAL" if stopped_reason else "OK"
            if stopped_reason:
                report["stopped_reason"] = stopped_reason
            report["query_only"] = int(connection.execute("PRAGMA query_only").fetchone()[0])
        finally:
            connection.close()
        report["disk_after"] = _disk_snapshot(lane.db_path)
        report["deleted_total"] = sum(int(item["deleted"]) for item in report["tables"])
    return report


def _write_report(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform guarded batched deletes")
    parser.add_argument("--as-of", help="UTC boundary, default: current UTC")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT)
    parser.add_argument("--max-run-seconds", type=float, default=MAX_RUN_SECONDS_DEFAULT)
    parser.add_argument("--minimum-free-bytes", type=int, default=MIN_FREE_BYTES_DEFAULT)
    parser.add_argument("--lane", choices=("A", "B", "all"), default="all")
    parser.add_argument("--report-dir", type=Path)
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="one-shot backlog catch-up: lift per-plan per-run orphan caps and "
        "loop within the policy until drained or the time budget is exhausted",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.batch_size < 1 or args.batch_size > 10_000:
        print("retention error: batch size must be between 1 and 10000", file=sys.stderr)
        return 2
    if args.max_run_seconds <= 0 or args.max_run_seconds > 3600:
        print("retention error: max run seconds must be between 0 and 3600", file=sys.stderr)
        return 2
    if args.minimum_free_bytes < 0:
        print("retention error: minimum free bytes must be non-negative", file=sys.stderr)
        return 2
    as_of = _utc(args.as_of)
    lanes = LANES if args.lane == "all" else tuple(lane for lane in LANES if lane.name == args.lane)
    results: list[dict[str, object]] = []
    failed = False
    for lane in lanes:
        try:
            result = run_lane(
                lane,
                apply=args.apply,
                as_of=as_of,
                batch_size=args.batch_size,
                max_run_seconds=args.max_run_seconds,
                minimum_free_bytes=args.minimum_free_bytes,
                catch_up=args.catch_up,
            )
        except (FileNotFoundError, RetentionError, sqlite3.Error, OSError) as exc:
            result = {
                "schema_version": 1,
                "policy_version": POLICY_VERSION,
                "source_policy_version": SOURCE_POLICY_VERSION,
                "lane": lane.name,
                "mode": "APPLY" if args.apply else "DRY_RUN",
                "as_of_utc": as_of.isoformat(),
                "status": "ERROR",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            failed = True
        results.append(result)
        report_dir = args.report_dir or (lane.runtime_path / "research-retention-batch.json").parent
        _write_report(report_dir / f"research-retention-batch-{lane.name}.json", result)
        print(
            f"lane={lane.name} status={result.get('status')} "
            f"mode={result.get('mode')} deleted={result.get('deleted_total', 0)}"
        )
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
