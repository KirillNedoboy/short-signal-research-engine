"""Offline, read-only SQLite to Parquet research archive tooling.

This module is intentionally outside the live runtime path. It never opens a
source database for writing and has no retention-delete operation.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

RESEARCH_APPEND_ONLY = "RESEARCH_APPEND_ONLY"
OPERATIONAL_RECENT = "OPERATIONAL_RECENT"
OPERATIONAL_CRITICAL = "OPERATIONAL_CRITICAL"
DERIVED_REBUILDABLE = "DERIVED_REBUILDABLE"

TABLE_CLASSIFICATION: dict[str, str] = {
    "__db_heartbeat": OPERATIONAL_CRITICAL,
    "event_states": OPERATIONAL_CRITICAL,
    "signals": OPERATIONAL_CRITICAL,
    "signal_provenance": OPERATIONAL_CRITICAL,
    "signal_outcomes": OPERATIONAL_CRITICAL,
    "telegram_delivery_outbox": OPERATIONAL_CRITICAL,
    "watch_candidates": OPERATIONAL_CRITICAL,
    "climax_root_events": OPERATIONAL_CRITICAL,
    "climax_entry_attempts": OPERATIONAL_CRITICAL,
    "runtime_heartbeats": OPERATIONAL_RECENT,
    "runtime_heartbeat_history": OPERATIONAL_RECENT,
    "climax_evaluations": RESEARCH_APPEND_ONLY,
    "volume_climax_observations": RESEARCH_APPEND_ONLY,
    "strategy_observations": RESEARCH_APPEND_ONLY,
    "climax_entry_attempt_events": RESEARCH_APPEND_ONLY,
    "climax_monitor_events": RESEARCH_APPEND_ONLY,
    "market_coverage_ledger": RESEARCH_APPEND_ONLY,
    "market_scan_cycles": RESEARCH_APPEND_ONLY,
    "market_scan_rotations": RESEARCH_APPEND_ONLY,
    "market_scan_symbol_results": RESEARCH_APPEND_ONLY,
    "reject_stats": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_candidates": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_episodes": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_observations": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_episode_outcomes": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_episode_root_links": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_legacy_mappings": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_v2_roots": RESEARCH_APPEND_ONLY,
    "root_detector_shadow_v2_outcomes": RESEARCH_APPEND_ONLY,
    "current_root_outcomes": RESEARCH_APPEND_ONLY,
    "current_root_predicate_snapshots": RESEARCH_APPEND_ONLY,
}

TIMESTAMP_PRIORITY = (
    "created_at",
    "evaluation_time",
    "observed_at",
    "logged_at",
    "signal_time",
    "scheduled_at",
    "cycle_started_at",
    "rotation_started_at",
    "first_seen_at",
    "opened_at",
    "updated_at",
)


@dataclass(frozen=True)
class TablePlan:
    name: str
    classification: str
    columns: tuple[dict[str, Any], ...]
    primary_key: tuple[str, ...]
    timestamp_column: str | None


@dataclass(frozen=True)
class ArchiveResult:
    manifest_path: Path | None
    manifest: dict[str, Any]


class ArchiveError(RuntimeError):
    """Raised when an archive cannot be safely verified or published."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("UTC timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _open_read_only(db_path: str | Path) -> sqlite3.Connection:
    resolved = Path(db_path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
        connection.close()
        raise ArchiveError("source connection is not query_only")
    return connection


def sqlite_observation(db_path: str | Path) -> dict[str, Any]:
    """Return read-only SQLite/WAL facts without checkpointing or mutation."""
    resolved = Path(db_path).expanduser().resolve()
    before = _sha256_file(resolved)
    with _open_read_only(resolved) as connection:
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        query_only = int(connection.execute("PRAGMA query_only").fetchone()[0])
    wal_path = Path(f"{resolved}-wal")
    after = _sha256_file(resolved)
    return {
        "database_path": resolved.name,
        "database_size_bytes": resolved.stat().st_size,
        "wal_path": wal_path.name,
        "wal_size_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
        "journal_mode": journal_mode,
        "page_count": page_count,
        "page_size": page_size,
        "user_version": user_version,
        "query_only": query_only,
        "source_sha256_before": before,
        "source_sha256_after": after,
        "source_unchanged": before == after,
        "checkpoint_performed": False,
    }


def discover_tables(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [str(row[0]) for row in rows]


def _select_timestamp_column(columns: Sequence[dict[str, Any]]) -> str | None:
    names = {str(column["name"]) for column in columns}
    selected = next((name for name in TIMESTAMP_PRIORITY if name in names), None)
    if selected is not None:
        return selected
    return next(
        (
            str(column["name"])
            for column in columns
            if str(column["name"]).lower().endswith(("_at", "_time", "_date", "timestamp"))
        ),
        None,
    )


def build_table_plans(connection: sqlite3.Connection, requested: Sequence[str] | None = None) -> list[TablePlan]:
    tables = discover_tables(connection)
    selected = list(requested) if requested else [
        table for table in tables if TABLE_CLASSIFICATION.get(table) == RESEARCH_APPEND_ONLY
    ]
    unknown = sorted(set(selected) - set(tables))
    if unknown:
        raise ArchiveError(f"tables not present in source: {', '.join(unknown)}")
    blocked = [table for table in selected if TABLE_CLASSIFICATION.get(table) != RESEARCH_APPEND_ONLY]
    if blocked:
        raise ArchiveError(
            "refusing to archive non-research table(s): " + ", ".join(sorted(blocked))
        )
    plans: list[TablePlan] = []
    for table in selected:
        columns = tuple(dict(row) for row in connection.execute(f"PRAGMA table_info({_quote(table)})"))
        if not columns:
            raise ArchiveError(f"table has no columns: {table}")
        primary_key = tuple(row["name"] for row in sorted(columns, key=lambda row: int(row["pk"])) if row["pk"])
        timestamp_column = _select_timestamp_column(columns)
        plans.append(TablePlan(table, TABLE_CLASSIFICATION[table], columns, primary_key, timestamp_column))
    return plans


def archive_database(
    db_path: str | Path,
    output_dir: str | Path,
    cutoff: str | datetime,
    *,
    tables: Sequence[str] | None = None,
    batch_size: int = 50_000,
    dry_run: bool = False,
) -> ArchiveResult:
    """Export selected research rows before ``cutoff`` without source writes."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    cutoff_utc = parse_utc(cutoff)
    source_path = Path(db_path).expanduser().resolve()
    source_sha = _sha256_file(source_path)
    output = Path(output_dir).expanduser().resolve()
    with _open_read_only(source_path) as connection:
        plans = build_table_plans(connection, tables)
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        selected_names = [plan.name for plan in plans]
        run_id = hashlib.sha256(
            _canonical_json(
                {
                    "source_sha256": source_sha,
                    "cutoff_utc": cutoff_utc.isoformat(),
                    "tables": selected_names,
                    "schema_version": user_version,
                }
            ).encode()
        ).hexdigest()[:32]
        if dry_run:
            table_reports = [_collect_table_report(connection, plan, cutoff_utc, None) for plan in plans]
            return ArchiveResult(
                None,
                _base_manifest(
                    run_id,
                    source_path,
                    source_sha,
                    user_version,
                    cutoff_utc,
                    table_reports,
                    batch_size_rows=batch_size,
                    verification_status="DRY_RUN",
                ),
            )

        manifest_dir = output / "manifests"
        complete_path = manifest_dir / f"archive-run-{run_id}.json"
        if complete_path.exists():
            existing = json.loads(complete_path.read_text(encoding="utf-8"))
            if existing.get("verification_status") != "COMPLETE":
                raise ArchiveError(f"existing manifest is not complete: {complete_path}")
            verification = verify_manifest(complete_path, source_path)
            if verification["verification_status"] != "PASS":
                raise ArchiveError(f"existing archive failed verification: {verification}")
            return ArchiveResult(complete_path, existing)

        output.mkdir(parents=True, exist_ok=True)
        manifest_dir.mkdir(parents=True, exist_ok=True)
        attempt_id = uuid.uuid4().hex[:12]
        staging = output / ".staging" / f"{run_id}-{attempt_id}"
        staging.mkdir(parents=True, exist_ok=False)
        incomplete_path = manifest_dir / f"archive-run-{run_id}.incomplete-{attempt_id}.json"
        incomplete = _base_manifest(
            run_id,
            source_path,
            source_sha,
            user_version,
            cutoff_utc,
            [],
            batch_size_rows=batch_size,
            verification_status="INCOMPLETE",
            warnings=["staging publish has not completed"],
        )
        _atomic_json_write(incomplete_path, incomplete)
        try:
            table_reports: list[dict[str, Any]] = []
            for plan in plans:
                report = _write_table_staging(
                    connection,
                    plan,
                    cutoff_utc,
                    staging,
                    output,
                    run_id,
                    batch_size,
                )
                table_reports.append(report)
            manifest = _base_manifest(
                run_id,
                source_path,
                source_sha,
                user_version,
                cutoff_utc,
                table_reports,
                batch_size_rows=batch_size,
                verification_status="STAGED",
            )
            staged_manifest = staging / "manifest.json"
            _atomic_json_write(staged_manifest, manifest)
            staged_verification = verify_manifest(staged_manifest, source_path, allow_staged=True)
            if staged_verification["verification_status"] != "PASS":
                raise ArchiveError(f"staged archive verification failed: {staged_verification}")
            _publish_staged_files(staging, output)
            manifest["verification_status"] = "COMPLETE"
            manifest["verification"] = staged_verification
            _atomic_json_write(complete_path, manifest)
            _fsync_directory(manifest_dir)
            shutil.rmtree(staging, ignore_errors=True)
            return ArchiveResult(complete_path, manifest)
        except Exception as exc:
            incomplete["errors"] = [str(exc)]
            _atomic_json_write(incomplete_path, incomplete)
            raise


def verify_manifest(
    manifest_path: str | Path,
    source_db: str | Path | None = None,
    *,
    allow_staged: bool = False,
) -> dict[str, Any]:
    """Verify file hashes and optionally compare exported rows to source SQLite."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("verification_status") == "INCOMPLETE" and not allow_staged:
        return {"verification_status": "INCOMPLETE", "errors": ["incomplete manifest is not publishable"]}
    errors: list[str] = []
    archive_root = path.parent if path.name == "manifest.json" else path.parent.parent
    for table in manifest.get("tables", []):
        archived_rows: list[dict[str, Any]] = []
        for file_info in table.get("archive_files", []):
            parquet_path = archive_root / file_info["path"]
            if not parquet_path.exists():
                errors.append(f"missing archive file: {parquet_path}")
                continue
            if _sha256_file(parquet_path) != file_info["sha256"]:
                errors.append(f"hash mismatch: {parquet_path}")
            if parquet_path.stat().st_size != file_info["file_size_bytes"]:
                errors.append(f"size mismatch: {parquet_path}")
            try:
                actual_rows = _read_parquet_rows(parquet_path)
                archived_rows.extend(actual_rows)
            except Exception as exc:
                errors.append(f"cannot read parquet {parquet_path}: {exc}")
                continue
            if len(actual_rows) != file_info["row_count"]:
                errors.append(f"row mismatch: {parquet_path}")
        if table.get("archive_row_count") != len(archived_rows):
            errors.append(f"manifest file row total mismatch: {table['table_name']}")
        archive_mismatches = _compare_table_reports(
            table,
            _report_from_archive_rows(table, archived_rows),
        )
        errors.extend(f"{table['table_name']}: archived {message}" for message in archive_mismatches)

    source_verification: dict[str, Any] | None = None
    if source_db is not None:
        source_path = Path(source_db).expanduser().resolve()
        with _open_read_only(source_path) as connection:
            source_verification = {"tables": {}}
            cutoff = parse_utc(manifest["cutoff_utc"])
            for table in manifest.get("tables", []):
                plan = next(plan for plan in build_table_plans(connection, [table["table_name"]]))
                actual = _collect_table_report(connection, plan, cutoff, None)
                mismatches = _compare_table_reports(table, actual)
                source_verification["tables"][table["table_name"]] = mismatches
                errors.extend(f"{table['table_name']}: source {message}" for message in mismatches)
            if _sha256_file(source_path) != manifest.get("source_db_sha256"):
                errors.append("source database hash changed from manifest")
    return {
        "verification_status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "source": source_verification,
    }


def inventory_database(db_path: str | Path) -> dict[str, Any]:
    """Inventory every user table through a query-only snapshot connection."""
    source_path = Path(db_path).expanduser().resolve()
    observation = sqlite_observation(source_path)
    with _open_read_only(source_path) as connection:
        tables: list[dict[str, Any]] = []
        for table_name in discover_tables(connection):
            columns = tuple(dict(row) for row in connection.execute(f"PRAGMA table_info({_quote(table_name)})"))
            timestamp_column = _select_timestamp_column(columns)
            rows = [
                row[0]
                for row in connection.execute(
                    f"SELECT {_quote(timestamp_column)} FROM {_quote(table_name)} WHERE {_quote(timestamp_column)} IS NOT NULL"
                )
            ] if timestamp_column else []
            timestamps = [parsed for value in rows if (parsed := _try_parse_datetime(value)) is not None]
            approx_size = None
            try:
                approx_size = connection.execute(
                    "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE name = ?",
                    (table_name,),
                ).fetchone()[0]
            except sqlite3.DatabaseError:
                pass
            classification = TABLE_CLASSIFICATION.get(table_name, "UNCLASSIFIED")
            tables.append(
                {
                    "table_name": table_name,
                    "classification": classification,
                    "primary_key": [
                        str(column["name"])
                        for column in sorted(columns, key=lambda item: int(item["pk"]))
                        if column["pk"]
                    ],
                    "timestamp_columns": [
                        str(column["name"])
                        for column in columns
                        if any(token in str(column["name"]).lower() for token in ("time", "date"))
                    ],
                    "selected_timestamp_column": timestamp_column,
                    "row_count": int(connection.execute(f"SELECT COUNT(*) FROM {_quote(table_name)}").fetchone()[0]),
                    "approx_size_bytes": int(approx_size) if approx_size is not None else None,
                    "min_timestamp": min(timestamps).isoformat() if timestamps else None,
                    "max_timestamp": max(timestamps).isoformat() if timestamps else None,
                    "live_runtime_critical": classification == OPERATIONAL_CRITICAL,
                    "research_or_audit": classification == RESEARCH_APPEND_ONLY,
                    "rebuildable": False,
                    "current_retention": "no automated purge found",
                    "forensic_value": "high" if classification == RESEARCH_APPEND_ONLY else "critical" if classification == OPERATIONAL_CRITICAL else "medium",
                }
            )
    return {
        "source": observation,
        "table_count": len(tables),
        "tables": tables,
        "schema_version": observation["user_version"],
    }


def disk_budget_report(
    db_path: str | Path,
    archive_dir: str | Path,
    *,
    warning_free_bytes: int = 10 * 1024**3,
    critical_free_bytes: int = 2 * 1024**3,
) -> dict[str, Any]:
    """Estimate storage pressure without changing DB, WAL, or archive files."""
    db = Path(db_path).expanduser().resolve()
    archive = Path(archive_dir).expanduser().resolve()
    db_size = db.stat().st_size
    wal = Path(f"{db}-wal")
    archive_size = sum(path.stat().st_size for path in archive.rglob("*") if path.is_file()) if archive.exists() else 0
    usage = shutil.disk_usage(db.parent)
    with _open_read_only(db) as connection:
        plans = build_table_plans(connection)
        growth_rows = 0
        oldest: datetime | None = None
        newest: datetime | None = None
        for plan in plans:
            if plan.timestamp_column is None:
                continue
            rows = connection.execute(
                f"SELECT {_quote(plan.timestamp_column)} FROM {_quote(plan.name)} WHERE {_quote(plan.timestamp_column)} IS NOT NULL"
            ).fetchall()
            parsed = [parsed for row in rows if (parsed := _try_parse_datetime(row[0])) is not None]
            if parsed:
                growth_rows += len(parsed)
                oldest = min(oldest, min(parsed)) if oldest else min(parsed)
                newest = max(newest, max(parsed)) if newest else max(parsed)
    span_days = max((newest - oldest).total_seconds() / 86400, 1.0) if oldest and newest else None
    daily_rows = growth_rows / span_days if span_days else None
    daily_db_growth = db_size / span_days if span_days else None
    days_to_warning = ((usage.free - warning_free_bytes) / daily_db_growth) if daily_db_growth and usage.free > warning_free_bytes else 0.0
    return {
        "active_db_size_bytes": db_size,
        "wal_size_bytes": wal.stat().st_size if wal.exists() else 0,
        "archive_size_bytes": archive_size,
        "free_disk_bytes": usage.free,
        "daily_research_rows_estimate": daily_rows,
        "daily_db_growth_bytes_estimate": daily_db_growth,
        "warning_free_bytes": warning_free_bytes,
        "critical_free_bytes": critical_free_bytes,
        "estimated_days_to_warning_threshold": max(0.0, days_to_warning),
        "status": "CRITICAL" if usage.free <= critical_free_bytes else "WARNING" if usage.free <= warning_free_bytes else "OK",
        "method": "rough observed snapshot span; no alert integration",
    }


def simulate_cleanup_on_copy(db_copy: str | Path, table: str, cutoff: str | datetime) -> dict[str, Any]:
    """Perform a hypothetical delete only on a caller-supplied disposable copy."""
    path = Path(db_copy).expanduser().resolve()
    cutoff_utc = parse_utc(cutoff)
    before_hash = _sha256_file(path)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            plans = build_table_plans(connection, [table])
            plan = plans[0]
            if plan.timestamp_column is None:
                raise ArchiveError(f"table has no timestamp column: {table}")
            before = connection.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0]
            eligible = connection.execute(
                f"SELECT COUNT(*) FROM {_quote(table)} WHERE {_quote(plan.timestamp_column)} < ?",
                (cutoff_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),),
            ).fetchone()[0]
            connection.execute(
                f"DELETE FROM {_quote(table)} WHERE {_quote(plan.timestamp_column)} < ?",
                (cutoff_utc.strftime("%Y-%m-%d %H:%M:%S.%f"),),
            )
            remaining = connection.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0]
        return {
            "table": table,
            "rows_before": before,
            "rows_eligible": eligible,
            "rows_remaining": remaining,
            "copy_sha256_before": before_hash,
            "copy_sha256_after": _sha256_file(path),
            "vacuum_performed": False,
        }
    finally:
        connection.close()


def _write_table_staging(
    connection: sqlite3.Connection,
    plan: TablePlan,
    cutoff: datetime,
    staging: Path,
    output: Path,
    run_id: str,
    batch_size: int,
) -> dict[str, Any]:
    rows = [dict(row) for row in connection.execute(f"SELECT * FROM {_quote(plan.name)}")]
    selected = [row for row in rows if _row_before(row.get(plan.timestamp_column) if plan.timestamp_column else None, cutoff)]
    selected.sort(key=lambda row: _sort_key(row, plan.primary_key, plan.columns))
    partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    warnings: list[str] = []
    for row in selected:
        partition = _partition_for(row.get(plan.timestamp_column) if plan.timestamp_column else None)
        if partition == "year=unknown/month=unknown":
            warnings.append("rows with null/unparseable timestamp placed in unknown partition")
        partitions[partition].append(row)

    archive_files: list[dict[str, Any]] = []
    for partition, partition_rows in sorted(partitions.items()):
        for chunk_index in range(0, len(partition_rows), batch_size):
            chunk = partition_rows[chunk_index : chunk_index + batch_size]
            relative = Path(f"dataset={plan.name}") / partition / f"part-{run_id}-{chunk_index // batch_size:05d}.parquet"
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            _write_parquet(target, chunk, plan.columns)
            archive_files.append(
                {
                    "path": relative.as_posix(),
                    "row_count": len(chunk),
                    "file_size_bytes": target.stat().st_size,
                    "sha256": _sha256_file(target),
                }
            )
    return _collect_table_report(connection, plan, cutoff, selected, archive_files, warnings)


def _collect_table_report(
    connection: sqlite3.Connection,
    plan: TablePlan,
    cutoff: datetime,
    selected_rows: Sequence[dict[str, Any]] | None,
    archive_files: Sequence[dict[str, Any]] | None = None,
    warnings: Sequence[str] | None = None,
) -> dict[str, Any]:
    if selected_rows is None:
        rows = [dict(row) for row in connection.execute(f"SELECT * FROM {_quote(plan.name)}")]
        selected_rows = [row for row in rows if _row_before(row.get(plan.timestamp_column) if plan.timestamp_column else None, cutoff)]
        selected_rows = sorted(selected_rows, key=lambda row: _sort_key(row, plan.primary_key, plan.columns))
    source_row_count = int(connection.execute(f"SELECT COUNT(*) FROM {_quote(plan.name)}").fetchone()[0])
    rows = list(selected_rows)
    null_counts = {
        str(column["name"]): sum(row.get(str(column["name"])) is None for row in rows)
        for column in plan.columns
    }
    numeric: dict[str, dict[str, Any]] = {}
    for column in plan.columns:
        name = str(column["name"])
        declared = str(column.get("type") or "").upper()
        values = [row[name] for row in rows if row.get(name) is not None]
        if not values or not any(token in declared for token in ("INT", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
            continue
        numeric[name] = {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "sum": sum(values),
        }
    timestamp_values = [_try_parse_datetime(row.get(plan.timestamp_column)) for row in rows] if plan.timestamp_column else []
    timestamp_values = [value for value in timestamp_values if value is not None]
    schema = [
        {"name": str(column["name"]), "type": str(column["type"] or ""), "notnull": int(column["notnull"]), "pk": int(column["pk"])}
        for column in plan.columns
    ]
    identities = [_identity_value(row, plan) for row in rows]
    return {
        "table_name": plan.name,
        "classification": plan.classification,
        "primary_key": list(plan.primary_key),
        "timestamp_column": plan.timestamp_column,
        "source_row_count": source_row_count,
        "selected_row_count": len(rows),
        "archive_row_count": len(rows),
        "min_timestamp": min(timestamp_values).isoformat() if timestamp_values else None,
        "max_timestamp": max(timestamp_values).isoformat() if timestamp_values else None,
        "archive_files": list(archive_files or []),
        "schema": schema,
        "schema_fingerprint": hashlib.sha256(_canonical_json(schema).encode()).hexdigest(),
        "null_counts": null_counts,
        "numeric_aggregates": numeric,
        "identity_digest": _digest(identities),
        "warnings": list(warnings or []),
    }



def _report_from_archive_rows(table: dict[str, Any], rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    columns = tuple(table.get("schema", []))
    plan = TablePlan(
        name=str(table["table_name"]),
        classification=str(table.get("classification", RESEARCH_APPEND_ONLY)),
        columns=columns,
        primary_key=tuple(table.get("primary_key", [])),
        timestamp_column=table.get("timestamp_column"),
    )
    null_counts = {
        str(column["name"]): sum(row.get(str(column["name"])) is None for row in rows)
        for column in columns
    }
    numeric: dict[str, dict[str, Any]] = {}
    for column in columns:
        name = str(column["name"])
        declared = str(column.get("type") or "").upper()
        values = [row[name] for row in rows if row.get(name) is not None]
        if not values or not any(token in declared for token in ("INT", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
            continue
        numeric[name] = {"count": len(values), "min": min(values), "max": max(values), "sum": sum(values)}
    timestamp_values = [_try_parse_datetime(row.get(plan.timestamp_column)) for row in rows] if plan.timestamp_column else []
    timestamp_values = [value for value in timestamp_values if value is not None]
    return {
        "table_name": plan.name,
        "selected_row_count": len(rows),
        "archive_row_count": len(rows),
        "min_timestamp": min(timestamp_values).isoformat() if timestamp_values else None,
        "max_timestamp": max(timestamp_values).isoformat() if timestamp_values else None,
        "schema_fingerprint": hashlib.sha256(_canonical_json(list(columns)).encode()).hexdigest(),
        "null_counts": null_counts,
        "numeric_aggregates": numeric,
        "identity_digest": _digest([_identity_value(row, plan) for row in rows]),
    }

def _compare_table_reports(expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    checks = (
        ("selected_row_count", "selected row count"),
        ("archive_row_count", "archive row count"),
        ("min_timestamp", "minimum timestamp"),
        ("max_timestamp", "maximum timestamp"),
        ("null_counts", "NULL counts"),
        ("numeric_aggregates", "numeric aggregates"),
        ("identity_digest", "identity digest"),
        ("schema_fingerprint", "schema fingerprint"),
    )
    return [f"{label} mismatch" for key, label in checks if expected.get(key) != actual.get(key)]


def _publish_staged_files(staging: Path, output: Path) -> None:
    for source in sorted(path for path in staging.rglob("*.parquet") if path.is_file()):
        relative = source.relative_to(staging)
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if _sha256_file(target) != _sha256_file(source):
                raise ArchiveError(f"refusing to overwrite divergent archive file: {target}")
            continue
        os.replace(source, target)
    _fsync_directory(output)


def _write_parquet(path: Path, rows: Sequence[dict[str, Any]], columns: Sequence[dict[str, Any]]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ArchiveError("pyarrow is required for archive export; install requirements-research.txt") from exc
    arrays: dict[str, Any] = {}
    for column in columns:
        name = str(column["name"])
        declared = str(column.get("type") or "")
        values = [_arrow_value(row.get(name), declared) for row in rows]
        arrays[name] = pa.array(values, type=_arrow_type(declared))
    table = pa.table(arrays)
    pq.write_table(table, path, compression="zstd", row_group_size=max(1, len(rows)))


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ArchiveError("pyarrow is required for archive verification") from exc
    return pq.read_table(path).to_pylist()


def _arrow_type(declared: str) -> Any:
    import pyarrow as pa

    upper = declared.upper()
    if "BOOL" in upper:
        return pa.bool_()
    if "INT" in upper:
        return pa.int64()
    if any(token in upper for token in ("REAL", "FLOA", "DOUB", "NUMERIC", "DECIMAL")):
        return pa.float64()
    if "DATE" in upper or "TIME" in upper:
        return pa.timestamp("us", tz="UTC")
    if "BLOB" in upper:
        return pa.binary()
    return pa.string()


def _arrow_value(value: Any, declared: str) -> Any:
    if value is None:
        return None
    upper = declared.upper()
    if "BOOL" in upper:
        return bool(value)
    if "INT" in upper:
        return int(value)
    if any(token in upper for token in ("REAL", "FLOA", "DOUB", "NUMERIC", "DECIMAL")):
        return float(value)
    if "DATE" in upper or "TIME" in upper:
        parsed = _try_parse_datetime(value)
        if parsed is None:
            raise ArchiveError(f"invalid timestamp value {value!r}")
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    if "BLOB" in upper:
        return bytes(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    return str(value)


def _base_manifest(
    run_id: str,
    source_path: Path,
    source_sha: str,
    schema_version: int,
    cutoff: datetime,
    tables: Sequence[dict[str, Any]],
    *,
    batch_size_rows: int,
    verification_status: str,
    warnings: Sequence[str] | None = None,
) -> dict[str, Any]:
    return {
        "archive_run_id": run_id,
        "created_at_utc": utc_now().isoformat(),
        "source_db_path_identifier": source_path.name,
        "source_db_sha256": source_sha,
        "source_code_sha": _resolve_code_sha(),
        "schema_version": schema_version,
        "dataset_epoch": None,
        "cutoff_utc": cutoff.isoformat(),
        "archive_format": "parquet",
        "partitioning": "dataset=<table>/year=<UTC year>/month=<UTC month>",
        "batch_size_rows": batch_size_rows,
        "tables": list(tables),
        "verification_status": verification_status,
        "errors": [],
        "warnings": list(warnings or []),
    }


def _identity_value(row: dict[str, Any], plan: TablePlan) -> Any:
    if plan.primary_key:
        return [row.get(name) for name in plan.primary_key]
    return {name: row.get(name) for name in sorted(row)}


def _sort_key(row: dict[str, Any], primary_key: Sequence[str], columns: Sequence[dict[str, Any]]) -> tuple[str, ...]:
    names = primary_key or tuple(str(column["name"]) for column in columns)
    return tuple(_canonical_scalar(row.get(name)) for name in names)


def _row_before(value: Any, cutoff: datetime) -> bool:
    if value is None:
        return True
    parsed = _try_parse_datetime(value)
    return parsed is not None and parsed < cutoff


def _partition_for(value: Any) -> str:
    parsed = _try_parse_datetime(value)
    if parsed is None:
        return "year=unknown/month=unknown"
    return f"year={parsed.year:04d}/month={parsed.month:02d}"


def _try_parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, bytes):
        return _try_parse_datetime(value.decode("utf-8", errors="replace"))
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_json_default)


def _resolve_code_sha() -> str:
    configured = os.getenv("SHORT_BOT_CODE_VERSION", "").strip()
    if configured:
        return configured[:64]
    repository_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip()[:64] or "unknown"


def _canonical_scalar(value: Any) -> str:
    return _canonical_json(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _digest(values: Iterable[Any]) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = _canonical_json(value).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quote(identifier: str) -> str:
    if not identifier or "\x00" in identifier:
        raise ValueError("invalid SQLite identifier")
    return '"' + identifier.replace('"', '""') + '"'


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
