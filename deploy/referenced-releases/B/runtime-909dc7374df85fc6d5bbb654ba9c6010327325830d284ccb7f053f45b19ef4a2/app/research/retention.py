"""Read-only, lifecycle-aware retention eligibility for research telemetry."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Mapping


DecisionName = Literal["KEEP", "ARCHIVE_ONLY", "ARCHIVE_AND_DELETE_ELIGIBLE"]
TERMINAL_ATTEMPT_STATES = {"SHADOW_ACTIONABLE", "EXPIRED", "INVALIDATED", "ROOT_REPLACED"}
TERMINAL_SCAN_STATUSES = {"EXCLUDED", "SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
TERMINAL_OUTCOME_STATUSES = {"MATURE"}


@dataclass(frozen=True)
class TableSpec:
    name: str
    category: str
    lifecycle_type: str
    identity_columns: tuple[str, ...]
    timestamp_column: str | None
    terminal_rule: str
    outcome_rule: str
    active_reference_guards: tuple[str, ...]
    archive_condition: str
    delete_condition: str
    hot_window_key: str | None
    max_maturity_minutes: int | None
    policy_version: str


@dataclass(frozen=True)
class RetentionPolicy:
    policy_version: str
    table_specs: Mapping[str, TableSpec]
    dependency_graph: tuple[dict[str, str], ...]
    hot_window_default: timedelta | None = None
    hot_window_by_table: Mapping[str, timedelta] = field(default_factory=dict)
    artifact_sha256: str = ""


@dataclass(frozen=True)
class RetentionContext:
    as_of_utc: datetime
    active_event_ids: frozenset[str] = frozenset()
    active_root_event_ids: frozenset[str] = frozenset()
    active_attempt_ids: frozenset[str] = frozenset()
    pending_signal_ids: frozenset[int] = frozenset()
    pending_outcome_ids: frozenset[tuple[str, str]] = frozenset()
    pending_scheduler_ids: frozenset[tuple[str, str]] = frozenset()
    matured_outcome_ids: frozenset[tuple[str, str]] = frozenset()
    verified_archive_identities: frozenset[tuple[str, str]] = frozenset()
    hot_window_by_table: Mapping[str, timedelta] = field(default_factory=dict)
    dependency_complete: bool = True


@dataclass(frozen=True)
class RetentionDecision:
    table: str
    identity: dict[str, object]
    decision: DecisionName
    reason_code: str
    protection_reasons: tuple[str, ...]
    policy_version: str


@dataclass(frozen=True)
class RetentionDryRun:
    policy_version: str
    as_of_utc: str
    source_sha256_before: str
    source_sha256_after: str
    source_unchanged: bool
    table_count: int
    tables: tuple[dict[str, object], ...]
    dependency_graph: tuple[dict[str, str], ...]
    representative_cases: tuple[dict[str, object], ...]
    dependency_graph_sha256: str = ""
    dependency_graph_complete: bool = True
    protected_identity_counts: dict[str, int] = field(default_factory=dict)
    reason_counts: dict[str, int] = field(default_factory=dict)
    policy_sha256: str = ""
    query_only: int = 1
    verification_status: str = "DRY_RUN"


def load_policy(path: Path) -> RetentionPolicy:
    """Load and validate an immutable JSON policy definition."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    version = str(payload.get("policy_version") or "")
    if not version:
        raise ValueError("retention policy has no policy_version")
    raw_tables = payload.get("tables")
    if not isinstance(raw_tables, dict) or not raw_tables:
        raise ValueError("retention policy has no tables")
    specs: dict[str, TableSpec] = {}
    for name, raw in raw_tables.items():
        if not isinstance(raw, dict) or raw.get("category") not in {"A", "B", "C", "D", "E", "F"}:
            raise ValueError(f"invalid retention metadata for table: {name}")
        required_fields = {
            "category", "lifecycle_type", "identity", "timestamp", "terminal_rule", "outcome_rule",
            "active_reference_guards", "archive_condition", "delete_condition", "hot_window_key",
            "max_maturity_minutes",
        }
        if not required_fields.issubset(raw):
            missing = ", ".join(sorted(required_fields - set(raw)))
            raise ValueError(f"retention metadata missing for table {name}: {missing}")
        identity = tuple(str(value) for value in raw.get("identity", ()))
        if not identity:
            raise ValueError(f"table has no row identity: {name}")
        specs[str(name)] = TableSpec(
            name=str(name),
            category=str(raw["category"]),
            lifecycle_type=str(raw.get("lifecycle_type") or "UNKNOWN"),
            identity_columns=identity,
            timestamp_column=str(raw["timestamp"]) if raw.get("timestamp") else None,
            terminal_rule=str(raw.get("terminal_rule") or "UNKNOWN"),
            outcome_rule=str(raw.get("outcome_rule") or "NONE"),
            active_reference_guards=tuple(str(value) for value in raw.get("active_reference_guards", ())),
            archive_condition=str(raw.get("archive_condition") or "UNKNOWN"),
            delete_condition=str(raw.get("delete_condition") or "UNKNOWN"),
            hot_window_key=str(raw["hot_window_key"]) if raw.get("hot_window_key") else None,
            max_maturity_minutes=(int(raw["max_maturity_minutes"]) if raw.get("max_maturity_minutes") is not None else None),
            policy_version=version,
        )
    graph = tuple(
        {str(key): str(value) for key, value in edge.items()}
        for edge in payload.get("dependency_graph", ())
        if isinstance(edge, dict)
    )
    raw_hot_windows = payload.get("hot_windows_minutes") or {}
    if not isinstance(raw_hot_windows, dict):
        raise ValueError("hot_windows_minutes must be an object")
    hot_windows: dict[str, timedelta] = {}
    for table, minutes in raw_hot_windows.items():
        if int(minutes) < 0:
            raise ValueError(f"hot window must be non-negative: {table}")
        hot_windows[str(table)] = timedelta(minutes=int(minutes))
    return RetentionPolicy(
        policy_version=version,
        table_specs=specs,
        dependency_graph=graph,
        hot_window_by_table=hot_windows,
        artifact_sha256=_sha256(Path(path)),
    )


def build_retention_context(connection: sqlite3.Connection, as_of_utc: datetime | str) -> RetentionContext:
    """Read runtime protection identities from an already opened read-only DB."""

    as_of = _utc(as_of_utc)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    active_event_ids: set[str] = set()
    active_root_event_ids: set[str] = set()
    active_attempt_ids: set[str] = set()
    pending_signal_ids: set[int] = set()
    pending_outcome_ids: set[tuple[str, str]] = set()
    pending_scheduler_ids: set[tuple[str, str]] = set()
    matured_outcome_ids: set[tuple[str, str]] = set()
    dependency_complete = True

    if _has_table(connection, "event_states") and {"event_id", "state", "expires_at"}.issubset(_table_columns(connection, "event_states")):
        for row in connection.execute("SELECT event_id, state, expires_at FROM event_states"):
            expires = _parse_optional(row["expires_at"])
            if row["event_id"] and row["state"] not in {"idle", "expired"} and (expires is None or expires > as_of):
                active_event_ids.add(str(row["event_id"]))
    if _has_table(connection, "climax_root_events") and {"root_event_id", "invalidated_at"}.issubset(_table_columns(connection, "climax_root_events")):
        for row in connection.execute("SELECT root_event_id FROM climax_root_events WHERE invalidated_at IS NULL"):
            if row["root_event_id"]:
                active_root_event_ids.add(str(row["root_event_id"]))
    if _has_table(connection, "climax_entry_attempts") and {"attempt_id", "attempt_state", "attempt_closed_at"}.issubset(_table_columns(connection, "climax_entry_attempts")):
        for row in connection.execute("SELECT attempt_id, attempt_state, attempt_closed_at FROM climax_entry_attempts"):
            if row["attempt_id"] and row["attempt_closed_at"] is None and row["attempt_state"] not in TERMINAL_ATTEMPT_STATES:
                active_attempt_ids.add(str(row["attempt_id"]))
    signals_present = _has_table(connection, "signals")
    signal_outcomes_present = _has_table(connection, "signal_outcomes")
    if signals_present != signal_outcomes_present:
        dependency_complete = False
    if signals_present and signal_outcomes_present:
        if not {"id"}.issubset(_table_columns(connection, "signals")) or not {"signal_id", "price_after_4h"}.issubset(_table_columns(connection, "signal_outcomes")):
            dependency_complete = False
        else:
            for row in connection.execute(
                "SELECT s.id FROM signals AS s LEFT JOIN signal_outcomes AS o ON o.signal_id = s.id "
                "WHERE o.signal_id IS NULL OR o.price_after_4h IS NULL"
            ):
                pending_signal_ids.add(int(row[0]))
    if _has_table(connection, "strategy_observations"):
        columns = _table_columns(connection, "strategy_observations")
        if not {"observation_id", "outcome_status"}.issubset(columns):
            dependency_complete = False
        else:
            next_column = "outcome_next_attempt_at" if "outcome_next_attempt_at" in columns else "NULL"
            for row in connection.execute(
                f"SELECT observation_id, outcome_status, {next_column} AS next_attempt_at FROM strategy_observations"
            ):
                status = str(row["outcome_status"] or "")
                if status in {"", "incomplete"} or (status == "unknown" and row["next_attempt_at"] is not None):
                    pending_outcome_ids.add(("strategy_observations", str(row["observation_id"])))
    for table, identity, status_column, due_column in (
        ("root_detector_shadow_episode_outcomes", "episode_id", "outcome_status", "outcome_next_due_at"),
        ("root_detector_shadow_v2_outcomes", "shadow_v2_root_id", "outcome_status", "outcome_next_due_at"),
        ("current_root_outcomes", "root_event_id", "outcome_status", "outcome_next_due_at"),
    ):
        if not _has_table(connection, table):
            continue
        if not {identity, status_column, due_column}.issubset(_table_columns(connection, table)):
            dependency_complete = False
            continue
        for row in connection.execute(f"SELECT {identity}, {status_column}, {due_column} FROM {table}"):
            key = (table, str(row[identity]))
            status = str(row[status_column] or "")
            if status == "MATURE" and row[due_column] is None:
                matured_outcome_ids.add(key)
            elif status != "MATURE" or row[due_column] is not None:
                pending_scheduler_ids.add(key)
    return RetentionContext(
        as_of_utc=as_of,
        active_event_ids=frozenset(active_event_ids),
        active_root_event_ids=frozenset(active_root_event_ids),
        active_attempt_ids=frozenset(active_attempt_ids),
        pending_signal_ids=frozenset(pending_signal_ids),
        pending_outcome_ids=frozenset(pending_outcome_ids),
        pending_scheduler_ids=frozenset(pending_scheduler_ids),
        matured_outcome_ids=frozenset(matured_outcome_ids),
        dependency_complete=dependency_complete,
    )


def evaluate_row(
    row: Mapping[str, object],
    table_spec: TableSpec,
    context: RetentionContext,
) -> RetentionDecision:
    """Evaluate one row using fail-closed lifecycle precedence."""

    identity = {column: row.get(column) for column in table_spec.identity_columns}
    if any(value is None for value in identity.values()) or not context.dependency_complete:
        return _decision(table_spec, identity, "KEEP", "KEEP_UNKNOWN_DEPENDENCY")
    identity_key = _identity_key(identity)
    if table_spec.category in {"A", "B"} or table_spec.terminal_rule == "NEVER":
        return _decision(table_spec, identity, "KEEP", "KEEP_OPERATIONAL_STATE")
    if _value(row, "event_id") in context.active_event_ids:
        return _decision(table_spec, identity, "KEEP", "KEEP_ACTIVE_EVENT", ("event_id",))
    if _value(row, "root_event_id") in context.active_root_event_ids or _value(row, "live_root_event_id") in context.active_root_event_ids:
        return _decision(table_spec, identity, "KEEP", "KEEP_ACTIVE_ROOT", ("root_event_id",))
    if _value(row, "attempt_id") in context.active_attempt_ids:
        return _decision(table_spec, identity, "KEEP", "KEEP_ACTIVE_ATTEMPT", ("attempt_id",))
    signal_id = row.get("signal_id")
    if signal_id is not None and int(signal_id) in context.pending_signal_ids:
        return _decision(table_spec, identity, "KEEP", "KEEP_PENDING_SIGNAL_OUTCOME", ("signal_id",))
    row_status = str(row.get("outcome_status") or "")
    if row_status in {"incomplete", "PARTIAL", "CENSORED"}:
        return _decision(table_spec, identity, "KEEP", "KEEP_PENDING_OUTCOME", ("outcome_status",))
    if row_status == "unknown" and row.get("outcome_next_attempt_at") is not None:
        return _decision(table_spec, identity, "KEEP", "KEEP_RETRYABLE_ERROR", ("outcome_status",))
    if (table_spec.name, identity_key) in context.pending_outcome_ids:
        reason = "KEEP_RETRYABLE_ERROR" if row_status in {"unknown", "RETRYABLE_ERROR"} else "KEEP_PENDING_OUTCOME"
        return _decision(table_spec, identity, "KEEP", reason, ("outcome_status",))
    if (table_spec.name, identity_key) in context.pending_scheduler_ids:
        reason = "KEEP_RETRYABLE_ERROR" if row_status in {"unknown", "RETRYABLE_ERROR"} else "KEEP_PENDING_OUTCOME"
        return _decision(table_spec, identity, "KEEP", reason, ("scheduler_due",))
    if not _terminal(row, table_spec, context):
        return _decision(table_spec, identity, "KEEP", "KEEP_INCOMPLETE_MATURATION")
    timestamp = _parse_optional(row.get(table_spec.timestamp_column)) if table_spec.timestamp_column else None
    if timestamp is None:
        return _decision(table_spec, identity, "KEEP", "KEEP_UNKNOWN_DEPENDENCY")
    hot_window = context.hot_window_by_table.get(table_spec.hot_window_key or table_spec.name)
    if hot_window is not None and timestamp + hot_window >= context.as_of_utc:
        return _decision(table_spec, identity, "ARCHIVE_ONLY", "KEEP_HOT_WINDOW")
    archive_key = (table_spec.name, identity_key)
    if archive_key not in context.verified_archive_identities:
        return _decision(table_spec, identity, "ARCHIVE_ONLY", "ARCHIVE_TERMINAL_MATURE")
    if hot_window is None:
        return _decision(table_spec, identity, "ARCHIVE_ONLY", "ARCHIVE_TERMINAL_MATURE")
    return _decision(table_spec, identity, "ARCHIVE_AND_DELETE_ELIGIBLE", "DELETE_ELIGIBLE_TERMINAL_ARCHIVED")


def evaluate_database(
    db_path: str | Path,
    policy: RetentionPolicy,
    *,
    as_of_utc: datetime | str | None = None,
    sample_limit: int = 30,
) -> RetentionDryRun:
    """Evaluate every source table without creating files or modifying SQLite."""

    source_path = Path(db_path).expanduser().resolve()
    as_of = _utc(as_of_utc or datetime.now(timezone.utc))
    before_hash = _sha256(source_path)
    connection = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        query_only = int(connection.execute("PRAGMA query_only").fetchone()[0])
        context = build_retention_context(connection, as_of)
        context = replace(
            context,
            hot_window_by_table=policy.hot_window_by_table,
        )
        source_tables = [str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        reports: list[dict[str, object]] = []
        cases: list[dict[str, object]] = []
        overall_reasons = Counter()
        dependency_complete = True
        for table_name in source_tables:
            spec = policy.table_specs.get(table_name)
            total = int(connection.execute(f"SELECT COUNT(*) FROM {_quote(table_name)}").fetchone()[0])
            if spec is None:
                dependency_complete = False
                overall_reasons["KEEP_UNKNOWN_DEPENDENCY"] += total
                report = _empty_report(table_name, total, "KEEP_UNKNOWN_DEPENDENCY")
                reports.append(report)
                if total and len(cases) < sample_limit:
                    cases.append({"table": table_name, "identity": {}, "reason_code": "KEEP_UNKNOWN_DEPENDENCY"})
                continue
            columns = _table_columns(connection, table_name)
            required = set(spec.identity_columns)
            if spec.timestamp_column:
                required.add(spec.timestamp_column)
            if not required.issubset(columns):
                dependency_complete = False
                overall_reasons["KEEP_UNKNOWN_DEPENDENCY"] += total
                reports.append(_empty_report(table_name, total, "KEEP_UNKNOWN_DEPENDENCY", category=spec.category))
                continue
            selected = sorted(required | {
                name for name in columns if name in {
                    "event_id", "root_event_id", "live_root_event_id", "attempt_id", "signal_id", "evaluation_id",
                    "source_evaluation_id", "legacy_candidate_id", "episode_id", "rotation_id", "cycle_id",
                    "outcome_status", "outcome_next_attempt_at", "outcome_next_due_at", "outcome_json", "episode_status",
                    "closed_at", "invalidated_at", "event_type", "new_state", "terminal_status", "scan_status", "status",
                    "completed_at", "cycle_completed_at", "rotation_completed_at", "evaluation_completed_at", "lifecycle_state",
                    "live_root_created", "state",
                }
            })
            cursor = connection.execute(
                f"SELECT {', '.join(_quote(name) for name in selected)} FROM {_quote(table_name)}"
            )
            counts = Counter()
            reasons = Counter()
            min_timestamp: datetime | None = None
            max_timestamp: datetime | None = None
            for raw in iter(lambda: cursor.fetchmany(1000), []):
                for sqlite_row in raw:
                    row = dict(sqlite_row)
                    decision = evaluate_row(row, spec, context)
                    counts[decision.decision.lower()] += 1
                    reasons[decision.reason_code] += 1
                    overall_reasons[decision.reason_code] += 1
                    timestamp = _parse_optional(row.get(spec.timestamp_column)) if spec.timestamp_column else None
                    if timestamp is not None:
                        min_timestamp = timestamp if min_timestamp is None else min(min_timestamp, timestamp)
                        max_timestamp = timestamp if max_timestamp is None else max(max_timestamp, timestamp)
                    if len(cases) < sample_limit and _is_diagnostic_case(decision.reason_code, timestamp, spec, as_of):
                        cases.append({
                            "table": table_name,
                            "identity": decision.identity,
                            "timestamp": timestamp.isoformat() if timestamp else None,
                            "age_seconds": round((as_of - timestamp).total_seconds(), 3) if timestamp else None,
                            "reason_code": decision.reason_code,
                            "protection_reasons": list(decision.protection_reasons),
                        })
            reports.append({
                "table_name": table_name,
                "category": spec.category,
                "lifecycle_type": spec.lifecycle_type,
                "total": total,
                "keep": counts["keep"],
                "archive_only": counts["archive_only"],
                "delete_eligible": counts["archive_and_delete_eligible"],
                "unknown": reasons["KEEP_UNKNOWN_DEPENDENCY"],
                "reason_counts": dict(sorted(reasons.items())),
                "min_timestamp": min_timestamp.isoformat() if min_timestamp else None,
                "max_timestamp": max_timestamp.isoformat() if max_timestamp else None,
            })
        after_hash = _sha256(source_path)
        graph = tuple(sorted(policy.dependency_graph, key=lambda edge: tuple(edge.items())))
        graph_json = json.dumps(graph, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return RetentionDryRun(
            policy_version=policy.policy_version,
            as_of_utc=as_of.isoformat(),
            source_sha256_before=before_hash,
            source_sha256_after=after_hash,
            source_unchanged=before_hash == after_hash,
            table_count=len(source_tables),
            tables=tuple(reports),
            dependency_graph=graph,
            representative_cases=tuple(sorted(cases, key=lambda case: (str(case.get("table")), json.dumps(case.get("identity", {}), sort_keys=True)))),
            dependency_graph_sha256=hashlib.sha256(graph_json).hexdigest(),
            dependency_graph_complete=dependency_complete and context.dependency_complete,
            protected_identity_counts={
                "active_event": len(context.active_event_ids),
                "active_root": len(context.active_root_event_ids),
                "active_attempt": len(context.active_attempt_ids),
                "pending_signal_outcome": len(context.pending_signal_ids),
                "pending_outcome": len(context.pending_outcome_ids),
                "pending_scheduler": len(context.pending_scheduler_ids),
            },
            reason_counts=dict(sorted(overall_reasons.items())),
            policy_sha256=policy.artifact_sha256,
            query_only=query_only,
        )
    finally:
        connection.close()


def _terminal(row: Mapping[str, object], spec: TableSpec, context: RetentionContext) -> bool:
    rule = spec.terminal_rule
    if rule in {"APPEND_ONLY_TERMINAL", "APPEND_ONLY_REFERENCED", "HEARTBEAT_HISTORY_RECORD", "MAPPED", "ROOT_LINK"}:
        return True
    if rule == "STRATEGY_OUTCOME":
        return row.get("outcome_status") == "complete"
    if rule == "SHADOW_OUTCOME":
        status = str(row.get("outcome_status") or "")
        if status == "MATURE":
            return True
        if status == "DATA_GAP" and row.get("outcome_next_due_at") is None:
            return _data_gap_is_explicitly_terminal(row.get("outcome_json"))
        return (spec.name, _identity_key({key: row.get(key) for key in spec.identity_columns})) in context.matured_outcome_ids
    if rule == "EVALUATION_COMPLETED":
        return row.get("evaluation_completed_at") is not None or row.get("lifecycle_state") == "EVALUATED"
    if rule == "ATTEMPT_EVENT":
        return row.get("event_type") in {"attempt_closed", "attempt_state_changed"} or row.get("new_state") in TERMINAL_ATTEMPT_STATES
    if rule == "COVERAGE_ROW":
        return row.get("scan_status") in TERMINAL_SCAN_STATUSES
    if rule == "SCAN_RESULT":
        return row.get("terminal_status") in TERMINAL_SCAN_STATUSES and row.get("completed_at") is not None
    if rule == "SCAN_CYCLE":
        return row.get("status") in {"COMPLETED", "PARTIAL"} and row.get("cycle_completed_at") is not None
    if rule == "SCAN_ROTATION":
        return row.get("status") in {"COMPLETED", "INCOMPLETE", "ABORTED_RESTART", "FAILED"} and row.get("rotation_completed_at") is not None
    if rule == "CANDIDATE_OUTCOME":
        if row.get("outcome_status") == "MATURE":
            return True
        if row.get("outcome_status") == "CENSORED" and row.get("outcome_next_due_at") is None:
            return True
        if row.get("outcome_status") == "DATA_GAP" and row.get("outcome_next_due_at") is None:
            return _data_gap_is_explicitly_terminal(row.get("outcome_json"))
        return False
    if rule == "EPISODE_CLOSED":
        if row.get("episode_status") == "CLOSED" and row.get("closed_at") is not None:
            return True
        key = ("root_detector_shadow_episode_outcomes", str(row.get("episode_id")))
        return key in context.matured_outcome_ids
    if rule == "V2_ROOT":
        key = ("root_detector_shadow_v2_outcomes", str(row.get("shadow_v2_root_id")))
        return key in context.matured_outcome_ids
    return False


def _data_gap_is_explicitly_terminal(value: object) -> bool:
    if not value:
        return False
    try:
        payload = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError):
        return False
    horizons = payload.get("horizons") if isinstance(payload, dict) else None
    return bool(horizons) and all(
        (isinstance(item, dict) and (item.get("status") == "DATA_GAP" or item.get("price") is not None))
        for item in horizons.values()
    )


def _decision(spec: TableSpec, identity: dict[str, object], decision: DecisionName, reason: str, protections: tuple[str, ...] = ()) -> RetentionDecision:
    return RetentionDecision(spec.name, identity, decision, reason, protections, spec.policy_version)


def _empty_report(table_name: str, total: int, reason: str, *, category: str = "F") -> dict[str, object]:
    return {
        "table_name": table_name, "category": category, "lifecycle_type": "UNKNOWN", "total": total,
        "keep": total, "archive_only": 0, "delete_eligible": 0, "unknown": total,
        "reason_counts": {reason: total}, "min_timestamp": None, "max_timestamp": None,
    }


def _is_diagnostic_case(reason: str, timestamp: datetime | None, spec: TableSpec, as_of: datetime) -> bool:
    return reason.startswith("KEEP_ACTIVE") or reason in {"KEEP_PENDING_OUTCOME", "KEEP_RETRYABLE_ERROR"} and timestamp is not None and spec.max_maturity_minutes is not None and timestamp + timedelta(minutes=spec.max_maturity_minutes) < as_of


def _has_table(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({_quote(table)})")}


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _value(row: Mapping[str, object], key: str) -> str | None:
    value = row.get(key)
    return str(value) if value is not None else None


def _identity_key(identity: Mapping[str, object]) -> str:
    values = [str(value) for value in identity.values()]
    return values[0] if len(values) == 1 else "|".join(values)


def _parse_optional(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return _utc(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
