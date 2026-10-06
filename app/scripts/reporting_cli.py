from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


def add_report_window_args(parser: argparse.ArgumentParser) -> None:
    window_group = parser.add_mutually_exclusive_group()
    window_group.add_argument(
        "--since",
        type=_parse_since_arg,
        help="Include rows at or after this ISO timestamp, e.g. 2026-06-20T04:26:33Z",
    )
    window_group.add_argument(
        "--since-minutes",
        type=_positive_int,
        help="Include rows from the last N minutes.",
    )


def resolve_since(args: argparse.Namespace, *, now: datetime | None = None) -> datetime | None:
    if getattr(args, "since", None) is not None:
        return args.since
    since_minutes = getattr(args, "since_minutes", None)
    if since_minutes is None:
        return None
    current_time = now or datetime.now(timezone.utc)
    return current_time - timedelta(minutes=since_minutes)


def _parse_since_arg(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid --since timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--since-minutes must be > 0")
    return parsed


def _timestamp(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = _parse_since_arg(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _time_clause(column: str, since: str | None, until: str | None) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []
    if since is not None:
        clauses.append(f"datetime({column}) >= datetime(?)")
        params.append(since)
    if until is not None:
        clauses.append(f"datetime({column}) < datetime(?)")
        params.append(until)
    return (" AND ".join(clauses) or "1=1", params)


def _count(connection: sqlite3.Connection, table: str, where: str, params: list[Any]) -> int:
    if not _table_exists(connection, table):
        return 0
    return int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params).fetchone()[0])


def _incomplete_mapping_report() -> dict[str, Any]:
    diagnostic = "invalid explicit A/B lane mapping"
    return {
        "coverage": "INCOMPLETE",
        "window": {"since": None, "until": None, "status": "INCOMPLETE"},
        "lanes": [],
        "mapping": {},
        "diagnostic": diagnostic,
        "diagnostics": [diagnostic],
    }


def build_two_lane_coverage_report(
    connection: sqlite3.Connection,
    lane_runtime_mapping: Mapping[str, str],
    *,
    since: datetime | str | None = None,
    until: datetime | str | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Build a sanitized, SELECT-only A/B report from persisted telemetry.

    Runtime IDs are accepted only through the explicit A/B mapping.  Missing
    telemetry is represented as zeroes and ``INCOMPLETE``, never as setup.
    """
    if not isinstance(lane_runtime_mapping, Mapping):
        return _incomplete_mapping_report()
    if set(lane_runtime_mapping) != {"A", "B"} or any(
        not isinstance(value, str) or not value.strip() for value in lane_runtime_mapping.values()
    ):
        return _incomplete_mapping_report()
    normalized_mapping = {lane: lane_runtime_mapping[lane].strip() for lane in ("A", "B")}
    if normalized_mapping["A"] == normalized_mapping["B"]:
        return _incomplete_mapping_report()
    connection.execute("PRAGMA query_only=ON")
    since_value = _timestamp(since)
    until_value = _timestamp(until)
    now_value = _timestamp(now) or datetime.now(timezone.utc).isoformat()
    lanes: list[dict[str, Any]] = []

    for lane in ("A", "B"):
        runtime_id = normalized_mapping[lane]
        result: dict[str, Any] = {
            "lane": lane,
            "runtime_instance_id": runtime_id,
            "window": {"since": since_value, "until": until_value},
            "scheduled": 0,
            "completed": 0,
            "scanned_ok": 0,
            "scan_failed_by_reason": {},
            "actionable_initial": 0,
            "final_actionable": 0,
            "blocked_by_recheck": 0,
            "signals_persisted": 0,
            "outbox_sent": 0,
            "heartbeat_age": None,
            "max_duration": None,
            "max_event_loop_lag": None,
        }
        scan_where, scan_params = _time_clause("scheduled_at", since_value, until_value)
        scan_where = f"runtime_instance_id = ? AND {scan_where}"
        scan_params = [runtime_id, *scan_params]
        if _table_exists(connection, "market_scan_symbol_results"):
            result["scheduled"] = _count(connection, "market_scan_symbol_results", scan_where, scan_params)
            completed_where = f"{scan_where} AND completed_at IS NOT NULL"
            result["completed"] = _count(connection, "market_scan_symbol_results", completed_where, scan_params)
            result["scanned_ok"] = _count(
                connection, "market_scan_symbol_results",
                f"{scan_where} AND UPPER(terminal_status) IN ('SCANNED_OK', 'OK', 'SUCCESS')",
                scan_params,
            )
            failures = connection.execute(
                f"SELECT COALESCE(reason_code, 'UNKNOWN') AS reason, COUNT(*) AS n "
                f"FROM market_scan_symbol_results WHERE {scan_where} "
                "AND UPPER(terminal_status) NOT IN ('SCANNED_OK', 'OK', 'SUCCESS') "
                "GROUP BY reason_code ORDER BY reason_code",
                scan_params,
            ).fetchall()
            result["scan_failed_by_reason"] = {str(row["reason"]): int(row["n"]) for row in failures}
            max_duration = connection.execute(
                f"SELECT MAX(duration_ms) FROM market_scan_symbol_results WHERE {scan_where}", scan_params
            ).fetchone()[0]
            result["max_duration"] = max_duration

        obs_where, obs_params = _time_clause("observed_at", since_value, until_value)
        obs_where = f"runtime_instance_id = ? AND {obs_where}"
        obs_params = [runtime_id, *obs_params]
        telemetry_gaps: list[str] = []
        observation_count = 0
        if _table_exists(connection, "strategy_observations"):
            observation_count = _count(connection, "strategy_observations", obs_where, obs_params)
            result["actionable_initial"] = _count(
                connection, "strategy_observations", f"{obs_where} AND UPPER(initial_decision) = 'ACTIONABLE'", obs_params
            )
            result["final_actionable"] = _count(
                connection, "strategy_observations",
                f"{obs_where} AND UPPER(final_decision) = 'ACTIONABLE' AND UPPER(final_reason) = 'FINAL_ACTIONABLE'",
                obs_params,
            )
            result["blocked_by_recheck"] = _count(
                connection, "strategy_observations", f"{obs_where} AND UPPER(final_reason) = 'BLOCKED_BY_RECHECK'", obs_params
            )
        if _table_exists(connection, "signal_provenance") and _table_exists(connection, "signals"):
            signal_where, signal_params = _time_clause("p.decision_at", since_value, until_value)
            result["signals_persisted"] = int(connection.execute(
                f"SELECT COUNT(DISTINCT p.signal_id) FROM signal_provenance p JOIN signals s ON s.id=p.signal_id "
                f"WHERE p.runtime_instance_id=? AND {signal_where}", [runtime_id, *signal_params]
            ).fetchone()[0])
            if _table_exists(connection, "telegram_delivery_outbox"):
                result["outbox_sent"] = int(connection.execute(
                    f"SELECT COUNT(DISTINCT d.entity_id) FROM telegram_delivery_outbox d "
                    f"JOIN signal_provenance p ON p.signal_id=d.entity_id WHERE d.entity_type='SIGNAL' "
                    f"AND d.status='SENT' AND p.runtime_instance_id=? AND {signal_where}", [runtime_id, *signal_params]
                ).fetchone()[0])

        heartbeat = None
        if _table_exists(connection, "runtime_heartbeat_history"):
            heartbeat_where, heartbeat_params = _time_clause("created_at", since_value, until_value)
            heartbeat = connection.execute(
                "SELECT created_at, event_loop_lag_ms FROM runtime_heartbeat_history "
                f"WHERE runtime_instance_id=? AND {heartbeat_where} "
                "ORDER BY datetime(created_at) DESC, rowid DESC LIMIT 1",
                [runtime_id, *heartbeat_params],
            ).fetchone()
            if heartbeat:
                heartbeat_time = _parse_since_arg(str(heartbeat["created_at"]))
                current = _parse_since_arg(now_value)
                result["heartbeat_age"] = max(0.0, (current - heartbeat_time).total_seconds())
                result["max_event_loop_lag"] = connection.execute(
                    f"SELECT MAX(event_loop_lag_ms) FROM runtime_heartbeat_history "
                    f"WHERE runtime_instance_id=? AND {heartbeat_where}",
                    [runtime_id, *heartbeat_params],
                ).fetchone()[0]

        if result["scheduled"] == 0:
            telemetry_gaps.append("no scheduled scan telemetry")
        if result["completed"] < result["scheduled"]:
            telemetry_gaps.append("scan completion telemetry is incomplete")
        if result["scheduled"] and heartbeat is None:
            telemetry_gaps.append("heartbeat telemetry is missing")
        if result["scheduled"] and observation_count == 0:
            telemetry_gaps.append("observation telemetry is missing")
        required_tables = (
            "market_scan_symbol_results",
            "strategy_observations",
            "signals",
            "signal_provenance",
            "telegram_delivery_outbox",
            "runtime_heartbeat_history",
        )
        for table in required_tables:
            if not _table_exists(connection, table):
                telemetry_gaps.append(f"required telemetry table is missing: {table}")
        provenance_count = 0
        if _table_exists(connection, "signal_provenance"):
            provenance_where, provenance_params = _time_clause("decision_at", since_value, until_value)
            provenance_count = _count(
                connection,
                "signal_provenance",
                f"runtime_instance_id = ? AND {provenance_where}",
                [runtime_id, *provenance_params],
            )
        if result["actionable_initial"] and provenance_count == 0:
            telemetry_gaps.append("signal provenance telemetry is missing")
        if result["signals_persisted"] and not _table_exists(connection, "telegram_delivery_outbox"):
            telemetry_gaps.append("delivery outbox telemetry is missing")
        result["coverage"] = "INCOMPLETE" if telemetry_gaps else "COMPLETE"
        result["window"]["status"] = result["coverage"]
        if telemetry_gaps:
            result["diagnostics"] = telemetry_gaps
        lanes.append(result)

    overall = "COMPLETE" if all(lane["coverage"] == "COMPLETE" for lane in lanes) else "INCOMPLETE"
    return {
        "coverage": overall,
        "window": {"since": since_value, "until": until_value, "status": overall},
        "lanes": lanes,
        "mapping": normalized_mapping,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only two-lane coverage report")
    parser.add_argument("database", type=Path)
    parser.add_argument("--lane-map", required=True, type=Path, help="JSON object containing explicit A and B runtime IDs")
    parser.add_argument("--since", type=_parse_since_arg)
    parser.add_argument("--until", type=_parse_since_arg)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    mapping = json.loads(args.lane_map.read_text(encoding="utf-8"))
    with sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        print(json.dumps(build_two_lane_coverage_report(connection, mapping, since=args.since, until=args.until), default=str, sort_keys=True))


if __name__ == "__main__":
    main()
