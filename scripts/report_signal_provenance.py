"""Read-only export of deterministic signal provenance and Telegram delivery state."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


def _connect_read_only(database_path: Path) -> sqlite3.Connection:
    uri = database_path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _delivery_state(raw_status: str | None) -> str:
    if raw_status == "SENT":
        return "SENT"
    if raw_status in {"PENDING", "RETRY", "IN_FLIGHT"}:
        return "PENDING"
    if raw_status == "DEAD":
        return "FAILED"
    return "NO_OUTBOX"


def _has_table(connection: sqlite3.Connection, table_name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
    ).fetchone() is not None


def _provenance_source(connection: sqlite3.Connection) -> str:
    if _has_table(connection, "signal_provenance"):
        return """
            SELECT signal_id, strategy_family, strategy_branch, root_event_id,
                   decision_evaluation_id, admission_evaluation_id, code_version,
                   config_hash, runtime_instance_id, runtime_started_at, decision_at,
                   signal_created_at
            FROM signal_provenance
        """
    return """
        SELECT NULL AS signal_id, NULL AS strategy_family, NULL AS strategy_branch,
               NULL AS root_event_id, NULL AS decision_evaluation_id,
               NULL AS admission_evaluation_id, NULL AS code_version, NULL AS config_hash,
               NULL AS runtime_instance_id, NULL AS runtime_started_at, NULL AS decision_at,
               NULL AS signal_created_at
        WHERE 0
    """


def _signal_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        WITH provenance_rows AS ({_provenance_source(connection)})
        SELECT
            s.id AS signal_id,
            s.symbol,
            s.event_id,
            s.signal_time,
            COALESCE(p.strategy_branch, 'LEGACY_UNKNOWN_BRANCH') AS strategy_branch,
            p.strategy_family,
            p.root_event_id,
            p.decision_evaluation_id,
            p.admission_evaluation_id,
            p.code_version,
            p.config_hash,
            p.runtime_instance_id,
            p.runtime_started_at,
            p.decision_at,
            p.signal_created_at,
            d.status AS raw_outbox_status,
            d.attempt_count AS outbox_attempt_count,
            d.sent_at AS outbox_sent_at
        FROM signals AS s
        LEFT JOIN provenance_rows AS p ON p.signal_id = s.id
        LEFT JOIN telegram_delivery_outbox AS d ON d.id = (
            SELECT MAX(candidate.id)
            FROM telegram_delivery_outbox AS candidate
            WHERE candidate.entity_type = 'SIGNAL' AND candidate.entity_id = s.id
        )
        ORDER BY s.id
        """
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["delivery_state"] = _delivery_state(item["raw_outbox_status"])
        result.append(item)
    return result


def _legacy_dry_run(connection: sqlite3.Connection) -> dict[str, Any]:
    statuses = connection.execute(
        """
        SELECT status, COUNT(*) AS row_count
        FROM telegram_delivery_outbox
        WHERE entity_type = 'SIGNAL'
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()
    provenance_exists = _has_table(connection, "signal_provenance")
    observation_exists = _has_table(connection, "strategy_observations")
    if provenance_exists and observation_exists:
        exact_observation_links = connection.execute(
            """
            SELECT COUNT(DISTINCT p.signal_id)
            FROM signal_provenance AS p
            JOIN strategy_observations AS observation
              ON observation.strategy = p.strategy_branch
             AND observation.evaluation_id IN (p.decision_evaluation_id, p.admission_evaluation_id)
            """
        ).fetchone()[0]
    else:
        exact_observation_links = 0
    signals_total = connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    exact_provenance_rows = (
        connection.execute("SELECT COUNT(*) FROM signal_provenance").fetchone()[0]
        if provenance_exists
        else 0
    )
    return {
        "signals_total": signals_total,
        "exact_provenance_rows": exact_provenance_rows,
        "signals_without_provenance": signals_total - exact_provenance_rows,
        "exact_observation_links": exact_observation_links,
        "outbox_status_counts": {row["status"]: row["row_count"] for row in statuses},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path, help="SQLite database path; opened with mode=ro")
    parser.add_argument("--format", choices=("csv", "json"), default="csv")
    parser.add_argument("--legacy-dry-run", action="store_true", help="Print persisted-link counts without writing")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    with _connect_read_only(args.database) as connection:
        if args.legacy_dry_run:
            print(json.dumps(_legacy_dry_run(connection), sort_keys=True))
            return
        rows = _signal_rows(connection)
    if args.format == "json":
        print(json.dumps(rows, default=str, ensure_ascii=False))
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]) if rows else ["signal_id"])
    writer.writeheader()
    writer.writerows(rows)


if __name__ == "__main__":
    main()
