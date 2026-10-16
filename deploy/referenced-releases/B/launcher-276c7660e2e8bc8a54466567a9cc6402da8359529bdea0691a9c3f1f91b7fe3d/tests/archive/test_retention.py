from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.research.retention import (
    RetentionContext,
    RetentionDecision,
    evaluate_database,
    evaluate_row,
    load_policy,
)


POLICY_PATH = Path(__file__).parents[2] / "app" / "research" / "retention_policy_v1.json"
AS_OF = datetime(2026, 8, 28, 13, 0, tzinfo=timezone.utc)


def _policy():
    return load_policy(POLICY_PATH)


def _context(**overrides) -> RetentionContext:
    values = dict(
        as_of_utc=AS_OF,
        active_event_ids=frozenset(),
        active_root_event_ids=frozenset(),
        active_attempt_ids=frozenset(),
        pending_signal_ids=frozenset(),
        pending_outcome_ids=frozenset(),
        pending_scheduler_ids=frozenset(),
        verified_archive_identities=frozenset(),
        hot_window_by_table={},
        dependency_complete=True,
    )
    values.update(overrides)
    return RetentionContext(**values)


def _row(**overrides) -> dict[str, object]:
    row = {
        "observation_id": "obs-1",
        "observed_at": "2026-08-20 12:00:00.000000",
        "root_event_id": None,
        "event_id": "event-1",
        "attempt_id": None,
        "signal_id": None,
        "outcome_status": "complete",
        "outcome_next_attempt_at": None,
    }
    row.update(overrides)
    return row


def _decision(row: dict[str, object], context: RetentionContext | None = None) -> RetentionDecision:
    policy = _policy()
    return evaluate_row(row, policy.table_specs["strategy_observations"], context or _context())


def test_active_event_never_delete_eligible() -> None:
    result = _decision(_row(event_id="active-event"), _context(active_event_ids=frozenset({"active-event"})))

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_ACTIVE_EVENT"


def test_active_root_never_delete_eligible() -> None:
    result = _decision(_row(root_event_id="root-1"), _context(active_root_event_ids=frozenset({"root-1"})))

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_ACTIVE_ROOT"


def test_active_attempt_never_delete_eligible() -> None:
    result = _decision(_row(attempt_id="attempt-1"), _context(active_attempt_ids=frozenset({"attempt-1"})))

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_ACTIVE_ATTEMPT"


def test_pending_signal_outcome_is_kept() -> None:
    result = _decision(_row(signal_id=17), _context(pending_signal_ids=frozenset({17})))

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_PENDING_SIGNAL_OUTCOME"


def test_retryable_error_is_not_terminal() -> None:
    result = _decision(
        _row(outcome_status="unknown", outcome_next_attempt_at="2026-08-28 12:59:00.000000"),
        _context(pending_outcome_ids=frozenset({("strategy_observations", "obs-1")})),
    )

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_RETRYABLE_ERROR"


def test_incomplete_outcome_is_kept() -> None:
    result = _decision(_row(outcome_status="incomplete"))

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_PENDING_OUTCOME"


def test_mature_terminal_row_is_archive_only_without_verified_archive() -> None:
    result = _decision(_row())

    assert result.decision == "ARCHIVE_ONLY"
    assert result.reason_code == "ARCHIVE_TERMINAL_MATURE"


def test_verified_archive_and_explicit_hot_window_enable_delete_candidate() -> None:
    table = _policy().table_specs["strategy_observations"]
    table = replace(table, hot_window_key="strategy_observations")
    context = _context(
        verified_archive_identities=frozenset({("strategy_observations", "obs-1")}),
        hot_window_by_table={"strategy_observations": timedelta(hours=1)},
    )

    result = evaluate_row(_row(), table, context)

    assert result.decision == "ARCHIVE_AND_DELETE_ELIGIBLE"
    assert result.reason_code == "DELETE_ELIGIBLE_TERMINAL_ARCHIVED"


def test_same_age_rows_split_by_lifecycle_not_timestamp() -> None:
    terminal = _decision(_row(observation_id="terminal"))
    pending = _decision(_row(observation_id="pending", outcome_status="incomplete"))

    assert terminal.decision == "ARCHIVE_ONLY"
    assert pending.decision == "KEEP"


def test_unknown_dependency_fails_closed() -> None:
    result = _decision(_row(), _context(dependency_complete=False))

    assert result.decision == "KEEP"
    assert result.reason_code == "KEEP_UNKNOWN_DEPENDENCY"


def test_archive_eligibility_precedes_delete_eligibility() -> None:
    table = _policy().table_specs["strategy_observations"]
    archive_only = _decision(_row())
    delete_context = _context(
        verified_archive_identities=frozenset({("strategy_observations", "obs-1")}),
        hot_window_by_table={table.name: timedelta(hours=1)},
    )
    delete_candidate = evaluate_row(_row(), table, delete_context)

    assert archive_only.decision == "ARCHIVE_ONLY"
    assert delete_candidate.decision == "ARCHIVE_AND_DELETE_ELIGIBLE"


def test_policy_version_is_emitted_on_every_decision() -> None:
    result = _decision(_row())

    assert result.policy_version == "phase1c-v1"


def test_no_default_hot_window_never_emits_delete() -> None:
    policy = _policy()
    table = policy.table_specs["strategy_observations"]
    context = _context(verified_archive_identities=frozenset({("strategy_observations", "obs-1")}))

    result = evaluate_row(_row(), table, context)

    assert result.decision == "ARCHIVE_ONLY"
    assert result.reason_code == "ARCHIVE_TERMINAL_MATURE"


def test_utc_boundary_is_strict_and_timezone_normalized() -> None:
    table = _policy().table_specs["strategy_observations"]
    row = _row(observed_at="2026-08-28 12:00:00.000000")
    context = _context(
        verified_archive_identities=frozenset({("strategy_observations", "obs-1")}),
        hot_window_by_table={table.name: timedelta(hours=1)},
    )

    result = evaluate_row(row, table, context)

    assert result.decision == "ARCHIVE_ONLY"
    assert result.reason_code == "KEEP_HOT_WINDOW"


def test_database_partition_is_complete_and_repeatable(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.sqlite"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY,
            observed_at DATETIME NOT NULL,
            event_id TEXT,
            root_event_id TEXT,
            attempt_id TEXT,
            signal_id INTEGER,
            outcome_status TEXT,
            outcome_next_attempt_at DATETIME
        );
        INSERT INTO strategy_observations VALUES
            ('terminal', '2026-08-20 12:00:00', 'event-terminal', NULL, NULL, NULL, 'complete', NULL),
            ('pending', '2026-08-20 12:00:00', 'event-pending', NULL, NULL, NULL, 'incomplete', '2026-08-28 14:00:00');
        """
    )
    connection.commit()
    connection.close()

    first = evaluate_database(db_path, _policy(), as_of_utc=AS_OF)
    second = evaluate_database(db_path, _policy(), as_of_utc=AS_OF)
    table = next(item for item in first.tables if item["table_name"] == "strategy_observations")

    assert first == second
    assert table["total"] == table["keep"] + table["archive_only"] + table["delete_eligible"]
    assert table["unknown"] == 0


def test_database_source_is_read_only_and_cli_output_is_machine_readable(tmp_path: Path, capsys) -> None:
    from scripts.archive_research import main

    db_path = tmp_path / "retention.sqlite"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE signals (id INTEGER PRIMARY KEY, signal_time DATETIME);
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY,
            observed_at DATETIME NOT NULL,
            outcome_status TEXT,
            outcome_next_attempt_at DATETIME
        );
        INSERT INTO strategy_observations VALUES ('obs-1', '2026-08-20 12:00:00', 'complete', NULL);
        """
    )
    connection.commit()
    connection.close()
    before_hash = hashlib.sha256(db_path.read_bytes()).hexdigest()
    output = tmp_path / "must-not-be-created"

    assert main([
        "--db", str(db_path), "--output", str(output), "--retention-dry-run",
        "--policy", str(POLICY_PATH), "--as-of", "2026-08-28T13:00:00Z",
    ]) == 0
    result = json.loads(capsys.readouterr().out)

    assert result["policy_version"] == "phase1c-v1"
    assert result["source_unchanged"] is True
    assert not output.exists()
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before_hash


def test_policy_classifies_all_31_known_tables() -> None:
    policy = _policy()

    assert len(policy.table_specs) == 31
    assert set(policy.table_specs) == {
        "__db_heartbeat", "event_states", "signals", "signal_provenance", "signal_outcomes",
        "telegram_delivery_outbox", "watch_candidates", "climax_root_events", "climax_entry_attempts",
        "runtime_heartbeats", "runtime_heartbeat_history", "climax_evaluations", "volume_climax_observations",
        "strategy_observations", "climax_entry_attempt_events", "climax_monitor_events", "market_coverage_ledger",
        "market_scan_cycles", "market_scan_rotations", "market_scan_symbol_results", "reject_stats",
        "root_detector_shadow_candidates", "root_detector_shadow_episodes", "root_detector_shadow_observations",
        "root_detector_shadow_episode_outcomes", "root_detector_shadow_episode_root_links",
        "root_detector_shadow_legacy_mappings", "root_detector_shadow_v2_roots", "root_detector_shadow_v2_outcomes",
        "current_root_outcomes", "current_root_predicate_snapshots",
    }
    assert all(spec.policy_version == "phase1c-v1" for spec in policy.table_specs.values())


def test_unknown_source_table_is_reported_as_unknown_and_kept(tmp_path: Path) -> None:
    db_path = tmp_path / "unknown.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE mystery (id INTEGER PRIMARY KEY, created_at DATETIME)")
    connection.execute("INSERT INTO mystery VALUES (1, '2026-08-20 12:00:00')")
    connection.commit()
    connection.close()

    result = evaluate_database(db_path, _policy(), as_of_utc=AS_OF)
    table = next(item for item in result.tables if item["table_name"] == "mystery")

    assert table["unknown"] == 1
    assert table["keep"] == 1
    assert table["reason_counts"] == {"KEEP_UNKNOWN_DEPENDENCY": 1}


def test_known_table_with_unsupported_schema_fails_closed(tmp_path: Path) -> None:
    db_path = tmp_path / "unsupported.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE strategy_observations (observation_id TEXT PRIMARY KEY)")
    connection.execute("INSERT INTO strategy_observations VALUES ('obs-1')")
    connection.commit()
    connection.close()

    result = evaluate_database(db_path, _policy(), as_of_utc=AS_OF)
    table = next(item for item in result.tables if item["table_name"] == "strategy_observations")

    assert result.dependency_graph_complete is False
    assert table["unknown"] == 1
    assert table["keep"] == 1


@pytest.mark.parametrize("table_name", [
    "signals", "signal_provenance", "signal_outcomes", "telegram_delivery_outbox", "event_states",
])
def test_protected_tables_are_never_delete_selected(table_name: str, tmp_path: Path) -> None:
    db_path = tmp_path / f"{table_name}.sqlite"
    connection = sqlite3.connect(db_path)
    column = "id" if table_name != "event_states" else "symbol"
    connection.execute(f"CREATE TABLE {table_name} ({column} TEXT PRIMARY KEY)")
    connection.execute(f"INSERT INTO {table_name} VALUES ('one')")
    connection.commit()
    connection.close()

    result = evaluate_database(db_path, _policy(), as_of_utc=AS_OF)
    table = next(item for item in result.tables if item["table_name"] == table_name)

    assert table["delete_eligible"] == 0
