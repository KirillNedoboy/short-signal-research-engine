from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from app.domain import SignalOutcome
from app.scripts.short_derivatives_report import main as derivatives_report_main
from app.scripts.short_outcome_quality_report import main as outcome_report_main
from app.scripts.short_reject_report import main as reject_report_main
from app.scripts.reporting_cli import build_two_lane_coverage_report
from app.storage.db import Database
from app.storage.repository import BotRepository


def _write_config(tmp_path: Path, db_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump({"db_url": f"sqlite:///{db_path.as_posix()}"}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def test_write_config_serializes_windows_unsafe_paths_as_valid_yaml(tmp_path) -> None:
    directory = tmp_path / "Рабочий стол" / "reports db"
    directory.mkdir(parents=True)
    db_path = directory / "bot.sqlite"

    _write_config(tmp_path, db_path)

    config = yaml.safe_load((tmp_path / "config.yaml").read_text(encoding="utf-8"))
    assert config["db_url"] == f"sqlite:///{db_path.as_posix()}"


def _seed_reject_rows(repository: BotRepository) -> None:
    repository.record_reject_stat(
        symbol="OLDUSDT",
        timeframe="15m",
        decision_type="REJECT",
        score=41,
        reasons=["derivatives_missing", "oi_missing"],
        blockers=["derivatives_missing"],
        risk_flags=["legacy_row"],
        close_to_watch=False,
        squeeze_risk_level="MEDIUM",
        derivatives_status="RATE_LIMITED",
        derivatives_reasons=["bybit_rate_limit"],
        data_quality_warnings=["derivatives_missing", "oi_missing"],
        logged_at=datetime(2026, 6, 20, 3, 30, tzinfo=timezone.utc),
    )
    repository.record_reject_stat(
        symbol="NEWUSDT",
        timeframe="1h",
        decision_type="REJECT",
        score=55,
        reasons=["spread_too_wide"],
        blockers=["spread_too_wide"],
        risk_flags=[],
        close_to_watch=True,
        squeeze_risk_level="HIGH",
        derivatives_status="OK",
        derivatives_reasons=[],
        data_quality_warnings=[],
        logged_at=datetime(2026, 6, 20, 4, 31, tzinfo=timezone.utc),
    )


def _seed_outcomes(repository: BotRepository, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    state = repository.upsert_event_state(make_event_state())
    old_signal = repository.save_signal(
        make_signal_decision(
            symbol="OLDUSDT",
            event_id="OLDUSDT:15m:1:111",
            signal_time=datetime(2026, 6, 20, 3, 0, tzinfo=timezone.utc),
        ),
        state,
        telegram_sent=False,
        provenance=make_signal_provenance(event_id="OLDUSDT:15m:1:111"),
    )
    repository.upsert_signal_outcome(
        SignalOutcome(
            signal_id=old_signal.id,
            tp1_hit=False,
            stopped_virtual=True,
            risk_adjusted_status="SQUEEZE_BEFORE_TP",
            mae_pct=8.0,
            mfe_pct=1.0,
            squeeze_extension_pct=12.0,
            updated_at=datetime(2026, 6, 20, 3, 10, tzinfo=timezone.utc),
        )
    )

    new_signal = repository.save_signal(
        make_signal_decision(
            symbol="NEWUSDT",
            event_id="NEWUSDT:15m:1:222",
            signal_time=datetime(2026, 6, 20, 4, 35, tzinfo=timezone.utc),
        ),
        state,
        telegram_sent=False,
        provenance=make_signal_provenance(event_id="NEWUSDT:15m:1:222"),
    )
    repository.upsert_signal_outcome(
        SignalOutcome(
            signal_id=new_signal.id,
            tp1_hit=True,
            stopped_virtual=False,
            risk_adjusted_status="CLEAN_TP",
            mae_pct=1.0,
            mfe_pct=6.0,
            squeeze_extension_pct=0.5,
            updated_at=datetime(2026, 6, 20, 4, 40, tzinfo=timezone.utc),
        )
    )


def test_reject_report_without_since_keeps_aggregate_behavior(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "reports.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    repository = BotRepository(database)
    _seed_reject_rows(repository)
    _write_config(tmp_path, db_path)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["short_reject_report"])

    reject_report_main()

    report = json.loads(capsys.readouterr().out)
    assert report["checked_candidates"] == 2
    assert report["rows_in_window"] == 2
    assert report["by_symbol"] == {"OLDUSDT": 1, "NEWUSDT": 1}


def test_reject_report_with_since_excludes_older_legacy_rows(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "reports-since.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    repository = BotRepository(database)
    _seed_reject_rows(repository)
    _write_config(tmp_path, db_path)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["short_reject_report", "--since", "2026-06-20T04:00:00Z"])

    reject_report_main()

    report = json.loads(capsys.readouterr().out)
    assert report["since"] == "2026-06-20T04:00:00+00:00"
    assert report["checked_candidates"] == 1
    assert report["rows_in_window"] == 1
    assert report["by_symbol"] == {"NEWUSDT": 1}
    assert report["by_reason"] == {"spread_too_wide": 1}


def test_derivatives_report_with_since_shows_only_post_restart_statuses(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "derivatives-report.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    repository = BotRepository(database)
    _seed_reject_rows(repository)
    _write_config(tmp_path, db_path)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["short_derivatives_report", "--since", "2026-06-20T04:00:00Z"])

    derivatives_report_main()

    report = json.loads(capsys.readouterr().out)
    assert report["since"] == "2026-06-20T04:00:00+00:00"
    assert report["rows_in_window"] == 1
    assert report["by_derivatives_status"] == {
        "OK": 1,
        "MISSING": 0,
        "API_ERROR": 0,
        "RATE_LIMITED": 0,
        "UNSUPPORTED_SYMBOL": 0,
    }
    assert report["derivatives_reason_counts"] == {}
    assert report["data_quality_counts"] == {}


def test_outcome_report_with_since_excludes_older_rows(tmp_path, monkeypatch, capsys, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    db_path = tmp_path / "outcomes-report.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    repository = BotRepository(database)
    _seed_outcomes(repository, make_event_state, make_signal_decision, make_signal_provenance)
    _write_config(tmp_path, db_path)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["short_outcome_quality_report", "--since", "2026-06-20T04:00:00Z"])

    outcome_report_main()

    report = json.loads(capsys.readouterr().out)
    assert report["since"] == "2026-06-20T04:00:00+00:00"
    assert report["rows_in_window"] == 1
    assert report["raw_summary"] == {"TP": 1}
    assert report["risk_adjusted_summary"] == {"CLEAN_TP": 1}
    assert report["by_symbol"] == {"NEWUSDT": 1}


def test_outcome_report_empty_window_returns_valid_empty_json(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "outcomes-empty.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    _write_config(tmp_path, db_path)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["short_outcome_quality_report", "--since", "2030-01-01T00:00:00Z"])

    outcome_report_main()

    report = json.loads(capsys.readouterr().out)
    assert report["rows_in_window"] == 0
    assert report["raw_summary"] == {}
    assert report["risk_adjusted_summary"] == {}


def test_reject_report_invalid_since_returns_useful_error(tmp_path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "invalid-since.db"
    database = Database(f"sqlite:///{db_path}")
    database.create_all()
    _write_config(tmp_path, db_path)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["short_reject_report", "--since", "not-a-timestamp"])

    with pytest.raises(SystemExit) as exc_info:
        reject_report_main()

    assert exc_info.value.code == 2
    assert "Invalid --since timestamp" in capsys.readouterr().err


def test_two_lane_coverage_report_reconciles_rows_and_labels_incomplete(tmp_path) -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE market_scan_symbol_results (
            id INTEGER PRIMARY KEY, rotation_id TEXT, symbol TEXT,
            terminal_status TEXT, reason_code TEXT, scheduled_at TEXT,
            completed_at TEXT, duration_ms REAL, runtime_instance_id TEXT
        );
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY, runtime_instance_id TEXT,
            observed_at TEXT, initial_decision TEXT, final_decision TEXT,
            final_reason TEXT, signal_id INTEGER
        );
        CREATE TABLE signals (id INTEGER PRIMARY KEY, created_at TEXT);
        CREATE TABLE signal_provenance (
            signal_id INTEGER, runtime_instance_id TEXT, decision_at TEXT
        );
        CREATE TABLE telegram_delivery_outbox (
            entity_type TEXT, entity_id INTEGER, status TEXT, sent_at TEXT
        );
        CREATE TABLE runtime_heartbeat_history (
            runtime_instance_id TEXT, created_at TEXT, event_loop_lag_ms REAL
        );
        INSERT INTO market_scan_symbol_results VALUES
          (1, 'ra', 'BTC', 'SCANNED_OK', 'ok', '2026-06-20T04:00:00+00:00', '2026-06-20T04:00:01+00:00', 120, 'runtime-a'),
          (2, 'rb', 'ETH', 'SCAN_FAILED', 'timeout', '2026-06-20T04:00:00+00:00', NULL, 900, 'runtime-b');
        INSERT INTO strategy_observations VALUES
          ('oa', 'runtime-a', '2026-06-20T04:00:02+00:00', 'ACTIONABLE', 'ACTIONABLE', 'final_actionable', 10),
          ('ob', 'runtime-b', '2026-06-20T04:00:02+00:00', 'ACTIONABLE', 'BLOCKED', 'blocked_by_recheck', NULL);
        INSERT INTO signals VALUES (10, '2026-06-20T04:00:03+00:00');
        INSERT INTO signal_provenance VALUES (10, 'runtime-a', '2026-06-20T04:00:03+00:00');
        INSERT INTO telegram_delivery_outbox VALUES ('SIGNAL', 10, 'SENT', '2026-06-20T04:00:04+00:00');
        INSERT INTO runtime_heartbeat_history VALUES ('runtime-a', '2026-06-20T04:05:00+00:00', 12.5);
        """
    )

    report = build_two_lane_coverage_report(
        connection,
        {"A": "runtime-a", "B": "runtime-b"},
        since="2026-06-20T04:00:00+00:00",
        until="2026-06-20T05:00:00+00:00",
        now="2026-06-20T05:00:00+00:00",
    )

    assert [row["lane"] for row in report["lanes"]] == ["A", "B"]
    assert report["lanes"][0]["scheduled"] == 1
    assert report["lanes"][0]["completed"] == 1
    assert report["lanes"][0]["scanned_ok"] == 1
    assert report["lanes"][0]["signals_persisted"] == 1
    assert report["lanes"][0]["outbox_sent"] == 1
    assert report["lanes"][1]["scan_failed_by_reason"] == {"timeout": 1}
    assert report["lanes"][1]["blocked_by_recheck"] == 1
    assert report["lanes"][1]["coverage"] == "INCOMPLETE"
    assert "mfe" not in json.dumps(report).lower()


def test_two_lane_coverage_report_returns_incomplete_without_explicit_b_mapping() -> None:
    import sqlite3

    report = build_two_lane_coverage_report(sqlite3.connect(":memory:"), {"A": "runtime-a"})

    assert report["coverage"] == "INCOMPLETE"
    assert report["window"]["status"] == "INCOMPLETE"
    assert report["diagnostic"] == "invalid explicit A/B lane mapping"


@pytest.mark.parametrize("mapping", [{"A": "same", "B": "same"}, {"A": "", "B": "runtime-b"}, {"A": "runtime-a"}])
def test_two_lane_coverage_report_sanitizes_invalid_lane_mapping(mapping) -> None:
    import sqlite3

    report = build_two_lane_coverage_report(sqlite3.connect(":memory:"), mapping)

    assert report["coverage"] == "INCOMPLETE"
    assert report["mapping"] == {}
    assert "same" not in json.dumps(report)


def test_two_lane_coverage_report_requires_telemetry_and_windows_heartbeat_metrics() -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE market_scan_symbol_results (
            id INTEGER PRIMARY KEY, terminal_status TEXT, reason_code TEXT,
            scheduled_at TEXT, completed_at TEXT, duration_ms REAL, runtime_instance_id TEXT
        );
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY, runtime_instance_id TEXT,
            observed_at TEXT, initial_decision TEXT, final_decision TEXT, final_reason TEXT, signal_id INTEGER
        );
        CREATE TABLE signals (id INTEGER PRIMARY KEY, created_at TEXT);
        CREATE TABLE signal_provenance (signal_id INTEGER, runtime_instance_id TEXT, decision_at TEXT);
        CREATE TABLE telegram_delivery_outbox (entity_type TEXT, entity_id INTEGER, status TEXT, sent_at TEXT);
        CREATE TABLE runtime_heartbeat_history (
            runtime_instance_id TEXT, created_at TEXT, event_loop_lag_ms REAL
        );
        INSERT INTO market_scan_symbol_results VALUES
          (1, 'SCANNED_OK', 'ok', '2026-06-20T04:00:00+00:00', '2026-06-20T04:00:01+00:00', 120, 'runtime-a');
        INSERT INTO strategy_observations VALUES
          ('oa', 'runtime-a', '2026-06-20T04:00:02+00:00', 'ACTIONABLE', 'BLOCKED', 'x', NULL);
        INSERT INTO runtime_heartbeat_history VALUES
          ('runtime-a', '2026-06-20T03:59:00+00:00', 1.0),
          ('runtime-a', '2026-06-20T04:30:00+00:00', 7.0),
          ('runtime-a', '2026-06-20T05:01:00+00:00', 99.0);
        """
    )

    report = build_two_lane_coverage_report(
        connection, {"A": "runtime-a", "B": "runtime-b"},
        since="2026-06-20T04:00:00+00:00", until="2026-06-20T05:00:00+00:00",
        now="2026-06-20T05:00:00+00:00",
    )

    lane_a, lane_b = report["lanes"]
    assert lane_a["coverage"] == "INCOMPLETE"
    assert lane_a["heartbeat_age"] == 1800.0
    assert lane_a["max_event_loop_lag"] == 7.0
    assert lane_b["coverage"] == "INCOMPLETE"


def test_two_lane_coverage_report_trims_lane_ids_and_rejects_whitespace_collision() -> None:
    import sqlite3

    report = build_two_lane_coverage_report(
        sqlite3.connect(":memory:"), {"A": " runtime-a ", "B": "runtime-a"}
    )

    assert report["coverage"] == "INCOMPLETE"
    assert report["mapping"] == {}
    assert "runtime-a" not in json.dumps(report)


def test_two_lane_coverage_report_anchors_scan_metrics_to_scheduled_window() -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript("""
        CREATE TABLE market_scan_symbol_results (
            id INTEGER PRIMARY KEY, terminal_status TEXT, reason_code TEXT,
            scheduled_at TEXT, completed_at TEXT, duration_ms REAL, runtime_instance_id TEXT
        );
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY, runtime_instance_id TEXT,
            observed_at TEXT, initial_decision TEXT, final_decision TEXT, final_reason TEXT, signal_id INTEGER
        );
        CREATE TABLE signals (id INTEGER PRIMARY KEY, created_at TEXT);
        CREATE TABLE signal_provenance (signal_id INTEGER, runtime_instance_id TEXT, decision_at TEXT);
        CREATE TABLE telegram_delivery_outbox (entity_type TEXT, entity_id INTEGER, status TEXT, sent_at TEXT);
        CREATE TABLE runtime_heartbeat_history (runtime_instance_id TEXT, created_at TEXT, event_loop_lag_ms REAL);
        INSERT INTO market_scan_symbol_results VALUES
          (1, 'SCANNED_OK', 'ok', '2026-06-20T04:59:59+00:00', '2026-06-20T05:00:01+00:00', 120, 'runtime-a'),
          (2, 'SCANNED_OK', 'ok', '2026-06-20T05:00:00+00:00', '2026-06-20T04:59:59+00:00', 999, 'runtime-a');
        INSERT INTO strategy_observations VALUES
          ('oa', 'runtime-a', '2026-06-20T04:30:00+00:00', 'BLOCKED', 'BLOCKED', 'x', NULL);
        INSERT INTO runtime_heartbeat_history VALUES ('runtime-a', '2026-06-20T04:30:00+00:00', 1.0);
    """)

    report = build_two_lane_coverage_report(
        connection, {"A": "runtime-a", "B": "runtime-b"},
        since="2026-06-20T04:00:00+00:00", until="2026-06-20T05:00:00+00:00",
        now="2026-06-20T05:00:00+00:00",
    )

    lane_a = report["lanes"][0]
    assert lane_a["scheduled"] == 1
    assert lane_a["completed"] == 1
    assert lane_a["scanned_ok"] == 1
    assert lane_a["max_duration"] == 120


def test_two_lane_coverage_report_counts_signals_without_observations() -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE market_scan_symbol_results (
            id INTEGER PRIMARY KEY, terminal_status TEXT, reason_code TEXT,
            scheduled_at TEXT, completed_at TEXT, duration_ms REAL, runtime_instance_id TEXT
        );
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY, runtime_instance_id TEXT,
            observed_at TEXT, initial_decision TEXT, final_decision TEXT, final_reason TEXT, signal_id INTEGER
        );
        CREATE TABLE signals (id INTEGER PRIMARY KEY, created_at TEXT);
        CREATE TABLE signal_provenance (signal_id INTEGER, runtime_instance_id TEXT, decision_at TEXT);
        CREATE TABLE telegram_delivery_outbox (entity_type TEXT, entity_id INTEGER, status TEXT, sent_at TEXT);
        CREATE TABLE runtime_heartbeat_history (runtime_instance_id TEXT, created_at TEXT, event_loop_lag_ms REAL);
        INSERT INTO market_scan_symbol_results VALUES
          (1, 'SCANNED_OK', 'ok', '2026-06-20T04:00:00+00:00', '2026-06-20T04:00:01+00:00', 120, 'runtime-a');
        INSERT INTO signals VALUES (10, '2026-06-20T04:00:03+00:00');
        INSERT INTO signal_provenance VALUES (10, 'runtime-a', '2026-06-20T04:00:03+00:00');
        INSERT INTO telegram_delivery_outbox VALUES ('SIGNAL', 10, 'SENT', '2026-06-20T04:00:04+00:00');
        INSERT INTO runtime_heartbeat_history VALUES ('runtime-a', '2026-06-20T04:05:00+00:00', 12.5);
        """
    )

    report = build_two_lane_coverage_report(
        connection, {"A": "runtime-a", "B": "runtime-b"},
        since="2026-06-20T04:00:00+00:00", until="2026-06-20T05:00:00+00:00",
        now="2026-06-20T05:00:00+00:00",
    )

    assert report["lanes"][0]["signals_persisted"] == 1
    assert report["lanes"][0]["outbox_sent"] == 1
