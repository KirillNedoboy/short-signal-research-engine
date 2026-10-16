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
