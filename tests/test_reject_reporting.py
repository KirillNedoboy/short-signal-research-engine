from datetime import datetime, timezone

from app.storage.db import Database
from app.storage.repository import BotRepository


def test_reject_reason_counted_and_report_generated(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'rejects.db'}")
    database.create_all()
    repository = BotRepository(database)

    repository.record_reject_stat(
        symbol="ONTUSDT",
        timeframe="15m",
        decision_type="REJECT",
        score=48,
        reasons=["score_too_low", "shallow_pullback"],
        blockers=["shallow_pullback"],
        risk_flags=["weak_rejection"],
        close_to_watch=True,
        squeeze_risk_level="MEDIUM",
        logged_at=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
    )

    summary = repository.reject_reason_summary(hours=24)

    assert summary["checked_candidates"] == 1
    assert summary["by_reason"]["score_too_low"] == 1
    assert summary["by_reason"]["shallow_pullback"] == 1
    assert summary["close_to_watch"] == 1


def test_reject_report_top_blockers_include_squeeze_risk(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'rejects-top.db'}")
    database.create_all()
    repository = BotRepository(database)

    repository.record_reject_stat(
        symbol="RAVEUSDT",
        timeframe="15m",
        decision_type="REJECT",
        score=44,
        reasons=["squeeze_risk", "funding_negative_trap"],
        blockers=["squeeze_risk"],
        risk_flags=["funding_negative_trap"],
        close_to_watch=True,
        squeeze_risk_level="HIGH",
        logged_at=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
    )

    summary = repository.reject_reason_summary(hours=24)

    assert summary["top_blockers"][0][0] == "squeeze_risk"
    assert summary["blocked_by_squeeze_risk"] == 1


def test_reject_report_includes_derivatives_diagnostics(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'rejects-derivatives.db'}")
    database.create_all()
    repository = BotRepository(database)

    repository.record_reject_stat(
        symbol="LABUSDT",
        timeframe="15m",
        decision_type="WATCH",
        score=52,
        reasons=["derivatives_missing", "oi_missing"],
        blockers=["derivatives_missing"],
        risk_flags=["Data quality: derivatives_missing, oi_missing."],
        close_to_watch=True,
        squeeze_risk_level="MEDIUM",
        derivatives_status="RATE_LIMITED",
        derivatives_reasons=["bybit_rate_limit"],
        data_quality_warnings=["derivatives_missing", "oi_missing"],
        logged_at=datetime(2026, 4, 13, 12, 10, tzinfo=timezone.utc),
    )

    summary = repository.reject_reason_summary(hours=24)

    assert summary["by_derivatives_status"]["RATE_LIMITED"] == 1
    assert summary["derivatives_reason_counts"]["bybit_rate_limit"] == 1
    assert summary["data_quality_counts"]["derivatives_missing"] == 1
    assert summary["data_quality_counts"]["oi_missing"] == 1
