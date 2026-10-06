from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from app.market_data.models import HealthStatus, MarketDataHealth, SubscriptionState
from app.market.scanner import _normalize_derivatives_result
from scripts.validate_market_data_ws_shadow import build_report


def test_validation_report_contains_shadow_observation_fields() -> None:
    health = MarketDataHealth(
        state=HealthStatus.HEALTHY,
        updated_at=datetime.now(UTC),
        parse_errors=2,
        reconnects=1,
    )
    subscriptions = SubscriptionState(
        expected_topics=frozenset({"tickers.AAAUSDT"}),
        acknowledged_topics=frozenset({"tickers.AAAUSDT"}),
        failed_topics=frozenset(),
        pending_topics=frozenset(),
    )

    report = build_report(
        symbols=("AAAUSDT",),
        health=health,
        subscriptions=subscriptions,
        duration_sec=120,
        cpu_seconds=1.5,
        rss_kib=42,
    )

    assert report["state"] == "HEALTHY"
    assert report["symbols"] == 1
    assert report["expected_topics"] == 1
    assert report["messages"] == 0
    assert report["parse_errors"] == 2
    assert report["reconnects"] == 1
    assert report["rss_kib"] == 42


def test_validation_script_can_be_invoked_directly() -> None:
    script = Path(__file__).parents[1] / "scripts" / "validate_market_data_ws_shadow.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "Finite, read-only Ubuntu validation" in result.stdout


def test_derivative_failure_is_degraded_when_derivatives_are_not_required() -> None:
    result = _normalize_derivatives_result(
        "AAAUSDT",
        [],
        [],
        derivatives_required=False,
    )

    assert result["derivatives_status"] == "MISSING"
    assert result["scan_failure"] is None
    assert "derivatives_missing" in result["data_quality_warnings"]


def test_required_missing_derivatives_are_scan_failure_incomplete() -> None:
    result = _normalize_derivatives_result(
        "AAAUSDT",
        [],
        [],
        derivatives_required=True,
    )

    assert result["scan_failure"] == {
        "terminal_status": "SCAN_FAILED",
        "reason_code": "MARKET_DATA_INCOMPLETE",
    }
