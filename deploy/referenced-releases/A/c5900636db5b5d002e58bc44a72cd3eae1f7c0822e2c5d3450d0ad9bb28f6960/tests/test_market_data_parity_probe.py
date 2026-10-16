from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from app.domain import MarketSnapshot
from scripts.run_marketdata_parity import (
    ARTIFACT_ESTIMATE_BYTES,
    _markdown_report,
    build_report,
    select_kline_sample,
    write_evidence,
)


def snapshot(symbol: str, turnover: float) -> MarketSnapshot:
    return MarketSnapshot(
        symbol=symbol,
        last_price=1,
        price_24h_pct=0,
        turnover_24h=turnover,
        volume_24h=1,
        mark_price=1,
        open_interest=1,
        timestamp=None,
    )


def test_kline_sample_is_deterministic_top_middle_bottom_nine() -> None:
    rows = [snapshot(f"S{index:02}USDT", float(20 - index)) for index in range(20)]
    selected = select_kline_sample(reversed(rows))

    assert selected == (
        "S00USDT", "S01USDT", "S02USDT",
        "S09USDT", "S10USDT", "S11USDT",
        "S17USDT", "S18USDT", "S19USDT",
    )


def test_report_success_is_probe_only_classification() -> None:
    report = build_report(
        ticker_probes=20,
        ticker_field_counts={"EXACT_OBSERVED_IN_WINDOW": 10},
        kline_counts={"EXACT_MATCH": 27},
        kline_sample=("BTCUSDT",),
        kline_per_symbol={"BTCUSDT": 3},
        reconnect={"reconnect": True, "resubscribe": True, "new_epoch": True, "ticker_reinitialized": True},
        backfill={"status": "SUCCESS", "future_leakage_violations": 0},
        resources={"cpu_seconds": 1.0, "peak_rss_kib": 2, "artifact_bytes": 0},
        universe={"rest_symbols": [], "ws_symbols": [], "intersection": [], "rest_only": [], "ws_only": []},
        duration_seconds=600,
    )
    assert report["phase6"] == "PASS"
    assert report["classification"] == "PARITY_PASS_BACKFILL_PROBE_ONLY"
    assert report["rest_remains_canonical"] is True
    assert report["production_backfill_enabled"] is False


def test_rate_limit_blocker_takes_precedence_when_probe_budget_is_not_met() -> None:
    report = build_report(
        ticker_probes=19,
        ticker_field_counts={},
        kline_counts={},
        kline_sample=("BTCUSDT",),
        kline_per_symbol={"BTCUSDT": 0},
        reconnect={"reconnect": True, "resubscribe": True, "new_epoch": True, "ticker_reinitialized": True},
        backfill={"status": "SUCCESS", "future_leakage_violations": 0},
        resources={},
        universe={},
        duration_seconds=600,
        rest_rate_limit_errors=1,
    )

    assert report["classification"] == "REST_RATE_LIMIT_SAFETY_BLOCKED"
    assert report["rate_limit_impact"]["classification"] == "BLOCKING"


def test_evidence_publication_runs_capacity_gate_and_writes_bounded_json(tmp_path, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr("scripts.run_marketdata_parity.read_capacity", lambda _path: "snapshot")
    monkeypatch.setattr("scripts.run_marketdata_parity.derive_reserve_bytes", lambda *_args, **_kwargs: 123)
    monkeypatch.setattr(
        "scripts.run_marketdata_parity.require_capacity",
        lambda snapshot, **kwargs: calls.append((snapshot, kwargs)),
    )
    report = {"phase6": "BLOCKED", "classification": "GAP_DETECTION_NOT_PROVEN"}

    path = write_evidence(report, output_dir=tmp_path, database_path=tmp_path / "db.sqlite", log_wal_margin_bytes=1, minimum_reserve_bytes=2)

    assert calls[0][1]["artifact_estimate_bytes"] == ARTIFACT_ESTIMATE_BYTES
    assert json.loads(path.read_text(encoding="utf-8"))["classification"] == "GAP_DETECTION_NOT_PROVEN"
    assert path.stat().st_size < ARTIFACT_ESTIMATE_BYTES
    markdown = path.with_suffix(".md").read_text(encoding="utf-8")
    for heading in (
        "## Source", "## Ticker parity", "## Closed kline parity",
        "## Gap detection", "## Reconnect", "## Backfill",
        "## Rate-limit impact", "## Resource usage", "## Disk health",
        "## Strategy isolation", "## Production",
    ):
        assert heading in markdown


def test_evidence_publication_serializes_backfill_values_canonically(
    tmp_path, monkeypatch
) -> None:
    observed = datetime(2026, 9, 5, 17, 8, tzinfo=UTC)
    report = {
        "phase6": "PASS",
        "classification": "PARITY_PASS_BACKFILL_PROBE_ONLY",
        "backfill": {
            "request_started_at": observed,
        },
    }
    monkeypatch.setattr("scripts.run_marketdata_parity.read_capacity", lambda _path: "snapshot")
    monkeypatch.setattr("scripts.run_marketdata_parity.derive_reserve_bytes", lambda *_args, **_kwargs: 123)
    monkeypatch.setattr("scripts.run_marketdata_parity.require_capacity", lambda *_args, **_kwargs: None)

    first = _markdown_report(report)
    second = _markdown_report(report)
    path = write_evidence(
        report,
        output_dir=tmp_path,
        database_path=tmp_path / "db.sqlite",
        log_wal_margin_bytes=1,
        minimum_reserve_bytes=2,
    )

    assert first == second
    assert "## Backfill" in first
    assert '"request_started_at": "2026-09-05T17:08:00+00:00"' in first
    assert path.exists()
    assert path.with_suffix(".md").exists()


def test_probe_script_can_be_invoked_directly() -> None:
    script = Path(__file__).parents[1] / "scripts" / "run_marketdata_parity.py"
    result = subprocess.run([sys.executable, str(script), "--help"], capture_output=True, text=True, check=False)
    assert result.returncode == 0
    assert "REST/WS parity" in result.stdout
