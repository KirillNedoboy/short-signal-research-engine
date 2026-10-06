from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pandas as pd

from app.market.coverage import build_coverage_rows
from app.observability.root_detector_shadow import (
    ShadowCandidateInput,
    candidate_episode_key,
    evaluate_shadow_outcome,
    should_create_shadow_candidate,
)


def test_coverage_row_created_deterministically_and_preserves_exclusion_reason():
    rows = build_coverage_rows(
        rotation_id="r1",
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        exchange_symbols=["AAAUSDT", "BBBUSDT"],
        eligible_symbols=["AAAUSDT"],
        excluded=[("BBBUSDT", "LIQUIDITY_FILTER")],
        scheduled_symbols=["AAAUSDT"],
        symbol_results=[{"symbol": "AAAUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"}],
    )
    assert rows == [
        {
            "rotation_id": "r1", "observed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "symbol": "AAAUSDT", "exchange_present": True, "eligible": True,
            "exclusion_reason": None, "scheduled": True, "scanned": True,
            "scan_status": "SCANNED_OK", "unexpected_result_present": False, "evidence_json": {"reason_code": "SCANNED_OK", "observed_terminal_status": None},
        },
        {
            "rotation_id": "r1", "observed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "symbol": "BBBUSDT", "exchange_present": True, "eligible": False,
            "exclusion_reason": "LIQUIDITY_FILTER", "scheduled": False, "scanned": False,
            "scan_status": "EXCLUDED", "unexpected_result_present": False, "evidence_json": {"reason_code": "LIQUIDITY_FILTER", "observed_terminal_status": None},
        },
    ]


def test_shadow_candidate_trigger_is_observational_and_deduplicates_episode():
    candidate = ShadowCandidateInput(
        symbol="AAAUSDT", first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        rotation_id="r1", price=110, event_high=110, high_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        pump_5m=3, pump_15m=6, pump_1h=9, pump_4h=12, volume_ratio=1, volume_z=0,
    )
    assert should_create_shadow_candidate(candidate) is True
    assert candidate_episode_key(candidate) == candidate_episode_key(candidate)
    later = replace(candidate, price=111, first_seen_at=candidate.first_seen_at + timedelta(minutes=1))
    assert candidate_episode_key(later) == candidate_episode_key(candidate)


def test_shadow_outcome_uses_only_closed_candles_and_censors_unmatured_horizons():
    observed = datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
    frame = pd.DataFrame([
        {"timestamp": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc), "open": 100, "high": 101, "low": 99, "close": 100},
        {"timestamp": datetime(2026, 1, 1, 0, 16, tzinfo=timezone.utc), "open": 100, "high": 102, "low": 95, "close": 96},
    ])
    result = evaluate_shadow_outcome(observed_at=observed, entry_price=100, event_high=110, frame_5m=frame, market_asof=datetime(2026, 1, 1, 0, 17, tzinfo=timezone.utc))
    assert result["horizons"]["15m"]["price"] == 96
    assert result["horizons"]["30m"]["price"] is None
    assert result["mfe_pct"] == 5.0
    assert result["outcome_status"] == "PARTIAL"


def test_shadow_outcome_marks_overdue_missing_candles_as_data_gap():
    observed = datetime(2026, 1, 1, tzinfo=timezone.utc)
    result = evaluate_shadow_outcome(
        observed_at=observed, entry_price=100, event_high=110,
        frame_5m=pd.DataFrame(columns=["timestamp", "high", "low", "close"]),
        market_asof=observed + timedelta(hours=2),
    )
    assert result["outcome_status"] == "DATA_GAP"


def test_shadow_outcome_handles_timestamp_index_from_scanner_frame():
    observed = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    frame = pd.DataFrame([
        {"timestamp": datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "high": 101, "low": 99, "close": 100},
    ]).set_index("timestamp")
    result = evaluate_shadow_outcome(
        observed_at=observed,
        entry_price=100,
        event_high=100,
        frame_5m=frame,
        market_asof=datetime(2026, 1, 1, 0, 6, tzinfo=timezone.utc),
    )
    assert result["horizons"]["15m"]["price"] is None
