from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app.domain import EventState, EventStatus
from app.runtime.shadow_comparison import (
    ShadowCaptureRecorder,
    compare_results,
)


def _snapshot() -> SimpleNamespace:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {
            "timestamp": start + timedelta(minutes=index),
            "open": 100.0,
            "high": 100.2,
            "low": 99.8,
            "close": 100.0,
            "volume": 100.0,
            "turnover": 10_000.0,
        }
        for index in range(80)
    ]
    frame = pd.DataFrame(rows).set_index("timestamp", drop=False)
    holder = SimpleNamespace(
        frame=frame,
        source="REST",
        captured_at_utc=start + timedelta(minutes=80),
        max_input_availability_time_utc=start + timedelta(minutes=80),
    )
    return SimpleNamespace(
        symbol="BTCUSDT",
        frame_1m=holder,
        derivatives={"derivatives_status": "OK", "open_interest": [], "funding": []},
        liquidity={
            "spread_pct": 0.1,
            "slippage_pct": 0.1,
            "orderbook_depth_usdt_1pct": 100_000,
            "orderbook_depth_usdt_2pct": 200_000,
        },
        captured_at_utc=start + timedelta(minutes=80),
        max_input_availability_time_utc=start + timedelta(minutes=80),
    )


def _state() -> EventState:
    return EventState(
        symbol="BTCUSDT",
        event_id="event-1",
        state=EventStatus.IDLE,
        event_features_snapshot={"safe": True},
    )


def _result() -> dict[str, object]:
    return {
        "features": {"price": 100.0},
        "state": {"event_id": "event-1", "state": "idle"},
        "decision": None,
        "evaluations": [],
        "blocked_reason": "NO_ACTIONABLE_STRATEGY",
        "actionable": False,
        "identity": None,
    }


def test_capture_serializes_raw_input_and_redacts_credentials_and_paths(tmp_path: Path) -> None:
    state = _state()
    recorder = ShadowCaptureRecorder(tmp_path, max_bytes=100_000)
    assert recorder.record(
        state,
        _snapshot(),
        _result(),
        {
            "decision_at": datetime(2026, 1, 1, 1, 20, tzinfo=timezone.utc),
            "strategy_config": {
                "event_ret_15m_min": 6.0,
                "telegram_token": "do-not-write",
                "db_url": "sqlite:///secret.sqlite",
                "research_spool_root": "/secret/path",
            },
            "strategy_config_fingerprint": "fingerprint",
        },
    )
    # The caller may mutate its working state after the queued write is made;
    # the record must retain the BEFORE copy.
    state.event_features_snapshot["safe"] = False
    raw = json.loads((tmp_path / "raw" / "shadow-inputs.jsonl").read_text(encoding="utf-8"))
    encoded = json.dumps(raw, ensure_ascii=False)
    assert "do-not-write" not in encoded
    assert "secret.sqlite" not in encoded
    assert "/secret/path" not in encoded
    assert raw["before_state"]["event_features_snapshot"]["safe"] is True
    assert raw["strategy_config"]["payload"] == {"event_ret_15m_min": 6.0}
    assert len(raw["market"]["frame_1m"]["rows"]) == 80


def test_capture_sampling_and_budget_keep_bounded_aggregates(tmp_path: Path) -> None:
    recorder = ShadowCaptureRecorder(
        tmp_path,
        sample_every=2,
        max_bytes=1_000,
        max_record_bytes=100_000,
    )
    for _ in range(4):
        recorder.record(_state(), _snapshot(), _result(), {})
    assert recorder.stats.sampled_out == 2
    assert recorder.stats.dropped_budget >= 1
    assert recorder.raw_capture_enabled is False
    aggregate_lines = (tmp_path / "comparisons" / "shadow-aggregates.jsonl").read_text(encoding="utf-8").splitlines()
    assert aggregate_lines
    assert all(json.loads(line)["status"] == "UNKNOWN" for line in aggregate_lines)
    raw_path = tmp_path / "raw" / "shadow-inputs.jsonl"
    assert (raw_path.stat().st_size if raw_path.exists() else 0) <= 1_000


def test_compare_results_pass_mismatch_and_unknown() -> None:
    old = {"features": {"x": 1}, "state": {}, "decision": None, "evaluations": [], "identity": None, "actionable": False, "blocked_reason": "NO"}
    assert compare_results(old, dict(old)).status == "PASS"
    changed = dict(old, features={"x": 2})
    assert compare_results(old, changed).status == "MISMATCH"
    unknown = compare_results(old, changed, input_status="sampled_out")
    assert unknown.status == "UNKNOWN"
    assert "input_sampled_out" in unknown.unknown_reasons
    recheck = compare_results(old, old, final_recheck_required=True, final_recheck_captured=False)
    assert recheck.status == "UNKNOWN"
    assert "final_fresh_recheck_not_captured" in recheck.unknown_reasons


def test_frozen_reference_replay_invokes_actual_process_symbol_and_detects_mismatch(
    tmp_path: Path,
) -> None:
    recorder = ShadowCaptureRecorder(tmp_path, max_bytes=200_000)
    assert recorder.record(_state(), _snapshot(), _result(), {})
    record_path = tmp_path / "raw" / "shadow-inputs.jsonl"
    worker = Path(__file__).parents[1] / "tools" / "shadow_replay_worker.py"
    old_root = (
        Path(__file__).parents[1]
        / "release"
        / "sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a"
        / "app"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(worker),
            "--source-root",
            str(old_root),
            "--record",
            str(record_path),
            "--implementation",
            "old",
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    replayed = json.loads(completed.stdout)
    assert replayed["reference_scope"] == "frozen.app.main.ShortSignalBot._process_symbol"
    assert replayed["harness"]["actual_process_symbol"] is True
    assert replayed["harness"]["process_symbol_calls"] == 1
    assert replayed["harness"]["feature_builder_calls"] >= 1
    assert replayed["harness"]["provider_network_calls"] == 0
    assert replayed["harness"]["notifier_send_calls"] == 0
    changed = dict(replayed, features={"tampered": True})
    assert compare_results(replayed, changed).status == "MISMATCH"


def test_frozen_fast_reference_dispatches_climax_only_without_baseline_or_trapped(
    tmp_path: Path,
) -> None:
    recorder = ShadowCaptureRecorder(tmp_path, max_bytes=200_000)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    active_state = EventState(
        symbol="BTCUSDT",
        event_id="event-1",
        state=EventStatus.PUMP_DETECTED,
        event_start_time=start,
        event_high=101.0,
        event_high_time=start + timedelta(minutes=60),
        event_base_price=90.0,
        event_features_snapshot={"safe": True},
    )
    fast_market = _snapshot()
    # The frozen registry evaluates the real trapped-longs adapter even though
    # the fast dispatch admits only climax.  Give its shadow metrics a valid
    # ATR so the fixture reaches the real registry without manufacturing an
    # evaluator result.
    rows = []
    for index in range(300):
        stamp = start + timedelta(minutes=index)
        close = 100.0 + float(index // 15)
        rows.append(
            {
                "timestamp": stamp,
                "open": close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 100.0,
                "turnover": 10_000.0,
            }
        )
    fast_market.frame_1m.frame = pd.DataFrame(rows).set_index(
        "timestamp", drop=False
    )
    captured_at = start + timedelta(minutes=300)
    fast_market.frame_1m.captured_at_utc = captured_at
    fast_market.frame_1m.max_input_availability_time_utc = captured_at
    fast_market.captured_at_utc = captured_at
    fast_market.max_input_availability_time_utc = captured_at
    assert recorder.record(
        active_state,
        fast_market,
        _result(),
        {
            "fast_monitor": True,
            "poll_sequence": 7,
            "strategy_config": {
                "climax_short_enabled": True,
                "climax_fast_monitor_enabled": True,
            },
        },
    )
    record_path = tmp_path / "raw" / "shadow-inputs.jsonl"
    worker = Path(__file__).parents[1] / "tools" / "shadow_replay_worker.py"
    old_root = (
        Path(__file__).parents[1]
        / "release"
        / "sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a"
        / "app"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(worker),
            "--source-root",
            str(old_root),
            "--record",
            str(record_path),
            "--implementation",
            "old",
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    replayed = json.loads(completed.stdout)
    assert replayed["reference_scope"] == (
        "frozen.app.main.ShortSignalBot._evaluate_and_send_climax"
    )
    harness = replayed["harness"]
    assert harness["actual_climax_method"] is True
    assert harness["climax_method_calls"] == 1
    assert harness["actual_process_symbol"] is False
    assert harness["process_symbol_calls"] == 0
    assert harness["baseline_calls"] == 0
    assert harness["trapped_calls"] == 0

    candidate_completed = subprocess.run(
        [
            sys.executable,
            str(worker),
            "--source-root",
            str(Path(__file__).parents[1] / "src"),
            "--record",
            str(record_path),
            "--implementation",
            "candidate",
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONNOUSERSITE": "1"},
    )
    assert candidate_completed.returncode == 0, (
        candidate_completed.stderr or candidate_completed.stdout
    )
    candidate = json.loads(candidate_completed.stdout)
    assert candidate["reference_scope"] == (
        "candidate.runtime.pipeline.LaneCStrategyPipeline.evaluate_climax_only"
    )
    # The strict semantic projection must pass the real no-action fast path;
    # no synthetic blocked reason is accepted as a parity shim.
    assert compare_results(replayed, candidate).status == "PASS"

