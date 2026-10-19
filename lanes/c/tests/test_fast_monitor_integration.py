from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from app.config import AppConfig
from app.domain import EventState, EventStatus
from app.main import LaneCCompositionRoot
from app.runtime.fast_monitor import BoundedFastMonitor, CandidatePool
from app.runtime.pipeline import LaneCStrategyPipeline
from app.runtime.shadow_comparison import ShadowCaptureRecorder


@dataclass
class _Clock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def _frame(start: datetime, rows: int = 300) -> pd.DataFrame:
    values = []
    for index in range(rows):
        stamp = start + timedelta(minutes=index)
        close = 100.0 + float(index) * 0.01
        values.append(
            {
                "timestamp": stamp,
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 100.0,
                "turnover": 10_000.0,
            }
        )
    return pd.DataFrame(values).set_index("timestamp", drop=False)


@pytest.mark.asyncio
async def test_fast_monitor_20s_logical_clock_uses_climax_helper_and_closed_shadow(
    tmp_path,
    monkeypatch,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    clock = _Clock(start + timedelta(minutes=1))
    state = EventState(
        symbol="BTCUSDT",
        event_id="event-1",
        state=EventStatus.PUMP_DETECTED,
        event_start_time=start,
        event_high=102.0,
        event_high_time=start,
        event_base_price=90.0,
        event_features_snapshot={"root_event_revision": 1},
    )
    frame = _frame(start)
    market = SimpleNamespace(
        frame_1m=SimpleNamespace(frame=frame),
        derivatives={},
        liquidity={
            "spread_pct": 0.1,
            "slippage_pct": 0.1,
            "orderbook_depth_usdt_1pct": 100_000.0,
            "orderbook_depth_usdt_2pct": 200_000.0,
        },
        captured_at_utc=clock.value,
        max_input_availability_time_utc=clock.value,
        generation=1,
    )
    config = AppConfig(
        autoexecution="OFF",
        derivatives_enabled=False,
        climax_short_enabled=True,
        climax_fast_monitor_enabled=True,
        climax_fast_poll_sec=20,
    )
    pipeline = LaneCStrategyPipeline(config, config_fingerprint="test-config")
    monkeypatch.setattr(
        pipeline.engine,
        "_evaluate_registry",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("fast helper must not evaluate baseline/trapped registry")
        ),
    )

    pool_events: list[dict[str, object]] = []
    pool = CandidatePool(
        ttl_minutes=30,
        max_active_symbols=10,
        now_fn=clock,
        event_sink=pool_events.append,
    )
    pool.track(state, now=clock.value, generation=1)
    evaluated: list[object] = []

    async def frames(symbols: tuple[str, ...]):
        assert symbols == ("BTCUSDT",)
        return {"BTCUSDT": market}

    async def evaluate(candidate, snapshot, poll_sequence):
        result = pipeline.evaluate_climax_only(
            candidate.symbol,
            snapshot.frame_1m.frame,
            state,
            derivatives=snapshot.derivatives,
            liquidity=snapshot.liquidity,
            decision_at=snapshot.captured_at_utc,
            poll_sequence=poll_sequence,
        )
        evaluated.append(result)
        return result

    @asynccontextmanager
    async def mutation(_: str):
        yield

    async def state_version(_: str):
        return {"asof": clock.value, "generation": 1}

    monitor = BoundedFastMonitor(
        pool,
        frames_callback=frames,
        evaluate_callback=evaluate,
        mutation_lock=mutation,
        state_version_callback=state_version,
        poll_interval_sec=20,
        clock=clock,
    )
    first = await monitor.poll_once(now=clock.value)
    assert monitor.poll_interval_sec == 20
    assert first.processed == ("BTCUSDT",)
    assert len(evaluated) == 1
    assert evaluated[0].evaluations
    assert all(item.strategy_type == "CLIMAX_EXHAUSTION" for item in evaluated[0].evaluations)

    recorder = ShadowCaptureRecorder(
        tmp_path,
        sample_every=1,
        max_bytes=2_000_000,
        max_record_bytes=1_000_000,
    )
    capture_context = {
        "fast_monitor": True,
        "poll_sequence": first.poll_sequence,
        "strategy_config": config,
        "strategy_config_fingerprint": "test-config",
    }
    capture_result = LaneCCompositionRoot._shadow_result_payload(evaluated[0])
    captured = recorder.record(
        state,
        market,
        capture_result,
        capture_context,
    )
    assert captured
    assert (tmp_path / "raw" / "shadow-inputs.jsonl").exists()

    clock.value = start + timedelta(minutes=31)
    second = await monitor.poll_once(now=clock.value)
    assert second.selected == ()
    assert len(pool) == 0
    assert any(item["action"] == "candidate_removed" for item in pool_events)
