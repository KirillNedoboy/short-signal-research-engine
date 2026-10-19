from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from app.config import AppConfig
from app.main import LaneCCompositionRoot


def _frame() -> pd.DataFrame:
    end = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    timestamps = [end - timedelta(minutes=299 - index) for index in range(300)]
    close = [100.0 + index * 0.01 for index in range(300)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 0.2 for value in close],
            "low": [value - 0.2 for value in close],
            "close": close,
            "volume": [1000.0 + index for index in range(300)],
            "turnover": [100000.0 + index for index in range(300)],
        }
    ).set_index("timestamp", drop=False)


class _Repository:
    def list_active_event_states(self, *, now):
        return []

    def get_event_state(self, symbol):
        return None

    def upsert_event_state(self, state):
        return state


class _Provider:
    _scanner = object()
    last_universe_telemetry = None

    def __init__(self) -> None:
        self.snapshot_calls: list[bool] = []

    async def start(self):
        return None

    async def stop(self):
        return None

    async def build_scan_snapshot(self):
        return SimpleNamespace(snapshots=(SimpleNamespace(symbol="SOLUSDT"),))

    def shortlist(self, snapshots):
        return list(snapshots)

    def update_universe(self, symbols):
        return None

    async def fetch_symbol_frames(self, symbols):
        return {symbol: _frame() for symbol in symbols}

    async def capture_decision_snapshot(
        self, symbol, *, frame_1m, include_liquidity, derivatives=None
    ):
        self.snapshot_calls.append(include_liquidity)
        return SimpleNamespace(
            frame_1m=SimpleNamespace(frame=frame_1m),
            derivatives={},
            liquidity=None,
            captured_at_utc=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_composition_runs_one_window_without_legacy_run_cycle() -> None:
    provider = _Provider()
    runtime = LaneCCompositionRoot(
        AppConfig(autoexecution="OFF"), repository=_Repository(), provider=provider
    )
    await runtime.start()
    try:
        report = await runtime.run_window(window_id="smoke-1")
    finally:
        await runtime.stop()
    assert report.scheduled == 1
    assert report.coverage == 1.0
    assert report.statuses["SOLUSDT"] == "SCANNED_OK"
    assert provider.snapshot_calls == [False]

