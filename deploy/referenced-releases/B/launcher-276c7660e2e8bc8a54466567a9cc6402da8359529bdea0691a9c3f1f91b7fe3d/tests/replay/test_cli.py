from __future__ import annotations

import json
import subprocess
import sys

import pytest

pytest.importorskip("pyarrow")


def test_cli_verify_reports_sealed_bundle(tmp_path, make_event_state, make_features) -> None:
    from app.config import AppConfig
    from app.domain import EventStatus, ShortZone
    from app.replay.contracts import BaselineStrategyInput, strategy_input_to_dict
    from app.research.replay import build_bundle
    from datetime import datetime, timezone

    item = BaselineStrategyInput(make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE), make_features(), ShortZone(110, 114, "event_range"), datetime(2026, 4, 13, 12, tzinfo=timezone.utc))
    spec = {"schema_version": 1, "dataset_cutoff_utc": "2026-04-13T12:00:00Z", "candles": [{"exchange": "BYBIT", "market_type": "LINEAR", "settle_coin": "USDT", "symbol": "ONTUSDT", "interval": "1m", "open": 112, "high": 113, "low": 111, "close": 112, "volume": 1, "open_time_utc": "2026-04-13T11:59:00Z", "close_time_utc": "2026-04-13T12:00:00Z", "available_time_utc": "2026-04-13T12:00:00Z", "source": "fixture", "stable_row_id": "candle-1"}], "inputs": [strategy_input_to_dict(item)]}
    bundle = build_bundle(spec, AppConfig(), tmp_path, run_id="fixture", allow_dirty=True)

    result = subprocess.run([sys.executable, "-m", "scripts.replay_bundle", "verify", str(bundle)], capture_output=True, text=True, check=True)

    assert json.loads(result.stdout)["valid"] is True
