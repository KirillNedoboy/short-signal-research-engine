import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app.signals.low_volume_v2 import LowVolumeV2Observation, advance_low_volume_v2, new_episode


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "low_volume_v2_regression_fixtures.json"
FIXTURES = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["cases"]


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        low_volume_v2_min_closed_candles_after_retest=2,
        low_volume_v2_max_episode_lifetime_minutes=45,
        low_volume_v2_new_high_tolerance_pct=0.30,
        low_volume_v2_require_breakout_close=True,
        low_volume_v2_require_lower_high_or_swing_break=True,
        low_volume_v2_block_oi_acceleration_pct=1.0,
        low_volume_v2_block_price_acceleration_resumed=True,
        low_volume_v2_max_entry_distance_below_high_pct=15.0,
    )


def _production_inputs(case: dict) -> tuple[SimpleNamespace, pd.DataFrame, LowVolumeV2Observation]:
    observed_at = datetime(2026, 10, 3, 12, 10, tzinfo=UTC)
    frame = pd.DataFrame(
        [{"timestamp": observed_at, "high": 100.0, "close": 95.0, "volume": 1.0}]
    ).set_index("timestamp", drop=False)
    features = SimpleNamespace(asof=observed_at, frame_rows=len(frame), fixture_name=case["name"])
    observation = LowVolumeV2Observation(
        observed_at=features.asof,
        current_high=100.0,
        retest_high=99.0,
        closed_candles=2,
        last_closed_candle_time=observed_at - timedelta(minutes=1),
        close=95.0,
        breakout_reference=100.0,
        lower_high_or_swing_break=True,
        extension_below_threshold=bool(case.get("extension_below_threshold", False)),
    )
    return features, frame, observation


def test_every_production_derived_v2_fixture_executes_state_path():
    assert FIXTURES
    config = _config()
    for case in FIXTURES:
        features, frame, observation = _production_inputs(case)
        state = new_episode(
            case.get("root_event_id", case["name"]),
            int(case.get("event_revision", 1)),
            100.0,
            datetime(2026, 10, 3, 12, 0, tzinfo=UTC),
        )
        decision = advance_low_volume_v2(state, observation, config)
        expected_actionable = bool(case.get("expected_actionable", True))
        assert (decision is not None) is expected_actionable, case["name"]
        assert len(frame) == features.frame_rows
        if decision is not None:
            assert decision.root_event_id == case.get("root_event_id", case["name"])
            assert decision.event_revision == int(case.get("event_revision", 1))
            assert decision.metadata["counterfactual"]["extension_below_threshold"] == observation.extension_below_threshold
        if "expected_cache_key" in case:
            assert (state.root_event_id, state.event_revision) == tuple(case["expected_cache_key"])
