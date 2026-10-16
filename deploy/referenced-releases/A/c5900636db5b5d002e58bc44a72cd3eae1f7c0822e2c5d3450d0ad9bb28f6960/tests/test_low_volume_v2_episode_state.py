from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from app.signals.low_volume_v2 import (
    LowVolumeV2Observation,
    advance_low_volume_v2,
    new_episode,
)


def cfg(**overrides):
    values = dict(
        low_volume_v2_min_closed_candles_after_retest=2,
        low_volume_v2_max_episode_lifetime_minutes=45,
        low_volume_v2_new_high_tolerance_pct=0.30,
        low_volume_v2_require_breakout_close=True,
        low_volume_v2_require_lower_high_or_swing_break=True,
        low_volume_v2_block_oi_acceleration_pct=1.0,
        low_volume_v2_block_price_acceleration_resumed=True,
        low_volume_v2_max_entry_distance_below_high_pct=15.0,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def obs(at, high, **kwargs):
    values = dict(close=95, breakout_reference=100, closed_candles=2, lower_high_or_swing_break=True)
    values.update(kwargs)
    return LowVolumeV2Observation(observed_at=at, current_high=high, **values)


def test_sand_shaped_episode_resets_then_emits_once():
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("sand-root", 1, 100, start)
    assert advance_low_volume_v2(state, obs(start + timedelta(minutes=2), 100, failed_high=False), cfg()) is None
    assert state.v2_state == "FAILED_HIGH_PENDING"
    assert advance_low_volume_v2(state, obs(start + timedelta(minutes=5), 100.5), cfg()) is None
    assert state.reset_count == 1
    decision = advance_low_volume_v2(state, obs(start + timedelta(minutes=10), 100.4), cfg())
    assert decision is not None
    assert decision.strategy_subtype == "LOW_VOLUME_EXTENSION_FAILURE_V2"
    assert advance_low_volume_v2(state, obs(start + timedelta(minutes=11), 100.4), cfg()) is None


def test_repeated_new_highs_reset_anchor_without_decisions():
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("root", 1, 100, start)
    for index, high in enumerate((101, 102, 103), 1):
        assert advance_low_volume_v2(state, obs(start + timedelta(minutes=index), high), cfg()) is None
    assert state.current_anchor_high == 103
    assert state.reset_count == 3


def test_new_high_before_delivery_blocks_old_entry():
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("root", 1, 100, start)
    assert advance_low_volume_v2(state, obs(start + timedelta(minutes=1), 100, closed_candles=1), cfg()) is None
    assert advance_low_volume_v2(state, obs(start + timedelta(minutes=2), 101), cfg()) is None
    assert state.reset_count == 1
    assert state.entry_trigger_time is None


def test_stale_data_uses_dedicated_invalidation_reason():
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("stale-root", 1, 100, start)
    assert advance_low_volume_v2(
        state,
        obs(start + timedelta(minutes=1), 100, data_fresh=False),
        cfg(),
    ) is None
    assert state.v2_state == "INVALIDATED"
    assert state.invalidated_reason == "stale_data"


def test_expiry_and_malformed_data_fail_closed():
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("root", 1, 100, start)
    assert advance_low_volume_v2(state, {"observed_at": start + timedelta(minutes=46), "current_high": 100}, cfg()) is None
    assert state.v2_state == "EXPIRED"
    malformed = new_episode("root-2", 1, 100, start)
    assert advance_low_volume_v2(malformed, {"observed_at": start, "current_high": float("nan")}, cfg()) is None
    assert malformed.v2_state == "INVALIDATED"
