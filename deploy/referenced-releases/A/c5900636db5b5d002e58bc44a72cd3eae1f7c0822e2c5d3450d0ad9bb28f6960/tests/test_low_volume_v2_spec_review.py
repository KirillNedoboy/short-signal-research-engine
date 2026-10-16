from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from app.signals.climax import _low_volume_v2
from app.signals.low_volume_v2 import LowVolumeV2State
from app.storage.db import Database
from app.storage.repository import BotRepository
from tests.contracts.helpers import climax_config


def _base(*, current_leg_high=100.0, closed_high=100.0, closed_available=True):
    return {
        "current_leg_high": current_leg_high,
        "closed_high": closed_high,
        "closed_high_available": closed_available,
        "post_high_high": closed_high,
        "post_high_retest_high": 99.0,
        "closed_candles_after_high": 2,
        "last_closed_candle_close_time": "2026-04-13T12:09:00+00:00",
        "current_close": 95.0,
        "breakout_reference": 99.5,
        "current_previous_volume_ratio": 0.2,
        "rejection_pct": 3.0,
        "entry_distance_below_high_pct": 5.0,
    }


def _state():
    return SimpleNamespace(
        event_id="root",
        event_high=100.0,
        event_high_time=datetime(2026, 4, 13, 12, 5, tzinfo=UTC),
    )


def _features(*, asof=None, ret_5m=4.0):
    return SimpleNamespace(
        asof=asof or datetime(2026, 4, 13, 12, 10, tzinfo=UTC),
        symbol="BTCUSDT",
        ret_5m=ret_5m,
        rejection_from_high_pct=3.0,
        latest_failed_retest=True,
        oi_change_pct=-2.0,
        data_quality_warnings=[],
        liquidity_available=True,
        spread_pct=0.05,
        slippage_pct=0.05,
        orderbook_depth_usdt_1pct=100000.0,
        orderbook_depth_usdt_2pct=200000.0,
    )


def test_live_v2_episode_cache_reuses_state_and_emitted_guard():
    cache = {}
    config = climax_config(low_volume_extension_v2_enabled=True)
    first = _low_volume_v2(_base(), _state(), _features(ret_5m=6.0), config, episode_cache=cache)
    second = _low_volume_v2(_base(), _state(), _features(asof=datetime(2026, 4, 13, 12, 11, tzinfo=UTC), ret_5m=6.0), config, episode_cache=cache)
    assert len(cache) == 1
    assert first.metadata["reset_count"] == second.metadata["reset_count"]
    assert second.metadata["emitted"] is True
    assert second.subtype is None
    assert second.veto_reasons == ["v2_actionable"]


def test_v2_uses_closed_high_not_forming_candle_high():
    cache = {}
    config = climax_config(low_volume_extension_v2_enabled=True)
    result = _low_volume_v2(_base(current_leg_high=105.0, closed_high=100.0), _state(), _features(), config, episode_cache=cache)
    episode = next(iter(cache.values()))
    assert result.metadata["current_anchor_high"] == 100.0
    assert episode.reset_count == 0


def test_v2_fails_closed_without_closed_high_and_exposes_extension_quality():
    cache = {}
    config = climax_config(low_volume_extension_v2_enabled=True)
    result = _low_volume_v2(_base(closed_high=0.0, closed_available=False), _state(), _features(ret_5m=4.0), config, episode_cache=cache)
    assert "closed_candles_unavailable" in result.veto_reasons
    assert result.metadata["extension_below_threshold"] is True
    assert "extension_below_threshold" in result.data_quality
    assert result.metadata["counterfactual"]["extension_below_threshold"] is True


def test_v2_hydrates_every_persisted_lifecycle_field_from_repository(tmp_path):
    repo = BotRepository(Database(f"sqlite:///{tmp_path / 'hydration.db'}"))
    repo._db.create_all()
    start = datetime(2026, 4, 13, 12, 5, tzinfo=UTC)
    persisted = LowVolumeV2State(
        root_event_id="root",
        event_revision=1,
        first_event_high=98.5,
        current_anchor_high=101.5,
        current_anchor_high_time=start + timedelta(minutes=1),
        last_retest_high=100.25,
        last_closed_candle_time=start + timedelta(minutes=4),
        episode_started_at=start,
        reset_count=3,
        entry_trigger_time=start + timedelta(minutes=3),
        invalidated_reason="stale_data",
        v2_state="INVALIDATED",
        emitted=True,
    )
    repo.upsert_low_volume_v2_episode_state(persisted, symbol="BTCUSDT")

    cache = {}
    _low_volume_v2(_base(), _state(), _features(), climax_config(low_volume_extension_v2_enabled=True), episode_cache=cache, state_repository=repo)
    hydrated = cache[("root", 1)]

    assert hydrated.root_event_id == persisted.root_event_id
    assert hydrated.event_revision == persisted.event_revision
    assert hydrated.first_event_high == persisted.first_event_high
    assert hydrated.current_anchor_high == persisted.current_anchor_high
    assert hydrated.current_anchor_high_time == persisted.current_anchor_high_time.replace(tzinfo=None)
    assert hydrated.last_retest_high == persisted.last_retest_high
    assert hydrated.last_closed_candle_time == persisted.last_closed_candle_time.replace(tzinfo=None)
    assert hydrated.episode_started_at == persisted.episode_started_at.replace(tzinfo=None)
    assert hydrated.reset_count == persisted.reset_count
    assert hydrated.entry_trigger_time == persisted.entry_trigger_time.replace(tzinfo=None)
    assert hydrated.v2_state == persisted.v2_state
    assert hydrated.emitted == persisted.emitted
    assert hydrated.invalidated_reason == persisted.invalidated_reason
