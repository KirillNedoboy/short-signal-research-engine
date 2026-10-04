from __future__ import annotations

from datetime import datetime, timezone

from app.config import AppConfig
from app.domain import EventStatus, ShortZone
from app.replay.contracts import BaselineStrategyInput, ClimaxStrategyInput, LOW_VOLUME_EXTENSION_FAILURE, VOLUME_CLIMAX_UNWIND
from app.replay.evaluator import evaluate_strategy_input
from app.signals.climax import evaluate_climax_bundle
from app.signals.engine import SignalEngine
from tests.contracts.helpers import climax_config


def test_baseline_replay_dispatches_to_current_signal_engine(make_event_state, make_features) -> None:
    state = make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE)
    features = make_features()
    decision_time = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    item = BaselineStrategyInput(state, features, ShortZone(110.0, 114.0, "event_range"), decision_time)
    config = AppConfig()

    assert evaluate_strategy_input(item, config) == SignalEngine(config).analyze(state, features, item.short_zone, decision_time)


def test_volume_replay_dispatches_to_current_climax_bundle(make_event_state, make_features, make_frame):
    state = make_event_state()
    features = make_features(ret_5m=10.0, ret_15m=15.0, vol_zscore_30m=8.0, oi_change_pct=-4.0, rejection_from_high_pct=3.0, latest_failed_retest=True, current_volume=3000.0)
    frame = make_frame([100 + index * 0.1 for index in range(30)])
    config = climax_config(low_volume_extension_enabled=False)
    item = ClimaxStrategyInput(state, features, frame, features.asof, VOLUME_CLIMAX_UNWIND)
    assert evaluate_strategy_input(item, config) == evaluate_climax_bundle(state, features, frame, config, strict_closed_candles=True)


def test_low_volume_replay_dispatches_to_current_climax_bundle(make_features, make_event_state, make_frame):
    from tests.contracts.helpers import ready_low_volume_inputs

    state, features, frame = ready_low_volume_inputs(make_features, make_event_state, make_frame)
    config = climax_config(volume_climax_unwind_enabled=False)
    item = ClimaxStrategyInput(state, features, frame, features.asof, LOW_VOLUME_EXTENSION_FAILURE)
    assert evaluate_strategy_input(item, config) == evaluate_climax_bundle(state, features, frame, config, strict_closed_candles=True)
