from __future__ import annotations

from datetime import datetime, timezone

from app.config import AppConfig
from app.domain import EventStatus, ShortZone
import pytest

from app.replay.config import strategy_config_fingerprint, strategy_config_payload
from app.replay.contracts import BaselineStrategyInput, strategy_input_from_dict, strategy_input_to_dict


def test_replay_package_exports_low_volume_extension_failure_v2() -> None:
    from app.replay import LOW_VOLUME_EXTENSION_FAILURE_V2

    assert LOW_VOLUME_EXTENSION_FAILURE_V2 == "LOW_VOLUME_EXTENSION_FAILURE_V2"


def test_baseline_input_round_trips_without_runtime_dependencies(make_event_state, make_features) -> None:
    source = BaselineStrategyInput(
        state=make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE),
        features=make_features(),
        short_zone=ShortZone(low=110.0, high=114.0, mode="event_range"),
        decision_time_utc=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
    )

    restored = strategy_input_from_dict(strategy_input_to_dict(source))

    assert isinstance(restored, BaselineStrategyInput)
    assert restored == source


def test_strategy_config_allowlist_excludes_credentials_and_operational_fields() -> None:
    config = AppConfig(telegram_token="secret", signal_chat_id="42", db_url="sqlite:///secret.db")

    payload = strategy_config_payload(config)

    assert "telegram_token" not in payload
    assert "signal_chat_id" not in payload
    assert "db_url" not in payload
    assert payload["event_ret_15m_min"] == config.event_ret_15m_min
    assert payload["volume_climax_min_ret_5m_pct"] == config.volume_climax_min_ret_5m_pct
    assert "climax_fast_poll_sec" not in payload
    assert "climax_root_event_tracking_enabled" not in payload


def test_all_trapped_longs_v2_fields_are_replay_fingerprinted():
    config = AppConfig()
    fields = {
        "trapped_longs_min_closed_5m_candles_after_breakout": 3,
        "trapped_longs_block_price_oi_acceleration_together": False,
        "trapped_longs_price_acceleration_veto_pct": 1.5,
        "trapped_longs_oi_acceleration_veto_pct": 2.5,
        "trapped_longs_v2_min_oi_retained_increase_pct": 3.5,
    }
    payload = strategy_config_payload(config)
    assert all(name in payload for name in fields)
    baseline = strategy_config_fingerprint(config)
    for name, value in fields.items():
        assert strategy_config_fingerprint(config.model_copy(update={name: value})) != baseline


@pytest.mark.parametrize("value", [0, 1, 3, 5, -1])
def test_app_config_requires_exactly_four_oi_sequence_points(value):
    with pytest.raises(ValueError, match="exactly 4"):
        AppConfig(trapped_longs_require_oi_sequence_points=value)
