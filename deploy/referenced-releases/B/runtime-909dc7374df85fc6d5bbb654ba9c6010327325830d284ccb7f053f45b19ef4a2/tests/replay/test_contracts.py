from __future__ import annotations

from datetime import datetime, timezone

from app.config import AppConfig
from app.domain import EventStatus, ShortZone
from app.replay.config import strategy_config_payload
from app.replay.contracts import BaselineStrategyInput, strategy_input_from_dict, strategy_input_to_dict


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
