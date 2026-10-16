import pytest

from app.config import AppConfig
from app.replay.config import strategy_config_fingerprint, strategy_config_payload


def test_v2_defaults_are_disabled_and_safe():
    config = AppConfig()
    assert config.low_volume_extension_v2_enabled is False
    assert config.low_volume_extension_v2_live_delivery_enabled is False
    assert config.low_volume_v2_max_episode_lifetime_minutes == 45


def test_v2_fields_are_in_replay_fingerprint():
    first = AppConfig()
    second = AppConfig(low_volume_v2_recheck_interval_sec=61)
    assert "low_volume_v2_recheck_interval_sec" in strategy_config_payload(first)
    assert strategy_config_fingerprint(first) != strategy_config_fingerprint(second)


@pytest.mark.parametrize("field,value", [
    ("low_volume_v2_max_episode_lifetime_minutes", 0),
    ("low_volume_v2_min_closed_candles_after_retest", 0),
    ("low_volume_v2_recheck_interval_sec", 0),
    ("low_volume_v2_new_high_tolerance_pct", -1.0),
])
def test_v2_invalid_values_fail_closed(field, value):
    with pytest.raises(ValueError):
        AppConfig(**{field: value})
