from app.config import AppConfig
from app.replay.config import strategy_config_fingerprint, strategy_config_payload


def test_low_volume_v2_extension_policy_is_explicit_and_fingerprinted():
    config = AppConfig()
    assert config.low_volume_v2_extension_below_threshold_policy == "metadata_only_after_confirmation"
    assert strategy_config_payload(config)["low_volume_v2_extension_below_threshold_policy"] == "metadata_only_after_confirmation"
    changed = config.model_copy(update={"low_volume_v2_extension_below_threshold_policy": "metadata_only_after_confirmation"})
    assert strategy_config_fingerprint(config) == strategy_config_fingerprint(changed)
