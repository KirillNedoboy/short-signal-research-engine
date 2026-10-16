from pathlib import Path

from app.config import load_config


def test_trapped_longs_v2_is_explicitly_disabled_by_default(tmp_path):
    cfg = load_config(tmp_path / "config.yaml", tmp_path / ".env")
    assert cfg.trapped_longs_reversal_v2_enabled is False


def test_trapped_longs_v2_flag_is_loaded_without_enabling_delivery(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("trapped_longs_reversal_v2_enabled: true\ntrapped_longs_live_delivery_enabled: false\n")
    cfg = load_config(path, tmp_path / ".env")
    assert cfg.trapped_longs_reversal_v2_enabled is True
    assert cfg.trapped_longs_live_delivery_enabled is False


def test_trapped_longs_v2_has_distinct_delivery_flag(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "trapped_longs_reversal_enabled: false\n"
        "trapped_longs_reversal_v2_enabled: true\n"
        "trapped_longs_live_delivery_enabled: false\n"
        "trapped_longs_v2_live_delivery_enabled: true\n"
    )
    cfg = load_config(path, tmp_path / ".env")
    assert cfg.trapped_longs_reversal_enabled is False
    assert cfg.trapped_longs_reversal_v2_enabled is True
    assert cfg.trapped_longs_v2_live_delivery_enabled is True
