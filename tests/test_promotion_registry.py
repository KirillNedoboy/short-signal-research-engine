"""Tests for the code-enforced promotion registry (V6)."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from app.config import AppConfig, load_config
from app.main import ShortSignalBot
from app.promotions import REGISTRY_PATH, validate_promotion_registry
from app.storage.db import Database
from app.storage.repository import BotRepository

ALL_OFF = {
    "baseline_live_delivery_enabled": False,
    "volume_climax_live_delivery_enabled": False,
    "low_volume_live_delivery_enabled": False,
    "trapped_longs_live_delivery_enabled": False,
}


def test_registry_missing_strategy_refuses_startup(tmp_path) -> None:
    registry = tmp_path / "promotion_registry.json"
    registry.write_text(json.dumps({"strategies": {}}), encoding="utf-8")
    config = AppConfig(**{**ALL_OFF, "trapped_longs_live_delivery_enabled": True})
    with pytest.raises(RuntimeError) as excinfo:
        validate_promotion_registry(config, registry_path=registry)
    assert "TRAPPED_LONGS_REVERSAL" in str(excinfo.value)
    assert "trapped_longs_live_delivery_enabled" in str(excinfo.value)


def test_unvalidated_or_evidenceless_entry_refuses(tmp_path) -> None:
    registry = tmp_path / "promotion_registry.json"
    registry.write_text(
        json.dumps(
            {
                "strategies": {
                    "TRAPPED_LONGS_REVERSAL": {"status": "CANDIDATE"},
                    "BASELINE_PULLBACK": {
                        "status": "VALIDATED",
                        "recorded_at": "2026-09-23",
                        "source": "s",
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    config = AppConfig(**{**ALL_OFF, "trapped_longs_live_delivery_enabled": True})
    with pytest.raises(RuntimeError, match="TRAPPED_LONGS_REVERSAL"):
        validate_promotion_registry(config, registry_path=registry)
    config = AppConfig(**{**ALL_OFF, "baseline_live_delivery_enabled": True})
    with pytest.raises(RuntimeError, match="evidence"):
        validate_promotion_registry(config, registry_path=registry)


def test_no_flags_enabled_needs_no_registry(tmp_path) -> None:
    validate_promotion_registry(
        AppConfig(**ALL_OFF), registry_path=tmp_path / "absent.json"
    )


def test_bot_startup_fails_closed_without_entry(tmp_path) -> None:
    registry = tmp_path / "promotion_registry.json"
    registry.write_text(json.dumps({"strategies": {}}), encoding="utf-8")

    class _NoTelegram:
        async def send_signal(self, _m: str) -> bool:
            raise AssertionError("telegram must not be used in this test")

        async def send_alert(self, _m: str) -> bool:
            raise AssertionError("telegram must not be used in this test")

    async def _cycle() -> None:
        database = Database(f"sqlite:///{tmp_path / 'registry.db'}")
        database.create_all()
        bot = ShortSignalBot(
            config=AppConfig(
                **{**ALL_OFF, "trapped_longs_live_delivery_enabled": True}
            ),
            repository=BotRepository(database),
            notifier=_NoTelegram(),
        )
        await bot.startup()

    import app.main as main_module

    original = main_module.validate_promotion_registry
    main_module.validate_promotion_registry = lambda config: original(
        config, registry_path=registry
    )
    try:
        with pytest.raises(RuntimeError, match="TRAPPED_LONGS_REVERSAL"):
            asyncio.run(_cycle())
    finally:
        main_module.validate_promotion_registry = original


def test_shipped_registry_covers_shipped_config() -> None:
    # The config.yaml and promotion_registry.json in this release must agree:
    # every flag true in config.yaml has a VALIDATED entry, proven by loading
    # the real files.
    config = load_config(
        config_path=Path(__file__).resolve().parents[1] / "config.yaml",
        env_path=Path(__file__).resolve().parents[1] / ".env.missing",
    )
    validate_promotion_registry(config, registry_path=REGISTRY_PATH)
