from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from app.infra.provider_errors import ProviderRateLimited
from app.config import AppConfig
from app.main import LaneCCompositionRoot
from app.market.bybit_client import BybitClient
from app.runtime.shadow import (
    ShadowControl,
    ShadowIsolationError,
    ShadowPaths,
    validate_shadow_config,
)


def test_shadow_paths_are_rooted_and_reject_escape_or_production(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    root.mkdir()
    paths = ShadowPaths.from_environment(
        {
            "SHADOW_MODE": "1",
            "SHADOW_ROOT": str(root),
            "SHADOW_CONFIG_PATH": str(root / "config.yaml"),
            "SHADOW_ENV_PATH": str(root / "shadow.env"),
            "SHADOW_DB_PATH": str(root / "db" / "shadow.sqlite"),
            "SHADOW_RUNTIME_ROOT": str(root / "runtime"),
            "SHADOW_LOCK_PATH": str(root / "locks" / "shadow.lock"),
            "SHADOW_CHECKPOINT_ROOT": str(root / "checkpoints"),
        }
    )
    assert paths.db_path.is_relative_to(root)
    assert paths.lock_path.is_relative_to(root)
    assert paths.checkpoint_root.is_relative_to(root)
    shadow_config = validate_shadow_config(AppConfig(autoexecution="OFF"), paths)
    assert shadow_config.db_url == paths.db_uri
    assert Path(shadow_config.market_frame_snapshot_path).is_relative_to(root)
    assert Path(shadow_config.research_spool_root).is_relative_to(root)
    missing_config = {
        "SHADOW_MODE": "1",
        "SHADOW_ROOT": str(root),
        "SHADOW_ENV_PATH": str(root / "shadow.env"),
        "SHADOW_DB_PATH": str(root / "db" / "shadow.sqlite"),
    }
    with pytest.raises(ShadowIsolationError, match="fallback is disabled"):
        ShadowPaths.from_environment(missing_config)
    with pytest.raises(ShadowIsolationError, match="production"):
        validate_shadow_config(
            AppConfig(
                autoexecution="OFF",
                db_url="sqlite:////opt/short-telegram-bot-lite/data/bot.sqlite",
            ),
            paths,
        )
    with pytest.raises(ShadowIsolationError, match="Telegram credentials"):
        validate_shadow_config(
            AppConfig(autoexecution="OFF", telegram_token="secret"), paths
        )

    escaped = dict(os.environ)
    escaped.update(
        {
            "SHADOW_MODE": "1",
            "SHADOW_ROOT": str(root),
            "SHADOW_CONFIG_PATH": str(tmp_path / "outside.yaml"),
            "SHADOW_ENV_PATH": str(root / "shadow.env"),
            "SHADOW_DB_PATH": str(root / "db" / "shadow.sqlite"),
        }
    )
    with pytest.raises(ShadowIsolationError, match="under SHADOW_ROOT"):
        ShadowPaths.from_environment(escaped)

    production = dict(os.environ)
    production.update(
        {
            "SHADOW_MODE": "1",
            "SHADOW_ROOT": str(tmp_path / "short-bot-lane-c"),
            "SHADOW_CONFIG_PATH": str(tmp_path / "short-bot-lane-c" / "config.yaml"),
            "SHADOW_ENV_PATH": str(tmp_path / "short-bot-lane-c" / "shadow.env"),
            "SHADOW_DB_PATH": str(tmp_path / "short-bot-lane-c" / "db.sqlite"),
        }
    )
    with pytest.raises(ShadowIsolationError, match="production"):
        ShadowPaths.from_environment(production)


def test_shadow_control_is_bounded_and_acknowledges_raw_capture_disable(
    tmp_path: Path,
) -> None:
    paths = ShadowPaths.for_root(tmp_path / "shadow")
    control = ShadowControl(paths, max_events=4, max_event_bytes=2048)
    control.emit_capability()
    control.set_readiness(True, reason="started")
    paths.raw_control_path.write_text(
        json.dumps({"action": "disable_raw_capture"}), encoding="utf-8"
    )
    assert control.poll_control() == "disable_raw_capture"
    assert control.raw_capture_enabled is False
    control.set_readiness(False, reason="shutdown")

    events = [json.loads(line) for line in paths.event_path.read_text().splitlines()]
    assert [event["type"] for event in events] == [
        "capability",
        "readiness",
        "raw_capture_disabled",
        "readiness",
    ]
    assert all("payload" not in event for event in events)
    assert paths.event_path.stat().st_size <= 2048


def test_shadow_control_rate_limit_hook_emits_rest_and_ws_stop_events(
    tmp_path: Path,
) -> None:
    paths = ShadowPaths.for_root(tmp_path / "shadow")
    control = ShadowControl(paths)

    assert control.record_rate_limit(ProviderRateLimited(10006), transport="rest")
    assert control.record_rate_limit(
        RuntimeError("HTTP 429 from websocket"), transport="ws"
    )

    events = [json.loads(line) for line in paths.event_path.read_text().splitlines()]
    assert [event["type"] for event in events] == ["rest_429", "ws_429"]
    assert control.rate_limit_seen is True


@pytest.mark.asyncio
async def test_main_shadow_control_readiness_and_rest_boundary_event(tmp_path: Path, monkeypatch) -> None:
    """Composition readiness follows market coverage and REST 10006 is observable."""

    import app.runtime.shadow as shadow_module

    class _Repository:
        def list_active_event_states(self, *, now):
            return []

        def get_event_state(self, symbol):
            return None

        def upsert_event_state(self, state):
            return state

    class _Provider:
        _scanner = object()

        async def start(self):
            return None

        async def stop(self):
            return None

    shadow_module._ACTIVE_CONTROL = None
    root_path = tmp_path / "shadow-main"
    monkeypatch.setenv("SHADOW_MODE", "1")
    monkeypatch.setenv("SHADOW_ROOT", str(root_path))
    monkeypatch.setenv("SHADOW_CONFIG_PATH", str(root_path / "config.yaml"))
    monkeypatch.setenv("SHADOW_ENV_PATH", str(root_path / "shadow.env"))
    monkeypatch.setenv("SHADOW_DB_PATH", str(root_path / "db" / "shadow.sqlite"))
    try:
        provider = _Provider()
        runtime = LaneCCompositionRoot(
            AppConfig(autoexecution="OFF"),
            repository=_Repository(),
            provider=provider,
        )
        runtime.market.readiness = lambda symbols=None: (True, "market_coverage_valid")
        await runtime.start()
        try:
            runtime._set_shadow_readiness(("SOLUSDT",))
            assert runtime.shadow_control is not None
            assert runtime.shadow_control.readiness is True
            with pytest.raises(ProviderRateLimited):
                BybitClient._call_once(lambda **_: {"retCode": 10006})
            events = [
                json.loads(line)
                for line in runtime.shadow_control.paths.event_path.read_text().splitlines()
            ]
            assert "readiness" in [event["type"] for event in events]
            assert "rest_429" in [event["type"] for event in events]
        finally:
            await runtime.stop()
    finally:
        shadow_module._ACTIVE_CONTROL = None
