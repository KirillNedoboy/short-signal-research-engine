from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.main import successful_evaluation_count
from app.runtime.contracts import MarketSnapshot, SnapshotQuality, SnapshotStatus
from app.runtime.coordinator import PerSymbolCoordinator
from app.runtime.scan import ScanPriority, ScanWindowCoordinator, TerminalStatus
from app.runtime.scheduling import next_scan_deadline


def _load_launcher_for_test(module_name: str):
    launcher = Path(__file__).parents[1] / "run_live.py"
    spec = importlib.util.spec_from_file_location(module_name, launcher)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    environment = os.environ.copy()
    try:
        spec.loader.exec_module(module)
    finally:
        os.environ.clear()
        os.environ.update(environment)
        sys.modules.pop(module_name, None)
    return module


def test_source_pin_and_release_boundary() -> None:
    launcher = Path(__file__).parents[1] / "run_live.py"
    text = launcher.read_text(encoding="utf-8")
    assert "SOURCE_ROOT" in text
    assert '"src"' in text or "'src'" in text
    assert "release/sha256-aa4f" not in text


def test_full_mode_launcher_accepts_existing_c_paths(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("SHADOW_MODE", raising=False)
    config_path = tmp_path / "production-c.yaml"
    env_path = tmp_path / "production-c.env"
    lock_path = tmp_path / "runtime" / "lane-c.lock"
    monkeypatch.setenv("LANE_C_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("LANE_C_ENV_PATH", str(env_path))
    monkeypatch.setenv("LANE_C_LOCK_PATH", str(lock_path))

    launcher = _load_launcher_for_test("_lane_c_launcher_override_test")

    assert launcher.CONFIG_PATH == config_path
    assert launcher.ENV_PATH == env_path
    assert launcher.LOCK_PATH == lock_path
    assert launcher.instance_fence().path == lock_path


def test_full_mode_launcher_blank_paths_keep_archive_defaults(monkeypatch) -> None:
    monkeypatch.delenv("SHADOW_MODE", raising=False)
    for name in ("LANE_C_CONFIG_PATH", "LANE_C_ENV_PATH", "LANE_C_LOCK_PATH"):
        monkeypatch.setenv(name, " ")

    launcher = _load_launcher_for_test("_lane_c_launcher_blank_override_test")
    root = Path(__file__).parents[1]

    assert launcher.CONFIG_PATH == root / "config.yaml"
    assert launcher.ENV_PATH == root / ".env"
    assert launcher.LOCK_PATH == root / "runtime" / "lane-c.lock"


def test_shadow_paths_take_precedence_over_full_mode_overrides(monkeypatch, tmp_path) -> None:
    shadow_root = tmp_path / "shadow-root"
    monkeypatch.setenv("SHADOW_MODE", "1")
    monkeypatch.setenv("SHADOW_ROOT", str(shadow_root))
    monkeypatch.setenv("SHADOW_CONFIG_PATH", str(shadow_root / "config.yaml"))
    monkeypatch.setenv("SHADOW_DB_PATH", str(shadow_root / "db" / "shadow.sqlite"))
    monkeypatch.setenv("LANE_C_CONFIG_PATH", str(tmp_path / "production.yaml"))
    monkeypatch.setenv("LANE_C_ENV_PATH", str(tmp_path / "production.env"))
    monkeypatch.setenv("LANE_C_LOCK_PATH", str(tmp_path / "production.lock"))

    launcher = _load_launcher_for_test("_lane_c_launcher_shadow_precedence_test")

    assert launcher.CONFIG_PATH == shadow_root.resolve() / "config.yaml"
    assert launcher.ENV_PATH == shadow_root.resolve() / "shadow.env"
    assert launcher.LOCK_PATH == shadow_root.resolve() / "locks" / "shadow.lock"


def test_full_mode_service_uses_production_c_inputs_and_separate_runtime() -> None:
    service = Path(__file__).parents[1] / "deploy" / "lane-c-runtime.service.example"
    text = service.read_text(encoding="utf-8")

    assert "WorkingDirectory=/opt/short-telegram-bot-lite-admin/lane-c" in text
    assert "ExecStart=/opt/short-bot-c-runtime-validation/candidate-venv/bin/python /opt/short-bot-lane-c-live/run_live.py" in text
    assert "Environment=LANE_C_CONFIG_PATH=/opt/short-telegram-bot-lite-admin/lane-c/config.yaml" in text
    assert "Environment=LANE_C_ENV_PATH=/opt/short-telegram-bot-lite-admin/lane-c/.env" in text
    assert "Environment=LANE_C_LOCK_PATH=/opt/short-telegram-bot-lite-admin/lane-c/runtime/lane-c.lock" in text
    assert "Environment=AUTOEXECUTION=OFF" in text
    assert "Environment=PYTHONDONTWRITEBYTECODE=1" in text
    assert "Environment=PYTHONPYCACHEPREFIX=/var/lib/short-bot-lane-c-live/runtime/pycache" in text
    assert "Environment=CANONICAL_MARKET_DATA_PROVIDER_EVIDENCE_PATH=/var/lib/short-bot-lane-c-live/runtime/canonical-marketdata-provider-evidence.json" in text
    assert "ReadOnlyPaths=/opt/short-bot-lane-c-live /opt/short-telegram-bot-lite-admin/lane-c" in text
    assert "ReadWritePaths=/opt/short-telegram-bot-lite-admin/lane-c/data /opt/short-telegram-bot-lite-admin/lane-c/runtime/lane-c.lock /var/lib/short-bot-lane-c-live/runtime" in text
    assert "EnvironmentFile=" not in text
    assert "Environment=CANONICAL_MARKET_DATA_PROVIDER_MODE" not in text
    assert "TELEGRAM_TOKEN" not in text


def test_snapshot_is_immutable_and_stale_is_not_actionable() -> None:
    now = datetime.now(UTC)
    snapshot = MarketSnapshot(
        symbol="BTCUSDT",
        asof=now - timedelta(seconds=2),
        received_at=now,
        quality=SnapshotQuality.COMPLETE,
        status=SnapshotStatus.READY,
        last_price=100.0,
        candles=(
            {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0},
        ),
    )
    assert snapshot.actionable(max_age=timedelta(seconds=10))
    with pytest.raises(AttributeError):
        snapshot.last_price = 101.0  # type: ignore[misc]
    stale = snapshot.with_status(SnapshotStatus.STALE, quality=SnapshotQuality.UNKNOWN)
    assert not stale.actionable(max_age=timedelta(seconds=10))
    assert stale.missing_fields == ("market_data",)


def test_mixed_terminal_statuses_count_only_successful_evaluations() -> None:
    statuses = (
        TerminalStatus.SCANNED_OK.value,
        TerminalStatus.NO_SETUP.value,
        TerminalStatus.STALE_DATA.value,
        TerminalStatus.INCOMPLETE_DATA.value,
        TerminalStatus.SCAN_ERROR.value,
    )
    assert successful_evaluation_count(statuses) == 2


def test_anchored_scheduler_skips_missed_ticks_without_catchup() -> None:
    decision = next_scan_deadline(
        scheduled_deadline=100.0,
        finished_at=261.0,
        interval_sec=60.0,
    )
    assert decision.next_deadline == 280.0
    assert decision.skipped_ticks == 2
    assert decision.cycle_overrun_ms == 101_000.0


@pytest.mark.asyncio
async def test_same_symbol_is_serialized_and_different_symbols_overlap() -> None:
    gate = PerSymbolCoordinator()
    active: list[tuple[str, int]] = []
    max_same = 0
    max_total = 0

    async def work(symbol: str, marker: int) -> None:
        nonlocal max_same, max_total
        async with gate.hold(symbol):
            active.append((symbol, marker))
            same = sum(item[0] == symbol for item in active)
            max_same = max(max_same, same)
            max_total = max(max_total, len(active))
            await asyncio.sleep(0.01)
            active.remove((symbol, marker))

    await asyncio.gather(work("ethusdt", 1), work("ETHUSDT", 2), work("BTCUSDT", 3))
    assert max_same == 1
    assert max_total >= 2


@pytest.mark.asyncio
async def test_window_has_one_terminal_status_and_p0_priority() -> None:
    coordinator = ScanWindowCoordinator(p0_deadline_sec=5, p1_deadline_sec=5)
    count = coordinator.open(
        "w1", datetime.now(UTC), active=("BTCUSDT",), shortlist=("ETHUSDT",)
    )
    assert count == 2
    first = await coordinator.acquire()
    assert first is not None and first.priority is ScanPriority.P0
    assert coordinator.complete(first, TerminalStatus.SCANNED_OK)
    assert not coordinator.complete(first, TerminalStatus.SCAN_ERROR)
    second = await coordinator.acquire()
    assert second is not None and second.symbol == "ETHUSDT"
    assert coordinator.complete(second, TerminalStatus.INCOMPLETE_DATA)
    assert coordinator.complete_for("w1")

