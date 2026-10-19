from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.main import successful_evaluation_count
from app.runtime.contracts import MarketSnapshot, SnapshotQuality, SnapshotStatus
from app.runtime.coordinator import PerSymbolCoordinator
from app.runtime.scan import ScanPriority, ScanWindowCoordinator, TerminalStatus
from app.runtime.scheduling import next_scan_deadline


def test_source_pin_and_release_boundary() -> None:
    launcher = Path(__file__).parents[1] / "run_live.py"
    text = launcher.read_text(encoding="utf-8")
    assert "SOURCE_ROOT" in text
    assert '"src"' in text or "'src'" in text
    assert "release/sha256-aa4f" not in text


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

