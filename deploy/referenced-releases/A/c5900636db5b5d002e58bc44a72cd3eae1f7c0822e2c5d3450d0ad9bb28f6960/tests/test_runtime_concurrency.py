"""Deterministic full-scan/fast-monitor ownership witnesses."""

from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timedelta, timezone

from app.domain import EventState, EventStatus
from app.runtime.symbol_mutation import SymbolMutationCoordinator


class _MemoryStateStore:
    def __init__(self, state: EventState) -> None:
        self.current = state

    def load(self, symbol: str) -> EventState | None:
        return copy.deepcopy(self.current) if symbol == self.current.symbol else None

    def save(self, state: EventState) -> None:
        self.current = copy.deepcopy(state)


def test_full_scan_can_stale_overwrite_fast_monitor_state_without_symbol_serialization() -> None:
    """The original detached-snapshot interleaving is safe with one lane."""

    async def scenario() -> tuple[_MemoryStateStore, list[str]]:
        now = datetime.now(timezone.utc)
        store = _MemoryStateStore(EventState(
            symbol="RACEUSDT", event_id="race-1", state=EventStatus.PUMP_DETECTED,
            event_start_time=now, event_high_time=now, event_high=110.0,
            event_base_price=100.0, expires_at=now + timedelta(hours=1),
            event_features_snapshot={"revision": "initial"},
        ))
        coordinator = SymbolMutationCoordinator()
        full_read = asyncio.Event()
        release_full = asyncio.Event()
        fast_attempted = asyncio.Event()
        trace: list[str] = []

        async def full_scan() -> None:
            async with coordinator.mutation("RACEUSDT"):
                state = store.load("RACEUSDT")
                assert state is not None
                trace.append("full-scan-read-initial")
                full_read.set()
                await release_full.wait()
                state.event_features_snapshot = {"revision": "full-scan-stale"}
                store.save(state)
                trace.append("full-scan-writes-stale")

        async def fast_monitor() -> None:
            await full_read.wait()
            fast_attempted.set()
            async with coordinator.mutation("raceusdt"):
                state = store.load("RACEUSDT")
                assert state is not None
                assert state.event_features_snapshot == {"revision": "full-scan-stale"}
                state.event_features_snapshot = {"revision": "fast-monitor-fresh"}
                state.state = EventStatus.SIGNAL_SENT
                state.signal_id = 42
                state.signal_sent_at = datetime.now(timezone.utc)
                store.save(state)
                trace.append("fast-monitor-writes-fresh")

        full_task = asyncio.create_task(full_scan())
        await full_read.wait()
        fast_task = asyncio.create_task(fast_monitor())
        await fast_attempted.wait()
        assert not fast_task.done()
        release_full.set()
        await asyncio.gather(full_task, fast_task)
        return store, trace

    store, trace = asyncio.run(scenario())
    assert trace == ["full-scan-read-initial", "full-scan-writes-stale", "fast-monitor-writes-fresh"]
    assert store.current.event_features_snapshot == {"revision": "fast-monitor-fresh"}
    assert store.current.signal_id == 42
    assert store.current.state is EventStatus.SIGNAL_SENT
