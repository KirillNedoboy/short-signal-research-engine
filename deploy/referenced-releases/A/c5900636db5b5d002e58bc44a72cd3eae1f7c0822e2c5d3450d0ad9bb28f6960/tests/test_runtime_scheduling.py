from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app import main as runtime_main
from app.runtime.scheduling import next_scan_deadline


def test_next_deadline_keeps_original_tick_after_short_cycle() -> None:
    decision = next_scan_deadline(
        scheduled_deadline=0.0, finished_at=45.0, interval_sec=60.0
    )

    assert decision.next_deadline == 60.0
    assert decision.cycle_overrun_ms == 0.0
    assert decision.skipped_ticks == 0


def test_next_deadline_skips_expired_ticks_after_overrun() -> None:
    decision = next_scan_deadline(
        scheduled_deadline=0.0, finished_at=161.0, interval_sec=60.0
    )

    assert decision.next_deadline == 180.0
    assert decision.cycle_overrun_ms == 101_000.0
    assert decision.skipped_ticks == 2


def test_run_forever_never_overlaps_or_replays_after_cancellation(monkeypatch) -> None:
    class Clock:
        now = 0.0

        def time(self) -> float:
            return self.now

    class Bot:
        def __init__(self) -> None:
            self._config = SimpleNamespace(scan_interval_sec=60)
            self._logger = SimpleNamespace(warning=lambda *_args, **_kwargs: None)
            self.starts: list[float] = []
            self.running = 0
            self.max_running = 0
            self.finalized: dict[str, object] = {}

        async def run_cycle(self, *, timing: object = None) -> None:
            self.starts.append(clock.now)
            self.running += 1
            self.max_running = max(self.max_running, self.running)
            if len(self.starts) == 1:
                clock.now += 45.0
                self.running -= 1
                return
            self.running -= 1
            raise asyncio.CancelledError

        def _finalize_cycle_timing(
            self, timing: object, *, cycle_overrun_ms: float, skipped_ticks: int
        ) -> None:
            self.finalized = {
                "cycle_overrun_ms": cycle_overrun_ms,
                "skipped_ticks": skipped_ticks,
            }

    clock = Clock()

    async def sleep(delay: float) -> None:
        clock.now += delay

    bot = Bot()
    monkeypatch.setattr(runtime_main.asyncio, "get_running_loop", lambda: clock)
    monkeypatch.setattr(runtime_main.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime_main.ShortSignalBot.run_forever(bot))

    assert bot.starts == [0.0, 60.0]
    assert bot.max_running == 1
    # The 45-second cycle finalized with zero overrun before the anchored 60s tick.
    assert bot.finalized == {"cycle_overrun_ms": 0.0, "skipped_ticks": 0}
