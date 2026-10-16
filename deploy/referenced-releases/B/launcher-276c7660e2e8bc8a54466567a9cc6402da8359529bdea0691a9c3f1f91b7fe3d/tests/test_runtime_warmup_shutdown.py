from __future__ import annotations

import asyncio
import signal

from app.runtime.shutdown_checkpoint import ShutdownCheckpointBarrier
from app.runtime.warmup import WarmupCoverage

_SIGUSR1 = getattr(signal, "SIGUSR1", signal.SIGTERM)


def test_warmup_reports_partial_ready_counts_before_ready_transition() -> None:
    coverage = WarmupCoverage()

    coverage.begin(("aaa", "BBB", "aaa"))
    assert coverage.snapshot() == {
        "state": "WARMING_UP",
        "total": 2,
        "ready": 0,
        "pending": 2,
        "failed": 0,
    }

    coverage.mark_ready("AAA")
    assert coverage.snapshot() == {
        "state": "WARMING_UP",
        "total": 2,
        "ready": 1,
        "pending": 1,
        "failed": 0,
    }

    coverage.mark_failed("bbb")
    assert coverage.snapshot() == {
        "state": "WARMING_UP",
        "total": 2,
        "ready": 1,
        "pending": 0,
        "failed": 1,
    }


def test_sigusr1_checkpoint_ack_binds_generation_pid_and_shutdown_barrier() -> None:
    async def run() -> None:
        checkpoint_started = asyncio.Event()
        release_checkpoint = asyncio.Event()
        calls: list[tuple[int, int]] = []
        sent: list[tuple[int, signal.Signals]] = []
        barrier: ShutdownCheckpointBarrier

        async def checkpoint(*, generation: int, pid: int) -> int:
            calls.append((generation, pid))
            checkpoint_started.set()
            await release_checkpoint.wait()
            return 3

        def send_signal(pid: int, signum: signal.Signals) -> None:
            sent.append((pid, signum))
            barrier.notify_signal()

        barrier = ShutdownCheckpointBarrier(
            checkpoint=checkpoint,
            pid_provider=lambda: 4321,
            signal_sender=send_signal,
        )
        request = asyncio.create_task(barrier.request(timeout_sec=1.0))
        await asyncio.wait_for(checkpoint_started.wait(), timeout=1.0)
        assert not request.done()
        assert sent == [(4321, _SIGUSR1)]
        assert calls == [(1, 4321)]

        release_checkpoint.set()
        ack = await request
        assert ack.acknowledged is True
        assert ack.generation == 1
        assert ack.pid == 4321
        assert ack.symbols == 3

    asyncio.run(run())


def test_warmup_never_marks_zero_ready_as_ready_and_uses_ninetyfive_threshold() -> None:
    coverage = WarmupCoverage()
    symbols = tuple(f"SYM{index:03d}USDT" for index in range(20))
    coverage.begin(symbols)
    for symbol in symbols[:18]:
        coverage.mark_ready(symbol)
    assert coverage.snapshot()["state"] == "WARMING_UP"
    coverage.mark_ready(symbols[18])
    assert coverage.snapshot()["state"] == "READY"
    assert coverage.snapshot()["ready"] == 19
    coverage.mark_failed(symbols[19])
    assert coverage.snapshot()["state"] == "READY"

    failed = WarmupCoverage()
    failed.begin(("ONLYUSDT",))
    failed.mark_failed("ONLYUSDT")
    assert failed.snapshot() == {
        "state": "WARMING_UP",
        "total": 1,
        "ready": 0,
        "pending": 0,
        "failed": 1,
    }
