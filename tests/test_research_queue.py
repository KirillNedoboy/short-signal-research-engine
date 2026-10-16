from __future__ import annotations

import asyncio

from app.runtime.research_queue import ResearchQueue


def test_overflow_drops_research_and_counts() -> None:
    queue = ResearchQueue(maxsize=2, batch_size=1)

    async def run() -> tuple[bool, bool, bool]:
        return (
            queue.submit("a", lambda: None),
            queue.submit("b", lambda: None),
            queue.submit("c", lambda: None),
        )

    accepted = asyncio.run(run())
    assert accepted == (True, True, False)
    stats = queue.stats()
    assert stats["dropped_total"] == 1
    assert stats["depth"] == 2


def test_consumer_drains_in_batches_and_survives_errors() -> None:
    written: list[str] = []
    errors: list[tuple[str, str]] = []
    queue = ResearchQueue(
        maxsize=10,
        batch_size=2,
        on_error=lambda exc, name: errors.append((name, type(exc).__name__)),
    )
    queue.submit("ok-1", lambda: written.append("ok-1"))
    queue.submit("boom", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    queue.submit("ok-2", lambda: written.append("ok-2"))
    queue.submit("ok-3", lambda: written.append("ok-3"))

    async def run() -> None:
        # A batch counts only successful writes; the error is contained.
        assert await queue.drain_once() == 2
        assert await queue.drain_once() == 1

    asyncio.run(run())
    assert written == ["ok-1", "ok-2", "ok-3"]
    assert errors == [("boom", "RuntimeError")]
    stats = queue.stats()
    assert stats["written_total"] == 3
    assert stats["errors_total"] == 1
    assert stats["depth"] == 0


def test_lifecycle_drains_accepted_payloads_on_stop() -> None:
    written: list[str] = []
    queue = ResearchQueue(maxsize=10, batch_size=10)

    async def run() -> None:
        queue.start()
        for index in range(5):
            queue.submit(f"j{index}", lambda i=index: written.append(f"j{i}"))
        await asyncio.sleep(0.05)
        await queue.stop()

    asyncio.run(run())
    assert sorted(written) == [f"j{i}" for i in range(5)]
    assert queue.stats()["dropped_total"] == 0


def test_submit_after_stop_drops_without_blocking() -> None:
    queue = ResearchQueue(maxsize=5, batch_size=5)

    async def run() -> bool:
        await queue.stop()
        return queue.submit("late", lambda: (_ for _ in ()).throw(AssertionError))

    assert asyncio.run(run()) is False
    assert queue.stats()["dropped_total"] == 1
