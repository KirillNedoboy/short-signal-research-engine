"""Focused tests for the C background worker boundaries."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.runtime.delivery_worker import DeliveryWorker
from app.runtime.outcome_worker import OutcomeWorker
from app.runtime.research_queue import ResearchQueue
from app.observability.outcome_scheduler import ShadowOutcomeScheduler


class _OutboxRepository:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.claims: list[dict[str, object]] = []
        self.sent: list[int] = []
        self.retries: list[tuple[int, str]] = []

    def claim_due_deliveries(self, now, *, limit, lease_seconds, entity_type):
        self.claims.append(
            {
                "now": now,
                "limit": limit,
                "lease_seconds": lease_seconds,
                "entity_type": entity_type,
            }
        )
        return list(self.rows) if entity_type == "SIGNAL" else []

    def mark_delivery_sent(self, outbox_id: int) -> None:
        self.sent.append(outbox_id)

    def mark_delivery_retry(self, outbox_id: int, *, error: str, next_attempt_at) -> None:
        self.retries.append((outbox_id, error))


class _Notifier:
    def __init__(self, result: bool = True, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.payloads: list[str] = []

    async def send_signal(self, payload: str) -> bool:
        self.payloads.append(payload)
        if self.error is not None:
            raise self.error
        return self.result


class _Throttle:
    def should_send(self, _key: str) -> bool:
        return True


@pytest.mark.asyncio
async def test_delivery_claims_lease_sends_then_marks_sent() -> None:
    repository = _OutboxRepository(
        [{"id": 7, "payload": "persisted-payload", "created_at": datetime.now(UTC)}]
    )
    notifier = _Notifier()
    worker = DeliveryWorker(
        repository,
        notifier,
        _Throttle(),
        send_watch_to_telegram=False,
        batch_size=3,
        lease_seconds=41,
        shadow_mode=False,
    )

    assert await worker.drain_once() == 1
    assert repository.sent == [7]
    assert repository.retries == []
    assert notifier.payloads == ["persisted-payload"]
    assert repository.claims[0]["limit"] == 3
    assert repository.claims[0]["lease_seconds"] == 41


@pytest.mark.asyncio
async def test_delivery_failure_marks_retry_and_never_marks_sent() -> None:
    repository = _OutboxRepository([{"id": 8, "payload": "retry-me"}])
    worker = DeliveryWorker(
        repository,
        _Notifier(error=RuntimeError("transport down")),
        _Throttle(),
        send_watch_to_telegram=False,
        shadow_mode=False,
    )

    assert await worker.drain_once() == 0
    assert repository.sent == []
    assert repository.retries and repository.retries[0][0] == 8
    assert "RuntimeError" in repository.retries[0][1]


@pytest.mark.asyncio
async def test_shadow_delivery_is_closed_before_claim_and_needs_no_sender() -> None:
    class _ForbiddenRepository:
        def claim_due_deliveries(self, *args, **kwargs):
            raise AssertionError("shadow worker claimed an outbox")

    worker = DeliveryWorker(
        _ForbiddenRepository(),
        None,
        None,
        send_watch_to_telegram=True,
        shadow_mode=True,
        delivery_gate="CLOSED",
    )

    assert await worker.drain_once() == 0
    assert worker.stats()["enabled"] is False


@pytest.mark.asyncio
async def test_outcome_worker_passes_bounded_limits_and_keeps_one_iteration() -> None:
    calls: list[tuple[int, int]] = []

    class _Tracker:
        async def update_due_outcomes(self, *, now, signal_limit, observation_limit):
            calls.append((signal_limit, observation_limit))
            return 2

    class _Scheduler:
        async def run_cycle(self, *, now):
            return SimpleNamespace(
                remaining_due=3,
                runtime_processed=1,
                legacy_processed=0,
                v2_processed=0,
                live_root_processed=0,
                retries=0,
                errors=0,
                oldest_overdue_seconds=4,
            )

    worker = OutcomeWorker(
        shadow_scheduler=_Scheduler(),
        outcome_tracker=_Tracker(),
        signal_batch_size=4,
        observation_batch_size=2,
        idle_poll_sec=1,
    )
    await worker._process_iteration()

    assert calls == [(4, 2)]
    assert worker.stats()["processed_total"] == 3
    assert worker.stats()["due_total"] == 3


@pytest.mark.asyncio
async def test_shadow_scheduler_moves_slow_repository_calls_off_event_loop() -> None:
    class _SlowRepository:
        def list_shadow_episode_outcomes_due(self, *, now, runtime_limit, legacy_limit):
            time.sleep(0.04)
            return {"runtime": [], "legacy": []}

    class _Client:
        async def fetch_klines(self, *args, **kwargs):
            return []

    scheduler = ShadowOutcomeScheduler(
        client=_Client(),
        repository=_SlowRepository(),
        runtime_batch_size=2,
        legacy_batch_size=1,
    )
    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        for _ in range(4):
            await asyncio.sleep(0.005)
            ticks += 1

    stats, _ = await asyncio.gather(scheduler.run_cycle(), heartbeat())
    assert stats.runtime_due_seen == 0
    assert ticks == 4


@pytest.mark.asyncio
async def test_research_queue_is_bounded_and_shutdown_drops_unfinished_jobs() -> None:
    seen: list[int] = []
    queue = ResearchQueue(maxsize=1, batch_size=1)
    assert queue.submit("first", lambda: seen.append(1))
    assert not queue.submit("second", lambda: seen.append(2))
    assert await queue.drain_once() == 1
    assert seen == [1]
    assert queue.stats()["dropped_total"] == 1

    blocker = asyncio.Event()

    async def slow_write() -> None:
        await blocker.wait()

    shutdown = ResearchQueue(
        maxsize=3,
        batch_size=1,
        shutdown_timeout_sec=0.02,
        shutdown_max_batches=1,
    )
    assert shutdown.submit("slow", slow_write)
    assert shutdown.submit("pending", lambda: seen.append(3))
    shutdown.start()
    await asyncio.sleep(0)
    await shutdown.stop()
    assert shutdown.stats()["dropped_total"] >= 1

