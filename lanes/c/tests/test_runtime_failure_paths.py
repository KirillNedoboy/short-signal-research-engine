from __future__ import annotations

import asyncio

import pytest

from app.runtime.delivery import DeliveryWorker
from app.runtime.outcomes import OutcomeScheduler
from app.runtime.research import BoundedResearchQueue


@pytest.mark.asyncio
async def test_delivery_worker_is_woken_after_persistence_and_never_sends() -> None:
    persisted = []
    worker = DeliveryWorker(enqueue=lambda item: persisted.append(item), poll_sec=0.01)
    await worker.start()
    assert await worker.enqueue_after_persist({"signal_id": 7})
    await asyncio.sleep(0.03)
    await worker.stop()
    assert persisted == [{"signal_id": 7}]
    assert worker.stats().sent == 0


@pytest.mark.asyncio
async def test_research_queue_drops_when_bounded_and_outcome_is_separate() -> None:
    seen = []
    queue = BoundedResearchQueue(maxsize=1)
    assert queue.submit(lambda: seen.append(1))
    assert not queue.submit(lambda: seen.append(2))
    await queue.drain_once()
    assert seen == [1]
    scheduler = OutcomeScheduler(max_concurrency=1)
    assert scheduler.is_background
    assert scheduler.live_slots_reserved == 1


@pytest.mark.asyncio
async def test_missing_data_blocks_evaluation_without_delivery() -> None:
    delivered = []
    worker = DeliveryWorker(enqueue=delivered.append)
    result = await worker.persist_decision(
        {"symbol": "SOLUSDT", "actionable": True},
        data_quality="STALE",
    )
    assert result.actionable is False
    assert delivered == []

