import asyncio
import logging
from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace

from app.main import ShortSignalBot
from app.market_data.repair_queue import RepairQueue, RecoveryBudget


def test_full_ranked_capacity_dedupe_fair_retry_and_overflow():
    queue = RepairQueue(capacity=150, cooldown=60)
    ranked = [f"S{i:03d}USDT" for i in range(150)]
    assert queue.offer(ranked, ranked, 0) == 150
    assert queue.offer(ranked, ranked, 1) == 0
    assert queue.overflow == 0
    assert queue.pop(0) == ranked[0]
    queue.finish(ranked[0], 0, ready=False)
    assert queue.pop(0) == ranked[1]
    queue.finish(ranked[1], 0, ready=False)
    assert queue.pop(0) == ranked[2]
    queue.finish(ranked[2], 0, ready=False)
    assert queue.offer(["EXTRAUSDT"], ranked + ["EXTRAUSDT"], 0) == 0
    assert queue.overflow == 1
    assert queue.pop(60) == ranked[3]  # failed A cannot preempt untried Z
    assert queue.queued == 149


def test_tie_fifo_rank_change_and_due_retry():
    q = RepairQueue(capacity=3, cooldown=60)
    q.offer(["Z", "A"], ["Z", "A"], 0)
    assert q.pop(0) == "Z"
    q.finish("Z", 0, ready=False)
    assert q.pop(0) == "A"
    q.finish("A", 0, ready=False)
    assert q.next_due(0) == 60
    assert q.pop(59) is None
    q.offer(["A", "Z"], ["A", "Z"], 59)
    assert q.pop(60) == "A"


def test_full_queue_replaces_lowest_priority_pending_for_new_top_ranked_demand():
    q = RepairQueue(capacity=3)
    q.offer(["OLD", "MID", "LOW"], ["OLD", "MID", "LOW"], 0)
    assert q.pop(0) == "OLD"
    assert q.offer(["TOP", "MID", "LOW"], ["TOP", "MID", "LOW"], 1) == 1
    assert q.pop(1) == "TOP"
    q.finish("TOP", 1, ready=True)
    q.finish("OLD", 1, ready=True)
    assert q.offer(["LOW"], ["LOW"], 2) == 1

def test_full_queue_rejects_lower_rank_without_eviction():
    q = RepairQueue(capacity=2)
    q.offer(["A", "B"], ["A", "B", "C"], 0)
    assert q.offer(["C"], ["A", "B", "C"], 1) == 0
    assert q.overflow == 1
    assert q.pop(1) == "A"

def test_recovered_without_rest_frees_capacity_for_new_ranked_symbol():
    q = RepairQueue(capacity=2)
    q.offer(["A", "B"], ["A", "B"], 0)
    q.reconcile(["B"])
    assert q.offer(["C"], ["B", "C"], 1) == 1
    assert q.pop(1) == "B"


def test_budget_requires_measurement_and_contracts_on_pressure():
    b = RecoveryBudget()
    assert b.limit == 8
    try:
        b.set_measured_limit(40, measured_spare_per_minute=0)
    except ValueError:
        pass
    else:
        assert False, "unmeasured capacity accepted"
    b.set_measured_limit(40, measured_spare_per_minute=40)
    assert b.limit == 40
    assert b.allowed(0, {"provider_request_queue_depth": {"LIVE_SCAN": 1}}) is False
    assert b.allowed(0, {"provider_request_queue_depth": {"LIVE_SCAN": 0, "FAST_MONITOR": 0}, "in_flight": {"LIVE_SCAN": 0, "FAST_MONITOR": 0}})
    for _ in range(40):
        b.record(0)
    assert not b.allowed(0, {})
    b.rate_limited(0)
    assert b.limit == 8
    assert b.next_due(0) >= 60


def test_bootstrap_budget_contracts_to_steady_limit_on_pressure():
    budget = RecoveryBudget(bootstrap_limit=16)
    assert budget.limit == 16
    for _ in range(16):
        budget.record(0)
    assert budget.allowed(0, {}) is False
    budget.rate_limited(0)
    assert budget.limit == 8


def test_mass_blocked_reconnect_queues_132_once_without_scan_wait():
    async def run():
        class Store:
            calls = []
            gate = asyncio.Event()
            def peek_snapshot(self, symbol, when):
                return SimpleNamespace(actionable=False, stale_reason="CONTINUITY_UNHEALTHY")
            async def backfill_missing(self, client, symbol, *, priority):
                self.calls.append(symbol)
                await self.gate.wait()
                return True
        store = Store()
        bot = ShortSignalBot.__new__(ShortSignalBot)
        bot._market_data_provider = SimpleNamespace(frame_store=store, client=object())
        bot._logger = logging.getLogger("ranked-repair")
        bot._request_scheduler = None
        bot._continuity_repair_task = None
        bot._continuity_repair_queue = RepairQueue()
        bot._continuity_repair_budget = RecoveryBudget()
        bot._continuity_repair_metrics = Counter()
        bot._continuity_repair_ready = set()
        symbols = [f"S{i:03d}USDT" for i in range(132)]
        bot._schedule_continuity_repair(symbols, ranked_symbols=symbols)
        bot._schedule_continuity_repair(symbols, ranked_symbols=symbols)
        await asyncio.sleep(0)
        assert bot._continuity_repair_queue.queued + len(store.calls) == 132
        assert store.calls == symbols[:1]
        store.gate.set()
        bot._continuity_repair_task.cancel()
        await asyncio.gather(bot._continuity_repair_task, return_exceptions=True)
    asyncio.run(run())
