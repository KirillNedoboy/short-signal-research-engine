"""Planned-rotation gaps must be repaired outside the scan admission path."""
import asyncio
import logging
import pytest
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from app.infra.request_scheduler import RequestPriority
from app.main import ShortSignalBot, _actionable_canonical_symbols
from app.market_data.continuity import GapTracker
from app.market_data.models import ConnectionEpoch, MarketCandle
from app.market_data.observers import ConnectionOpened
from app.market_data.store import CanonicalMarketStore
from app.market_data.repair_queue import RepairQueue, RecoveryBudget


class _Store:
    def __init__(self):
        self.blocked = True
        self.calls = []
        self.release = asyncio.Event()

    def peek_snapshot(self, symbol, decision_at):
        return SimpleNamespace(actionable=not self.blocked,
                               stale_reason="CONTINUITY_UNHEALTHY" if self.blocked else None)

    async def backfill_missing(self, client, symbol, *, priority, deadline_monotonic=None):
        self.calls.append((symbol, priority))
        await self.release.wait()
        self.blocked = False
        return True


def _bot(store, client=None):
    bot = ShortSignalBot.__new__(ShortSignalBot)
    bot._market_data_provider = SimpleNamespace(frame_store=store, client=client or object())
    bot._logger = logging.getLogger("continuity-repair-test")
    bot._continuity_repair_queue = RepairQueue()
    bot._continuity_repair_budget = RecoveryBudget()
    bot._continuity_repair_metrics = Counter()
    bot._continuity_repair_ready = set()
    bot._request_scheduler = None
    bot._continuity_repair_task = None
    bot._continuity_repair_inflight = None
    bot._continuity_repair_last_attempt = {}
    bot._continuity_repair_request_times = []
    return bot


@pytest.mark.parametrize("reason", ["MISSING_DECISION_CANDLE", "CANDLE_STALE"])
def test_missing_decision_candle_enters_bounded_repair_queue(reason: str) -> None:
    class MissingMinuteStore(_Store):
        def peek_snapshot(self, symbol, decision_at):
            return SimpleNamespace(
                actionable=not self.blocked,
                stale_reason=reason if self.blocked else None,
            )

    async def run():
        store = MissingMinuteStore()
        bot = _bot(store)
        blocked = []
        now = datetime(2026, 10, 11, 13, 53, tzinfo=UTC)
        assert _actionable_canonical_symbols(
            store, ("AAAUSDT",), now, diagnostics={}, continuity_blocked=blocked
        ) == ()
        assert blocked == ["AAAUSDT"]
        bot._schedule_continuity_repair(blocked)
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=1)
        assert store.calls == [("AAAUSDT", RequestPriority.RECOVERY)]
        store.release.set()
        await asyncio.wait_for(bot._continuity_repair_task, timeout=1)
        assert _actionable_canonical_symbols(store, ("AAAUSDT",), now) == ("AAAUSDT",)

    asyncio.run(run())


def test_excluded_full_ring_gets_async_bounded_repair_then_readmission():
    async def run():
        store = _Store()
        bot = _bot(store)
        diagnostics = {}
        blocked = []
        now = datetime(2026, 10, 11, 13, 53, tzinfo=UTC)
        assert _actionable_canonical_symbols(store, ("AAAUSDT",), now, diagnostics=diagnostics, continuity_blocked=blocked) == ()
        assert diagnostics["reasons"] == {"CONTINUITY_UNHEALTHY": 1}
        assert blocked == ["AAAUSDT"]
        bot._schedule_continuity_repair(blocked)
        bot._schedule_continuity_repair(blocked)
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=1)
        assert store.calls == [("AAAUSDT", RequestPriority.RECOVERY)]
        assert _actionable_canonical_symbols(store, ("AAAUSDT",), now) == ()
        store.release.set()
        await asyncio.wait_for(bot._continuity_repair_task, timeout=1)
        assert _actionable_canonical_symbols(store, ("AAAUSDT",), now) == ("AAAUSDT",)
    asyncio.run(run())


def test_repair_failure_stays_excluded_and_does_not_block_scan():
    async def run():
        store = _Store()
        bot = _bot(store)
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=1)
        assert store.calls == [("AAAUSDT", RequestPriority.RECOVERY)]
        assert _actionable_canonical_symbols(store, ("AAAUSDT",), datetime.now(UTC)) == ()
        bot._continuity_repair_task.cancel()
        await asyncio.gather(bot._continuity_repair_task, return_exceptions=True)
    asyncio.run(run())


def test_real_tracker_gap_repairs_from_rest_without_dropping_continuity_gate():
    async def run():
        base = datetime(2026, 10, 11, 13, 46, tzinfo=UTC)
        now = base + timedelta(minutes=7)
        tracker = GapTracker(max_candles_per_symbol=7)
        store = CanonicalMarketStore(ring_size=5, continuity=tracker, clock=lambda: now)
        store.update_symbols(["AAAUSDT"])
        for minute in (1, 2, 4, 5, 6):
            opened = base + timedelta(minutes=minute)
            candle = MarketCandle(
                symbol="AAAUSDT", open_time=opened,
                close_time=opened + timedelta(milliseconds=59999), interval="1",
                open=Decimal(100), high=Decimal(101), low=Decimal(99),
                close=Decimal(100), volume=Decimal(1), turnover=Decimal(100),
                confirmed=True, exchange_timestamp=opened, received_at=now,
                connection_epoch=ConnectionEpoch(0, 1), source="BYBIT_PUBLIC_WS",
            )
            store.observe_closed_candle(candle)
            tracker.observe_closed_candle(candle)
        assert tracker.has_gap_since("AAAUSDT", base + timedelta(minutes=1))
        assert store.peek_snapshot("AAAUSDT", now).stale_reason == "CONTINUITY_UNHEALTHY"

        class Client:
            calls = []
            async def fetch_klines(self, symbol, interval, **kwargs):
                self.calls.append((symbol, kwargs))
                return [[str(int((base + timedelta(minutes=minute)).timestamp() * 1000)),
                         "100", "101", "99", "100", "1", "100"]
                        for minute in (6, 5, 4, 3, 2, 1)]

        client = Client()
        bot = _bot(store, client)
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert len(client.calls) == 1
        assert client.calls[0][1]["priority"] is RequestPriority.RECOVERY
        assert tracker.active_gaps == ()
        assert store.peek_snapshot("AAAUSDT", now).stale_reason != "CONTINUITY_UNHEALTHY"
        assert bot._continuity_repair_metrics["rest_ok"] == 1
        assert bot._continuity_repair_metrics["gap_closed"] == 1
    asyncio.run(run())


def test_rest_completed_without_gap_closure_is_not_readmitted():
    async def run():
        class Incomplete:
            async def backfill_missing(self, client, symbol, *, priority):
                return True
            def peek_snapshot(self, symbol, when):
                return SimpleNamespace(actionable=False, stale_reason="CONTINUITY_UNHEALTHY")
        bot = _bot(Incomplete())
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert bot._continuity_repair_metrics["rest_ok"] == 1
        assert bot._continuity_repair_metrics["gap_closed"] == 0
        assert bot._continuity_repair_metrics["frame_ready"] == 0
        assert bot._continuity_repair_metrics["not_readmitted"] == 1
        assert bot._continuity_repair_queue.queued == 1
    asyncio.run(run())


def test_snapshot_failure_before_repair_does_not_strand_inflight():
    async def run():
        class NoFrame:
            def peek_snapshot(self, symbol, when):
                raise RuntimeError("snapshot unavailable")
            async def backfill_missing(self, client, symbol, *, priority):
                return True
        bot = _bot(NoFrame())
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert bot._continuity_repair_queue.queued == 1
        assert bot._continuity_repair_inflight is None
        assert bot._continuity_repair_metrics["frame_ready"] == 0
        assert not bot._continuity_repair_ready
    asyncio.run(run())

def test_late_rate_limit_blocks_next_dequeue():
    async def run():
        class Scheduler:
            rate = 0
            def stats_snapshot(self):
                return {"metrics": {"bybit_rate_limit_total:RECOVERY": self.rate}}
        class Store:
            calls = []
            async def backfill_missing(self, client, symbol, *, priority):
                self.calls.append(symbol)
                if symbol == "A":
                    async def late():
                        await asyncio.sleep(0.02)
                        scheduler.rate += 1
                    asyncio.create_task(late())
        scheduler = Scheduler()
        bot = _bot(Store())
        bot._request_scheduler = scheduler
        bot._continuity_repair_budget.set_measured_limit(20, measured_spare_per_minute=20)
        bot._schedule_continuity_repair(["A", "B"])
        await asyncio.sleep(0.35)
        assert bot._continuity_repair_metrics["rate_limited"] == 1
        assert bot._continuity_repair_budget.limit == 8
        assert bot._continuity_repair_metrics["attempted"] == 1
        bot._continuity_repair_task.cancel()
        await asyncio.gather(bot._continuity_repair_task, return_exceptions=True)
    asyncio.run(run())

def test_readmission_requires_matching_repair_epoch_and_terminal_scan():
    class Store:
        frame = SimpleNamespace(actionable=True, connection_epoch="new")
        def peek_snapshot(self, symbol, when):
            return self.frame
    class Coordinator:
        def terminal_statuses(self, window):
            return {"A": "SCANNED_OK"} if window == "completed" else {}
    store = Store()
    bot = _bot(store)
    bot._continuity_repair_ready.add("A")
    bot._continuity_repair_epochs = {"A": "old"}
    assert bot._record_repair_readmissions(Coordinator(), "completed", ("A",)) == 0
    assert bot._continuity_repair_metrics["readmitted"] == 0
    bot._continuity_repair_epochs["A"] = "new"
    assert bot._record_repair_readmissions(Coordinator(), "pending", ("A",)) == 0
    assert bot._record_repair_readmissions(Coordinator(), "completed", ("A",)) == 1
    assert bot._record_repair_readmissions(Coordinator(), "completed", ("A",)) == 0

def test_repair_uses_public_snapshot_clock_not_private_store_now():
    async def run():
        when = datetime(2026, 10, 11, 13, 53, tzinfo=UTC)
        class TimedStore:
            def _now(self):
                raise AssertionError("private clock accessed")
            def snapshot_time(self):
                return when
            def peek_snapshot(self, symbol, decision_at):
                assert decision_at == when
                return SimpleNamespace(actionable=False, stale_reason="CONTINUITY_UNHEALTHY")
            async def backfill_missing(self, client, symbol, *, priority):
                return True
        bot = _bot(TimedStore())
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert bot._continuity_repair_metrics["not_readmitted"] == 1
    asyncio.run(run())


def test_repair_recheck_uses_provider_causal_minute_gate():
    async def run():
        when = datetime(2026, 10, 11, 13, 53, tzinfo=UTC)

        class Store:
            def peek_snapshot(self, symbol, decision_at):
                return SimpleNamespace(actionable=False, stale_reason="CONTINUITY_UNHEALTHY")

            async def backfill_missing(self, client, symbol, *, priority):
                return True

        class Provider:
            frame_store = Store()
            client = object()

            def recheck_after_backfill(self, symbol, *, backfill_started_at):
                assert backfill_started_at == when
                return SimpleNamespace(
                    actionable=True, stale_reason=None, connection_epoch="epoch"
                )

        provider = Provider()
        bot = _bot(provider.frame_store)
        bot._market_data_provider = provider
        bot._repair_snapshot_time = lambda store: when
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert bot._continuity_repair_metrics["frame_ready"] == 1
        assert "AAAUSDT" in bot._continuity_repair_ready

    asyncio.run(run())


def test_rate_limit_metric_contracts_measured_grant_even_if_store_swallows_error():
    async def run():
        class Scheduler:
            rate = 0
            def stats_snapshot(self):
                return {"metrics": {"bybit_rate_limit_total:RECOVERY": self.rate},
                        "provider_request_queue_depth": {"LIVE_SCAN": 0, "FAST_MONITOR": 0}}
        class Store:
            async def backfill_missing(self, client, symbol, *, priority):
                scheduler.rate += 1
                return False
        scheduler = Scheduler()
        bot = _bot(Store())
        bot._request_scheduler = scheduler
        bot._continuity_repair_budget.set_measured_limit(20, measured_spare_per_minute=20)
        bot._schedule_continuity_repair(["AAAUSDT", "ZZZUSDT"])
        await asyncio.sleep(0.3)
        assert bot._continuity_repair_budget.limit == 8
        assert bot._continuity_repair_metrics["rate_limited"] == 1
        assert bot._continuity_repair_metrics["attempted"] == 1
        bot._continuity_repair_task.cancel()
        await asyncio.gather(bot._continuity_repair_task, return_exceptions=True)
    asyncio.run(run())


def test_genuine_reconnect_rest_cannot_readmit_without_current_epoch_ws():
    async def run():
        now = datetime(2026, 10, 11, 13, 53, tzinfo=UTC)
        store = CanonicalMarketStore(ring_size=5, clock=lambda: now)
        store.update_symbols(["AAAUSDT"])
        store.observe_connection(ConnectionOpened(
            ConnectionEpoch(0, 2), ("kline.1.AAAUSDT",), now,
        ))

        class Client:
            async def fetch_klines(self, symbol, interval, **kwargs):
                return [[str(int((now - timedelta(minutes=minute)).timestamp() * 1000)),
                         "100", "101", "99", "100", "1", "100"]
                        for minute in range(1, 6)]

        bot = _bot(store, Client())
        bot._schedule_continuity_repair(["AAAUSDT"])
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert bot._continuity_repair_metrics["rest_ok"] == 1
        assert bot._continuity_repair_metrics["frame_ready"] == 0
        assert store.peek_snapshot("AAAUSDT", now).actionable is False
        assert bot._continuity_repair_queue.queued == 1
    asyncio.run(run())


def test_failed_first_symbol_does_not_starve_later_symbol():
    async def run():
        class FailingStore:
            def __init__(self):
                self.calls = []

            async def backfill_missing(self, client, symbol, *, priority):
                self.calls.append(symbol)
                return False

        store = FailingStore()
        bot = _bot(store)
        bot._schedule_continuity_repair(['AAAUSDT', 'ZZZUSDT'])

        async def requeue():
            for _ in range(10):
                await asyncio.sleep(0.03)
                bot._schedule_continuity_repair(['AAAUSDT'])

        await asyncio.create_task(requeue())
        await asyncio.wait_for(bot._continuity_repair_task, timeout=2)
        assert store.calls[:2] == ['AAAUSDT', 'ZZZUSDT']
        assert store.calls.count('AAAUSDT') == 1
    asyncio.run(run())


def test_recovery_request_budget_is_eight_per_rolling_minute():
    async def run():
        class Store:
            def __init__(self):
                self.calls = []

            async def backfill_missing(self, client, symbol, *, priority):
                self.calls.append(symbol)
                return False

        store = Store()
        bot = _bot(store)
        bot._schedule_continuity_repair([f"S{i:02d}USDT" for i in range(10)])
        await asyncio.sleep(2.2)
        assert len(store.calls) == 8
        assert bot._continuity_repair_queue.queued == 10
        bot._continuity_repair_task.cancel()
        await asyncio.gather(bot._continuity_repair_task, return_exceptions=True)

    asyncio.run(run())
