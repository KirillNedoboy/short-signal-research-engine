"""Selection accounting through the live gate and coordinator path."""
import asyncio
from datetime import UTC, datetime, timedelta

from app.main import _actionable_canonical_symbols
from app.runtime.scan_coordinator import ScanCoordinator, ScanPriority


def test_top100_gate_counts_single_peek_and_preserves_p0_order():
    now = datetime.now(UTC)
    symbols = tuple(f"S{i:03}USDT" for i in range(100))
    calls = []

    class Frame:
        def __init__(self, reason):
            self.stale_reason = reason
            self.actionable = reason is None

    class Store:
        def peek_snapshot(self, symbol, decision_at):
            calls.append((symbol, decision_at))
            if symbol == symbols[0]:
                return None
            return Frame("STALE_CANDLE" if symbol == symbols[1] else None)

    counts = {}
    eligible = _actionable_canonical_symbols(Store(), symbols, now, diagnostics=counts)
    assert eligible == symbols[2:]
    assert calls == [(symbol, now) for symbol in symbols]
    assert counts == {"original_top100": 100, "ready": 98,
                      "reasons": {"NO_FRAME": 1, "STALE_CANDLE": 1}}

    async def check_order():
        coordinator = ScanCoordinator(p0_deadline_sec=15, p1_deadline_sec=30)
        scheduled = coordinator.open_window("w", now, active_symbols=(symbols[2],), shortlist_symbols=eligible)
        assert scheduled == 98
        first = await coordinator.acquire()
        assert first.symbol == symbols[2]
        assert first.priority is ScanPriority.P0
        assert counts["ready"] == len(eligible)
        assert sum(counts["reasons"].values()) + counts["ready"] == counts["original_top100"]

    asyncio.run(check_order())


def test_event_window_admission_retries_after_repair_before_opening():
    async def run():
        from types import SimpleNamespace
        from app.main import ShortSignalBot

        now = datetime.now(UTC)
        class Store:
            ready = False
            def peek_snapshot(self, symbol, decision_at):
                return SimpleNamespace(actionable=self.ready,
                                       stale_reason=None if self.ready else "MISSING_DECISION_CANDLE")

        store = Store()
        bot = ShortSignalBot.__new__(ShortSignalBot)
        calls = []
        def schedule(blocked, *, ranked_symbols=None):
            calls.append((tuple(blocked), tuple(ranked_symbols or ())))
            store.ready = True
        bot._schedule_continuity_repair = schedule
        coordinator = ScanCoordinator(p0_deadline_sec=15, p1_deadline_sec=30)
        result = await bot._admit_event_window(
            coordinator, "retry-window", now - timedelta(seconds=0.1),
            store=store, active_symbols=(), shortlist_symbols=("AAAUSDT",), interval_sec=2,
        )
        scheduled, diagnostics, blocked, active, shortlist = result
        assert scheduled == 1
        assert calls == [(("AAAUSDT",), ("AAAUSDT",))]
        assert shortlist == ("AAAUSDT",)
        assert blocked == ()
        assert diagnostics["ready"] == 1

    asyncio.run(run())


def test_duplicate_symbol_gate_accounting_matches_existing_deduplication():
    now = datetime.now(UTC)
    class Frame:
        actionable = True
    class Store:
        def __init__(self):
            self.calls = []
        def peek_snapshot(self, symbol, decision_at):
            self.calls.append(symbol)
            return Frame()
    store = Store()
    counts = {}
    assert _actionable_canonical_symbols(store, ("ausdt", "AUSDT"), now, diagnostics=counts) == ("AUSDT",)
    assert store.calls == ["AUSDT"]
    assert counts == {"original_top100": 2, "ready": 1, "reasons": {"DUPLICATE": 1}}
