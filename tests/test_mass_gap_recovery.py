"""Deterministic offline capacity replay; no provider, DB, or live scheduler calls."""
from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace

from app.main import _actionable_canonical_symbols
from app.market_data.repair_queue import RecoveryBudget, RepairQueue


class ReplayStore:
    def __init__(self, ranked, blocked):
        self.ranked = ranked
        self.blocked = set(blocked)
        self.epoch = 1
        self.ws_epoch = {symbol: 1 for symbol in ranked}
        self.current_ticker = set(ranked)
        self.calls = Counter()
        self.outcomes = {}

    def peek_snapshot(self, symbol, when):
        reason = None
        if symbol not in self.current_ticker:
            reason = "TICKER_STALE"
        elif self.ws_epoch[symbol] != self.epoch:
            reason = "WS_EPOCH_STALE"
        elif symbol in self.blocked:
            reason = "CONTINUITY_UNHEALTHY"
        return SimpleNamespace(actionable=reason is None, stale_reason=reason)

    def repair(self, symbol):
        self.calls[symbol] += 1
        outcome = self.outcomes.get(symbol, "ok")
        if outcome == "ok":
            self.blocked.discard(symbol)
        return outcome

    def reconnect(self, symbols):
        self.epoch += 1
        self.blocked.update(symbols)

    def ws_close(self, symbols):
        for symbol in symbols:
            self.ws_epoch[symbol] = self.epoch


def _window(store, queue, budget, ranked, start, stats, *, attempts=150):
    boundary = datetime(2026, 10, 11, tzinfo=UTC)
    blocked = []
    ready = _actionable_canonical_symbols(store, ranked, boundary, diagnostics={}, continuity_blocked=blocked)
    queue.reconcile(blocked)
    queue.offer(blocked, ranked, start)
    scheduled = set(ready)
    scanned_ok = set(ready)  # fake scanner succeeds only for admitted frames
    for _ in range(attempts):
        if not budget.allowed(start, stats):
            break
        symbol = queue.pop(start)
        if symbol is None:
            break
        budget.record(start)
        outcome = store.repair(symbol)
        if outcome == "429":
            budget.rate_limited(start)
        queue.finish(symbol, start, ready=store.peek_snapshot(symbol, boundary).actionable)
    return scheduled, scanned_ok


def test_132_original_gaps_low_headroom_remain_excluded_without_p0_waits():
    ranked = [f"S{i:03d}USDT" for i in range(150)]
    blocked = ranked[:132]
    store = ReplayStore(ranked, blocked)
    queue, budget = RepairQueue(), RecoveryBudget()
    stats = {"provider_request_queue_depth": {"LIVE_SCAN": 0, "FAST_MONITOR": 0}}
    first, first_scanned = _window(store, queue, budget, ranked, 0, stats)
    second, second_scanned = _window(store, queue, budget, ranked, 60, stats)
    assert len(first) == len(first_scanned) == 18
    assert len(second) == len(second_scanned) == 26
    assert queue.overflow == 0
    assert len(store.calls) == len(blocked[:16]) == 16
    assert all(count == 1 for count in store.calls.values())
    assert all(s not in first | second for s in blocked[16:])


def test_132_gaps_two_window_target_requires_explicit_measured_headroom():
    ranked = [f"S{i:03d}USDT" for i in range(150)]
    store = ReplayStore(ranked, ranked[:132])
    queue, budget = RepairQueue(), RecoveryBudget()
    budget.set_measured_limit(82, measured_spare_per_minute=82)
    stats = {"provider_request_queue_depth": {"LIVE_SCAN": 0, "FAST_MONITOR": 0}}
    first, _ = _window(store, queue, budget, ranked, 0, stats)
    second, scanned = _window(store, queue, budget, ranked, 60, stats)
    assert len(first) == 18  # first window is not retrospectively counted as recovered
    assert len(second) >= 100
    assert len(scanned) >= 100
    assert len(store.calls) == 132
    assert all(count == 1 for count in store.calls.values())
    assert queue.queued == queue.overflow == 0
    assert not budget.allowed(60, {"provider_request_queue_depth": {"LIVE_SCAN": 1}})


def test_consecutive_reconnect_429_and_rest_success_without_readmission():
    ranked = [f"S{i:03d}USDT" for i in range(150)]
    store = ReplayStore(ranked, ranked[:132])
    store.outcomes[ranked[0]] = "429"
    store.outcomes[ranked[1]] = "incomplete"
    queue, budget = RepairQueue(), RecoveryBudget()
    budget.set_measured_limit(82, measured_spare_per_minute=82)
    stats = {"provider_request_queue_depth": {"LIVE_SCAN": 0, "FAST_MONITOR": 0}}
    _window(store, queue, budget, ranked, 0, stats)
    assert sum(store.calls.values()) == 1
    assert not store.peek_snapshot(ranked[0], datetime.now(UTC)).actionable
    store.reconnect(ranked)  # REST alone does not satisfy the new WS epoch
    store.ws_close(ranked[2:])
    second, _ = _window(store, queue, budget, ranked, 60, stats)
    assert not second
    assert store.calls[ranked[1]] == 0  # epoch-stale frames cannot queue gap repairs
    assert len(store.calls) <= 9  # contracted eight-start budget after 429
    store.outcomes[ranked[0]] = "incomplete"  # provider cooldown has elapsed
    store.ws_close(ranked)
    _window(store, queue, budget, ranked, 120, stats)
    assert store.calls[ranked[1]] == 1
    assert not store.peek_snapshot(ranked[1], datetime.now(UTC)).actionable
    assert queue.overflow == 0
    assert budget.limit == 8
    store.reconnect(ranked)
    _window(store, queue, budget, ranked, 180, stats)
    assert not _actionable_canonical_symbols(store, ranked, datetime.now(UTC))
    store.ws_close(ranked)
    store.current_ticker.remove(ranked[2])
    assert ranked[2] not in _actionable_canonical_symbols(store, ranked, datetime.now(UTC))
