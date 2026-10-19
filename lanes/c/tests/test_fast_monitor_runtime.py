from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

from app.runtime.fast_monitor import BoundedFastMonitor, CandidatePool


def _at(minutes: int = 0) -> datetime:
    return datetime(2026, 10, 19, 12, minutes, tzinfo=UTC)


def test_candidate_ttl_is_anchored_to_event_high_and_replacement_is_persistent() -> None:
    events: list[dict[str, object]] = []
    pool = CandidatePool(
        ttl_minutes=30,
        event_sink=events.append,
        root_event_tracking_enabled=True,
        root_event_preparer=lambda state, observed_at: 7,
    )

    state = {
        "symbol": "BTCUSDT",
        "event_id": "event-1",
        "event_high_time": _at(),
        "event_features_snapshot": {},
    }
    first = pool.track(state, now=_at(1))
    assert first is not None
    assert first.candidate_added_at == _at()

    # A repeated full scan must not move the TTL anchor or add a duplicate.
    assert pool.track(state, now=_at(10)) is first
    assert len(pool) == 1
    assert pool.expire(now=_at(29)) == ()
    assert pool.expire(now=_at(30))[0] is first
    assert len(pool) == 0

    replacement = dict(state, event_id="event-2", event_high_time=_at(32))
    assert pool.track(replacement, now=_at(32)) is not None
    assert pool.track(dict(replacement, event_id="event-3"), now=_at(33)) is not None
    assert pool.get("BTCUSDT", "event-2") is None
    assert pool.get("BTCUSDT", "event-3") is not None
    assert any(
        item["action"] == "candidate_removed"
        and item["reason"] == "replaced_by_new_event"
        for item in events
    )


def test_candidate_selection_is_fair_and_bounded_to_ten() -> None:
    pool = CandidatePool(ttl_minutes=30, max_active_symbols=10)
    for index in range(12):
        pool.track(
            {
                "symbol": f"S{index:02d}USDT",
                "event_id": f"event-{index}",
                "event_high_time": _at(),
                "event_features_snapshot": {},
            },
            now=_at(1),
        )

    first = pool.select(now=_at(2))
    second = pool.select(now=_at(3))
    assert len(first) == len(second) == 10
    assert [candidate.symbol for candidate in first] == [
        f"S{index:02d}USDT" for index in range(10)
    ]
    assert [candidate.symbol for candidate in second] == [
        "S10USDT",
        "S11USDT",
        *[f"S{index:02d}USDT" for index in range(8)],
    ]


@pytest.mark.asyncio
async def test_monitor_polls_only_selected_candidates_and_rejects_old_asof() -> None:
    pool = CandidatePool(ttl_minutes=30, max_active_symbols=10)
    for symbol in ("BTCUSDT", "ETHUSDT"):
        pool.track(
            {
                "symbol": symbol,
                "event_id": f"{symbol}-event",
                "event_high_time": _at(),
                "event_features_snapshot": {},
                "generation": 4,
            },
            now=_at(1),
        )

    frames_calls: list[tuple[str, ...]] = []
    results: list[str] = []

    async def frames(symbols: tuple[str, ...]) -> dict[str, dict[str, object]]:
        frames_calls.append(symbols)
        return {
            symbol: {"asof": _at(2), "generation": 4, "frame": symbol}
            for symbol in symbols
        }

    async def evaluate(candidate, snapshot, poll_sequence):
        return {"symbol": candidate.symbol, "snapshot": snapshot, "poll": poll_sequence}

    async def on_result(candidate, snapshot, result, poll_sequence):
        results.append(candidate.symbol)

    @asynccontextmanager
    async def mutation(symbol: str):
        yield

    # ETH has a newer main-state watermark.  Its older fast snapshot must be
    # skipped while BTC is evaluated through the same lock seam.
    async def current_version(symbol: str):
        return {"asof": _at(3), "generation": 4} if symbol == "ETHUSDT" else None

    monitor = BoundedFastMonitor(
        pool,
        frames_callback=frames,
        evaluate_callback=evaluate,
        on_result=on_result,
        mutation_lock=mutation,
        state_version_callback=current_version,
        poll_interval_sec=20,
    )
    report = await monitor.poll_once(now=_at(4))

    assert frames_calls == [("BTCUSDT", "ETHUSDT")]
    assert results == ["BTCUSDT"]
    assert report.processed == ("BTCUSDT",)
    assert report.skipped["ETHUSDT"] == "stale_snapshot"
    assert monitor.poll_interval_sec == 20
