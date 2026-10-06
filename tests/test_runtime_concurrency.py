"""Deterministic full-scan/fast-monitor ownership witnesses."""

from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pandas as pd
import pytest

from app.config import AppConfig
from app.domain import EventState, EventStatus
from app.market.scanner import MarketScanner
from app.market_data.provider import CanonicalMarketDataProvider
from app.runtime.symbol_mutation import SymbolMutationCoordinator


class _MemoryStateStore:
    def __init__(self, state: EventState) -> None:
        self.current = state

    def load(self, symbol: str) -> EventState | None:
        return copy.deepcopy(self.current) if symbol == self.current.symbol else None

    def save(self, state: EventState) -> None:
        self.current = copy.deepcopy(state)


def test_full_scan_can_stale_overwrite_fast_monitor_state_without_symbol_serialization() -> None:
    """The original detached-snapshot interleaving is safe with one lane."""

    async def scenario() -> tuple[_MemoryStateStore, list[str]]:
        now = datetime.now(timezone.utc)
        store = _MemoryStateStore(EventState(
            symbol="RACEUSDT", event_id="race-1", state=EventStatus.PUMP_DETECTED,
            event_start_time=now, event_high_time=now, event_high=110.0,
            event_base_price=100.0, expires_at=now + timedelta(hours=1),
            event_features_snapshot={"revision": "initial"},
        ))
        coordinator = SymbolMutationCoordinator()
        full_read = asyncio.Event()
        release_full = asyncio.Event()
        fast_attempted = asyncio.Event()
        trace: list[str] = []

        async def full_scan() -> None:
            async with coordinator.mutation("RACEUSDT"):
                state = store.load("RACEUSDT")
                assert state is not None
                trace.append("full-scan-read-initial")
                full_read.set()
                await release_full.wait()
                state.event_features_snapshot = {"revision": "full-scan-stale"}
                store.save(state)
                trace.append("full-scan-writes-stale")

        async def fast_monitor() -> None:
            await full_read.wait()
            fast_attempted.set()
            async with coordinator.mutation("raceusdt"):
                state = store.load("RACEUSDT")
                assert state is not None
                assert state.event_features_snapshot == {"revision": "full-scan-stale"}
                state.event_features_snapshot = {"revision": "fast-monitor-fresh"}
                state.state = EventStatus.SIGNAL_SENT
                state.signal_id = 42
                state.signal_sent_at = datetime.now(timezone.utc)
                store.save(state)
                trace.append("fast-monitor-writes-fresh")

        full_task = asyncio.create_task(full_scan())
        await full_read.wait()
        fast_task = asyncio.create_task(fast_monitor())
        await fast_attempted.wait()
        assert not fast_task.done()
        release_full.set()
        await asyncio.gather(full_task, fast_task)
        return store, trace

    store, trace = asyncio.run(scenario())
    assert trace == ["full-scan-read-initial", "full-scan-writes-stale", "fast-monitor-writes-fresh"]
    assert store.current.event_features_snapshot == {"revision": "fast-monitor-fresh"}
    assert store.current.signal_id == 42
    assert store.current.state is EventStatus.SIGNAL_SENT


ASOF = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _valid_kline() -> list[list[str]]:
    start_ms = int((ASOF - timedelta(minutes=2)).timestamp() * 1000)
    return [[str(start_ms), "1", "2", "0.5", "1.5", "10", "15"]]


class _FaultInjectingClient:
    def __init__(self, outcomes: dict[str, object]) -> None:
        self.outcomes = outcomes
        self.started: list[str] = []

    def extract_market_time(self) -> datetime:
        return ASOF

    async def fetch_klines(self, symbol: str, *_args, **_kwargs):
        self.started.append(symbol)
        outcome = self.outcomes[symbol]
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome == "slow":
            await asyncio.sleep(0.03)
            return _valid_kline()
        return outcome


class _DeadlineBoundClient:
    def __init__(self) -> None:
        self.started: list[str] = []
        self.slow_started = asyncio.Event()
        self.slow_cancelled = asyncio.Event()
        self.release_slow = asyncio.Event()

    def extract_market_time(self) -> datetime:
        return ASOF

    async def fetch_klines(self, symbol: str, *_args, **_kwargs):
        self.started.append(symbol)
        if symbol == "SLOWUSDT":
            self.slow_started.set()
            try:
                await asyncio.wait_for(self.release_slow.wait(), timeout=0.01)
            except asyncio.TimeoutError as exc:
                self.slow_cancelled.set()
                raise asyncio.TimeoutError("provider deadline") from exc
        return _valid_kline()


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (asyncio.TimeoutError("provider deadline"), "DEADLINE_EXCEEDED"),
        (RuntimeError("gateway unavailable"), "PROVIDER_ERROR"),
        ([], "EMPTY_RESPONSE"),
        ([["bad", "payload"]], "PROVIDER_ERROR"),
    ],
)
def test_runtime_faults_are_classified_without_aborting_other_symbols(
    outcome: object, expected: str
) -> None:
    async def run() -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, object]]]:
        client = _FaultInjectingClient({"FAULTUSDT": outcome, "GOODUSDT": _valid_kline()})
        scanner = MarketScanner(client=cast(Any, client), config=AppConfig())
        frames = await scanner.fetch_symbol_frames(["FAULTUSDT", "GOODUSDT"])
        return frames, scanner.last_scan_failures

    frames, failures = asyncio.run(run())

    assert list(frames) == ["GOODUSDT"]
    assert failures["FAULTUSDT"]["terminal_status"] == "SCAN_FAILED"
    assert failures["FAULTUSDT"]["reason_code"] == expected
    if expected in {"DEADLINE_EXCEEDED", "PROVIDER_ERROR"}:
        expected_exception = {
            "DEADLINE_EXCEEDED": "TimeoutError",
            "PROVIDER_ERROR": "RuntimeError" if isinstance(outcome, RuntimeError) else "ValueError",
        }[expected]
        assert failures["FAULTUSDT"]["details"] == {"exception_type": expected_exception}
    else:
        assert "details" not in failures["FAULTUSDT"]
    assert "GOODUSDT" not in failures


def test_stale_frame_is_classified_and_valid_lane_continues() -> None:
    async def run() -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, object]]]:
        stale_start = ASOF - timedelta(minutes=20)
        stale_ms = int(stale_start.timestamp() * 1000)
        stale = [[str(stale_ms), "1", "2", "0.5", "1.5", "10", "15"]]
        client = _FaultInjectingClient({"STALEUSDT": stale, "GOODUSDT": _valid_kline()})
        scanner = MarketScanner(client=cast(Any, client), config=AppConfig())
        return await scanner.fetch_symbol_frames(["STALEUSDT", "GOODUSDT"]), scanner.last_scan_failures

    frames, failures = asyncio.run(run())

    assert list(frames) == ["GOODUSDT"]
    assert failures["STALEUSDT"] == {
        "terminal_status": "SCAN_FAILED",
        "reason_code": "STALE_MARKET_DATA",
    }
    assert frames["GOODUSDT"].iloc[0][
        ["start_ms", "open", "high", "low", "close", "volume", "turnover"]
    ].to_dict() == {
        "start_ms": int((ASOF - timedelta(minutes=2)).timestamp() * 1000),
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "turnover": 15,
    }


def test_slow_symbol_deadline_cancels_only_slow_lane_and_keeps_fast_lane() -> None:
    async def run() -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, object]], _DeadlineBoundClient]:
        client = _DeadlineBoundClient()
        scanner = MarketScanner(client=cast(Any, client), config=AppConfig())
        scan_task = asyncio.create_task(
            scanner.fetch_symbol_frames(["SLOWUSDT", "GOODUSDT"])
        )
        await client.slow_started.wait()
        frames = await scan_task
        return frames, scanner.last_scan_failures, client

    frames, failures, client = asyncio.run(run())

    assert set(frames) == {"GOODUSDT"}
    assert failures["SLOWUSDT"] == {
        "terminal_status": "SCAN_FAILED",
        "reason_code": "DEADLINE_EXCEEDED",
        "details": {"exception_type": "TimeoutError"},
    }
    assert frames["GOODUSDT"]["close"].tolist() == [1.5]
    assert client.started == ["SLOWUSDT", "GOODUSDT"]
    assert client.slow_cancelled.is_set()
    assert not client.release_slow.is_set()


class _ConcurrentProviderClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def extract_market_time(self) -> datetime:
        return ASOF

    async def fetch_klines(self, symbol: str, *_args, **_kwargs):
        self.calls.append(("klines", symbol))
        if symbol not in {"FAULTUSDT", "GOODUSDT"}:
            raise AssertionError(f"unexpected symbol: {symbol}")
        return _valid_kline()

    async def fetch_open_interest(self, symbol: str):
        self.calls.append(("open_interest", symbol))
        if symbol == "FAULTUSDT":
            return []
        if symbol == "GOODUSDT":
            return [{"openInterest": "1"}]
        raise AssertionError(f"unexpected symbol: {symbol}")

    async def fetch_funding(self, symbol: str):
        self.calls.append(("funding", symbol))
        if symbol == "FAULTUSDT":
            return []
        if symbol == "GOODUSDT":
            return [{"fundingRate": "0"}]
        raise AssertionError(f"unexpected symbol: {symbol}")


def test_simultaneous_provider_operations_isolate_failed_lane() -> None:
    async def run():
        client = _ConcurrentProviderClient()
        scanner = MarketScanner(
            client=cast(Any, client), config=AppConfig(derivatives_enabled=True)
        )
        provider = CanonicalMarketDataProvider(
            scanner=scanner,
            config=AppConfig(derivatives_enabled=True),
            clock=lambda: ASOF,
        )
        results = await asyncio.gather(
            provider.capture_decision_snapshot("FAULTUSDT", include_liquidity=False),
            provider.capture_decision_snapshot("GOODUSDT", include_liquidity=False),
        )
        return results, client.calls, provider

    results, calls, provider = asyncio.run(run())

    assert results[0] is None
    assert results[1] is not None
    assert results[1].symbol == "GOODUSDT"
    assert results[1].frame_1m.frame["close"].tolist() == [1.5]
    assert results[1].derivatives == {
        "open_interest": [{"openInterest": "1"}],
        "funding": [{"fundingRate": "0"}],
        "derivatives_status": "OK",
        "derivatives_reasons": [],
        "data_quality_warnings": [],
        "symbol": "GOODUSDT",
        "scan_failure": None,
    }
    assert provider.evidence_snapshot()["details"][-1] == {
        "event": "decision_snapshot_rejected",
        "symbol": "FAULTUSDT",
        "reason": "MARKET_DATA_INCOMPLETE",
    }
    assert sorted(calls) == sorted(
        [
            ("klines", "FAULTUSDT"),
            ("klines", "GOODUSDT"),
            ("open_interest", "FAULTUSDT"),
            ("open_interest", "GOODUSDT"),
            ("funding", "FAULTUSDT"),
            ("funding", "GOODUSDT"),
        ]
    )
    assert all(symbol in {"FAULTUSDT", "GOODUSDT"} for _, symbol in calls)
