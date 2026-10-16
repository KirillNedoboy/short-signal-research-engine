from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

from app.infra.request_scheduler import RequestPriority
from app.main import ShortSignalBot
from app.runtime.warmup import WarmupCoverage


class _Frame:
    def __init__(self, stale_reason: str | None) -> None:
        self.stale_reason = stale_reason
        self.actionable = stale_reason is None


class _Store:
    def __init__(self) -> None:
        self.state: dict[str, str | None] = {}
        self.calls: list[tuple[tuple[str, ...], object]] = []
        self.fail_once = False

    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self.state))

    def is_complete(self, symbol: str, decision_at=None) -> bool:  # noqa: ANN001
        return self.state.get(symbol) is None

    def peek_snapshot(self, symbol: str, decision_at=None) -> _Frame:  # noqa: ANN001
        return _Frame(self.state.get(symbol, "WARMING"))

    async def warm_symbols(self, client, symbols, *, priority, **kwargs):  # noqa: ANN001, ANN003
        batch = tuple(str(symbol).upper() for symbol in symbols)
        self.calls.append((batch, priority))
        symbol = batch[0]
        if self.fail_once:
            self.fail_once = False
            self.state[symbol] = "WARMING"
            return {
                "seeded": 0,
                "failed": 1,
                "skipped": 0,
                "requests": 1,
                "timed_out": 0,
                "candle_failed": 1,
                "candle_timed_out": 0,
                "oi_failed": 0,
                "oi_timed_out": 0,
            }
        self.state[symbol] = "WAITING_FOR_WS"
        return {
            "seeded": 1,
            "failed": 0,
            "skipped": 0,
            "requests": 2,
            "timed_out": 0,
            "candle_failed": 0,
            "candle_timed_out": 0,
            "oi_failed": 0,
            "oi_timed_out": 0,
        }


def _bot(store: _Store) -> ShortSignalBot:
    bot = ShortSignalBot.__new__(ShortSignalBot)
    bot._market_data_provider = SimpleNamespace(frame_store=store, client=object())
    bot._warmup_coverage = WarmupCoverage()
    bot._warmup_coverage.begin(("AAAUSDT",))
    bot._frame_store_warm_pending = set()
    bot._frame_store_warm_waiting_ws = set()
    bot._frame_store_warm_retry_tasks = {}
    bot._frame_store_warm_task = None
    bot._lifecycle = "WARMING_UP"
    bot._ready = True
    bot._logger = logging.getLogger("v4-readiness-test")
    return bot


def test_late_ws_reconciliation_marks_ready_without_rewarming_waiting_symbol() -> None:
    async def run() -> None:
        store = _Store()
        bot = _bot(store)
        bot._schedule_frame_store_warmup(["AAAUSDT"])
        assert bot._frame_store_warm_task is not None
        await bot._frame_store_warm_task
        assert len(store.calls) == 1
        expected_priority = (
            RequestPriority.RECOVERY
            if hasattr(RequestPriority, "RECOVERY")
            else RequestPriority.P1
        )
        assert store.calls[0][1] is expected_priority
        assert bot.warmup_status()["state"] == "WARMING_UP"
        assert "AAAUSDT" in bot._frame_store_warm_waiting_ws

        bot._schedule_frame_store_warmup(["AAAUSDT"])
        await asyncio.sleep(0)
        assert len(store.calls) == 1

        store.state["AAAUSDT"] = None
        bot._reconcile_frame_store_warmup()
        assert bot.warmup_status() == {
            "state": "READY",
            "total": 1,
            "ready": 1,
            "pending": 0,
            "failed": 0,
        }

    asyncio.run(run())


def test_rest_failure_retries_once_with_deduplicated_symbol_attempt() -> None:
    async def run() -> None:
        store = _Store()
        store.fail_once = True
        bot = _bot(store)
        bot._schedule_frame_store_warmup(["AAAUSDT"])
        assert bot._frame_store_warm_task is not None
        await bot._frame_store_warm_task
        assert len(store.calls) == 1
        retry = bot._frame_store_warm_retry_tasks.get("AAAUSDT")
        assert retry is not None
        await retry
        next_task = bot._frame_store_warm_task
        assert next_task is not None
        await next_task
        assert len(store.calls) == 2
        assert bot._frame_store_warm_waiting_ws == {"AAAUSDT"}

    asyncio.run(run())


def test_startup_source_restores_before_first_universe_refresh() -> None:
    source = (
        __import__("pathlib").Path(__file__).parents[1] / "app" / "main.py"
    ).read_text()
    restore_at = source.index('restore = getattr(provider, \"restore_frame_store\"')
    refresh_at = source.index("telemetry = await self._refresh_event_universe(startup=True)")
    assert restore_at < refresh_at
