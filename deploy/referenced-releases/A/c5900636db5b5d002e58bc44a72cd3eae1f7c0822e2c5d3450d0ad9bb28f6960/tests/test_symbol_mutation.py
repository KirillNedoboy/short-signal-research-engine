from __future__ import annotations

import asyncio

import pytest

from app.runtime.symbol_mutation import SymbolMutationCoordinator


def test_same_symbol_is_serialized_and_case_insensitive() -> None:
    async def scenario() -> list[str]:
        coordinator = SymbolMutationCoordinator()
        active: list[str] = []
        trace: list[str] = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def writer(name: str, symbol: str) -> None:
            async with coordinator.mutation(symbol):
                active.append(name)
                trace.append(f"{name}:enter")
                if name == "first":
                    started.set()
                    await release.wait()
                assert len(active) == 1
                active.pop()
                trace.append(f"{name}:exit")

        first = asyncio.create_task(writer("first", "btcusdt"))
        await started.wait()
        second = asyncio.create_task(writer("second", "BTCUSDT"))
        await asyncio.sleep(0)
        assert not second.done()
        release.set()
        await asyncio.gather(first, second)
        return trace

    assert asyncio.run(scenario()) == ["first:enter", "first:exit", "second:enter", "second:exit"]


def test_different_symbols_overlap_and_reentrant_acquire_fails() -> None:
    async def scenario() -> tuple[int, type[Exception]]:
        coordinator = SymbolMutationCoordinator()
        active = 0
        maximum = 0
        entered = asyncio.Event()
        release = asyncio.Event()

        async def writer(symbol: str) -> None:
            nonlocal active, maximum
            async with coordinator.mutation(symbol):
                active += 1
                maximum = max(maximum, active)
                entered.set()
                await release.wait()
                active -= 1

        first = asyncio.create_task(writer("BTCUSDT"))
        second = asyncio.create_task(writer("ETHUSDT"))
        await entered.wait()
        release.set()
        await asyncio.gather(first, second)
        with pytest.raises(RuntimeError, match="reentrant"):
            async with coordinator.mutation("BTCUSDT"):
                async with coordinator.mutation("btcusdt"):
                    pass
        return maximum, RuntimeError

    maximum, _ = asyncio.run(scenario())
    assert maximum == 2


def test_exception_and_cancellation_release_lane() -> None:
    async def scenario() -> None:
        coordinator = SymbolMutationCoordinator()
        with pytest.raises(ValueError):
            async with coordinator.mutation("BTCUSDT"):
                raise ValueError("boom")
        async with coordinator.mutation("BTCUSDT"):
            pass

        entered = asyncio.Event()

        async def cancelled_writer() -> None:
            async with coordinator.mutation("ETHUSDT"):
                entered.set()
                await asyncio.Future()

        task = asyncio.create_task(cancelled_writer())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with coordinator.mutation("ETHUSDT"):
            pass

    asyncio.run(scenario())
