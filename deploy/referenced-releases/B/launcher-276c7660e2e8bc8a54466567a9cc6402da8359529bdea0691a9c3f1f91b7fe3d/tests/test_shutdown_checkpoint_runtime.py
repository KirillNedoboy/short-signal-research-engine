from __future__ import annotations

import asyncio

from app.main import ShortSignalBot


def test_bot_shutdown_checkpoint_forwards_generation_and_pid() -> None:
    calls: dict[str, object] = {}

    class Store:
        checkpoint_generation = 0

    store = Store()

    class Provider:
        frame_store = store

        async def checkpoint_frame_store(self, *, generation: int, metadata: dict[str, object]) -> int:
            calls.update(generation=generation, metadata=metadata)
            store.checkpoint_generation = generation
            return 11

    bot = object.__new__(ShortSignalBot)
    bot._market_data_provider = Provider()

    result = asyncio.run(bot._checkpoint_for_shutdown(generation=9, pid=5678))

    assert result == 11
    assert calls == {
        "generation": 9,
        "metadata": {"pid": 5678, "reason": "SIGUSR1_SHUTDOWN"},
    }
