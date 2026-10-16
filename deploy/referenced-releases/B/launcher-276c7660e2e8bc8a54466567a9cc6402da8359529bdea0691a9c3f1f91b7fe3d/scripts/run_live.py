"""Run the live polling loop."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config  # noqa: E402
from app.logger import configure_logging  # noqa: E402
from app.main import ShortSignalBot  # noqa: E402
from app.runtime.instance_fence import InstanceFence, run_fenced  # noqa: E402


_validated_config = None


def _instance_fence_from_environment() -> InstanceFence:
    """Build the process fence from env, preserving lane-A defaults when unset."""
    return InstanceFence(
        identity=os.environ.get(
            "SHORT_BOT_INSTANCE_IDENTITY", "short-telegram-bot-lite"
        ),
        path=Path(
            os.environ.get(
                "SHORT_BOT_INSTANCE_LOCK_PATH", "/run/short-telegram-bot-lite.lock"
            )
        ),
    )


async def _run_bot() -> None:
    async with ShortSignalBot(config=_validated_config) as bot:
        await bot.run_forever()


async def main() -> int:
    configure_logging()
    global _validated_config
    _validated_config = load_config()
    return await run_fenced(_run_bot, fence_factory=_instance_fence_from_environment)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
