"""Run the live polling loop."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config  # noqa: E402
from app.logger import configure_logging  # noqa: E402
from app.main import ShortSignalBot  # noqa: E402
from app.runtime.instance_fence import run_fenced  # noqa: E402


_validated_config = None


async def _run_bot() -> None:
    async with ShortSignalBot(config=_validated_config) as bot:
        await bot.run_forever()


async def main() -> int:
    configure_logging()
    global _validated_config
    _validated_config = load_config()
    return await run_fenced(_run_bot)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
