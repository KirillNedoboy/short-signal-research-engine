"""Recompute outcomes for stored signals."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import ShortSignalBot


async def main() -> None:
    async with ShortSignalBot.from_files() as bot:
        await bot.update_outcomes()


if __name__ == "__main__":
    asyncio.run(main())
