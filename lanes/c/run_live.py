"""Lane C foreground launcher with an explicit isolated process fence."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
os.environ.setdefault("PYTHONPYCACHEPREFIX", str(ROOT / "runtime" / "pycache-production"))
RELEASE_ID = "sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a"
SOURCE_ROOT = ROOT / "release" / RELEASE_ID
VENDOR_ROOT = SOURCE_ROOT / "vendor"
for import_root in (VENDOR_ROOT, SOURCE_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from app.config import load_config, require_manual_only  # noqa: E402
from app.logger import configure_logging  # noqa: E402
from app.main import ShortSignalBot  # noqa: E402
from app.runtime.instance_fence import InstanceFence, run_fenced  # noqa: E402


CONFIG_PATH = ROOT / "config.yaml"
ENV_PATH = ROOT / ".env"
LOCK_PATH = ROOT / "runtime" / "lane-c.lock"
INSTANCE_IDENTITY = "short-telegram-bot-lite-c"
_validated_config = None


def instance_fence() -> InstanceFence:
    """Return the Lane C fence; no environment override can select a shared lock."""

    return InstanceFence(identity=INSTANCE_IDENTITY, path=LOCK_PATH)


def load_runtime_config():
    """Load only the explicit C config and C env file, then fail closed."""

    return require_manual_only(
        load_config(config_path=CONFIG_PATH, env_path=ENV_PATH)
    )


async def _run_bot() -> None:
    async with ShortSignalBot(config=_validated_config) as bot:
        await bot.run_forever()


async def main() -> int:
    configure_logging()
    global _validated_config
    _validated_config = load_runtime_config()
    return await run_fenced(_run_bot, fence_factory=lambda: instance_fence())


if __name__ == "__main__":
    os.environ.setdefault("PYTHONNOUSERSITE", "1")
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    raise SystemExit(asyncio.run(main()))
