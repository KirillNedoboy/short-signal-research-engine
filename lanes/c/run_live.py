"""Lane C foreground launcher for the pinned event-driven source tree."""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
os.environ.setdefault("PYTHONNOUSERSITE", "1")
SOURCE_ROOT = ROOT / "src"
SOURCE_PIN_PATH = ROOT / "SOURCE_PIN"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

# Resolve shadow paths before importing the composition root.  This keeps a
# path escape, symlink, or production-path mistake ahead of any database
# construction or provider client setup.  ``--help`` is handled before this
# block so release archives remain probeable without a shadow root or config.
_HELP_REQUESTED = any(argument in {"-h", "--help"} for argument in sys.argv[1:])
_SHADOW_PATHS = None
_SHADOW_CONTROL = None
if not _HELP_REQUESTED and os.environ.get("SHADOW_MODE", "").strip().casefold() in {
    "1",
    "true",
    "yes",
    "on",
}:
    from app.runtime.shadow import (  # noqa: E402
        ShadowPaths,
        get_shadow_control,
        install_rate_limit_handler,
        prepare_shadow_environment,
    )

    _SHADOW_PATHS = ShadowPaths.from_environment()
    # Sanitizing here prevents an inherited production DB/Telegram value from
    # being reintroduced by the regular config loader.  Every replacement path
    # is derived from the already validated root.
    os.environ.update(prepare_shadow_environment(_SHADOW_PATHS))
    os.environ["PYTHONPYCACHEPREFIX"] = str(
        _SHADOW_PATHS.runtime_root / "pycache"
    )
    _SHADOW_CONTROL = get_shadow_control(_SHADOW_PATHS)
    _SHADOW_CONTROL.emit_capability()
    # Catch provider errors that cross a logger boundary before the main
    # composition root is started. Direct adapters should still call the
    # control hook with an explicit transport where they have that context.
    _SHADOW_RATE_LIMIT_HANDLER = install_rate_limit_handler(_SHADOW_CONTROL)
else:
    os.environ.setdefault(
        "PYTHONPYCACHEPREFIX", str(ROOT / "runtime" / "pycache-production")
    )

SHADOW_PATHS = _SHADOW_PATHS
SHADOW_CONTROL = _SHADOW_CONTROL
SHADOW_RATE_LIMIT_HANDLER = globals().get("_SHADOW_RATE_LIMIT_HANDLER")

from app.config import load_config, require_manual_only  # noqa: E402
from app.logger import configure_logging  # noqa: E402
from app.main import LaneCCompositionRoot  # noqa: E402
from app.runtime.instance_fence import InstanceFence, run_fenced  # noqa: E402
from app.runtime.shadow import load_shadow_config  # noqa: E402


def _path_override(environment_name: str, fallback: Path) -> Path:
    """Return a full-mode path override without changing shadow resolution.

    Production cutover keeps the existing C config and environment files in
    their installation root while placing the process fence in a separate
    writable state directory.  Empty overrides retain the archive defaults;
    shadow mode never reaches this helper because its validated paths are
    authoritative above.
    """

    value = os.environ.get(environment_name)
    if value is None or not value.strip():
        return fallback
    return Path(value).expanduser()


if _SHADOW_PATHS is not None:
    CONFIG_PATH = _SHADOW_PATHS.config_path
    ENV_PATH = _SHADOW_PATHS.env_path
    LOCK_PATH = _SHADOW_PATHS.lock_path
else:
    CONFIG_PATH = _path_override("LANE_C_CONFIG_PATH", ROOT / "config.yaml")
    ENV_PATH = _path_override("LANE_C_ENV_PATH", ROOT / ".env")
    LOCK_PATH = _path_override(
        "LANE_C_LOCK_PATH", ROOT / "runtime" / "lane-c.lock"
    )
INSTANCE_IDENTITY = "short-telegram-bot-lite-c"
_validated_config = None


def verify_source_pin() -> None:
    """Fail closed when the launcher is not running the reviewed source tree."""

    if not SOURCE_PIN_PATH.exists():
        raise RuntimeError("Lane C SOURCE_PIN is missing")
    lines: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.relative_to(ROOT).as_posix()}\n")
    actual = hashlib.sha256("".join(lines).encode()).hexdigest()
    expected = SOURCE_PIN_PATH.read_text(encoding="utf-8").strip()
    if actual != expected:
        raise RuntimeError("Lane C source pin mismatch")


def instance_fence() -> InstanceFence:
    """Return the Lane C fence for the selected full-mode state boundary."""

    if _SHADOW_PATHS is not None:
        return InstanceFence(
            identity=_SHADOW_PATHS.shadow_identity,
            path=_SHADOW_PATHS.lock_path,
        )
    return InstanceFence(identity=INSTANCE_IDENTITY, path=LOCK_PATH)


def load_runtime_config():
    """Load only the explicit C config/env pair, then fail closed."""

    if _SHADOW_PATHS is not None:
        return load_shadow_config(
            _SHADOW_PATHS,
            loader=load_config,
            manual_only=require_manual_only,
        )
    return require_manual_only(
        load_config(config_path=CONFIG_PATH, env_path=ENV_PATH)
    )


async def _run_bot() -> None:
    runtime = LaneCCompositionRoot(config=_validated_config)
    await runtime.run_forever()


def _help_requested() -> bool:
    return _HELP_REQUESTED


async def main() -> int:
    # Keep launcher help offline and before source/config validation.  Release
    # archives intentionally omit the live config and credentials, so a help
    # probe must never enter the runtime or attempt a network connection.
    if _help_requested():
        print("usage: python run_live.py [--help]\n\nStart the manual-only Lane C runtime.")
        return 0
    configure_logging()
    global _validated_config
    verify_source_pin()
    _validated_config = load_runtime_config()
    return await run_fenced(_run_bot, fence_factory=lambda: instance_fence())


if __name__ == "__main__":
    os.environ.setdefault("PYTHONNOUSERSITE", "1")
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    raise SystemExit(asyncio.run(main()))
