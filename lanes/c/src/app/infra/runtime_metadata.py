"""Stable non-secret metadata for one bot runtime."""

from __future__ import annotations

import os
import re
import subprocess
from functools import lru_cache
from pathlib import Path


CODE_VERSION_ENV = "SHORT_BOT_CODE_VERSION"
_SOURCE_PIN_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


def resolve_code_version() -> str:
    """Return an injected release version or the checkout commit SHA."""

    configured = os.getenv(CODE_VERSION_ENV, "").strip()
    if configured:
        return configured[:64]
    pinned = _release_source_pin()
    if pinned:
        return pinned
    return _checkout_code_version()


@lru_cache(maxsize=1)
def _release_source_pin() -> str | None:
    """Read the release source pin without contacting a repository or network."""

    for parent in Path(__file__).resolve().parents:
        candidate = parent / "SOURCE_PIN"
        try:
            value = candidate.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            continue
        if _SOURCE_PIN_PATTERN.fullmatch(value):
            return value.lower()
    return None


@lru_cache(maxsize=1)
def _checkout_code_version() -> str:
    repository_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    version = result.stdout.strip()
    return version[:64] if version else "unknown"
