"""Atomic, non-fatal runtime health publication for research storage."""

from __future__ import annotations

import logging
import json
import time
from pathlib import Path
from typing import Any, Mapping

from app.research.spool import atomic_write_json

logger = logging.getLogger(__name__)
MAX_HEALTH_BYTES = 64 * 1024


class ResearchStorageHealthPublisher:
    def __init__(self, path: str | Path, *, warning_interval_sec: float = 300.0) -> None:
        self.path = Path(path)
        self.warning_interval_sec = warning_interval_sec
        self._last_warning = 0.0

    def publish(self, report: Mapping[str, Any]) -> Path:
        try:
            encoded = json.dumps(
                report, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            if len(encoded) + 1 > MAX_HEALTH_BYTES:
                raise ValueError("research storage health report exceeds 64 KiB")
            return atomic_write_json(self.path, dict(report))
        except Exception:  # noqa: BLE001 - health publication is non-fatal
            now = time.monotonic()
            if now - self._last_warning >= self.warning_interval_sec:
                self._last_warning = now
                logger.warning("research storage health publication failed", exc_info=True)
            return self.path
