"""Code-enforced promotion registry.

A `*_live_delivery_enabled: true` config flag is a production promotion. This
module makes every promotion carry recorded evidence: `ShortSignalBot.startup()`
refuses to boot when a delivery flag is true but `promotion_registry.json` has
no VALIDATED entry (with evidence, recorded_at and source) for that strategy.

It does NOT change today's delivery behaviour — it makes the promotions that
already happened explicit and makes any future promotion impossible without a
recorded evidence entry.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REGISTRY_FILENAME = "promotion_registry.json"
REGISTRY_PATH = Path(__file__).resolve().parent.parent / REGISTRY_FILENAME

#: config flag -> strategy name recorded in the registry
DELIVERY_FLAG_TO_STRATEGY: dict[str, str] = {
    "baseline_live_delivery_enabled": "BASELINE_PULLBACK",
    "volume_climax_live_delivery_enabled": "VOLUME_CLIMAX_UNWIND",
    "low_volume_live_delivery_enabled": "LOW_VOLUME_EXTENSION_FAILURE",
    "trapped_longs_live_delivery_enabled": "TRAPPED_LONGS_REVERSAL",
}


def _entry_problem(entry: Any) -> str | None:
    if not isinstance(entry, dict):
        return "has no registry entry"
    if entry.get("status") != "VALIDATED":
        return f"entry status is {entry.get('status')!r}, not 'VALIDATED'"
    for field in ("evidence", "recorded_at", "source"):
        value = entry.get(field)
        if not value:
            return f"entry is missing recorded '{field}'"
    return None


def validate_promotion_registry(
    config: Any, registry_path: Path | str | None = None
) -> None:
    """Fail loudly when a live-delivery flag lacks a validated promotion entry."""
    path = Path(registry_path) if registry_path is not None else REGISTRY_PATH
    enabled = [
        (flag, strategy)
        for flag, strategy in DELIVERY_FLAG_TO_STRATEGY.items()
        if bool(getattr(config, flag, False))
    ]
    if not enabled:
        return
    if not path.exists():
        strategies = ", ".join(strategy for _flag, strategy in enabled)
        raise RuntimeError(
            "promotion registry check failed: "
            f"{path} does not exist but live delivery is enabled for {strategies}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"promotion registry check failed: {path} is not valid JSON: {exc}") from exc
    strategies_obj = None
    if isinstance(payload, dict):
        candidate = payload.get("strategies")
        if isinstance(candidate, dict):
            strategies_obj = candidate
    if strategies_obj is None:
        raise RuntimeError(
            f"promotion registry check failed: {path} has no valid 'strategies' object"
        )
    problems = []
    for flag, strategy in enabled:
        problem = _entry_problem(strategies_obj.get(strategy))
        if problem is not None:
            problems.append(
                f"{strategy}: live delivery flag '{flag}' is true but the registry {problem}"
            )
    if problems:
        raise RuntimeError("promotion registry check failed: " + " | ".join(problems))
