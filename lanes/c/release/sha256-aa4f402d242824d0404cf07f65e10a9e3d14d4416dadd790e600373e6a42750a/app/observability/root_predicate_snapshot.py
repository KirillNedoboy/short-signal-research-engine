"""Compact, non-invasive current-root detector predicate evidence."""
from __future__ import annotations

from typing import Any

_GROUPS = ("temporal", "structural", "safety", "liquidity", "oi_squeeze")


def _group(name: str) -> str:
    key = name.lower()
    if any(token in key for token in ("age", "matur", "ttl", "confirm", "time")):
        return "temporal"
    if any(token in key for token in ("retest", "breakdown", "pullback", "structure", "failure", "reference")):
        return "structural"
    if any(token in key for token in ("liquidity", "spread", "slippage", "depth")):
        return "liquidity"
    if any(token in key for token in ("oi", "open_interest", "squeeze")):
        return "oi_squeeze"
    return "safety"


def _status(value: Any) -> str:
    if isinstance(value, bool):
        return "PASS" if value else "FAIL"
    if value is None:
        return "MISSING"
    if isinstance(value, str) and value.upper() in {"PASS", "FAIL", "MISSING"}:
        return value.upper()
    return "PASS"


def build_current_root_predicate_snapshot(
    *,
    metadata: dict[str, Any] | None = None,
    veto_reasons: list[str] | None = None,
    passed_conditions: list[str] | None = None,
    feature_values: dict[str, Any] | None = None,
) -> dict[str, dict[str, str]]:
    """Normalize existing detector evidence; does not evaluate or alter predicates."""
    snapshot: dict[str, dict[str, str]] = {group: {} for group in _GROUPS}
    for source in (metadata or {}, feature_values or {}):
        for name, value in source.items():
            if isinstance(value, (bool, str)) or value is None:
                snapshot[_group(str(name))][str(name)] = _status(value)
    for name in passed_conditions or []:
        snapshot[_group(str(name))][str(name)] = "PASS"
    for reason in veto_reasons or []:
        key = f"veto:{reason}"
        snapshot[_group(key)][key] = "FAIL"
    return snapshot
