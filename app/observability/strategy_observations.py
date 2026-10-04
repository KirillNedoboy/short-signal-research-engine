"""Deterministic evidence for append-only strategy observations."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

import numpy as np

MAX_SNAPSHOT_BYTES = 32 * 1024
_SENSITIVE_KEY_PARTS = (
    "token",
    "secret",
    "password",
    "chat_id",
    "db_url",
    "database_url",
    "logging",
)


class ObservationWriteStatus(StrEnum):
    INSERTED = "INSERTED"
    DUPLICATE = "DUPLICATE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ObservationEvidence:
    input_fingerprint: str
    snapshot: dict[str, Any]
    snapshot_json: str
    warnings: list[dict[str, str]]


@dataclass(frozen=True, slots=True)
class StrategyObservation:
    observation_id: str
    idempotency_key: str
    run_id: str
    runtime_instance_id: str
    runtime_started_at: datetime
    code_version: str
    strategy_family: str
    strategy: str
    evaluation_phase: str
    symbol: str
    root_event_id: str | None
    event_revision: int | None
    attempt_id: str | None
    evaluation_id: int | None
    signal_id: int | None
    observed_at: datetime
    exchange_time: datetime | None
    market_asof: datetime | None
    live_decision: str
    shadow_decision: str
    score: int
    blockers: list[str]
    warnings: list[str]
    market_price: float | None
    event_high: float | None
    model_version: str
    config_hash: str
    input_fingerprint: str
    input_snapshot: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ObservationWriteResult:
    status: ObservationWriteStatus
    observation_id: str | None = None
    failure_classification: str | None = None


def build_observation_evidence(
    snapshot: Mapping[str, Any], *, max_bytes: int = MAX_SNAPSHOT_BYTES
) -> ObservationEvidence:
    """Build bounded, canonical, non-secret evidence for one observation."""

    canonical_value, warnings = _canonicalize(snapshot)
    if not isinstance(canonical_value, dict):
        raise TypeError("observation evidence root must be a mapping")
    if warnings:
        canonical_value["evidence_warnings"] = warnings
    canonical_json = json.dumps(
        canonical_value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    fingerprint = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    if len(canonical_json.encode("utf-8")) <= max_bytes:
        return ObservationEvidence(
            fingerprint, canonical_value, canonical_json, warnings
        )
    keys = sorted(canonical_value)[:128] if isinstance(canonical_value, dict) else []
    bounded = {
        "snapshot_truncated": True,
        "full_input_fingerprint": fingerprint,
        "omitted_top_level_keys": keys,
        "evidence_warnings": warnings,
    }
    bounded_json = json.dumps(
        bounded, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return ObservationEvidence(fingerprint, bounded, bounded_json, warnings)


def canonicalize_json_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return the project's explicit JSON-safe representation for a payload."""

    canonical_value, warnings = _canonicalize(value)
    if not isinstance(canonical_value, dict):
        raise TypeError("JSON payload root must be a mapping")
    if warnings:
        canonical_value["evidence_warnings"] = warnings
    json.dumps(
        canonical_value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return canonical_value


def make_observation_idempotency_key(
    *,
    strategy_family: str,
    strategy: str,
    symbol: str,
    root_event_id: str | None,
    event_revision: int | None,
    evaluation_phase: str,
    market_asof: datetime | None,
    input_fingerprint: str,
    model_version: str,
    config_hash: str,
) -> str:
    """Return a restart-stable observation identity."""

    return _fingerprint(
        {
            "strategy_family": strategy_family,
            "strategy": strategy,
            "symbol": symbol,
            "root_event_id": root_event_id,
            "event_revision": event_revision,
            "evaluation_phase": evaluation_phase,
            "market_asof": market_asof,
            "input_fingerprint": input_fingerprint,
            "model_version": model_version,
            "config_hash": config_hash,
        }
    )


def _fingerprint(value: Mapping[str, Any]) -> str:
    canonical_value, warnings = _canonicalize(value)
    if warnings:
        canonical_value["evidence_warnings"] = warnings
    canonical_json = json.dumps(
        canonical_value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _canonicalize(
    value: Any, *, key: str | None = None, path: str = ""
) -> tuple[Any, list[dict[str, str]]]:
    if key is not None and any(part in key.lower() for part in _SENSITIVE_KEY_PARTS):
        return "[excluded]", []
    if isinstance(value, Mapping):
        mapping_result: dict[str, Any] = {}
        mapping_warnings: list[dict[str, str]] = []
        for item_key, item_value in sorted(
            value.items(), key=lambda item: str(item[0])
        ):
            item_key_str = str(item_key)
            item_path = f"{path}.{item_key_str}" if path else item_key_str
            normalized, item_warnings = _canonicalize(
                item_value, key=item_key_str, path=item_path
            )
            mapping_result[item_key_str] = normalized
            mapping_warnings.extend(item_warnings)
        return mapping_result, mapping_warnings
    if isinstance(value, (list, tuple)):
        list_result: list[Any] = []
        list_warnings: list[dict[str, str]] = []
        for index, item in enumerate(value):
            normalized, item_warnings = _canonicalize(item, path=f"{path}[{index}]")
            list_result.append(normalized)
            list_warnings.extend(item_warnings)
        return list_result, list_warnings
    if isinstance(value, datetime):
        timestamp = (
            value.astimezone(timezone.utc)
            if value.tzinfo
            else value.replace(tzinfo=timezone.utc)
        )
        return timestamp.isoformat().replace("+00:00", "Z"), []
    if isinstance(value, (float, np.floating)):
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            if math.isnan(numeric_value):
                original = "NaN"
            elif numeric_value > 0:
                original = "+Inf"
            else:
                original = "-Inf"
            return None, [
                {
                    "path": path or "$",
                    "reason": "NON_FINITE_FLOAT",
                    "original": original,
                }
            ]
        return float(format(numeric_value, ".15g")), []
    if isinstance(value, (str, int, bool)) or value is None:
        return value, []
    raise TypeError(f"unsupported observation evidence value: {type(value).__name__}")
