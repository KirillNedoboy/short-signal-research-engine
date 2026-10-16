"""Bounded operational evidence for the canonical market-data provider.

This module deliberately has no dependency on the provider, strategies, storage,
or delivery layers.  The provider hands it a snapshot; the publisher writes one
small, replace-in-place JSON document for operational inspection.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


PROVIDER_EVIDENCE_SCHEMA_VERSION = 1
DEFAULT_PROVIDER_EVIDENCE_PATH = (
    "/opt/short-telegram-bot-lite-admin/shared/runtime/"
    "canonical-marketdata-provider-evidence.json"
)
PROVIDER_EVIDENCE_PATH_ENV = "CANONICAL_MARKET_DATA_PROVIDER_EVIDENCE_PATH"
MAX_EVIDENCE_BYTES = 64 * 1024
MAX_RECENT_SAMPLES = 32
MAX_SAMPLE_BYTES = 2 * 1024

_logger = logging.getLogger(__name__)
_SAMPLE_KEYS = frozenset(
    {
        "symbol",
        "evaluation_time",
        "group",
        "canonical_source",
        "candidate_source",
        "classification",
        "fallback_reason",
        "canonical_input_fingerprint",
        "candidate_input_fingerprint",
        "canonical_decision_fingerprint",
        "candidate_decision_fingerprint",
        "ages_ms",
    }
)


@dataclass(frozen=True, slots=True)
class ProviderEvidenceSample:
    """One compact comparison/evaluation sample safe to publish."""

    symbol: str | None = None
    evaluation_time: str | None = None
    group: str | None = None
    canonical_source: str | None = None
    candidate_source: str | None = None
    classification: str | None = None
    fallback_reason: str | None = None
    canonical_input_fingerprint: str | None = None
    candidate_input_fingerprint: str | None = None
    canonical_decision_fingerprint: str | None = None
    candidate_decision_fingerprint: str | None = None
    ages_ms: Mapping[str, int] | None = None

    def as_dict(self) -> dict[str, Any]:
        values = {
            key: value
            for key, value in {
                "symbol": self.symbol,
                "evaluation_time": self.evaluation_time,
                "group": self.group,
                "canonical_source": self.canonical_source,
                "candidate_source": self.candidate_source,
                "classification": self.classification,
                "fallback_reason": self.fallback_reason,
                "canonical_input_fingerprint": self.canonical_input_fingerprint,
                "candidate_input_fingerprint": self.candidate_input_fingerprint,
                "canonical_decision_fingerprint": self.canonical_decision_fingerprint,
                "candidate_decision_fingerprint": self.candidate_decision_fingerprint,
                "ages_ms": dict(self.ages_ms) if self.ages_ms is not None else None,
            }.items()
            if value is not None
        }
        return values


@dataclass(frozen=True, slots=True)
class ProviderEvidenceReport:
    """Immutable wrapper around the versioned JSON report mapping."""

    data: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.data)

    def __getitem__(self, key: str) -> Any:
        return self.data[key]


def normalize_provider_evidence(
    raw_snapshot: Mapping[str, Any],
    *,
    code_sha: str,
    strategy_fingerprint: str,
    provider_mode: str,
    generated_at_utc: datetime,
) -> dict[str, Any]:
    """Normalize provider-owned counters/details into the stable schema.

    Only an explicit allow-list from detail records reaches ``recent_samples``;
    raw ticker payloads, frames, and arbitrary diagnostic values are discarded.
    """

    if not isinstance(raw_snapshot, Mapping):
        raise TypeError("provider evidence snapshot must be a mapping")
    counters = raw_snapshot.get("counters") or {}
    if not isinstance(counters, Mapping):
        counters = {}
    details = raw_snapshot.get("details") or []
    if not isinstance(details, (list, tuple)):
        details = []

    def count(name: str, *fallback_names: str) -> int:
        for candidate in (name, *fallback_names):
            value = counters.get(candidate)
            if value is not None:
                try:
                    return max(0, int(value))
                except (TypeError, ValueError):
                    return 0
        return 0

    detail_maps = [item for item in details if isinstance(item, Mapping)]
    sample_maps = [_normalize_sample(item) for item in detail_maps]
    sample_maps = [item for item in sample_maps if item]
    sample_maps = sample_maps[-MAX_RECENT_SAMPLES:]

    canonical_rest = count("canonical_rest_groups")
    canonical_hub = count("canonical_hub_groups")
    hub_candidates = count("hub_candidates_total")
    if not canonical_rest and not canonical_hub:
        canonical_rest = sum(
            1
            for item in sample_maps
            if item.get("canonical_source") in {"REST", "REST_FALLBACK"}
        )
        canonical_hub = sum(1 for item in sample_maps if item.get("canonical_source") == "HUB")
        canonical_rest += count("rest_fallback") + count("hub_shadow_match") + count("hub_shadow_mismatch")
        canonical_hub += count("hub_selected")
    if not hub_candidates:
        hub_candidates = sum(1 for item in sample_maps if item.get("candidate_source") == "HUB")
        hub_candidates += count("hub_shadow_match", "hub_shadow_mismatch")

    source_comparisons = count("source_comparisons")
    decision_comparisons = count("decision_comparisons")
    if not source_comparisons:
        source_comparisons = sum(1 for item in sample_maps if item.get("candidate_source"))
    if not decision_comparisons:
        decision_comparisons = sum(
            1 for item in sample_maps if item.get("candidate_decision_fingerprint")
        )

    fallback_by_reason = _fallback_counts(counters, detail_maps)
    rest_fallback_total = count("rest_fallback_total", "rest_fallback")
    if not rest_fallback_total:
        rest_fallback_total = sum(fallback_by_reason.values())

    mode_value = _enum_value(provider_mode)
    hub_classifications = {
        "hub_exact": count("hub_exact") or _classification_count(sample_maps, "EXACT"),
        "hub_expected_temporal_difference": count("hub_expected_temporal_difference")
        or _classification_count(sample_maps, "EXPECTED_TEMPORAL_DIFFERENCE"),
        "hub_uninitialized": count("hub_uninitialized")
        or _classification_count(sample_maps, "WS_UNINITIALIZED"),
        "hub_stale": count("hub_stale") or _classification_count(sample_maps, "WS_STALE"),
        "hub_required_field_missing": count("hub_required_field_missing")
        or _classification_count(sample_maps, "WS_REQUIRED_FIELD_MISSING"),
        "hub_semantic_mismatch": count("hub_semantic_mismatch")
        or _classification_count(sample_maps, "SOURCE_SEMANTIC_MISMATCH"),
    }
    # REST_ONLY runtime records have no Hub candidate and therefore expose
    # zero Hub classifications.  Preserve explicitly supplied candidate
    # records for characterization/diagnostic calls even if the configured
    # mode is REST_ONLY.
    classifications = (
        hub_classifications
        if mode_value != "REST_ONLY" or hub_candidates > 0
        else {key: 0 for key in hub_classifications}
    )
    last_divergence = counters.get("last_divergence_at")
    if not last_divergence:
        divergence_times = [
            item.get("evaluation_time")
            for item in sample_maps
            if item.get("classification") == "DECISION_DIVERGENCE"
        ]
        last_divergence = divergence_times[-1] if divergence_times else None

    report = {
        "schema_version": PROVIDER_EVIDENCE_SCHEMA_VERSION,
        "generated_at_utc": _timestamp(generated_at_utc),
        "code_sha": str(code_sha),
        "provider_mode": mode_value,
        "strategy_config_fingerprint": str(strategy_fingerprint),
        "evaluations_total": count("evaluations_total", "evaluation"),
        "canonical_rest_groups": canonical_rest,
        "canonical_hub_groups": canonical_hub,
        "hub_candidates_total": hub_candidates,
        **classifications,
        "rest_fallback_total": rest_fallback_total,
        "fallback_by_reason": fallback_by_reason,
        "source_comparisons": source_comparisons,
        "source_comparison_divergences": count("source_comparison_divergences"),
        "decision_comparisons": decision_comparisons,
        "decision_divergences": count("decision_divergences"),
        "last_divergence_at": _timestamp_value(last_divergence),
        "recent_samples": sample_maps,
    }
    return _bound_report(report)


class ProviderEvidencePublisher:
    """Publish one bounded report using same-directory atomic replacement."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        logger: logging.Logger | None = None,
        warning_interval_seconds: float = 60.0,
    ) -> None:
        configured = path or os.getenv(PROVIDER_EVIDENCE_PATH_ENV) or DEFAULT_PROVIDER_EVIDENCE_PATH
        self.path = Path(configured)
        self._logger = logger or _logger
        self._warning_interval_seconds = warning_interval_seconds
        self._last_warning_at = 0.0
        self._replace = self._atomic_replace

    def publish(self, report: Mapping[str, Any]) -> Path:
        """Atomically publish ``report``; publication errors are isolated."""

        target = self.path
        temp_path: Path | None = None
        try:
            payload = _json_bytes(_bound_report(report)) + b"\n"
            if len(payload) >= MAX_EVIDENCE_BYTES:
                raise ValueError("provider evidence report exceeds 64 KiB")
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
            )
            temp_path = Path(temp_name)
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            self._replace(temp_path, target)
            self._fsync_directory(target.parent)
        except Exception as exc:  # noqa: BLE001 - operational evidence is best effort
            self._warn_failure(exc)
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
        return target

    @staticmethod
    def _atomic_replace(source: Path, target: Path) -> None:
        os.replace(source, target)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        try:
            flags = getattr(os, "O_DIRECTORY", 0)
            fd = os.open(directory, os.O_RDONLY | flags)
        except (AttributeError, OSError):
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _warn_failure(self, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_warning_at < self._warning_interval_seconds:
            return
        self._last_warning_at = now
        self._logger.warning(
            "provider evidence publication failed path=%s error=%s",
            self.path,
            type(exc).__name__,
        )


def _normalize_sample(detail: Mapping[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key in _SAMPLE_KEYS:
        if key not in detail:
            continue
        value = detail[key]
        if key == "ages_ms" and isinstance(value, Mapping):
            value = {
                str(age_key): _safe_int(age_value)
                for age_key, age_value in list(value.items())[:8]
            }
        elif isinstance(value, datetime):
            value = _timestamp(value)
        elif value is not None and not isinstance(value, (str, int, float, bool)):
            value = _enum_value(value)
        if value is not None:
            values[key] = value
    if "evaluation_time" not in values and "evaluated_at" in detail:
        values["evaluation_time"] = _timestamp_value(detail["evaluated_at"])
    return _bound_sample(values)


def _bound_sample(sample: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        key: value
        for key, value in sample.items()
        if key in _SAMPLE_KEYS and value is not None
    }
    encoded = _json_bytes(result)
    if len(encoded) <= MAX_SAMPLE_BYTES:
        return result
    result.pop("ages_ms", None)
    for key in (
        "candidate_decision_fingerprint",
        "canonical_decision_fingerprint",
        "candidate_input_fingerprint",
        "canonical_input_fingerprint",
    ):
        if len(_json_bytes(result)) <= MAX_SAMPLE_BYTES:
            break
        result.pop(key, None)
    if len(_json_bytes(result)) > MAX_SAMPLE_BYTES:
        for key, value in list(result.items()):
            if isinstance(value, str):
                result[key] = value[:256]
    return result


def _bound_report(report: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(report)
    samples = result.get("recent_samples") or []
    result["recent_samples"] = [
        _bound_sample(sample)
        for sample in list(samples)[-MAX_RECENT_SAMPLES:]
        if isinstance(sample, Mapping)
    ]
    while len(_json_bytes(result) + b"\n") >= MAX_EVIDENCE_BYTES and result["recent_samples"]:
        result["recent_samples"].pop(0)
    if len(_json_bytes(result) + b"\n") >= MAX_EVIDENCE_BYTES:
        raise ValueError("provider evidence report exceeds 64 KiB without samples")
    return result


def _fallback_counts(
    counters: Mapping[str, Any], details: list[Mapping[str, Any]]
) -> dict[str, int]:
    direct = counters.get("fallback_by_reason")
    if isinstance(direct, Mapping):
        return {str(key): _safe_int(value) for key, value in direct.items() if _safe_int(value) > 0}
    counts: dict[str, int] = {}
    for item in details:
        if item.get("event") != "rest_fallback":
            continue
        reason = item.get("fallback_reason") or item.get("reason")
        if reason is not None:
            key = _enum_value(reason)
            counts[key] = counts.get(key, 0) + 1
    return counts


def _classification_count(samples: list[Mapping[str, Any]], classification: str) -> int:
    return sum(1 for item in samples if item.get("classification") == classification)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _timestamp(value: datetime) -> str:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _timestamp_value(value: Any) -> str | None:
    if isinstance(value, datetime):
        return _timestamp(value)
    if value is None:
        return None
    return str(value)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return _timestamp(value)
    return _enum_value(value)
