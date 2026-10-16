"""Immutable, side-effect-free contracts shared by strategy adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, fields
from datetime import datetime
from math import isfinite
from types import MappingProxyType
from typing import Any

from app.domain import EventState, ShortZone, SymbolFeatures
from app.replay.canonical import canonicalize

BASELINE_PULLBACK = "BASELINE_PULLBACK"
LEGACY_MODEL_VERSION = "LEGACY_MODEL_VERSION"
VALID_INPUT_QUALITIES = frozenset({"VALID", "COMPLETE", "OK"})
BLOCKED_INPUT_QUALITIES = frozenset({"INCOMPLETE", "MISSING", "INVALID", "FAILED", "UNKNOWN", "STALE", "DISCONTINUOUS"})
SUPPORTED_INPUT_QUALITIES = VALID_INPUT_QUALITIES | BLOCKED_INPUT_QUALITIES
VALID_SHORT_ZONE_MODES = frozenset({"event_range", "atr_from_high"})


class StrategyEvaluationError(ValueError):
    """Raised when a strategy context or evaluation violates the contract."""


def _require_aware(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise StrategyEvaluationError(f"{name} must be timezone-aware")


def _freeze(value: Any) -> Any:
    """Freeze supported metadata containers while preserving canonical JSON shape."""
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise StrategyEvaluationError("strategy metadata keys must be strings")
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    try:
        canonicalize(value)
    except (TypeError, ValueError) as exc:
        raise StrategyEvaluationError("strategy metadata contains a non-canonical value") from exc
    return value


class _FrozenEventState(EventState):
    __slots__ = ()

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("canonical event state is immutable")


class _FrozenSymbolFeatures(SymbolFeatures):
    __slots__ = ()

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("canonical features are immutable")


class _FrozenShortZone(ShortZone):
    __slots__ = ()

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("short zone is immutable")


def _freeze_dataclass(value: Any, frozen_type: type) -> Any:
    frozen = object.__new__(frozen_type)
    for field in fields(value):
        object.__setattr__(frozen, field.name, _freeze(deepcopy(getattr(value, field.name))))
    return frozen


def _dataclass_payload(value: Any) -> dict[str, Any]:
    return {field.name: getattr(value, field.name) for field in fields(value)}


def _normalize_input_quality(value: Any) -> str:
    if not isinstance(value, str):
        raise StrategyEvaluationError("input_quality is not a supported market-data quality")
    normalized = value.strip().upper()
    if normalized not in SUPPORTED_INPUT_QUALITIES:
        raise StrategyEvaluationError("input_quality is not a supported market-data quality")
    return normalized


def _validate_short_zone(value: ShortZone) -> None:
    if not isinstance(value.low, (int, float)) or isinstance(value.low, bool) or not isfinite(value.low) or value.low <= 0:
        raise StrategyEvaluationError("short_zone.low must be finite and positive")
    if not isinstance(value.high, (int, float)) or isinstance(value.high, bool) or not isfinite(value.high) or value.high <= 0:
        raise StrategyEvaluationError("short_zone.high must be finite and positive")
    if value.low > value.high:
        raise StrategyEvaluationError("short_zone.low must not exceed high")
    if not isinstance(value.mode, str) or not value.mode.strip() or value.mode not in VALID_SHORT_ZONE_MODES:
        raise StrategyEvaluationError("short_zone.mode is not supported")


def _text_tuple(value: Sequence[str], name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise StrategyEvaluationError(f"{name} must be a sequence of strings")
    try:
        result = tuple(value)
    except TypeError as exc:
        raise StrategyEvaluationError(f"{name} must be a sequence of strings") from exc
    if any(not isinstance(item, str) for item in result):
        raise StrategyEvaluationError(f"{name} must contain only strings")
    return result


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Read-only inputs available to a pure strategy evaluation."""

    symbol: str
    event_id: str
    event_state: EventState
    features: SymbolFeatures
    short_zone: ShortZone | None
    decision_timestamp: datetime
    strategy_config_fingerprint: str
    input_quality: str

    def __post_init__(self) -> None:
        if not self.symbol or not isinstance(self.symbol, str):
            raise StrategyEvaluationError("symbol is required")
        if not self.event_id or not isinstance(self.event_id, str):
            raise StrategyEvaluationError("event_id is required")
        if not isinstance(self.event_state, EventState) or not isinstance(self.features, SymbolFeatures):
            raise StrategyEvaluationError("context must use canonical event and feature types")
        if self.short_zone is not None and not isinstance(self.short_zone, ShortZone):
            raise StrategyEvaluationError("short_zone must use the canonical ShortZone type")
        if self.event_state.symbol != self.symbol or self.event_state.event_id != self.event_id or self.features.symbol != self.symbol:
            raise StrategyEvaluationError("context identity must match event and features")
        _require_aware(self.decision_timestamp, "decision_timestamp")
        _require_aware(self.features.asof, "features.asof")
        if self.features.market_asof is None:
            raise StrategyEvaluationError("features.market_asof is required")
        _require_aware(self.features.market_asof, "features.market_asof")
        if self.features.asof > self.features.market_asof:
            raise StrategyEvaluationError("features.asof must not be newer than market_asof")
        if self.features.market_asof > self.decision_timestamp:
            raise StrategyEvaluationError("market_asof must not be newer than decision_timestamp")
        for name in ("last_high_time", "last_structural_close_time"):
            timestamp = getattr(self.features, name)
            if timestamp is not None:
                _require_aware(timestamp, f"features.{name}")
                if timestamp > self.decision_timestamp:
                    raise StrategyEvaluationError(f"features.{name} must not be newer than decision_timestamp")
        if self.short_zone is not None:
            _validate_short_zone(self.short_zone)
        if not isinstance(self.strategy_config_fingerprint, str) or not self.strategy_config_fingerprint:
            raise StrategyEvaluationError("strategy_config_fingerprint is required")
        object.__setattr__(self, "input_quality", _normalize_input_quality(self.input_quality))
        object.__setattr__(self, "event_state", _freeze_dataclass(self.event_state, _FrozenEventState))
        object.__setattr__(self, "features", _freeze_dataclass(self.features, _FrozenSymbolFeatures))
        if self.short_zone is not None:
            object.__setattr__(self, "short_zone", _freeze_dataclass(self.short_zone, _FrozenShortZone))

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize({
            "symbol": self.symbol,
            "event_id": self.event_id,
            "event_state": _dataclass_payload(self.event_state),
            "features": _dataclass_payload(self.features),
            "short_zone": _dataclass_payload(self.short_zone) if self.short_zone is not None else None,
            "decision_timestamp": self.decision_timestamp,
            "strategy_config_fingerprint": self.strategy_config_fingerprint,
            "input_quality": self.input_quality,
        })


_MISSING = object()


@dataclass(frozen=True, slots=True, init=False)
class StrategyEvaluation:
    """Immutable canonical result produced by a pure strategy adapter."""

    strategy_type: str
    strategy_subtype: str | None = field(default=None, kw_only=True)
    model_version: str | None
    actionable: bool
    score: int
    grade: str
    reasons: Sequence[str]
    blockers: Sequence[str]
    vetoes: Sequence[str]
    risk_flags: Sequence[str]
    strategy_metadata: Mapping[str, Any]
    symbol: str
    event_id: str
    decision_timestamp: datetime
    input_quality: str = "VALID"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Build an evaluation while retaining the legacy positional order."""
        names = (
            "strategy_type", "strategy_subtype", "model_version", "actionable", "score",
            "grade", "reasons", "blockers", "vetoes", "risk_flags", "strategy_metadata",
            "symbol", "event_id", "decision_timestamp", "input_quality",
        )
        if len(args) > len(names):
            raise TypeError(f"StrategyEvaluation expected at most {len(names)} arguments")
        values: dict[str, Any] = dict(zip(names, args))
        duplicate = values.keys() & kwargs.keys()
        if duplicate:
            name = next(iter(duplicate))
            raise TypeError(f"StrategyEvaluation got multiple values for argument '{name}'")
        unexpected = kwargs.keys() - names
        if unexpected:
            name = next(iter(unexpected))
            raise TypeError(f"StrategyEvaluation got an unexpected keyword argument '{name}'")
        values.update(kwargs)
        defaults = {"strategy_subtype": None, "model_version": None, "input_quality": "VALID"}
        required = set(names) - defaults.keys()
        missing = sorted(required - values.keys())
        if missing:
            raise TypeError(f"StrategyEvaluation missing required arguments: {', '.join(missing)}")
        for name in names:
            object.__setattr__(self, name, values.get(name, defaults.get(name, _MISSING)))
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_type, str) or not self.strategy_type:
            raise StrategyEvaluationError("strategy_type is required")
        subtype = self.strategy_subtype
        if subtype is None and self.strategy_type == BASELINE_PULLBACK:
            subtype = BASELINE_PULLBACK
        if subtype is None:
            raise StrategyEvaluationError("non-baseline evaluations require strategy_subtype")
        if not isinstance(subtype, str) or not subtype:
            raise StrategyEvaluationError("strategy_subtype must be a non-empty string")
        object.__setattr__(self, "strategy_subtype", subtype)
        if self.model_version is None:
            object.__setattr__(self, "model_version", LEGACY_MODEL_VERSION)
            metadata = dict(self.strategy_metadata) if isinstance(self.strategy_metadata, Mapping) else self.strategy_metadata
            if isinstance(metadata, Mapping):
                metadata.setdefault("original_model_version", None)
                object.__setattr__(self, "strategy_metadata", metadata)
        elif not isinstance(self.model_version, str) or not self.model_version:
            raise StrategyEvaluationError("model_version must be a non-empty string")
        if not isinstance(self.actionable, bool) or not isinstance(self.score, int) or isinstance(self.score, bool):
            raise StrategyEvaluationError("actionable and score have invalid types")
        if not isinstance(self.grade, str) or not self.grade:
            raise StrategyEvaluationError("grade is required")
        for name in ("reasons", "blockers", "vetoes", "risk_flags"):
            object.__setattr__(self, name, _text_tuple(getattr(self, name), name))
        if not isinstance(self.strategy_metadata, Mapping):
            raise StrategyEvaluationError("strategy_metadata must be a mapping")
        object.__setattr__(self, "strategy_metadata", _freeze(self.strategy_metadata))
        object.__setattr__(self, "input_quality", _normalize_input_quality(self.input_quality))
        if self.actionable and self.input_quality in BLOCKED_INPUT_QUALITIES:
            object.__setattr__(self, "actionable", False)
        if not isinstance(self.symbol, str) or not self.symbol or not isinstance(self.event_id, str) or not self.event_id:
            raise StrategyEvaluationError("symbol and event_id are required")
        _require_aware(self.decision_timestamp, "decision_timestamp")

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize({
            "strategy_type": self.strategy_type,
            "strategy_subtype": self.strategy_subtype,
            "model_version": self.model_version,
            "actionable": self.actionable,
            "score": self.score,
            "grade": self.grade,
            "reasons": self.reasons,
            "blockers": self.blockers,
            "vetoes": self.vetoes,
            "risk_flags": self.risk_flags,
            "strategy_metadata": self.strategy_metadata,
            "symbol": self.symbol,
            "event_id": self.event_id,
            "decision_timestamp": self.decision_timestamp,
            "input_quality": self.input_quality,
        })