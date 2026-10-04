"""Dependency-free replay input contracts and serializers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, TypeAlias

from app.domain import EventState, EventStatus, ShortZone, SymbolFeatures
from app.replay.canonical import canonicalize


BASELINE_PULLBACK = "BASELINE_PULLBACK"
VOLUME_CLIMAX_UNWIND = "VOLUME_CLIMAX_UNWIND"
LOW_VOLUME_EXTENSION_FAILURE = "LOW_VOLUME_EXTENSION_FAILURE"
STRATEGIES = frozenset({BASELINE_PULLBACK, VOLUME_CLIMAX_UNWIND, LOW_VOLUME_EXTENSION_FAILURE})


@dataclass(frozen=True, slots=True)
class BaselineStrategyInput:
    state: EventState
    features: SymbolFeatures
    short_zone: ShortZone
    decision_time_utc: datetime
    strategy: str = BASELINE_PULLBACK


@dataclass(frozen=True, slots=True)
class ClimaxStrategyInput:
    state: EventState
    features: SymbolFeatures
    # A stable row-id reference (or an in-memory frame when used directly by
    # the evaluator). Bundle serialization stores the reference, never a
    # filesystem path or a live data source handle.
    frame_ref: Any
    decision_time_utc: datetime
    strategy: str = VOLUME_CLIMAX_UNWIND


StrategyInput: TypeAlias = BaselineStrategyInput | ClimaxStrategyInput


@dataclass(frozen=True, slots=True)
class StrategyDecisionRecord:
    """Portable decision envelope persisted by a replay bundle."""

    run_id: str
    strategy: str
    symbol: str
    decision_time_utc: datetime
    actionable: bool | None
    selected_for_live_delivery: bool
    score: int | None
    grade: str | None
    blockers: list[str]
    warnings: list[str]
    input_fingerprint: str
    decision_fingerprint: str
    code_sha: str
    config_hash: str
    strategy_contract_version: str
    payload: dict[str, Any]


def strategy_input_to_dict(value: StrategyInput) -> dict[str, Any]:
    """Serialize an input using only canonical, portable built-in values."""

    payload: dict[str, Any] = {
        "strategy": value.strategy,
        "state": asdict(value.state),
        "features": asdict(value.features),
        "decision_time_utc": value.decision_time_utc,
    }
    if isinstance(value, BaselineStrategyInput):
        payload["short_zone"] = asdict(value.short_zone)
    else:
        payload["frame_ref"] = value.frame_ref
    return canonicalize(payload)


def strategy_input_from_dict(value: Mapping[str, Any]) -> StrategyInput:
    """Deserialize a replay input without accessing storage or a network."""

    strategy = str(value["strategy"])
    if strategy not in STRATEGIES:
        raise ValueError(f"unsupported replay strategy: {strategy}")
    state_data = dict(value["state"])
    state_data["state"] = EventStatus(state_data["state"])
    state = EventState(**_restore_datetimes(state_data))
    features = SymbolFeatures(**_restore_datetimes(dict(value["features"])))
    decision_time = _parse_datetime(value["decision_time_utc"])
    if strategy == BASELINE_PULLBACK:
        return BaselineStrategyInput(state, features, ShortZone(**dict(value["short_zone"])), decision_time, strategy)
    return ClimaxStrategyInput(state, features, str(value["frame_ref"]), decision_time, strategy)


def _restore_datetimes(value: dict[str, Any]) -> dict[str, Any]:
    datetime_fields = {
        "event_start_time", "event_high_time", "pullback_detected_at", "signal_sent_at", "expires_at", "updated_at",
        "asof", "market_asof", "last_high_time", "last_structural_close_time",
    }
    for key in datetime_fields & set(value):
        if value[key] is not None and isinstance(value[key], str):
            value[key] = _parse_datetime(value[key])
    return value


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise TypeError("replay timestamp must be an ISO-8601 string")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
