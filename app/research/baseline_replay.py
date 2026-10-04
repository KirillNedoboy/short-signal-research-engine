"""Offline, source-faithful BASELINE replay orchestration.

This module owns replay ordering, state storage, provenance and control gates.
It deliberately contains no scoring rules: every strategy decision is delegated
to :func:`app.replay.evaluator.evaluate_strategy_input`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import pandas as pd

from app.baseline.lifecycle import BaselineLifecycle, TransitionRecord
from app.config import AppConfig
from app.domain import EventState, EventStatus, SymbolFeatures
from app.replay.canonical import sha256_canonical
from app.replay.contracts import BaselineStrategyInput
from app.replay.evaluator import evaluate_strategy_input
from app.replay.market import CandleObservationKind, NormalizedMarketObservation

HISTORICAL_LIVE_CODE_SHA = "a51d2acc682388693247159f2e374ee23175c450"
CONTROL_EXPECTED = (
    ("TRUMP-1", "TRUMPUSDT", "2026-08-22T04:22:57Z", 73, "B", "Confirm"),
    ("TRUMP-2", "TRUMPUSDT", "2026-08-22T05:04:11Z", 77, "B", "Aggressive"),
)

@dataclass(frozen=True, slots=True)
class ReplayOpportunity:
    symbol: str
    decision_time_utc: datetime
    candles: Any
    kind: CandleObservationKind = CandleObservationKind.CLOSED_CANDLE
    derivatives: Mapping[str, Any] | None = None
    liquidity: Mapping[str, Any] | None = None
    sequence: int = 0
    trigger: str = "REPLAY_EVALUATION_TRIGGER"
    observability_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvaluationArtifact:
    decision_time_utc: datetime
    max_input_availability_time_utc: datetime
    input_fingerprint: str
    event_id: str
    context_identity: str
    observability_flags: tuple[str, ...] = ()
    result: Any = None


@dataclass(slots=True)
class ReplayResult:
    evaluations: list[EvaluationArtifact] = field(default_factory=list)
    transitions: list[TransitionRecord] = field(default_factory=list)
    states: dict[str, EventState] = field(default_factory=dict)
    terminality: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ControlExpectation:
    control_id: str
    symbol: str
    decision_time_utc: datetime
    score: int
    grade: str
    signal_type: str
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class ControlReplayReport:
    passed: bool
    classification: str
    mismatches: tuple[dict[str, Any], ...] = ()

    @property
    def broad_allowed(self) -> bool:
        return self.passed and self.classification == "CONTROL_REPLAY_PASS"


class ControlReplayMismatch(RuntimeError):
    """Raised when a control diverges before broad replay is allowed."""


def default_control_expectations() -> tuple[ControlExpectation, ...]:
    return tuple(ControlExpectation(cid, symbol, _utc(datetime.fromisoformat(timestamp.replace("Z", "+00:00"))), score, grade, signal_type) for cid, symbol, timestamp, score, grade, signal_type in CONTROL_EXPECTED)


class BaselineReplayDriver:
    """Replay normalized opportunities through the shared lifecycle."""

    def __init__(self, config: AppConfig, lifecycle: BaselineLifecycle | None = None) -> None:
        self.config = config
        self.lifecycle = lifecycle or BaselineLifecycle(config)

    def replay(
        self,
        opportunities: Sequence[ReplayOpportunity],
        *,
        initial_states: Mapping[str, EventState] | None = None,
        feature_overrides: Mapping[str, SymbolFeatures] | None = None,
        dataset_cutoff_utc: datetime | None = None,
    ) -> ReplayResult:
        result = ReplayResult(states=dict(initial_states or {}))
        ordered = sorted(opportunities, key=_opportunity_key)
        cutoff = _utc(dataset_cutoff_utc) if dataset_cutoff_utc else None
        for opportunity in ordered:
            now = _utc(opportunity.decision_time_utc)
            if cutoff is not None and now > cutoff:
                continue
            observation = NormalizedMarketObservation(
                opportunity.symbol, now, opportunity.kind, opportunity.candles,
            )
            observation.validate()
            features = (feature_overrides or {}).get(opportunity.symbol)
            prior = result.states.get(opportunity.symbol)
            if prior is not None and _has_minute_gap(opportunity.candles):
                result.terminality[prior.event_id] = "DATA_GAP_CENSORED"
                continue
            if prior is None or prior.state in {EventStatus.IDLE, EventStatus.EXPIRED}:
                lifecycle_result = self.lifecycle.detect_event(observation, features=features)
            elif features is not None and not hasattr(opportunity.candles, "columns"):
                lifecycle_result = self.lifecycle.advance_active_event_from_features(
                    prior, features, now, derivatives=opportunity.derivatives,
                    liquidity=opportunity.liquidity,
                )
            else:
                lifecycle_result = self.lifecycle.advance_active_event(
                    observation, prior, derivatives=opportunity.derivatives,
                    liquidity=opportunity.liquidity, features=features,
                )
            result.transitions.extend(lifecycle_result.transitions)
            state = lifecycle_result.state
            if state is None:
                continue
            result.states[opportunity.symbol] = state
            if state.state is EventStatus.EXPIRED:
                result.terminality[state.event_id] = "EXPIRED"
            item = lifecycle_result.evaluation_input
            if item is None:
                continue
            # This is the only scoring/evaluation call in the replay driver.
            decision_result = evaluate_strategy_input(item, self.config)
            max_availability = lifecycle_result.max_input_availability_time_utc or _utc(item.features.asof)
            if max_availability > now:
                raise ValueError("replay input availability exceeds decision time")
            artifact = EvaluationArtifact(
                decision_time_utc=now,
                max_input_availability_time_utc=max_availability,
                input_fingerprint=sha256_canonical(_input_payload(item)),
                event_id=state.event_id,
                context_identity=f"{state.symbol}:{state.event_id}:{now.isoformat()}",
                observability_flags=tuple(opportunity.observability_flags),
                result=decision_result,
            )
            result.evaluations.append(artifact)
            decision = getattr(decision_result, "decision", None)
            if decision is not None and getattr(decision, "actionable", False):
                marked = self.lifecycle.mark_signal_sent(state, len(result.evaluations), now)
                result.states[opportunity.symbol] = marked
                result.transitions.append(TransitionRecord(
                    symbol=marked.symbol, event_id=marked.event_id,
                    transition_time_utc=now, from_state=state.state.value,
                    to_state=marked.state.value, trigger="SIGNAL_MARKED",
                    source_market_time=item.features.asof, reason="evaluator actionable",
                    sequence=len(result.transitions),
                ))
                result.terminality[marked.event_id] = "SIGNAL_SENT"
        for state in result.states.values():
            if state.event_id not in result.terminality and state.state not in {EventStatus.IDLE, EventStatus.EXPIRED, EventStatus.SIGNAL_SENT}:
                result.terminality[state.event_id] = "ACTIVE_AT_DATASET_CUTOFF" if cutoff else "ACTIVE"
        return result

    def replay_controls(
        self,
        opportunities: Sequence[ReplayOpportunity],
        expectations: Sequence[ControlExpectation],
        **kwargs: Any,
    ) -> tuple[ReplayResult, ControlReplayReport]:
        replay = self.replay(opportunities, **kwargs)
        mismatches: list[dict[str, Any]] = []
        if len(expectations) != 2 or len({item.control_id for item in expectations}) != 2:
            mismatches.append({"control_id": "CONTROL_SET", "layer": "EVALUATION_RECORD", "field": "control_count", "expected": 2, "actual": len(expectations)})
        for expected in expectations:
            candidates = [item for item in replay.evaluations if item.decision_time_utc == _utc(expected.decision_time_utc)]
            actual = candidates[0] if candidates else None
            if actual is None:
                mismatches.append({"control_id": expected.control_id, "layer": "EVALUATION_RECORD", "field": "decision_time_utc", "expected": expected.decision_time_utc.isoformat(), "actual": None})
                continue
            score, grade, signal_type = _decision_summary(actual.result)
            for layer, mismatch_field, want, got in (
                ("EVALUATOR", "score", expected.score, score),
                ("EVALUATOR", "grade", expected.grade, grade),
                ("EVALUATOR", "signal_type", expected.signal_type, signal_type),
            ):
                if want != got:
                    mismatches.append({"control_id": expected.control_id, "layer": layer, "field": mismatch_field, "expected": want, "actual": got, "event_id": actual.event_id})
        report = ControlReplayReport(not mismatches and len(expectations) > 0, "CONTROL_REPLAY_PASS" if not mismatches and expectations else "CONTROL_REPLAY_MISMATCH", tuple(mismatches))
        return replay, report


def transition_to_dict(value: TransitionRecord) -> dict[str, Any]:
    """Convert a ledger record to the portable bundle row shape."""
    return {
        "symbol": value.symbol,
        "event_id": value.event_id,
        "transition_time_utc": _utc(value.transition_time_utc).isoformat().replace("+00:00", "Z"),
        "from_state": value.from_state,
        "to_state": value.to_state,
        "trigger": value.trigger,
        "source_market_time": value.source_market_time.isoformat().replace("+00:00", "Z") if value.source_market_time else None,
        "reason": value.reason,
        "sequence": value.sequence,
        "availability_time_utc": _utc(value.transition_time_utc).isoformat().replace("+00:00", "Z"),
        "event_type": "STATE_TRANSITION",
        "stable_event_id": f"{value.event_id}:{value.sequence}:{value.trigger}",
    }


def evaluation_artifact_to_dict(value: EvaluationArtifact) -> dict[str, Any]:
    return {
        "decision_time_utc": value.decision_time_utc.isoformat().replace("+00:00", "Z"),
        "max_input_availability_time_utc": value.max_input_availability_time_utc.isoformat().replace("+00:00", "Z"),
        "input_fingerprint": value.input_fingerprint,
        "event_id": value.event_id,
        "context_identity": value.context_identity,
        "observability_flags": list(value.observability_flags),
    }


def replay_semantic_fingerprint(value: ReplayResult) -> str:
    """Hash deterministic transitions and evaluation identity, excluding run metadata."""
    return sha256_canonical({
        "transitions": [transition_to_dict(item) for item in value.transitions],
        "evaluations": [evaluation_artifact_to_dict(item) for item in value.evaluations],
        "terminality": dict(sorted(value.terminality.items())),
    })


def _decision_summary(value: Any) -> tuple[Any, Any, Any]:
    return (
        getattr(value, "score", None),
        getattr(value, "grade", None),
        getattr(getattr(value, "decision", None), "signal_type", None),
    )


def _input_payload(item: BaselineStrategyInput) -> dict[str, Any]:
    return {
        "strategy": item.strategy, "symbol": item.features.symbol,
        "event_id": item.state.event_id, "decision_time_utc": item.decision_time_utc.isoformat(),
        "features": asdict(item.features),
        "zone": asdict(item.short_zone),
    }


def _opportunity_key(item: ReplayOpportunity) -> tuple[datetime, int, str, str]:
    return (_utc(item.decision_time_utc), item.sequence, item.symbol, item.trigger)


def _utc(value: datetime) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if stamp.tzinfo is None:
        return stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def _has_minute_gap(candles: Any) -> bool:
    if not hasattr(candles, "columns") or candles.empty:
        return False
    name = "open_time_utc" if "open_time_utc" in candles.columns else "timestamp" if "timestamp" in candles.columns else None
    if name is None:
        return False
    values = candles[name].sort_values()
    stamps = pd.to_datetime(values, utc=True, errors="coerce")
    return bool((stamps.diff().dropna() > pd.Timedelta(minutes=1)).any())
