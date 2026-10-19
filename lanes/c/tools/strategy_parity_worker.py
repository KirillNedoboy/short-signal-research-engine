"""One-source-process replay worker for C strategy admission fixtures."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from collections.abc import Mapping


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if dataclasses.is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(item) for item in value]
    return value


def _frame(rows: list[dict[str, Any]], volume_tail: list[float] | None):
    import pandas as pd

    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if volume_tail is not None:
        if len(volume_tail) != 5:
            raise ValueError("volume_tail must contain exactly five values")
        frame.loc[frame.index[-5:], "volume"] = volume_tail
        frame.loc[frame.index[-5:], "turnover"] = (
            frame.loc[frame.index[-5:], "close"] * frame.loc[frame.index[-5:], "volume"]
        )
    return frame.set_index("timestamp", drop=False)


def _datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    result = datetime.fromisoformat(value)
    return result if result.tzinfo is not None else result.replace(tzinfo=timezone.utc)


def _state(case: dict[str, Any]):
    from app.domain import EventState, EventStatus

    payload = dict(case["state"])
    payload["state"] = EventStatus(payload.get("state", "pullback_observed"))
    for name in ("event_start_time", "event_high_time", "pullback_detected_at", "signal_sent_at", "expires_at", "updated_at"):
        payload[name] = _datetime(payload.get(name))
    return EventState(**payload)


def _features(case: dict[str, Any], frame: Any, state: Any, config: Any, decision_at: datetime):
    from app.features.builder import FeatureBuilder

    derivatives = case.get("derivatives", {})
    liquidity = case.get("liquidity", {})
    features = FeatureBuilder().build(
        case["symbol"],
        frame,
        state=state,
        derivatives=derivatives,
        liquidity=liquidity,
        market_asof=decision_at,
    )
    overrides = dict(case.get("feature_overrides", {}))
    for name in ("asof", "market_asof", "last_high_time", "last_structural_close_time"):
        if name in overrides:
            overrides[name] = _datetime(overrides[name])
    return replace(features, **overrides)


def _registry(config: Any, frame: Any, case: dict[str, Any]):
    from app.strategies.baseline import BaselineAdapter
    from app.strategies.climax import ClimaxAdapter, LowVolumeExtensionFailureAdapter, VolumeClimaxUnwindAdapter
    from app.strategies.registry import StrategyRegistry
    from app.strategies.trapped_longs import TrappedLongsAdapter

    return StrategyRegistry(
        [
            BaselineAdapter(config),
            ClimaxAdapter(config, frame),
            VolumeClimaxUnwindAdapter(config, frame),
            LowVolumeExtensionFailureAdapter(config, frame),
            TrappedLongsAdapter(
                config,
                frame,
                attempt_created_at=_datetime(case.get("trapped_attempt_created_at")),
                confirmation_expires_at=_datetime(case.get("trapped_confirmation_expires_at")),
            ),
        ]
    )


def _select(evaluations: tuple[Any, ...], config: Any):
    from app.strategies.registry import CLIMAX_EXHAUSTION, TRAPPED_LONGS_REVERSAL

    by_type = {item.strategy_type: item for item in evaluations}
    ordered = []
    if config.climax_short_enabled:
        ordered.append(by_type.get(CLIMAX_EXHAUSTION))
    if config.trapped_longs_reversal_enabled:
        ordered.append(by_type.get(TRAPPED_LONGS_REVERSAL))
    ordered.append(by_type.get("BASELINE_PULLBACK"))
    return next((item for item in ordered if item is not None and item.actionable), None)


def _decision(state: Any, features: Any, zone: Any, evaluation: Any, fingerprint: str, *, fast_monitor: bool = False):
    from app.domain import SignalDecision, SignalType
    from app.strategies.registry import CLIMAX_EXHAUSTION, TRAPPED_LONGS_REVERSAL
    from app.signals.trapped_longs import trapped_longs_root_event_id

    metadata = dict(evaluation.strategy_metadata)
    event_id = state.event_id
    if evaluation.strategy_type == CLIMAX_EXHAUSTION:
        high = float(metadata.get("event_high") or features.price)
        low, high_zone = high * 0.97, high
        reasons = ["Climax exhaustion confirmed", "Failed continuation below event high"]
        lifecycle = "CLIMAX_SIGNAL_SENT"
        breakdown = {"climax_v1": float(evaluation.score)}
        metadata["veto_reasons"] = list(evaluation.vetoes)
    elif evaluation.strategy_type == TRAPPED_LONGS_REVERSAL:
        reference = float(metadata["breakout_reference"])
        low, high_zone = reference * 0.99, reference * 1.01
        reasons = list(evaluation.reasons)
        lifecycle = "TRAPPED_LONGS_ADMITTED"
        breakdown = {"registry_score": float(evaluation.score)}
        event_id = trapped_longs_root_event_id(features.symbol, state.event_id)
        metadata.setdefault("root_event_id", event_id)
    else:
        if zone is None:
            raise ValueError("baseline fixture requires a short zone")
        low, high_zone = zone.low, zone.high
        reasons = list(evaluation.reasons)
        lifecycle = state.state.value
        breakdown = {"registry_score": float(evaluation.score)}
        event_id = state.event_id
    metadata["strategy_config_fingerprint"] = fingerprint
    if fast_monitor:
        metadata["fast_monitor"] = True
    return SignalDecision(
        symbol=features.symbol,
        event_id=event_id,
        signal_type=(SignalType.WATCH if metadata.get("signal_type") == SignalType.WATCH.value else SignalType.CONFIRM),
        grade=evaluation.grade,
        score=evaluation.score,
        market_price=features.price,
        short_zone_low=low,
        short_zone_high=high_zone,
        signal_time=features.asof,
        reasons=reasons,
        risk_flags=list(evaluation.risk_flags),
        features_snapshot=dataclasses.asdict(features),
        score_breakdown=breakdown,
        decision_type="SIGNAL",
        actionable=True,
        lifecycle_state=lifecycle,
        blockers=list(evaluation.blockers),
        strategy_type=evaluation.strategy_type,
        strategy_subtype=evaluation.strategy_subtype,
        model_version=evaluation.model_version or "baseline-v1",
        strategy_metadata=metadata,
    )


def _apply_lifecycle_snapshots(state: Any, features: Any, evaluations: tuple[Any, ...], config: Any) -> None:
    """Mirror the historical C shadow lifecycle writes for envelope replay."""

    from app.signals.climax import advance_volume_climax_lifecycle
    from app.signals.trapped_longs import advance_trapped_longs_lifecycle
    from app.strategies.registry import CLIMAX_EXHAUSTION, TRAPPED_LONGS_REVERSAL

    snapshot = dict(state.event_features_snapshot or {})
    by_type = {item.strategy_type: item for item in evaluations}
    climax = by_type.get(CLIMAX_EXHAUSTION)
    if config.volume_climax_lifecycle_shadow_enabled and climax is not None:
        metadata = climax.strategy_metadata.get("volume_climax_metadata") or {}
        if climax.strategy_metadata.get("volume_climax_observed"):
            root = state.event_start_time or state.event_high_time or features.asof
            latest_high = float(snapshot.get("volume_climax_latest_high") or metadata.get("event_high") or features.price)
            latest_high_at = _datetime(snapshot.get("volume_climax_latest_high_at")) or state.event_high_time or root
            confirmation = _datetime(snapshot.get("volume_climax_confirmation_started_at")) or latest_high_at
            lifecycle = advance_volume_climax_lifecycle(
                root_created_at=root,
                latest_high=latest_high,
                latest_high_at=latest_high_at,
                confirmation_started_at=confirmation,
                last_observed_at=_datetime(snapshot.get("volume_climax_last_observed_at")) or root,
                event_revision=int(snapshot.get("volume_climax_event_revision") or 1),
                current_high=float(metadata.get("event_high") or features.price),
                observed_at=features.asof,
                closed_candles_after_high=int(metadata.get("closed_candles_after_high") or 0),
                min_closed_candles_after_high=config.volume_climax_min_closed_candles_after_high,
                max_lifetime_minutes=config.volume_climax_max_lifetime_minutes,
                confirmation_window_minutes=config.volume_climax_confirmation_window_minutes,
                price_acceleration_resumed=features.ret_5m > 0,
                active_short_squeeze=features.oi_change_pct is not None and features.oi_change_pct < 0 and features.ret_5m > 0,
                oi_continuation=features.oi_change_pct is None or features.oi_change_pct >= 0,
                rejection_ok=float(metadata.get("rejection_pct") or 0.0) >= config.volume_climax_min_rejection_pct,
                liquidity_ok=features.liquidity_available and not bool(metadata.get("liquidity_warning")),
                entry_distance_ok=float(metadata.get("entry_distance_below_high_pct") or 999.0) <= config.volume_climax_max_entry_distance_below_high_pct,
            )
            snapshot.update({
                "volume_climax_latest_high": lifecycle.latest_high,
                "volume_climax_latest_high_at": lifecycle.latest_high_at.isoformat(),
                "volume_climax_confirmation_started_at": lifecycle.confirmation_started_at.isoformat(),
                "volume_climax_last_observed_at": lifecycle.last_observed_at.isoformat(),
                "volume_climax_event_revision": lifecycle.event_revision,
                "volume_climax_lifecycle_state": lifecycle.state,
                "volume_climax_lifecycle_vetoes": list(lifecycle.veto_reasons),
            })
    trapped = by_type.get(TRAPPED_LONGS_REVERSAL)
    if config.trapped_longs_reversal_enabled and trapped is not None:
        metadata = trapped.strategy_metadata
        root = state.event_start_time or state.event_high_time or features.asof
        breakout = state.event_high_time or root
        lifecycle = advance_trapped_longs_lifecycle(
            root_created_at=root,
            breakout_at=breakout,
            observed_at=features.asof,
            event_revision=int(snapshot.get("trapped_longs_event_revision") or 1),
            breakout_confirmed="breakout_not_confirmed" not in trapped.vetoes,
            oi_confirmed="oi_missing" not in trapped.vetoes and "oi_below_threshold" not in trapped.vetoes,
            closed_candles=int(metadata.get("closed_structural_candles") or 0),
            close_below_reference=bool(metadata.get("close_below_breakout_reference")),
            failed_retest=bool(metadata.get("failed_retest_confirmed")),
            no_new_high=bool(metadata.get("no_new_high")),
            liquidity_ok=not any(reason in {"liquidity_unavailable", "liquidity_incomplete", "liquidity_block"} for reason in trapped.vetoes),
            rejection_ok="rejection_below_threshold" not in trapped.vetoes,
            max_lifetime_minutes=config.trapped_longs_max_lifetime_minutes,
        )
        snapshot.update({
            "trapped_longs_event_revision": lifecycle.event_revision,
            "trapped_longs_lifecycle_state": lifecycle.state,
            "trapped_longs_lifecycle_vetoes": list(lifecycle.veto_reasons),
            "trapped_longs_attempt_created_at": root.isoformat(),
            "trapped_longs_confirmation_expires_at": (
                breakout + timedelta(minutes=config.trapped_longs_max_lifetime_minutes)
            ).isoformat(),
        })
    state.event_features_snapshot = snapshot


def run(case: dict[str, Any], implementation: str) -> dict[str, Any]:
    from app.config import AppConfig
    from app.domain import ShortZone
    from app.strategies.contracts import StrategyContext

    config = AppConfig.model_validate(case["config"])
    decision_at = _datetime(case["decision_at"])
    assert decision_at is not None
    frame = _frame(case["frame_rows"], case.get("volume_tail"))
    state = _state(case)
    features = _features(case, frame, state, config, decision_at)
    zone_payload = case.get("short_zone")
    zone = ShortZone(**zone_payload) if zone_payload else None

    if implementation == "old":
        context = StrategyContext(
            symbol=features.symbol,
            event_id=state.event_id,
            event_state=state,
            features=features,
            short_zone=zone,
            decision_timestamp=decision_at,
            strategy_config_fingerprint=case["strategy_config_fingerprint"],
            input_quality="VALID",
        )
        evaluations = _registry(config, frame, case).evaluate_all(context)
        _apply_lifecycle_snapshots(state, features, evaluations, config)
        selected = _select(evaluations, config)
        decision = _decision(state, features, zone, selected, case["strategy_config_fingerprint"]) if selected is not None else None
        blocked = None if decision is not None else "NO_ACTIONABLE_STRATEGY"
    else:
        from app.runtime.pipeline import LaneCStrategyPipeline

        pipeline = LaneCStrategyPipeline(
            config,
            config_fingerprint=case["strategy_config_fingerprint"],
        )
        result = pipeline.evaluate_envelope(
            state=state,
            features=features,
            frame_1m=frame,
            short_zone=zone,
            decision_at=decision_at,
            trapped_attempt_created_at=_datetime(case.get("trapped_attempt_created_at")),
            trapped_confirmation_expires_at=_datetime(case.get("trapped_confirmation_expires_at")),
        )
        evaluations = result.evaluations
        decision = result.decision
        blocked = result.blocked_reason
    return {
        "strategy": case["name"],
        "features": _jsonable(features),
        "state": _jsonable(state),
        "evaluations": [_jsonable(item) for item in evaluations],
        "decision": _jsonable(decision),
        "blocked_reason": blocked,
        "actionable": bool(decision and decision.actionable),
        "identity": (
            {
                "symbol": decision.symbol,
                "event_id": decision.event_id,
                "strategy_type": decision.strategy_type,
                "strategy_subtype": decision.strategy_subtype,
                "model_version": decision.model_version,
            }
            if decision is not None
            else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--implementation", choices=("old", "candidate"), required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.source_root).resolve()))
    case = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
    print(json.dumps(run(case, args.implementation), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
