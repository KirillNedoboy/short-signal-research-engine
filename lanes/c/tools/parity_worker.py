"""Run one deterministic C semantic replay in an isolated source process.

The parent parity command invokes this worker once for the frozen C release
and once for the candidate ``src`` tree.  Keeping the source roots in separate
processes prevents Python's ``app`` package cache from mixing the two builds.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


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
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _frame(rows: list[dict[str, Any]]):
    import pandas as pd

    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame.set_index("timestamp", drop=False)


def _evaluation_payload(evaluation: Any) -> dict[str, Any]:
    return _jsonable(evaluation)


def _result_payload(*, features: Any, state: Any, decision: Any, evaluations: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "features": _jsonable(features),
        "state": _jsonable(state),
        "decision": _jsonable(decision),
        "evaluations": [_evaluation_payload(item) for item in evaluations],
    }


def _registry(config: Any, frame: Any) -> Any:
    from app.strategies.baseline import BaselineAdapter
    from app.strategies.climax import (
        ClimaxAdapter,
        LowVolumeExtensionFailureAdapter,
        VolumeClimaxUnwindAdapter,
    )
    from app.strategies.registry import StrategyRegistry
    from app.strategies.trapped_longs import TrappedLongsAdapter

    return StrategyRegistry(
        [
            BaselineAdapter(config),
            ClimaxAdapter(config, frame),
            VolumeClimaxUnwindAdapter(config, frame),
            LowVolumeExtensionFailureAdapter(config, frame),
            TrappedLongsAdapter(config, frame),
        ]
    )


def _old_run(fixture: dict[str, Any]) -> dict[str, Any]:
    import pandas as pd

    from app.baseline.lifecycle import BaselineLifecycle
    from app.config import AppConfig
    from app.domain import SignalType
    from app.features.builder import FeatureBuilder
    from app.replay.market import CandleObservationKind, NormalizedMarketObservation
    from app.strategies.contracts import StrategyContext

    config = AppConfig.model_validate(fixture["config"])
    symbol = fixture["symbol"]
    decision_at = datetime.fromisoformat(fixture["decision_at"])
    frames = [_frame(rows) for rows in fixture["frames"]]
    derivatives = fixture["derivatives"]
    liquidity = fixture["liquidity"]
    features_builder = FeatureBuilder()
    lifecycle = BaselineLifecycle(config, features_builder)
    first_frame = frames[0]
    first_features = features_builder.build(
        symbol,
        first_frame,
        derivatives=derivatives,
        market_asof=decision_at,
    )
    observation = NormalizedMarketObservation(
        symbol,
        decision_at,
        CandleObservationKind.PARTIAL_ASOF_CANDLE,
        first_frame,
    )
    first = lifecycle.detect_event(observation, features=first_features)
    first_payload = _result_payload(
        features=first.features,
        state=first.state,
        decision=None,
        evaluations=(),
    )
    state = first.state
    if state is None:
        return {"steps": [first_payload], "actionable": False, "signal_identity": None}

    second_frame = frames[1]
    second_features = features_builder.build(
        symbol,
        second_frame,
        state=state,
        derivatives=derivatives,
        liquidity=liquidity,
        market_asof=decision_at,
    )
    second_observation = NormalizedMarketObservation(
        symbol,
        decision_at,
        CandleObservationKind.PARTIAL_ASOF_CANDLE,
        second_frame,
    )
    advanced = lifecycle.advance_active_event(
        second_observation,
        state,
        derivatives=derivatives,
        liquidity=liquidity,
        features=second_features,
    )
    second_state = advanced.state
    evaluations: tuple[Any, ...] = ()
    decision = None
    if second_state is not None and advanced.features is not None and advanced.short_zone is not None:
        context = StrategyContext(
            symbol=symbol,
            event_id=second_state.event_id,
            event_state=second_state,
            features=advanced.features,
            short_zone=advanced.short_zone,
            decision_timestamp=decision_at,
            strategy_config_fingerprint=fixture["strategy_config_fingerprint"],
            input_quality="VALID",
        )
        evaluations = _registry(config, second_frame).evaluate_all(context)
        baseline = next(
            (item for item in evaluations if item.strategy_type == "BASELINE_PULLBACK"),
            None,
        )
        if baseline is not None and baseline.actionable:
            from dataclasses import asdict
            from app.domain import SignalDecision

            decision = SignalDecision(
                symbol=symbol,
                event_id=second_state.event_id,
                signal_type=(
                    SignalType.WATCH
                    if baseline.strategy_metadata.get("signal_type") == SignalType.WATCH.value
                    else SignalType.CONFIRM
                ),
                grade=baseline.grade,
                score=baseline.score,
                market_price=advanced.features.price,
                short_zone_low=advanced.short_zone.low,
                short_zone_high=advanced.short_zone.high,
                signal_time=decision_at,
                reasons=list(baseline.reasons),
                risk_flags=list(baseline.risk_flags),
                features_snapshot=asdict(advanced.features),
                score_breakdown={"registry_score": float(baseline.score)},
                decision_type="SIGNAL",
                actionable=baseline.actionable,
                lifecycle_state=second_state.state.value,
                blockers=list(baseline.blockers),
                strategy_type=baseline.strategy_type,
                strategy_subtype=baseline.strategy_subtype,
                model_version=baseline.model_version or "baseline-v1",
                strategy_metadata=dict(baseline.strategy_metadata),
            )
    second_payload = _result_payload(
        features=advanced.features,
        state=second_state,
        decision=decision,
        evaluations=evaluations,
    )
    identity = None
    if decision is not None:
        identity = {
            "symbol": decision.symbol,
            "event_id": decision.event_id,
            "strategy_type": decision.strategy_type,
            "strategy_subtype": decision.strategy_subtype,
            "model_version": decision.model_version,
        }
    return {
        "steps": [first_payload, second_payload],
        "actionable": bool(decision and decision.actionable),
        "signal_identity": identity,
    }


def _candidate_run(fixture: dict[str, Any]) -> dict[str, Any]:
    from app.config import AppConfig
    from app.runtime.pipeline import LaneCStrategyPipeline

    config = AppConfig.model_validate(fixture["config"])
    symbol = fixture["symbol"]
    decision_at = datetime.fromisoformat(fixture["decision_at"])
    frames = [_frame(rows) for rows in fixture["frames"]]
    pipeline = LaneCStrategyPipeline(
        config,
        config_fingerprint=fixture["strategy_config_fingerprint"],
    )
    first = pipeline.evaluate(
        symbol,
        frames[0],
        None,
        derivatives=fixture["derivatives"],
        liquidity=fixture["liquidity"],
        decision_at=decision_at,
    )
    first_payload = _result_payload(
        features=first.features,
        state=first.state,
        decision=first.decision,
        evaluations=first.evaluations,
    )
    if first.state is None:
        return {"steps": [first_payload], "actionable": False, "signal_identity": None}
    second = pipeline.evaluate(
        symbol,
        frames[1],
        first.state,
        derivatives=fixture["derivatives"],
        liquidity=fixture["liquidity"],
        decision_at=decision_at,
    )
    second_payload = _result_payload(
        features=second.features,
        state=second.state,
        decision=second.decision,
        evaluations=second.evaluations,
    )
    identity = None
    if second.decision is not None:
        identity = {
            "symbol": second.decision.symbol,
            "event_id": second.decision.event_id,
            "strategy_type": second.decision.strategy_type,
            "strategy_subtype": second.decision.strategy_subtype,
            "model_version": second.decision.model_version,
        }
    return {
        "steps": [first_payload, second_payload],
        "actionable": bool(second.decision and second.decision.actionable),
        "signal_identity": identity,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--implementation", choices=("old", "candidate"), required=True)
    args = parser.parse_args()
    source_root = Path(args.source_root).resolve()
    sys.path.insert(0, str(source_root))
    fixture = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
    result = _old_run(fixture) if args.implementation == "old" else _candidate_run(fixture)
    print(json.dumps(_jsonable(result), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
