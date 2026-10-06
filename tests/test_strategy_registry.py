from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.strategies.contracts import StrategyContext, StrategyEvaluation, StrategyEvaluationError
from app.strategies.registry import (
    REQUIRED_STRATEGY_TYPES,
    StrategyRegistry,
)
from app.strategies.climax import (
    ClimaxAdapter,
    LowVolumeExtensionFailureAdapter,
    VolumeClimaxUnwindAdapter,
)
from app.strategies.trapped_longs import TrappedLongsAdapter


class Adapter:
    def __init__(self, strategy_type: str, *, score: int = 1) -> None:
        self.strategy_type = strategy_type
        self.score = score

    def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
        return StrategyEvaluation(
            strategy_type=self.strategy_type,
            strategy_subtype=self.strategy_type,
            model_version="test-v1",
            actionable=False,
            score=self.score,
            grade="C",
            reasons=(), blockers=(), vetoes=(), risk_flags=(), strategy_metadata={},
            symbol=context.symbol,
            event_id=context.event_id,
            decision_timestamp=context.decision_timestamp,
        )


def adapter_set() -> list[Adapter]:
    return [Adapter(identifier) for identifier in REQUIRED_STRATEGY_TYPES]


def context() -> StrategyContext:
    from app.domain import EventState, EventStatus, SymbolFeatures

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    state = EventState(
        symbol="BTCUSDT", event_id="event-1", state=EventStatus.PULLBACK_OBSERVED,
        event_start_time=now, event_high=100.0, event_high_time=now,
        pullback_detected_at=now, signal_sent_at=None, expires_at=now,
        updated_at=now,
    )
    features = SymbolFeatures(
        symbol="BTCUSDT", asof=now, price=99.0, ret_5m=0.0, ret_15m=0.0,
        ret_1h=0.0, ret_4h=0.0, vwap=99.0, dist_to_vwap_pct=0.0,
        ema20=99.0, dist_to_ema20_pct=0.0, dist_to_ema20_atr=0.0,
        rsi_15m=50.0, upper_wick_ratio=0.0, lower_wick_ratio=0.0,
        body_pct=0.0, rejection_from_high_pct=1.0, close_position_in_range=0.5,
        vol_zscore_30m=0.0, vol_zscore_1h=0.0, atr_14=1.0, range_atr_ratio=1.0,
        oi_change_pct=0.0, derivatives_status="OK", latest_failed_retest=False,
        current_volume=1.0, market_asof=now, last_high_time=now,
        last_structural_close_time=now,
    )
    return StrategyContext(
        symbol="BTCUSDT", event_id="event-1", event_state=state, features=features,
        short_zone=None, decision_timestamp=now, strategy_config_fingerprint="hash",
        input_quality="VALID",
    )


def test_required_identifiers_are_present_in_stable_order():
    registry = StrategyRegistry(adapter_set())
    assert registry.identifiers == REQUIRED_STRATEGY_TYPES
    assert tuple(registry) == REQUIRED_STRATEGY_TYPES


@pytest.mark.parametrize(
    "adapters",
    [
        [],
        adapter_set()[:-1],
        [adapter for adapter in adapter_set() if adapter.strategy_type != "CLIMAX_EXHAUSTION"],
    ],
    ids=["empty", "subset", "missing-branch"],
)
def test_incomplete_registration_is_rejected(adapters):
    with pytest.raises(StrategyEvaluationError, match="complete strategy set"):
        StrategyRegistry(adapters)


def test_extra_identifier_is_rejected():
    adapters = adapter_set()
    adapters.append(Adapter("UNSUPPORTED_STRATEGY"))
    with pytest.raises(StrategyEvaluationError, match="unknown"):
        StrategyRegistry(adapters)


def test_duplicate_identifiers_are_rejected():
    adapters = adapter_set()
    with pytest.raises(StrategyEvaluationError, match="duplicate"):
        StrategyRegistry([adapters[0], adapters[0]])


def test_unknown_lookup_and_malformed_registration_fail_closed():
    registry = StrategyRegistry(adapter_set())
    with pytest.raises(StrategyEvaluationError, match="unknown"):
        registry.get("NOT_REGISTERED")
    with pytest.raises(StrategyEvaluationError, match="identifier"):
        StrategyRegistry([Adapter("")])


@pytest.mark.parametrize("adapter", [object(), type("Bad", (), {"strategy_type": "BASELINE_PULLBACK"})()])
def test_invalid_adapter_contract_is_rejected(adapter):
    with pytest.raises(StrategyEvaluationError):
        StrategyRegistry([adapter])


def test_evaluate_all_returns_canonical_evaluations_in_registry_order():
    registry = StrategyRegistry(adapter_set())
    evaluations = registry.evaluate_all(context())
    assert isinstance(evaluations, tuple)
    assert tuple(item.strategy_type for item in evaluations) == REQUIRED_STRATEGY_TYPES
    assert all(isinstance(item, StrategyEvaluation) for item in evaluations)


def test_malformed_evaluation_cannot_become_actionable():
    class Malformed(Adapter):
        def evaluate(self, context):
            return object()

    with pytest.raises(StrategyEvaluationError, match="StrategyEvaluation"):
        adapters = adapter_set()
        adapters[0] = Malformed("BASELINE_PULLBACK")
        StrategyRegistry(adapters).evaluate_all(context())


def test_concrete_adapters_cover_exact_registry_set():
    import pandas as pd

    adapters = [
        Adapter("BASELINE_PULLBACK"),
        ClimaxAdapter(None, pd.DataFrame()),
        VolumeClimaxUnwindAdapter(None, pd.DataFrame()),
        LowVolumeExtensionFailureAdapter(None, pd.DataFrame()),
        TrappedLongsAdapter(None, pd.DataFrame()),
    ]

    assert tuple(adapter.strategy_type for adapter in adapters) == REQUIRED_STRATEGY_TYPES
    registry = StrategyRegistry(adapters)
    assert tuple(registry.get(identifier).strategy_type for identifier in REQUIRED_STRATEGY_TYPES) == REQUIRED_STRATEGY_TYPES
