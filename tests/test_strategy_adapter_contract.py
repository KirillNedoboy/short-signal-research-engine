from __future__ import annotations

from dataclasses import fields
from datetime import datetime, timezone

import pytest

from app.config import AppConfig
from app.domain import EventState, EventStatus, ShortZone, SymbolFeatures
from app.replay.config import strategy_config_fingerprint
from app.signals.climax import evaluate_climax
from app.signals.engine import SignalEngine
from app.signals.trapped_longs import evaluate_trapped_longs_reversal
from tests.conftest import _make_event_state, _make_features, _make_frame
from tests.test_climax_engine import _config as climax_config
from tests.test_trapped_longs import config as trapped_config
from tests.test_trapped_longs import features as trapped_features
from tests.test_trapped_longs import frame as trapped_frame
from tests.test_trapped_longs import state as trapped_state
from tests.test_trapped_longs import T0


def test_strategy_context_is_immutable_and_uses_canonical_inputs():
    from app.strategies.contracts import StrategyContext

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol,
        event_id=state.event_id,
        event_state=state,
        features=features,
        short_zone=zone,
        decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash",
        input_quality="VALID",
    )

    assert context.symbol == state.symbol
    assert context.event_state is not state
    assert isinstance(context.event_state, EventState)
    assert context.canonical_payload()["event_state"]["state"] == "pullback_observed"
    with pytest.raises((AttributeError, TypeError)):
        context.symbol = "ETHUSDT"


def test_strategy_context_freezes_nested_canonical_inputs():
    from app.strategies.contracts import StrategyContext

    state, features, zone, _ = _baseline_fixture()
    state.event_features_snapshot["nested"] = {"values": [1]}
    features.derivatives_reasons.append("original")
    context = StrategyContext(
        symbol=state.symbol,
        event_id=state.event_id,
        event_state=state,
        features=features,
        short_zone=zone,
        decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash",
        input_quality="VALID",
    )

    with pytest.raises((AttributeError, TypeError)):
        context.event_state.event_id = "changed"
    with pytest.raises(TypeError):
        context.event_state.event_features_snapshot["nested"]["values"][0] = 2
    with pytest.raises(AttributeError):
        context.features.derivatives_reasons.append("changed")
    with pytest.raises(AttributeError):
        context.short_zone.low = 1.0


def test_strategy_context_requires_ordered_closed_candle_timestamps():
    from app.strategies.contracts import StrategyContext, StrategyEvaluationError

    state, features, zone, _ = _baseline_fixture()
    for updates in (
        {"asof": BASELINE_ASOF.replace(tzinfo=None)},
        {"market_asof": None},
        {"market_asof": BASELINE_ASOF.replace(minute=4)},
        {"last_high_time": BASELINE_ASOF.replace(tzinfo=None)},
        {"last_structural_close_time": BASELINE_ASOF.replace(minute=6)},
    ):
        values = {field.name: getattr(features, field.name) for field in fields(features)}
        values.update(updates)
        invalid_features = SymbolFeatures(**values)
        with pytest.raises(StrategyEvaluationError):
            StrategyContext(
                symbol=state.symbol,
                event_id=state.event_id,
                event_state=state,
                features=invalid_features,
                short_zone=zone,
                decision_timestamp=BASELINE_ASOF,
                strategy_config_fingerprint="config-hash",
                input_quality="VALID",
            )


def test_strategy_context_accepts_blocked_quality_and_validates_short_zone():
    from app.strategies.contracts import StrategyContext, StrategyEvaluationError

    state, features, _, _ = _baseline_fixture()
    for quality in ("UNKNOWN", "FAILED", "INCOMPLETE", "STALE", "DISCONTINUOUS"):
        context = StrategyContext(
            symbol=state.symbol,
            event_id=state.event_id,
            event_state=state,
            features=features,
            short_zone=None,
            decision_timestamp=BASELINE_ASOF,
            strategy_config_fingerprint="config-hash",
            input_quality=quality.lower(),
        )
        assert context.input_quality == quality
    for quality in ("not-a-quality", ""):
        with pytest.raises(StrategyEvaluationError):
            StrategyContext(
                symbol=state.symbol,
                event_id=state.event_id,
                event_state=state,
                features=features,
                short_zone=None,
                decision_timestamp=BASELINE_ASOF,
                strategy_config_fingerprint="config-hash",
                input_quality=quality,
            )
    for zone in (
        ShortZone(low=0.0, high=114.0, mode="event_range"),
        ShortZone(low=115.0, high=114.0, mode="event_range"),
        ShortZone(low=float("inf"), high=114.0, mode="event_range"),
        ShortZone(low=110.0, high=114.0, mode=""),
        ShortZone(low=110.0, high=114.0, mode="range"),
    ):
        with pytest.raises(StrategyEvaluationError):
            StrategyContext(
                symbol=state.symbol,
                event_id=state.event_id,
                event_state=state,
                features=features,
                short_zone=zone,
                decision_timestamp=BASELINE_ASOF,
                strategy_config_fingerprint="config-hash",
                input_quality="VALID",
            )
    with pytest.raises(StrategyEvaluationError):
        StrategyContext(
            symbol=state.symbol,
            event_id=state.event_id,
            event_state=state,
            features=features,
            short_zone=object(),
            decision_timestamp=BASELINE_ASOF,
            strategy_config_fingerprint="config-hash",
            input_quality="VALID",
        )


def test_blocked_actionable_evaluation_is_forced_fail_closed():
    from app.strategies.contracts import StrategyEvaluation, StrategyEvaluationError

    kwargs = dict(
        strategy_type="BASELINE_PULLBACK",
        strategy_subtype=None,
        model_version="baseline-v1",
        actionable=True,
        score=80,
        grade="A",
        reasons=["ok"],
        blockers=[],
        vetoes=[],
        risk_flags=[],
        strategy_metadata={},
        symbol="ONTUSDT",
        event_id="event-1",
        decision_timestamp=BASELINE_ASOF,
        input_quality="INCOMPLETE",
    )
    evaluation = StrategyEvaluation(**kwargs)
    assert evaluation.actionable is False
    assert evaluation.input_quality == "INCOMPLETE"


def test_strategy_evaluation_defaults_omitted_baseline_subtype():
    from app.strategies.contracts import StrategyEvaluation

    evaluation = StrategyEvaluation(
        strategy_type="BASELINE_PULLBACK",
        model_version="baseline-v1",
        actionable=False,
        score=0,
        grade="C",
        reasons=[], blockers=[], vetoes=[], risk_flags=[], strategy_metadata={},
        symbol="ONTUSDT", event_id="event-1", decision_timestamp=BASELINE_ASOF,
    )
    assert evaluation.strategy_subtype == "BASELINE_PULLBACK"


def test_baseline_empty_subtype_is_rejected():
    from app.strategies.contracts import StrategyEvaluation, StrategyEvaluationError

    with pytest.raises(StrategyEvaluationError):
        StrategyEvaluation(
            strategy_type="BASELINE_PULLBACK",
            strategy_subtype="",
            model_version="baseline-v1",
            actionable=False,
            score=0,
            grade="C",
            reasons=[],
            blockers=[],
            vetoes=[],
            risk_flags=[],
            strategy_metadata={},
            symbol="ONTUSDT",
            event_id="event-1",
            decision_timestamp=BASELINE_ASOF,
        )


def test_strategy_evaluation_normalizes_baseline_and_legacy_version():
    from app.strategies.contracts import StrategyEvaluation

    evaluation = StrategyEvaluation(
        strategy_type="BASELINE_PULLBACK",
        strategy_subtype=None,
        model_version=None,
        actionable=True,
        score=80,
        grade="A",
        reasons=["ok"],
        blockers=[],
        vetoes=[],
        risk_flags=[],
        strategy_metadata={"threshold": 1.0},
        symbol="ONTUSDT",
        event_id="event-1",
        decision_timestamp=BASELINE_ASOF,
    )

    assert evaluation.strategy_subtype == "BASELINE_PULLBACK"
    assert evaluation.model_version == "LEGACY_MODEL_VERSION"
    assert evaluation.strategy_metadata["original_model_version"] is None
    assert evaluation.canonical_payload()["strategy_subtype"] == "BASELINE_PULLBACK"


def test_strategy_evaluation_rejects_missing_non_baseline_subtype_and_bad_metadata():
    from app.strategies.contracts import StrategyEvaluation, StrategyEvaluationError

    kwargs = dict(
        strategy_type="CLIMAX_EXHAUSTION",
        strategy_subtype=None,
        model_version="climax-v1",
        actionable=False,
        score=0,
        grade="C",
        reasons=[],
        blockers=[],
        vetoes=[],
        risk_flags=[],
        symbol="ONTUSDT",
        event_id="event-1",
        decision_timestamp=BASELINE_ASOF,
    )
    with pytest.raises(StrategyEvaluationError):
        StrategyEvaluation(**kwargs, strategy_metadata={})
    with pytest.raises(StrategyEvaluationError):
        StrategyEvaluation(**{**kwargs, "strategy_subtype": "CLIMAX_UNWIND"}, strategy_metadata={"bad": object()})


@pytest.mark.parametrize("field", ["reasons", "blockers", "vetoes", "risk_flags"])
@pytest.mark.parametrize("value", [None, 1, 1.5, object()])
def test_strategy_evaluation_wraps_non_iterable_text_fields(field, value):
    from app.strategies.contracts import StrategyEvaluation, StrategyEvaluationError

    kwargs = dict(
        strategy_type="BASELINE_PULLBACK",
        model_version="baseline-v1",
        actionable=False,
        score=0,
        grade="C",
        reasons=[], blockers=[], vetoes=[], risk_flags=[], strategy_metadata={},
        symbol="ONTUSDT", event_id="event-1", decision_timestamp=BASELINE_ASOF,
    )
    kwargs[field] = value
    with pytest.raises(StrategyEvaluationError):
        StrategyEvaluation(**kwargs)


def test_strategy_evaluation_preserves_legacy_positional_field_order():
    from app.strategies.contracts import StrategyEvaluation

    evaluation = StrategyEvaluation(
        "CLIMAX_EXHAUSTION", "CLIMAX_UNWIND", "climax-v1", False, 12, "B",
        ["reason"], ["blocker"], ["veto"], ["risk"], {"source": "legacy"},
        "ONTUSDT", "event-1", BASELINE_ASOF,
    )

    assert evaluation.strategy_type == "CLIMAX_EXHAUSTION"
    assert evaluation.strategy_subtype == "CLIMAX_UNWIND"
    assert evaluation.model_version == "climax-v1"
    assert evaluation.actionable is False
    assert evaluation.score == 12
    assert evaluation.grade == "B"
    assert evaluation.reasons == ("reason",)
    assert evaluation.blockers == ("blocker",)
    assert evaluation.vetoes == ("veto",)
    assert evaluation.risk_flags == ("risk",)
    assert evaluation.strategy_metadata["source"] == "legacy"
    assert evaluation.symbol == "ONTUSDT"
    assert evaluation.event_id == "event-1"
    assert evaluation.decision_timestamp == BASELINE_ASOF


def test_strategy_adapter_protocol_and_boundary_accept_a_valid_fake_adapter():
    from app.strategies.adapters import StrategyAdapter, normalize_strategy_evaluation
    from app.strategies.contracts import StrategyContext, StrategyEvaluation

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash", input_quality="VALID",
    )

    class FakeAdapter:
        strategy_type = "BASELINE_PULLBACK"

        def evaluate(self, received_context: StrategyContext) -> StrategyEvaluation:
            assert received_context is context
            return StrategyEvaluation(
                strategy_type=self.strategy_type, model_version="fake-v1",
                actionable=True, score=80, grade="A", reasons=["ok"],
                blockers=[], vetoes=[], risk_flags=[], strategy_metadata={},
                symbol=context.symbol, event_id=context.event_id,
                decision_timestamp=context.decision_timestamp,
            )

    adapter = FakeAdapter()
    assert isinstance(adapter, StrategyAdapter)
    evaluation = normalize_strategy_evaluation(adapter, context, adapter.evaluate(context))
    assert evaluation.actionable is True


def test_strategy_adapter_boundary_rejects_missing_or_non_callable_evaluate():
    from app.strategies.adapters import normalize_strategy_evaluation
    from app.strategies.contracts import StrategyContext, StrategyEvaluationError

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash", input_quality="VALID",
    )

    class MissingEvaluate:
        strategy_type = "BASELINE_PULLBACK"

    class NonCallableEvaluate:
        strategy_type = "BASELINE_PULLBACK"
        evaluate = None

    for adapter in (MissingEvaluate(), NonCallableEvaluate()):
        with pytest.raises(StrategyEvaluationError, match="callable evaluate"):
            normalize_strategy_evaluation(adapter, context, object())


def test_strategy_adapter_boundary_rejects_malformed_output():
    from app.strategies.adapters import normalize_strategy_evaluation
    from app.strategies.contracts import StrategyContext, StrategyEvaluation, StrategyEvaluationError

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash", input_quality="VALID",
    )

    class FakeAdapter:
        strategy_type = "BASELINE_PULLBACK"

        def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
            raise AssertionError("normalizer test must not evaluate adapter")

    with pytest.raises(StrategyEvaluationError, match="StrategyEvaluation"):
        normalize_strategy_evaluation(FakeAdapter(), context, {"actionable": True})


@pytest.mark.parametrize("identifier", ["", " baseline_pullback ", "BASELINE PULLBACK", "bad-type", None, 1])
def test_strategy_adapter_boundary_rejects_invalid_identifiers(identifier):
    from app.strategies.adapters import validate_strategy_identifier
    from app.strategies.contracts import StrategyEvaluationError

    with pytest.raises(StrategyEvaluationError):
        validate_strategy_identifier(identifier)


@pytest.mark.parametrize("strategy_type", [None, "", "bad-type", "BASELINE PULLBACK", 1])
def test_strategy_adapter_boundary_rejects_invalid_adapter_strategy_type(strategy_type):
    from app.strategies.adapters import normalize_strategy_evaluation
    from app.strategies.contracts import StrategyContext, StrategyEvaluationError

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash", input_quality="VALID",
    )

    class FakeAdapter:
        def evaluate(self, context: StrategyContext):
            raise AssertionError("invalid adapter metadata must fail before evaluation")

    adapter = FakeAdapter()
    if strategy_type is not None:
        adapter.strategy_type = strategy_type
    with pytest.raises(StrategyEvaluationError, match="strategy_type"):
        normalize_strategy_evaluation(adapter, context, object())


def test_strategy_adapter_boundary_forces_blocked_context_actionable_false():
    from app.strategies.adapters import normalize_strategy_evaluation
    from app.strategies.contracts import StrategyContext, StrategyEvaluation

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash", input_quality="INCOMPLETE",
    )

    class FakeAdapter:
        strategy_type = "BASELINE_PULLBACK"

        def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
            raise AssertionError("normalizer test must not evaluate adapter")

    evaluation = StrategyEvaluation(
        strategy_type="BASELINE_PULLBACK", model_version="fake-v1",
        actionable=True, score=80, grade="A", reasons=["ok"], blockers=[],
        vetoes=[], risk_flags=[], strategy_metadata={}, symbol=context.symbol,
        event_id=context.event_id, decision_timestamp=context.decision_timestamp,
        input_quality="VALID",
    )
    normalized = normalize_strategy_evaluation(FakeAdapter(), context, evaluation)
    assert normalized.actionable is False
    assert normalized.input_quality == "VALID"


def test_strategy_adapter_boundary_forces_invalid_actionable_data_closed():
    from app.strategies.adapters import normalize_strategy_evaluation
    from app.strategies.contracts import StrategyContext, StrategyEvaluation, StrategyEvaluationError

    state, features, zone, _ = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint="config-hash", input_quality="VALID",
    )

    class FakeAdapter:
        strategy_type = "BASELINE_PULLBACK"

        def evaluate(self, context: StrategyContext) -> StrategyEvaluation:
            raise AssertionError("normalizer test must not evaluate adapter")

    evaluation = StrategyEvaluation(
        strategy_type="BASELINE_PULLBACK", model_version="fake-v1",
        actionable=True, score=1, grade="C", reasons=[], blockers=[],
        vetoes=[], risk_flags=[], strategy_metadata={}, symbol=context.symbol,
        event_id=context.event_id, decision_timestamp=context.decision_timestamp,
        input_quality="INCOMPLETE",
    )
    assert normalize_strategy_evaluation(FakeAdapter(), context, evaluation).actionable is False

    with pytest.raises(StrategyEvaluationError):
        normalize_strategy_evaluation(FakeAdapter(), context, StrategyEvaluation(
            strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="CLIMAX_UNWIND",
            model_version="fake-v1", actionable=False, score=0, grade="C",
            reasons=[], blockers=[], vetoes=[], risk_flags=[],
            strategy_metadata={}, symbol="OTHER", event_id=context.event_id,
            decision_timestamp=context.decision_timestamp,
        ))


def test_strategy_adapter_module_has_no_runtime_or_persistence_dependencies():
    from app.strategies import adapters

    source = __import__("inspect").getsource(adapters)
    for forbidden in ("sqlalchemy", "telegram", "exchange", "order", "execution", "repository"):
        assert forbidden not in source.lower()


def test_baseline_adapter_preserves_accepted_legacy_evaluation():
    from app.replay.evaluator import evaluate_strategy_input
    from app.replay.contracts import BaselineStrategyInput
    from app.strategies.baseline import BaselineAdapter
    from app.strategies.contracts import StrategyContext

    state, features, zone, config = _baseline_fixture()
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="VALID",
    )
    legacy = evaluate_strategy_input(
        BaselineStrategyInput(state, features, zone, BASELINE_ASOF), config,
    )
    evaluation = BaselineAdapter(config).evaluate(context)

    assert evaluation.strategy_type == "BASELINE_PULLBACK"
    assert evaluation.strategy_subtype == "BASELINE_PULLBACK"
    assert evaluation.model_version == legacy.decision.model_version
    assert evaluation.actionable == legacy.decision.actionable
    assert evaluation.score == legacy.decision.score
    assert evaluation.grade == legacy.decision.grade
    assert evaluation.reasons == tuple(legacy.decision.reasons)
    assert evaluation.blockers == tuple(legacy.decision.blockers)
    assert evaluation.vetoes == tuple(legacy.reject_reasons)
    assert evaluation.risk_flags == tuple(legacy.decision.risk_flags)
    assert dict(evaluation.strategy_metadata) == {
        **legacy.decision.strategy_metadata,
        "strategy_config_fingerprint": context.strategy_config_fingerprint,
    }
    assert evaluation.symbol == state.symbol
    assert evaluation.event_id == state.event_id
    assert evaluation.decision_timestamp == BASELINE_ASOF
    assert evaluation.input_quality == "VALID"


def test_baseline_adapter_preserves_blocked_and_missing_input_behavior():
    from dataclasses import replace
    from app.replay.evaluator import evaluate_strategy_input
    from app.replay.contracts import BaselineStrategyInput
    from app.strategies.baseline import BaselineAdapter
    from app.strategies.contracts import StrategyContext

    state, features, zone, config = _baseline_fixture()
    blocked_features = replace(
        features, dist_to_vwap_pct=4.0, vol_zscore_30m=0.2,
        pullback_from_high_pct=1.0, rejection_from_high_pct=0.2,
        upper_wick_ratio=0.05,
    )
    legacy = evaluate_strategy_input(
        BaselineStrategyInput(state, blocked_features, zone, BASELINE_ASOF), config,
    )
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=blocked_features, short_zone=zone, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="VALID",
    )
    blocked = BaselineAdapter(config).evaluate(context)
    assert blocked.actionable is False
    assert blocked.score == legacy.score
    assert blocked.grade == legacy.grade
    assert blocked.vetoes == tuple(legacy.reject_reasons)
    assert blocked.blockers == tuple(legacy.blockers)

    missing_context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=None, decision_timestamp=BASELINE_ASOF,
        strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="MISSING",
    )
    missing = BaselineAdapter(config).evaluate(missing_context)
    assert missing.actionable is False
    assert missing.input_quality == "MISSING"
    assert missing.vetoes == ("short_zone_missing",)
    assert missing.strategy_metadata["strategy_config_fingerprint"] == strategy_config_fingerprint(config)


BASELINE_ASOF = datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc)


@pytest.mark.parametrize("subtype", ["VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"])
def test_climax_adapter_preserves_selected_branch_and_legacy_payload(subtype):
    from dataclasses import replace
    from app.signals.climax import evaluate_climax_bundle
    from app.strategies.climax import ClimaxAdapter
    from app.strategies.contracts import StrategyContext

    state, features, frame, config = _climax_fixture(subtype)
    decision_timestamp = features.asof
    features = replace(features, market_asof=features.asof)
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=None, decision_timestamp=decision_timestamp,
        strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="VALID",
    )
    legacy = evaluate_climax_bundle(state, features, frame, config)
    evaluation = ClimaxAdapter(config, frame).evaluate(context)

    assert evaluation.strategy_type == "CLIMAX_EXHAUSTION"
    assert evaluation.strategy_subtype == legacy.selected.subtype
    assert evaluation.actionable == legacy.selected.actionable
    assert evaluation.score == legacy.selected.score
    assert evaluation.grade == legacy.selected.grade
    assert evaluation.blockers == tuple(legacy.selected.veto_reasons)
    assert evaluation.vetoes == tuple(legacy.selected.veto_reasons)
    assert evaluation.risk_flags == tuple(legacy.selected.data_quality)
    from app.replay.canonical import canonicalize

    assert evaluation.canonical_payload()["strategy_metadata"] == canonicalize({
        **legacy.selected.metadata,
        "strategy_config_fingerprint": context.strategy_config_fingerprint,
    })
    assert evaluation.decision_timestamp == decision_timestamp


def test_climax_branch_adapters_preserve_branch_evaluator_parity():
    from dataclasses import replace
    from app.signals.climax import evaluate_climax_bundle
    from app.strategies.climax import (
        LowVolumeExtensionFailureAdapter,
        VolumeClimaxUnwindAdapter,
    )
    from app.strategies.contracts import StrategyContext

    for subtype, adapter_type in (
        ("VOLUME_CLIMAX_UNWIND", VolumeClimaxUnwindAdapter),
        ("LOW_VOLUME_EXTENSION_FAILURE", LowVolumeExtensionFailureAdapter),
    ):
        state, features, frame, config = _climax_fixture(subtype)
        features = replace(features, market_asof=features.asof)
        context = StrategyContext(
            symbol=state.symbol, event_id=state.event_id, event_state=state,
            features=features, short_zone=None, decision_timestamp=features.asof,
            strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="VALID",
        )
        legacy = evaluate_climax_bundle(state, features, frame, config).branch_evaluations[subtype]
        evaluation = adapter_type(config, frame).evaluate(context)

        assert evaluation.strategy_type == subtype
        assert evaluation.strategy_subtype == legacy.subtype
        assert evaluation.actionable == legacy.actionable
        assert evaluation.score == legacy.score
        assert evaluation.grade == legacy.grade
        assert evaluation.blockers == tuple(legacy.veto_reasons)
        assert evaluation.vetoes == tuple(legacy.veto_reasons)
        assert evaluation.risk_flags == tuple(legacy.data_quality)
        assert evaluation.strategy_metadata["strategy_type"] == "CLIMAX_EXHAUSTION"


def test_climax_adapter_preserves_no_admission_and_selected_branch_parity():
    from dataclasses import replace
    from app.signals.climax import evaluate_climax_bundle
    from app.strategies.climax import ClimaxAdapter
    from app.strategies.contracts import StrategyContext

    state, features, frame, config = _climax_fixture("VOLUME_CLIMAX_UNWIND")
    config.volume_climax_unwind_enabled = False
    config.low_volume_extension_enabled = False
    features = replace(features, market_asof=features.asof)
    decision_timestamp = features.asof
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=None, decision_timestamp=decision_timestamp,
        strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="VALID",
    )
    legacy = evaluate_climax_bundle(state, features, frame, config).selected
    evaluation = ClimaxAdapter(config, frame).evaluate(context)

    assert legacy.subtype is None
    assert evaluation.strategy_subtype == "NO_CLIMAX_ADMISSION"
    assert evaluation.actionable is False
    assert evaluation.score == legacy.score
    assert evaluation.grade == legacy.grade
    assert evaluation.vetoes == tuple(legacy.veto_reasons)
    assert evaluation.risk_flags == tuple(legacy.data_quality)
    assert evaluation.strategy_metadata["strategy_config_fingerprint"] == context.strategy_config_fingerprint


def test_climax_adapter_preserves_low_volume_veto_and_data_quality():
    from dataclasses import replace
    from app.signals.climax import evaluate_climax_bundle
    from app.strategies.climax import ClimaxAdapter
    from app.strategies.contracts import StrategyContext

    state, features, frame, config = _climax_fixture("LOW_VOLUME_EXTENSION_FAILURE")
    features = replace(features, market_asof=features.asof, rejection_from_high_pct=1.0)
    decision_timestamp = features.asof
    context = StrategyContext(
        symbol=state.symbol, event_id=state.event_id, event_state=state,
        features=features, short_zone=None, decision_timestamp=decision_timestamp,
        strategy_config_fingerprint=strategy_config_fingerprint(config), input_quality="VALID",
    )
    legacy = evaluate_climax_bundle(state, features, frame, config).selected
    evaluation = ClimaxAdapter(config, frame).evaluate(context)

    assert evaluation.actionable is False
    assert "rejection_missing" in evaluation.vetoes
    assert evaluation.vetoes == tuple(legacy.veto_reasons)
    assert evaluation.risk_flags == tuple(legacy.data_quality)


def _baseline_fixture() -> tuple[EventState, SymbolFeatures, ShortZone, AppConfig]:
    config = AppConfig()
    state = _make_event_state(
        state=EventStatus.PULLBACK_OBSERVED,
        event_high=115.0,
        event_high_time=datetime(2026, 4, 13, 11, 55, tzinfo=timezone.utc),
        expires_at=datetime(2026, 4, 13, 15, 55, tzinfo=timezone.utc),
    )
    features = _make_features(
        asof=BASELINE_ASOF,
        price=112.0,
        ret_5m=10.0,
        ret_15m=15.0,
        rejection_from_high_pct=3.0,
        latest_failed_retest=True,
        oi_change_pct=-5.0,
        derivatives_status="OK",
        current_volume=3000.0,
        market_asof=BASELINE_ASOF,
    )
    return state, features, ShortZone(low=110.0, high=114.0, mode="event_range"), config


def _climax_fixture(subtype: str):
    config = climax_config(
        volume_climax_unwind_enabled=subtype == "VOLUME_CLIMAX_UNWIND",
        low_volume_extension_enabled=subtype == "LOW_VOLUME_EXTENSION_FAILURE",
    )
    state = _make_event_state(
        event_high=115.0,
        event_high_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
    )
    if subtype == "VOLUME_CLIMAX_UNWIND":
        features = _make_features(
            ret_5m=10.0,
            ret_15m=15.0,
            vol_zscore_30m=8.73,
            oi_change_pct=-5.77,
            derivatives_status="OK",
            rejection_from_high_pct=3.0,
            current_volume=3000.0,
        )
        frame = _make_frame([100.0 + index * 0.1 for index in range(30)])
    else:
        features = _make_features(
            asof=datetime(2026, 4, 13, 12, 10, tzinfo=timezone.utc),
            price=112.0,
            ret_5m=4.0,
            rejection_from_high_pct=3.0,
            current_volume=100.0,
            oi_change_pct=-5.0,
            derivatives_status="OK",
            latest_failed_retest=True,
        )
        frame = _make_frame(
            [110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 114.0, 113.0, 112.0, 111.0, 110.5]
        )
        frame.loc[frame.index[1:6], "volume"] = 1000.0
        frame.loc[frame.index[6:11], "volume"] = 100.0
    return state, features, frame, config


def test_baseline_pullback_characterization_contract():
    state, features, zone, config = _baseline_fixture()
    evaluation = SignalEngine(config).analyze(state, features, zone, BASELINE_ASOF)
    decision = evaluation.decision

    assert decision is not None
    assert {
        "strategy_type": decision.strategy_type,
        "strategy_subtype": decision.strategy_subtype,
        "model_version": decision.model_version,
        "score": decision.score,
        "grade": decision.grade,
        "actionable": decision.actionable,
    } == {
        "strategy_type": "BASELINE_PULLBACK",
        "strategy_subtype": None,
        "model_version": "baseline-v1",
        "score": 84,
        "grade": "A",
        "actionable": True,
    }
    assert evaluation.reject_reasons == ["derivatives_missing", "oi_missing"]
    assert evaluation.blockers == ["derivatives_missing", "oi_missing"]
    assert decision.risk_flags == ["Data quality: derivatives_missing, oi_missing."]
    assert decision.strategy_metadata == {}
    assert strategy_config_fingerprint(config) == "bdc0e50eaf9b1ffb88a5444abdf33a9125f4addf8fd286d6af206e21c438e46b"


def test_climax_exhaustion_volume_branch_characterization_contract():
    state, features, frame, config = _climax_fixture("VOLUME_CLIMAX_UNWIND")
    result = evaluate_climax(state, features, frame, config)

    assert result.subtype == "VOLUME_CLIMAX_UNWIND"
    assert result.score == 85
    assert result.grade == "A"
    assert result.actionable
    assert result.veto_reasons == []
    assert result.data_quality == []
    assert set(result.metadata) >= {
        "strategy_type", "strategy_subtype", "model_version", "volume_ratio",
        "volume_climax_veto_reasons", "volume_climax_metadata",
    }
    assert result.metadata["strategy_type"] == "CLIMAX_EXHAUSTION"
    assert result.metadata["strategy_subtype"] == "VOLUME_CLIMAX_UNWIND"
    assert result.metadata["model_version"] == "climax-v1"
    assert result.metadata["volume_climax_veto_reasons"] == []
    assert strategy_config_fingerprint(config) == "263e53128d84296749891dcd19c9964f2640416e68398ec0450304f767f94314"


def test_climax_exhaustion_low_volume_branch_characterization_contract():
    state, features, frame, config = _climax_fixture("LOW_VOLUME_EXTENSION_FAILURE")
    result = evaluate_climax(state, features, frame, config)

    assert result.subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    assert result.score == 100
    assert result.grade == "B"
    assert result.actionable
    assert result.veto_reasons == []
    assert result.data_quality == ["extension_below_threshold"]
    assert set(result.metadata) >= {
        "strategy_type", "strategy_subtype", "model_version", "volume_efficiency_ratio",
        "extension_gate_value", "failed_retest_confirmed", "oi_confirmation_state",
    }
    assert result.metadata["strategy_type"] == "CLIMAX_EXHAUSTION"
    assert result.metadata["strategy_subtype"] == "LOW_VOLUME_EXTENSION_FAILURE"
    assert result.metadata["model_version"] == "climax-v1"
    assert result.metadata["extension_gate_value"] == 4.0
    assert strategy_config_fingerprint(config) == "1c415afc4792b6b70adffc0298c352109dc5ae45a8a01a3405b620651f1984c8"


def test_trapped_longs_reversal_characterization_contract():
    config = trapped_config()
    result = evaluate_trapped_longs_reversal(
        trapped_state(),
        trapped_features(asof=T0.replace(minute=15)),
        trapped_frame(),
        config,
    )

    assert result.subtype == "TRAPPED_LONGS_REVERSAL"
    assert result.score == 100
    assert result.grade == "A"
    assert result.actionable
    assert result.veto_reasons == []
    assert result.data_quality == []
    assert set(result.metadata) >= {
        "strategy_type", "strategy_subtype", "model_version", "breakout_reference",
        "failed_retest_quality", "oi_sequence_classification",
    }
    assert result.metadata["strategy_type"] == "TRAPPED_LONGS"
    assert result.metadata["strategy_subtype"] == "TRAPPED_LONGS_REVERSAL"
    assert result.metadata["model_version"] == "trapped-longs-v1"
    assert strategy_config_fingerprint(config) == "e84ad78b4778e17a7df57fb33fa1ae8b0149688e9f73ae6cb15b063fdd9ce8c1"


def _trapped_context(*, features=None, state=None, config=None, input_quality="VALID"):
    from dataclasses import replace
    from app.strategies.contracts import StrategyContext

    config = config or trapped_config()
    features = features or trapped_features(asof=T0.replace(minute=15))
    if features.market_asof is None:
        features = replace(features, market_asof=features.asof)
    state = state or trapped_state()
    return StrategyContext(
        symbol=state.symbol,
        event_id=state.event_id,
        event_state=state,
        features=features,
        short_zone=None,
        decision_timestamp=features.asof,
        strategy_config_fingerprint=strategy_config_fingerprint(config),
        input_quality=input_quality,
    )


def test_trapped_longs_adapter_preserves_accepted_legacy_evaluation():
    from app.strategies.trapped_longs import TrappedLongsAdapter

    config = trapped_config()
    context = _trapped_context(config=config)
    legacy = evaluate_trapped_longs_reversal(
        context.event_state, context.features, trapped_frame(), config,
        decision_time=context.decision_timestamp,
    )
    evaluation = TrappedLongsAdapter(config, trapped_frame()).evaluate(context)

    assert evaluation.strategy_type == "TRAPPED_LONGS_REVERSAL"
    assert evaluation.strategy_subtype == legacy.subtype
    assert evaluation.model_version == legacy.metadata["model_version"]
    assert evaluation.actionable == legacy.actionable
    assert evaluation.score == legacy.score
    assert evaluation.grade == legacy.grade
    assert evaluation.reasons == ()
    assert evaluation.blockers == tuple(legacy.veto_reasons)
    assert evaluation.vetoes == tuple(legacy.veto_reasons)
    assert evaluation.risk_flags == tuple(legacy.data_quality)
    assert dict(evaluation.strategy_metadata) == {
        **legacy.metadata,
        "strategy_config_fingerprint": context.strategy_config_fingerprint,
    }
    assert evaluation.symbol == context.symbol
    assert evaluation.event_id == context.event_id
    assert evaluation.decision_timestamp == context.decision_timestamp
    assert evaluation.input_quality == context.input_quality


def test_trapped_longs_adapter_preserves_veto_and_missing_input_behavior():
    from app.strategies.trapped_longs import TrappedLongsAdapter

    config = trapped_config()
    frame = trapped_frame()
    blocked_features = trapped_features(
        asof=T0.replace(minute=15), oi_change_15m=None, derivatives_status="MISSING",
    )
    blocked_context = _trapped_context(features=blocked_features, config=config)
    legacy = evaluate_trapped_longs_reversal(
        blocked_context.event_state, blocked_context.features, frame, config,
    )
    blocked = TrappedLongsAdapter(config, frame).evaluate(blocked_context)

    assert blocked.actionable is False
    assert blocked.vetoes == tuple(legacy.veto_reasons)
    assert blocked.blockers == tuple(legacy.veto_reasons)
    assert blocked.risk_flags == tuple(legacy.data_quality)
    assert blocked.strategy_metadata["strategy_config_fingerprint"] == blocked_context.strategy_config_fingerprint

    missing_context = _trapped_context(
        state=trapped_state(breakout_reference=None), config=config,
    )
    missing = TrappedLongsAdapter(config, frame).evaluate(missing_context)
    assert missing.actionable is False
    assert "breakout_reference_missing" in missing.vetoes


def test_trapped_longs_adapter_preserves_expired_confirmation_behavior():
    from app.strategies.trapped_longs import TrappedLongsAdapter

    config = trapped_config()
    context = _trapped_context(config=config)
    adapter = TrappedLongsAdapter(
        config,
        trapped_frame(),
        attempt_created_at=T0,
        confirmation_expires_at=T0.replace(minute=15),
    )
    legacy = evaluate_trapped_longs_reversal(
        context.event_state, context.features, trapped_frame(), config,
        attempt_created_at=T0,
        confirmation_expires_at=T0.replace(minute=15),
        decision_time=context.decision_timestamp,
    )
    evaluation = adapter.evaluate(context)

    assert evaluation.actionable is False
    assert evaluation.vetoes == tuple(legacy.veto_reasons)
    assert "CONFIRMATION_WINDOW_EXPIRED" in evaluation.vetoes


def test_trapped_longs_adapter_rejects_malformed_frame_and_context():
    from app.strategies.trapped_longs import TrappedLongsAdapter

    config = trapped_config()
    context = _trapped_context(config=config)
    with pytest.raises(TypeError, match="pandas DataFrame"):
        TrappedLongsAdapter(config, [])
    with pytest.raises(TypeError, match="StrategyContext"):
        TrappedLongsAdapter(config, trapped_frame()).evaluate(object())
