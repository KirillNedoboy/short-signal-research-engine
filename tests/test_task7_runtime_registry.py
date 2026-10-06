from __future__ import annotations

from datetime import datetime, timezone
import asyncio

import pytest
import pandas as pd

import app.main as main_module
from app.main import ShortSignalBot
from app.domain import EventStatus, ShortZone
from app.strategies.contracts import StrategyEvaluation


@pytest.mark.parametrize(
    ("label", "strategy_type", "subtype"),
    [
        ("baseline", "BASELINE_PULLBACK", "BASELINE_PULLBACK"),
        ("climax", "CLIMAX_EXHAUSTION", "VOLUME_CLIMAX_UNWIND"),
        ("trapped", "TRAPPED_LONGS_REVERSAL", "TRAPPED_LONGS_REVERSAL"),
        ("fresh_recheck", "CLIMAX_EXHAUSTION", "LOW_VOLUME_EXTENSION_FAILURE"),
    ],
)
def test_selected_registry_evaluation_controls_signal_decision(
    label: str,
    strategy_type: str,
    subtype: str,
    make_signal_decision,
) -> None:
    selected = StrategyEvaluation(
        strategy_type=strategy_type,
        strategy_subtype=subtype,
        model_version=f"registry-{label}",
        actionable=True,
        score=91,
        grade="A",
        reasons=[f"registry_{label}"],
        blockers=[],
        vetoes=[],
        risk_flags=["registry-risk"],
        strategy_metadata={"registry_source": label},
        symbol="ONTUSDT",
        event_id="event-1",
        decision_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    evaluations = (selected,)
    chosen = ShortSignalBot._registered_evaluation(evaluations, strategy_type)
    assert chosen is selected
    projected = ShortSignalBot._decision_from_registered_evaluation(
        make_signal_decision(), chosen
    )

    assert projected.strategy_type == strategy_type
    assert projected.strategy_subtype == subtype
    assert projected.model_version == f"registry-{label}"
    assert projected.score == 91
    assert projected.grade == "A"
    assert projected.strategy_metadata["registry_source"] == label


def test_actionable_registry_baseline_builds_decision_without_legacy_decision(
    make_event_state, make_features
) -> None:
    state = make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE)
    features = make_features(market_asof=datetime(2026, 1, 1, tzinfo=timezone.utc))
    zone = ShortZone(low=110.0, high=114.0, mode="PULLBACK")
    evaluation = StrategyEvaluation(
        strategy_type="BASELINE_PULLBACK",
        strategy_subtype="BASELINE_PULLBACK",
        model_version="registry-baseline-v1",
        actionable=True,
        score=88,
        grade="A",
        reasons=["canonical baseline"],
        blockers=[],
        vetoes=[],
        risk_flags=[],
        strategy_metadata={},
        symbol=state.symbol,
        event_id=state.event_id,
        decision_timestamp=features.asof,
    )

    decision = ShortSignalBot._new_decision_from_registered_evaluation(
        state=state,
        features=features,
        zone=zone,
        signal_time=features.asof,
        evaluation=evaluation,
    )

    assert decision is not None
    assert decision.actionable is True
    assert decision.score == 88
    assert decision.strategy_type == "BASELINE_PULLBACK"


def test_missing_market_asof_returns_no_registered_evaluations() -> None:
    bot = object.__new__(ShortSignalBot)
    bot._strategy_config_hash = "a" * 64
    state = __import__("tests.conftest", fromlist=["_make_event_state"])._make_event_state()
    features = __import__("tests.conftest", fromlist=["_make_features"])._make_features()

    assert ShortSignalBot._evaluate_registered_strategies(
        bot,
        state=state,
        features=features,
        frame_1m=__import__("pandas").DataFrame(),
        short_zone=None,
        decision_timestamp=features.asof,
    ) == ()
    assert bot._strategy_registry is None


def test_missing_market_asof_cannot_select_legacy_climax(monkeypatch, make_event_state, make_features) -> None:
    bot = object.__new__(ShortSignalBot)
    bot._config = type("Config", (), {"climax_short_enabled": True})()
    bot._evaluate_registered_strategies = lambda **_kwargs: ()
    state = make_event_state()
    features = make_features()

    def fail_legacy_selector(*_args, **_kwargs):
        raise AssertionError("legacy climax selector must not run without canonical context")

    monkeypatch.setattr(main_module, "evaluate_climax_bundle", fail_legacy_selector)
    assert asyncio.run(bot._evaluate_and_send_climax(
        state.symbol, pd.DataFrame(), state, features=features
    )) is None




@pytest.mark.parametrize("market_asof", [
    datetime(2026, 1, 1),
    datetime(2026, 1, 1, tzinfo=timezone.utc).replace(year=2027),
    "not-a-timestamp",
])
def test_invalid_market_asof_fails_closed_without_registry_or_legacy_selection(
    market_asof, make_event_state, make_features, monkeypatch
) -> None:
    bot = object.__new__(ShortSignalBot)
    bot._strategy_config_hash = "a" * 64
    bot._strategy_registry = None
    state = make_event_state()
    features = make_features(market_asof=market_asof)
    registry_calls = []

    class ExplodingRegistry:
        def __init__(self, *_args, **_kwargs):
            registry_calls.append(True)
            raise AssertionError("invalid market context must not build a registry")

    monkeypatch.setattr(main_module, "StrategyRegistry", ExplodingRegistry)
    monkeypatch.setattr(
        main_module.SignalEngine,
        "analyze",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid market context must not select legacy engine")
        ),
    )

    assert ShortSignalBot._evaluate_registered_strategies(
        bot,
        state=state,
        features=features,
        frame_1m=pd.DataFrame(),
        short_zone=None,
        decision_timestamp=features.asof,
    ) == ()
    assert registry_calls == []
    assert bot._strategy_registry is None


def test_trapped_longs_runtime_uses_registry_evaluation_over_conflicting_legacy(
    make_event_state, make_features, monkeypatch
) -> None:
    class Config:
        trapped_longs_reversal_enabled = True
        trapped_longs_max_lifetime_minutes = 15
        trapped_longs_live_delivery_enabled = True
        timezone = "UTC"

    class StateStore:
        def save(self, _state):
            return None

    class Repository:
        def __init__(self):
            self.persisted = False
            self.model_versions = []
            self.duplicate_lookup = None

        def get_shadow_entry_attempt(self, *, attempt_id):
            return None

        def upsert_shadow_entry_attempt(self, **kwargs):
            self.model_versions.append(kwargs["model_version"])

        def transition_shadow_entry_attempt(self, **kwargs):
            self.model_versions.append(kwargs["model_version"])

        def record_climax_evaluation(self, **kwargs):
            self.model_versions.append(kwargs["model_version"])
            return 1

        def has_signal_for_event(self, *args):
            self.duplicate_lookup = args
            assert args[-1] == "registry-trapped-v2"
            return True

        def persist_final_signal_bundle(self, *_args, **_kwargs):
            self.persisted = True
            raise AssertionError("delivery must remain gated by runtime config")

    bot = object.__new__(ShortSignalBot)
    bot._config = Config()
    bot._repository = Repository()
    bot._state_store = StateStore()
    bot._runtime_instance_id = "runtime"
    bot._strategy_config_hash = "a" * 64
    bot._evaluate_registered_strategies = lambda **_kwargs: (
        StrategyEvaluation(
            strategy_type="TRAPPED_LONGS_REVERSAL",
            strategy_subtype="TRAPPED_LONGS_REVERSAL",
            model_version="registry-trapped-v2",
            actionable=True,
            score=93,
            grade="A",
            reasons=["registry trapped result"],
            blockers=[],
            vetoes=[],
            risk_flags=["registry-risk"],
            strategy_metadata={
                "breakout_reference": 100.0,
                "closed_structural_candles": 2,
                "close_below_breakout_reference": True,
                "failed_retest_confirmed": True,
                "no_new_high": True,
                "event_high": 101.0,
            },
            symbol=_kwargs["features"].symbol,
            event_id=_kwargs["state"].event_id,
            decision_timestamp=_kwargs["decision_timestamp"],
        ),
    )
    monkeypatch.setattr(
        main_module,
        "evaluate_trapped_longs_reversal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("conflicting legacy trapped-longs selector must not run")
        ),
    )
    state = make_event_state()
    features = make_features(market_asof=make_features().asof)

    result = asyncio.run(
        bot._evaluate_and_send_trapped_longs(state, features, pd.DataFrame())
    )

    assert result is None
    assert bot._repository.model_versions == [
        "registry-trapped-v2",
        "registry-trapped-v2",
    ]
    assert bot._repository.duplicate_lookup is not None
    assert bot._repository.persisted is False
