from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.config import AppConfig
from app.domain import SignalDecision, SignalType
from app.signals.delivery_policy import live_delivery_enabled


def _decision(*, strategy_type: str, strategy_subtype: str | None = None, signal_type: SignalType = SignalType.CONFIRM) -> SignalDecision:
    return SignalDecision(
        symbol="TESTUSDT",
        event_id="TESTUSDT:15m:1",
        signal_type=signal_type,
        grade="B",
        score=70,
        market_price=100.0,
        short_zone_low=99.0,
        short_zone_high=101.0,
        signal_time=datetime.now(timezone.utc),
        reasons=[],
        risk_flags=[],
        features_snapshot={},
        score_breakdown={},
        strategy_type=strategy_type,
        strategy_subtype=strategy_subtype,
    )


@pytest.mark.parametrize(
    ("decision", "config", "expected"),
    [
        (_decision(strategy_type="BASELINE_PULLBACK"), AppConfig(), True),
        (_decision(strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND"), AppConfig(), True),
        (_decision(strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE"), AppConfig(), True),
        (_decision(strategy_type="BASELINE_PULLBACK"), AppConfig(baseline_live_delivery_enabled=False), False),
        (_decision(strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND"), AppConfig(volume_climax_live_delivery_enabled=False), False),
        (_decision(strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE"), AppConfig(low_volume_live_delivery_enabled=False), False),
        (_decision(strategy_type="TRAPPED_LONGS_REVERSAL", strategy_subtype="TRAPPED_LONGS_REVERSAL"), AppConfig(trapped_longs_live_delivery_enabled=True), True),
        (_decision(strategy_type="TRAPPED_LONGS_REVERSAL", strategy_subtype="TRAPPED_LONGS_REVERSAL"), AppConfig(trapped_longs_live_delivery_enabled=False), False),
        (_decision(strategy_type="UNKNOWN"), AppConfig(), False),
        (_decision(strategy_type="BASELINE_PULLBACK", signal_type=SignalType.WATCH), AppConfig(send_watch_to_telegram=False), False),
    ],
)
def test_live_delivery_policy_maps_only_known_actionable_strategies(
    decision: SignalDecision,
    config: AppConfig,
    expected: bool,
) -> None:
    assert live_delivery_enabled(decision, config) is expected
