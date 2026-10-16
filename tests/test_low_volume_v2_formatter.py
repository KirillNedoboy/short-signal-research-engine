from datetime import UTC, datetime

from app.domain import SignalDecision, SignalType
from app.signals.formatter import format_signal_message


def _decision(metadata):
    return SignalDecision(
        symbol="BTCUSDT",
        event_id="event-1",
        signal_type=SignalType.CONFIRM,
        grade="A",
        score=100,
        market_price=105.0,
        short_zone_low=100.0,
        short_zone_high=110.0,
        signal_time=datetime(2026, 10, 3, 12, tzinfo=UTC),
        reasons=["confirmed"],
        risk_flags=[],
        features_snapshot={},
        score_breakdown={},
        strategy_type="CLIMAX_EXHAUSTION",
        strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE_V2",
        strategy_metadata=metadata,
    )


def test_v2_formatter_uses_current_anchor_and_same_metadata_timestamps():
    message = format_signal_message(
        _decision(
            {
                "event_high": 100.0,
                "current_anchor_high": 110.0,
                "current_anchor_high_time": "2026-10-03T11:55:00+00:00",
                "entry_distance_below_high_pct": 4.55,
                "entry_trigger_time": "2026-10-03T12:00:00+00:00",
                "decision_at": "2026-10-03T12:00:00+00:00",
                "v2_state": "ACTIONABLE",
            }
        ),
        "UTC",
    )
    assert "Event high: 110.00000000" in message
    assert "Anchor high time: 2026-10-03T11:55:00+00:00" in message
    assert "V2 entry trigger / decision: 2026-10-03T12:00:00+00:00" in message


def test_v1_formatter_contract_remains_event_high_based():
    decision = _decision({"event_high": 100.0, "entry_distance_below_high_pct": 0.0})
    decision.strategy_subtype = "LOW_VOLUME_EXTENSION_FAILURE"
    message = format_signal_message(decision, "UTC")
    assert "Event high: 100.00000000" in message
    assert "V2 entry trigger / decision" not in message
