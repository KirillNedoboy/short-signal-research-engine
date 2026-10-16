from datetime import UTC, datetime, timedelta
import pytest
from app.replay.contracts import TrappedLongsV2StrategyInput
from app.replay.evaluator import evaluate_strategy_input


def test_contract_rejects_malformed_oi():
    with pytest.raises(ValueError):
        TrappedLongsV2StrategyInput(
            "X", "e", (), "b", datetime.now(UTC), 1, 1, (), datetime.now(UTC)
        )


def test_replay_missing_reference_preserves_blocker():
    t = datetime(2026, 1, 1, tzinfo=UTC)
    rows = tuple(
        {
            "timestamp": t + timedelta(minutes=i),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "turnover": 1,
        }
        for i in range(10)
    )
    oi = tuple({"observed_at": t.isoformat(), "value": 1} for _ in range(4))
    value = TrappedLongsV2StrategyInput(
        "X", "e", rows, "b", t, None, 1, oi, t + timedelta(minutes=10)
    )
    result = evaluate_strategy_input(value, None)
    assert "frozen_breakout_reference_missing" in result["blockers"]
