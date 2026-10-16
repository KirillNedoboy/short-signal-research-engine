from datetime import UTC, datetime
from app.replay.contracts import (
    TrappedLongsV2StrategyInput,
    strategy_input_from_dict,
    strategy_input_to_dict,
)


def test_trapped_v2_round_trip_preserves_portable_primitives():
    value = TrappedLongsV2StrategyInput(
        "X",
        "e",
        (
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "turnover": 1,
            },
        ),
        "b",
        datetime(2026, 1, 1, tzinfo=UTC),
        1.0,
        2,
        tuple({"observed_at": "2026-01-01T00:00:00Z", "value": 1} for _ in range(4)),
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    restored = strategy_input_from_dict(strategy_input_to_dict(value))
    assert restored == value
