from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from app.config import AppConfig
from app.features.builder import FeatureBuilder
from app.runtime.pipeline import LaneCStrategyPipeline
from app.strategies.registry import REQUIRED_STRATEGY_TYPES


def _frame(rows: int = 300) -> pd.DataFrame:
    end = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    timestamps = [end - timedelta(minutes=rows - index) for index in range(rows)]
    close = [100.0 + index * 0.01 for index in range(rows)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 0.2 for value in close],
            "low": [value - 0.2 for value in close],
            "close": close,
            "volume": [1000.0 + index for index in range(rows)],
            "turnover": [100000.0 + index for index in range(rows)],
        }
    ).set_index("timestamp", drop=False)


def test_pipeline_keeps_c_feature_values_and_unknown_derivatives() -> None:
    config = AppConfig(autoexecution="OFF")
    frame = _frame()
    direct = FeatureBuilder().build("SOLUSDT", frame, market_asof=datetime.now(UTC))
    result = LaneCStrategyPipeline(config).evaluate(
        "SOLUSDT", frame, None, decision_at=datetime.now(UTC)
    )
    assert result.features is not None
    assert result.features.price == direct.price
    assert result.features.ret_15m == direct.ret_15m
    assert result.features.oi_change_15m is None
    assert result.features.funding_rate is None


def test_c_registry_contract_is_explicit_and_complete() -> None:
    assert REQUIRED_STRATEGY_TYPES == (
        "BASELINE_PULLBACK",
        "CLIMAX_EXHAUSTION",
        "VOLUME_CLIMAX_UNWIND",
        "LOW_VOLUME_EXTENSION_FAILURE",
        "TRAPPED_LONGS_REVERSAL",
    )

