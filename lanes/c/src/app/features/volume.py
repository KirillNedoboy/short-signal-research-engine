"""Volume anomaly helpers."""

from __future__ import annotations

import pandas as pd


def rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """Compute a rolling z-score."""

    mean = series.rolling(window=window, min_periods=max(2, window // 2)).mean()
    std = series.rolling(window=window, min_periods=max(2, window // 2)).std(ddof=0)
    return (series - mean) / std.replace(0, pd.NA)
