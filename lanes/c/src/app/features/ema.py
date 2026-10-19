"""EMA helpers."""

from __future__ import annotations

import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    """Return an exponential moving average."""

    return series.ewm(span=span, adjust=False).mean()
