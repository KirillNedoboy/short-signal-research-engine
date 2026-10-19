"""VWAP helpers."""

from __future__ import annotations

import pandas as pd


def cumulative_vwap(frame: pd.DataFrame) -> pd.Series:
    """Compute VWAP on the provided OHLCV frame."""

    if frame.empty:
        return pd.Series(dtype="float64")
    typical = (frame["high"] + frame["low"] + frame["close"]) / 3
    turnover = typical * frame["volume"]
    return turnover.cumsum() / frame["volume"].replace(0, pd.NA).cumsum()
