"""Return helpers."""

from __future__ import annotations


def pct_return(current: float, previous: float) -> float:
    """Return percentage move between two prices."""

    if previous == 0:
        return 0.0
    return ((current / previous) - 1) * 100
