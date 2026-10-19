"""Core filter checks."""

from __future__ import annotations

from app.config import AppConfig
from app.domain import SymbolFeatures


def evaluate_core_filters(features: SymbolFeatures, config: AppConfig) -> dict[str, bool]:
    """Evaluate post-pullback hard filters."""

    return {
        "dist_to_vwap": features.dist_to_vwap_pct >= config.dist_to_vwap_min,
        "rejection": (
            features.upper_wick_ratio >= config.upper_wick_min
            or features.rejection_from_high_pct >= config.rejection_min
        ),
        "volume": features.vol_zscore_30m >= config.vol_zscore_min,
        "pullback": features.pullback_from_high_pct is not None
        and config.pullback_min_pct <= features.pullback_from_high_pct <= config.pullback_max_pct,
    }
