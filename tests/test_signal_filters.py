from app.config import AppConfig
from app.signals.filters import evaluate_core_filters


def test_core_filters_accept_conservative_candidate_search_expansion(make_features) -> None:
    config = AppConfig()
    features = make_features(
        dist_to_vwap_pct=7.6,
        vol_zscore_30m=0.82,
        pullback_from_high_pct=2.45,
        upper_wick_ratio=0.16,
        rejection_from_high_pct=0.7,
    )

    result = evaluate_core_filters(features, config)

    assert result == {
        "dist_to_vwap": True,
        "rejection": True,
        "volume": True,
        "pullback": True,
    }
