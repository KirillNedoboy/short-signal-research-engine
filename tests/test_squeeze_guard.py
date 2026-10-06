from app.config import AppConfig
from app.signals.squeeze_guard import evaluate_squeeze_guard


def test_squeeze_guard_negative_funding_plus_rising_oi_is_high_risk(make_features) -> None:
    config = AppConfig(enable_squeeze_guard=True, squeeze_guard_mode="warn_only")
    features = make_features(
        funding_rate=-0.02,
        oi_change_15m=12.0,
        oi_change_1h=18.0,
        pullback_from_high_pct=1.0,
        latest_failed_retest=False,
    )

    result = evaluate_squeeze_guard(features, config)

    assert result.level in {"HIGH", "EXTREME"}
    assert result.action == "WARNING"
    assert "funding_negative_trap" in result.reasons


def test_squeeze_guard_missing_derivatives_is_data_quality_warning(make_features) -> None:
    config = AppConfig(enable_squeeze_guard=True, squeeze_guard_mode="warn_only")
    features = make_features(funding_rate=None, oi_change_15m=None, oi_change_1h=None)

    result = evaluate_squeeze_guard(features, config)

    assert "derivatives_missing" in result.data_quality
    assert result.level in {"LOW", "MEDIUM"}


def test_squeeze_guard_warn_only_does_not_block(make_features) -> None:
    config = AppConfig(enable_squeeze_guard=True, squeeze_guard_mode="warn_only")
    features = make_features(funding_rate=-0.02, oi_change_15m=10.0, oi_change_1h=14.0, pullback_from_high_pct=1.0)

    result = evaluate_squeeze_guard(features, config)

    assert result.action == "WARNING"
    assert result.block_signal is False


def test_squeeze_guard_watch_only_downgrades_to_watch(make_features) -> None:
    config = AppConfig(enable_squeeze_guard=True, squeeze_guard_mode="watch_only")
    features = make_features(funding_rate=-0.02, oi_change_15m=10.0, oi_change_1h=14.0, pullback_from_high_pct=1.0)

    result = evaluate_squeeze_guard(features, config)

    assert result.action == "WATCH_ONLY"
    assert result.force_watch is True


def test_squeeze_guard_block_extreme_blocks_only_extreme(make_features) -> None:
    config = AppConfig(enable_squeeze_guard=True, squeeze_guard_mode="block_extreme")
    extreme = make_features(
        funding_rate=-0.04,
        oi_change_15m=20.0,
        oi_change_1h=25.0,
        pullback_from_high_pct=0.3,
        latest_failed_retest=False,
        spread_pct=0.6,
        orderbook_depth_usdt_1pct=5_000.0,
        orderbook_depth_usdt_2pct=10_000.0,
    )
    medium = make_features(funding_rate=-0.01, oi_change_15m=7.0, oi_change_1h=9.0, pullback_from_high_pct=1.6)

    extreme_result = evaluate_squeeze_guard(extreme, config)
    medium_result = evaluate_squeeze_guard(medium, config)

    assert extreme_result.level == "EXTREME"
    assert extreme_result.block_signal is True
    assert medium_result.block_signal is False
