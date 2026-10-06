from app.config import AppConfig
from app.domain import EventStatus, ShortZone, SignalType
from app.signals.engine import SignalEngine


def test_signal_engine_emits_aggressive_signal(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(price=112.0, inside_short_zone_flag=True)

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.AGGRESSIVE
    assert decision.grade == "A"
    assert decision.score >= 80


def test_signal_engine_blocks_actionable_signal_when_liquidity_missing(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(price=112.0, inside_short_zone_flag=True, liquidity_available=False)

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None


def test_signal_engine_delays_signal_when_breakout_risk_is_active(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        recent_high_breakout=True,
        latest_body_atr_ratio=1.2,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None


def test_signal_engine_rejects_bad_liquidity(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        spread_pct=0.35,
        slippage_pct=0.30,
        orderbook_depth_usdt_1pct=10_000.0,
        orderbook_depth_usdt_2pct=20_000.0,
        liquidity_available=True,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None


def test_signal_engine_sends_watch_for_moderate_liquidity_downgrade(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig(enable_watch_candidates=True, watch_min_score=50))
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_1h=10.0,
        ret_4h=24.0,
        dist_to_ema20_atr=3.0,
        range_atr_ratio=1.6,
        spread_pct=0.31,
        slippage_pct=0.05,
        orderbook_depth_usdt_1pct=100_000.0,
        orderbook_depth_usdt_2pct=200_000.0,
        liquidity_available=True,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.WATCH
    assert decision.grade == "B"
    assert decision.actionable is False
    assert "Orderbook spread is too wide." in decision.risk_flags


def test_signal_engine_sends_watch_for_moderately_weak_volume(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig(enable_watch_candidates=True, watch_min_score=50))
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_1h=10.0,
        ret_4h=24.0,
        dist_to_ema20_atr=3.0,
        range_atr_ratio=1.6,
        vol_zscore_30m=0.72,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.WATCH
    assert decision.grade == "B"
    assert decision.actionable is False
    assert "Volume z-score is moderately below actionable threshold." in decision.risk_flags


def test_signal_engine_sends_watch_for_near_threshold_volume_without_weakening_signal_admission(
    make_event_state,
    make_features,
) -> None:
    engine = SignalEngine(AppConfig(enable_watch_candidates=True, watch_min_score=50, vol_zscore_min=0.9))
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_1h=10.0,
        ret_4h=24.0,
        dist_to_ema20_atr=3.0,
        range_atr_ratio=1.6,
        vol_zscore_30m=0.69,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.WATCH
    assert decision.actionable is False
    assert "Volume z-score is moderately below actionable threshold." in decision.risk_flags


def test_signal_engine_sends_watch_for_moderately_weak_rejection(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig(enable_watch_candidates=True, watch_min_score=50))
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_1h=15.0,
        ret_4h=35.0,
        dist_to_ema20_atr=4.5,
        range_atr_ratio=2.2,
        upper_wick_ratio=0.13,
        rejection_from_high_pct=0.70,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.WATCH
    assert decision.grade == "C"
    assert decision.actionable is False
    assert "Rejection is moderately below actionable threshold." in decision.risk_flags


def test_signal_engine_blocks_missing_liquidity(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(price=112.0, inside_short_zone_flag=True, liquidity_available=False)

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None


def test_signal_engine_suppresses_c_grade(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_15m=6.0,
        ret_1h=8.0,
        ret_4h=20.0,
        dist_to_vwap_pct=8.0,
        dist_to_ema20_atr=2.0,
        upper_wick_ratio=0.18,
        rejection_from_high_pct=0.8,
        close_position_in_range=0.6,
        vol_zscore_30m=1.1,
        range_atr_ratio=1.3,
        pullback_from_high_pct=3.0,
        latest_failed_retest=False,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None


def test_signal_engine_grade_c_defaults_to_watch_not_actionable(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig(enable_watch_candidates=True, watch_min_score=50))
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_15m=7.0,
        ret_1h=9.0,
        ret_4h=21.0,
        dist_to_vwap_pct=13.0,
        dist_to_ema20_atr=1.8,
        upper_wick_ratio=0.16,
        rejection_from_high_pct=0.9,
        close_position_in_range=0.55,
        vol_zscore_30m=1.4,
        range_atr_ratio=1.3,
        pullback_from_high_pct=3.9,
        latest_failed_retest=False,
        funding_rate=-0.0009,
        oi_change_15m=1.2,
        oi_change_1h=0.6,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.WATCH
    assert decision.actionable is False
    assert decision.grade == "C"
    assert decision.score == 51


def test_signal_engine_grade_c_can_be_suppressed_by_config(make_event_state, make_features) -> None:
    engine = SignalEngine(
        AppConfig(
            enable_watch_candidates=True,
            watch_min_score=50,
            grade_c_mode="suppress",
        )
    )
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_15m=7.0,
        ret_1h=9.0,
        ret_4h=21.0,
        dist_to_vwap_pct=13.0,
        dist_to_ema20_atr=1.8,
        upper_wick_ratio=0.16,
        rejection_from_high_pct=0.9,
        close_position_in_range=0.55,
        vol_zscore_30m=1.4,
        range_atr_ratio=1.3,
        pullback_from_high_pct=3.9,
        latest_failed_retest=False,
        funding_rate=-0.0009,
        oi_change_15m=1.2,
        oi_change_1h=0.6,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None


def test_signal_engine_uses_90_minute_signal_age(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    features = make_features(price=112.0, inside_short_zone_flag=True)
    state = make_event_state(
        state=EventStatus.PULLBACK_OBSERVED,
        zone_low=110.5,
        zone_high=113.8,
        event_high_time=features.asof.replace(hour=10, minute=31),
    )
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None


def test_signal_engine_near_signal_becomes_watch_with_blockers(make_event_state, make_features) -> None:
    engine = SignalEngine(
        AppConfig(
            enable_watch_candidates=True,
            watch_min_score=40,
            enable_squeeze_guard=True,
            squeeze_guard_mode="warn_only",
        )
    )
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        pullback_from_high_pct=1.2,
        upper_wick_ratio=0.11,
        rejection_from_high_pct=0.65,
        latest_failed_retest=False,
        funding_rate=-0.02,
        oi_change_15m=12.0,
        oi_change_1h=18.0,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is not None
    assert decision.signal_type == SignalType.WATCH
    assert decision.actionable is False
    assert decision.decision_type == "WATCH"
    assert "shallow_pullback" in decision.blockers
    assert "weak_rejection" in decision.blockers


def test_signal_engine_watch_disabled_by_default(make_event_state, make_features) -> None:
    engine = SignalEngine(AppConfig())
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    features = make_features(
        price=112.0,
        inside_short_zone_flag=True,
        ret_1h=10.0,
        ret_4h=24.0,
        dist_to_ema20_atr=3.0,
        range_atr_ratio=1.6,
        spread_pct=0.31,
        slippage_pct=0.05,
        orderbook_depth_usdt_1pct=100_000.0,
        orderbook_depth_usdt_2pct=200_000.0,
        liquidity_available=True,
    )

    decision = engine.evaluate(state, features, zone, features.asof)

    assert decision is None
