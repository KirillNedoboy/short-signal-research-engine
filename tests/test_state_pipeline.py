from datetime import datetime, timedelta, timezone

from app.config import AppConfig
from app.domain import EventStatus
from app.events.pump_detector import PumpDetector
from app.events.pullback_tracker import PullbackTracker
from app.events.short_zone import ShortZoneBuilder


def test_pullback_tracker_promotes_event_to_pullback_observed(make_event_state, make_features) -> None:
    config = AppConfig()
    tracker = PullbackTracker(config)
    zone_builder = ShortZoneBuilder(config)
    state = make_event_state()
    features = make_features(
        price=111.5,
        pullback_from_high_pct=3.04,
        dist_to_vwap_pct=9.0,
        last_low=111.0,
        last_high=114.0,
    )

    updated = tracker.advance(state, features, features.asof)
    zone = zone_builder.build(updated, features)

    assert updated.state == EventStatus.PULLBACK_OBSERVED
    assert updated.pullback_detected_at == features.asof
    assert zone is not None
    assert zone.low <= features.price <= zone.high


def test_pullback_tracker_waits_for_deeper_pullback_before_promotion(make_event_state, make_features) -> None:
    config = AppConfig()
    tracker = PullbackTracker(config)
    state = make_event_state()
    features = make_features(
        price=112.7,
        pullback_from_high_pct=2.0,
        dist_to_vwap_pct=7.0,
        last_low=112.5,
        last_high=114.0,
    )

    updated = tracker.advance(state, features, features.asof)

    assert config.pullback_min_pct == 2.4
    assert config.pullback_hold_vwap_min == 5.5
    assert config.short_zone_range_low_pct == 0.70
    assert config.short_zone_range_high_pct == 0.92
    assert config.vol_zscore_min == 0.8
    assert updated.state == EventStatus.PUMP_DETECTED
    assert updated.pullback_detected_at is None


def test_pullback_tracker_promotes_conservative_2_4_percent_pullback(make_event_state, make_features) -> None:
    config = AppConfig()
    tracker = PullbackTracker(config)
    zone_builder = ShortZoneBuilder(config)
    state = make_event_state()
    features = make_features(
        price=112.2,
        pullback_from_high_pct=2.43,
        dist_to_vwap_pct=8.0,
        last_low=112.0,
        last_high=114.0,
    )

    updated = tracker.advance(state, features, features.asof)
    zone = zone_builder.build(updated, features)

    assert updated.state == EventStatus.PULLBACK_OBSERVED
    assert updated.pullback_detected_at == features.asof
    assert zone is not None
    assert zone.low == 110.5
    assert zone.low <= features.price <= zone.high


def test_pullback_tracker_expires_stale_signal_after_90_minutes(make_event_state, make_features) -> None:
    config = AppConfig()
    tracker = PullbackTracker(config)
    state = make_event_state(expires_at=make_features().asof)
    features = make_features()

    updated = tracker.advance(state, features, features.asof)

    assert config.signal_expiry_minutes == 90
    assert updated.state == EventStatus.EXPIRED


def test_pullback_tracker_resets_stale_pullback_after_confirmed_new_high(make_event_state, make_features) -> None:
    tracker = PullbackTracker(AppConfig())
    confirmed_at = datetime(2026, 7, 24, 12, 5, tzinfo=timezone.utc)
    state = make_event_state(
        state=EventStatus.SHORT_ZONE_ACTIVE,
        pullback_detected_at=confirmed_at,
        pullback_depth_pct=4.0,
        pullback_low_price=109.0,
        zone_low=110.0,
        zone_high=113.0,
        expires_at=confirmed_at + timedelta(hours=1),
    )
    original_event_id = state.event_id
    features = make_features(last_high=116.0, last_high_time=confirmed_at, price=112.0)

    reset = tracker.reset_after_confirmed_high(state, features, confirmed_at)

    assert reset is True
    assert state.event_id == original_event_id
    assert state.state == EventStatus.PUMP_DETECTED
    assert state.event_high == 116.0
    assert state.event_high_time == confirmed_at
    assert state.pullback_detected_at is None
    assert state.pullback_depth_pct is None
    assert state.pullback_low_price is None
    assert state.zone_low is None
    assert state.zone_high is None


def test_pullback_tracker_keeps_active_pullback_when_high_is_not_confirmed(make_event_state, make_features) -> None:
    tracker = PullbackTracker(AppConfig())
    state = make_event_state(
        state=EventStatus.PULLBACK_OBSERVED,
        pullback_detected_at=make_features().asof,
        pullback_depth_pct=3.0,
        pullback_low_price=111.0,
        zone_low=110.0,
        zone_high=113.0,
    )
    features = make_features(last_high=115.0, last_high_time=None)

    reset = tracker.reset_after_confirmed_high(state, features, features.asof)

    assert reset is False
    assert state.state == EventStatus.PULLBACK_OBSERVED
    assert state.zone_low == 110.0


def test_pullback_tracker_requires_a_new_pullback_after_confirmed_high_reset(make_event_state, make_features) -> None:
    tracker = PullbackTracker(AppConfig())
    confirmed_at = datetime(2026, 7, 24, 12, 5, tzinfo=timezone.utc)
    state = make_event_state(
        state=EventStatus.SHORT_ZONE_ACTIVE,
        pullback_detected_at=confirmed_at,
        pullback_depth_pct=4.0,
        pullback_low_price=109.0,
        zone_low=110.0,
        zone_high=113.0,
        expires_at=confirmed_at + timedelta(hours=1),
    )
    reset_features = make_features(last_high=116.0, last_high_time=confirmed_at, price=114.0)
    tracker.reset_after_confirmed_high(state, reset_features, confirmed_at)

    shallow = make_features(last_high=116.0, last_high_time=confirmed_at, price=114.0, last_low=113.5)
    after_shallow = tracker.advance(state, shallow, confirmed_at + timedelta(minutes=1))
    assert after_shallow.state == EventStatus.PUMP_DETECTED
    assert after_shallow.pullback_detected_at is None

    deep = make_features(last_high=116.0, last_high_time=confirmed_at, price=112.0, last_low=111.0)
    after_deep = tracker.advance(after_shallow, deep, confirmed_at + timedelta(minutes=2))

    assert after_deep.state == EventStatus.PULLBACK_OBSERVED
    assert after_deep.pullback_detected_at == confirmed_at + timedelta(minutes=2)


def test_pullback_tracker_interprets_naive_sqlite_expiry_as_utc(make_event_state, make_features) -> None:
    tracker = PullbackTracker(AppConfig())
    now = datetime(2026, 7, 24, 12, 5, tzinfo=timezone.utc)
    state = make_event_state(expires_at=datetime(2026, 7, 24, 12, 5))

    updated = tracker.advance(state, make_features(asof=now), now)

    assert updated.state == EventStatus.EXPIRED
    assert updated.expires_at == now


def test_pump_detector_uses_or_trigger_for_event_windows(make_features) -> None:
    detector = PumpDetector(AppConfig())
    features = make_features(
        ret_15m=1.0,
        ret_1h=9.0,
        ret_4h=1.0,
        dist_to_vwap_pct=7.0,
        dist_to_ema20_atr=0.5,
        vol_zscore_30m=0.2,
        range_atr_ratio=0.2,
    )

    qualifies, trigger_window = detector.qualifies(features)

    assert qualifies is True
    assert trigger_window == "1h"
