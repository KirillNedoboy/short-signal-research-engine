"""Lane-B Part A: focused tests for the normalized score paths.

Covers ``score_norm_for_climax`` (boundedness, monotonicity, None handling,
custom weights), the extra ``score_norm`` element returned by ``score_setup``,
and the ``climax_score_weights`` config validator.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.config import AppConfig
from app.domain import ShortZone, SymbolFeatures
from app.signals.climax import DEFAULT_CLIMAX_SCORE_WEIGHTS, score_norm_for_climax
from app.signals.scoring import grade_from_empirical_cdf, score_setup

NEUTRAL = dict(
    rejection_pct=0.0,
    oi_change_15m_pct=0.0,
    current_previous_volume_ratio=1.0,
    ret_5m_pct=0.0,
    latest_failed_retest=False,
)


def call(**overrides) -> int:
    return score_norm_for_climax(**{**NEUTRAL, **overrides})


def test_bounded_zero_to_hundred():
    assert 0 <= call() <= 100
    saturated = call(
        rejection_pct=999,
        oi_change_15m_pct=-999,
        current_previous_volume_ratio=99,
        ret_5m_pct=999,
        latest_failed_retest=True,
    )
    # Default weights cap the reachable maximum at 97 (x_confirm tops out at 1.0).
    assert 90 <= saturated <= 100
    # With weights that put the whole budget on rej+confirm, 100 is attainable.
    assert call(
        rejection_pct=999,
        oi_change_15m_pct=0.0,
        current_previous_volume_ratio=1.0,
        ret_5m_pct=0.0,
        latest_failed_retest=True,
        weights={"rej": 60.0, "oi": 0.0, "vol": 0.0, "price": 0.0, "confirm": 60.0},
    ) == 100


def test_monotone_in_rejection():
    xs = [call(rejection_pct=v) for v in (0.0, 1.0, 2.0, 3.0, 4.0)]
    assert all(b >= a for a, b in zip(xs, xs[1:]))
    assert xs[1] > xs[0]


def test_monotone_in_negative_oi_change():
    xs = [call(oi_change_15m_pct=v) for v in (1.0, 0.0, -0.5, -1.5, -3.0)]
    assert all(b >= a for a, b in zip(xs, xs[1:]))
    assert xs[-1] > xs[0]


def test_monotone_in_abs_volume_ratio_deviation():
    xs = [call(current_previous_volume_ratio=r) for r in (1.0, 1.1, 1.3, 1.6, 2.0)]
    assert all(b >= a for a, b in zip(xs, xs[1:]))
    assert xs[-1] > xs[0]
    ys = [call(current_previous_volume_ratio=r) for r in (1.0, 0.9, 0.7, 0.4, 0.0)]
    assert all(b >= a for a, b in zip(ys, ys[1:]))
    assert ys[-1] > ys[0]


def test_monotone_in_ret_5m():
    xs = [call(ret_5m_pct=v) for v in (0.0, 1.0, 3.0, 4.5, 6.0)]
    assert all(b >= a for a, b in zip(xs, xs[1:]))
    assert xs[-1] > xs[0]


def test_failed_retest_scores_higher():
    assert call(latest_failed_retest=True) > call(latest_failed_retest=False)


def test_none_inputs_do_not_raise():
    value = score_norm_for_climax(
        rejection_pct=0.0,
        oi_change_15m_pct=None,
        current_previous_volume_ratio=None,
        ret_5m_pct=0.0,
        latest_failed_retest=False,
    )
    assert 0 <= value <= 100
    assert value < 50  # neutral inputs give a low score


def test_custom_weights_honoured():
    heavy_rej = {"rej": 100.0, "oi": 0.0, "vol": 0.0, "price": 0.0, "confirm": 0.0}
    partial = {"rej": 90.0}
    got = call(rejection_pct=3.0, weights=partial)
    expected = call(
        rejection_pct=3.0, weights={**DEFAULT_CLIMAX_SCORE_WEIGHTS, "rej": 90.0}
    )
    assert got == expected
    assert call(rejection_pct=3.0, weights=heavy_rej) > call(rejection_pct=3.0)


def test_default_path_equals_explicit_defaults():
    assert call(rejection_pct=2.5) == call(
        rejection_pct=2.5, weights=dict(DEFAULT_CLIMAX_SCORE_WEIGHTS)
    )
    assert call(rejection_pct=2.5, weights=None) == call(rejection_pct=2.5)


def _max_features() -> tuple[SymbolFeatures, ShortZone]:
    # Every scoring component sits at its per-segment maximum => raw_total = 90.
    features = SymbolFeatures(
        symbol="TESTUSDT",
        asof=datetime(2026, 9, 15, tzinfo=timezone.utc),
        price=111.0,
        ret_5m=0.0,
        ret_15m=12.0,
        ret_1h=15.0,
        ret_4h=35.0,
        vwap=100.0,
        dist_to_vwap_pct=12.0,
        ema20=100.0,
        dist_to_ema20_pct=11.0,
        dist_to_ema20_atr=4.0,
        rsi_15m=50.0,
        upper_wick_ratio=0.25,
        lower_wick_ratio=0.1,
        body_pct=1.0,
        rejection_from_high_pct=1.6,
        close_position_in_range=0.2,
        vol_zscore_30m=2.0,
        vol_zscore_1h=1.0,
        atr_14=1.0,
        range_atr_ratio=2.2,
        oi_change_15m=1.0,
        oi_change_1h=1.0,
        funding_rate=0.001,
        event_range_pct=22.0,
        pullback_from_high_pct=3.5,
        latest_failed_retest=True,
    )
    zone = ShortZone(low=100.0, high=120.0, mode="event_range")
    return features, zone


def test_score_setup_returns_five_elements():
    features, zone = _max_features()
    result = score_setup(features, zone, AppConfig())
    assert len(result) == 5
    total, breakdown, flags, _breakout_risk, score_norm = result
    assert isinstance(total, int)
    assert isinstance(breakdown, dict)
    assert isinstance(flags, list)
    assert isinstance(score_norm, int)
    assert 0 <= score_norm <= 100


def test_score_norm_is_hundred_at_raw_total_ninety():
    features, zone = _max_features()
    _, breakdown, _, _, score_norm = score_setup(features, zone, AppConfig())
    assert breakdown["raw_total"] == pytest.approx(90.0)
    assert breakdown["score_norm"] == pytest.approx(100.0)
    assert score_norm == 100


def test_score_norm_formula_matches_breakdown():
    features, zone = _max_features()
    _, breakdown, _, _, score_norm = score_setup(features, zone, AppConfig())
    expected = int(round(min(100.0, breakdown["raw_total"] / 90.0 * 100.0)))
    assert score_norm == expected


def test_weights_not_summing_to_hundred_rejected():
    with pytest.raises(Exception):
        AppConfig(climax_score_weights={"rej": 50.0, "oi": 20.0})


def test_missing_weight_keys_filled_from_defaults():
    # rej=40 + confirm=0; oi/vol/price auto-filled at 20 each => sums to 100.
    config = AppConfig(climax_score_weights={"rej": 40.0, "confirm": 0.0})
    assert config.climax_score_weights == {
        "rej": 40.0,
        "oi": 20.0,
        "vol": 20.0,
        "price": 20.0,
        "confirm": 0.0,
    }


def test_empirical_grade_uses_right_continuous_ecdf():
    reference = [10, 20, 20, 30, 40, 50, 60, 70, 80, 90]
    assert grade_from_empirical_cdf(80, reference, min_samples=10) == "A"
    assert grade_from_empirical_cdf(50, reference, min_samples=10) == "B"
    assert grade_from_empirical_cdf(10, reference, min_samples=10) == "C"


def test_empirical_grade_falls_back_when_reference_is_small_or_invalid():
    assert grade_from_empirical_cdf(80, [10, 20, float("nan")], min_samples=3) is None
    assert grade_from_empirical_cdf(80, [10, 20], min_samples=3) is None


def test_empirical_grade_rejects_invalid_percentile_order():
    with pytest.raises(ValueError):
        grade_from_empirical_cdf(80, [10], min_samples=1, a_percentile=0.5, b_percentile=0.5)


def test_grade_ecdf_default_disabled():
    config = AppConfig()
    assert config.grade_ecdf_enabled is False
    assert config.grade_ecdf_min_samples == 100
    assert config.climax_score_weights["vol"] == 20.0


def test_grade_ecdf_percentile_order_is_validated():
    with pytest.raises(Exception):
        AppConfig(grade_ecdf_a_percentile=0.5, grade_ecdf_b_percentile=0.5)
