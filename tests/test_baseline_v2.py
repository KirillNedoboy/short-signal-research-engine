from datetime import UTC, datetime, timedelta

from app.signals.baseline_v2 import BaselineV2Config, BaselineV2Input, evaluate_baseline_pullback_v2


T = datetime(2026, 1, 1, tzinfo=UTC)
C = BaselineV2Config(2, 10, 1, 0.5, 1, 20)


def value(**overrides):
    values = dict(
        symbol="X", event_id="e", event_started_at=T, event_high_at=T + timedelta(minutes=5),
        event_base=100, event_high=110, decision_at=T + timedelta(minutes=10), decision_price=105,
        pullback_pct=4, rejection_pct=2, wick_ratio=0.2, volume_z=2, oi_change=None, funding=None,
        spread_pct=None, depth=None, data_quality="OK",
    )
    values.update(overrides)
    return BaselineV2Input(**values)


def test_valid_input_is_actionable():
    assert evaluate_baseline_pullback_v2(value(), C).actionable


def test_chronology_is_a_hard_gate():
    result = evaluate_baseline_pullback_v2(value(event_started_at=T + timedelta(minutes=6)), C)
    assert "causal_chronology_invalid" in result.hard_gates


def test_quality_is_a_hard_gate():
    result = evaluate_baseline_pullback_v2(value(data_quality="MISSING"), C)
    assert "data_quality_not_ok" in result.hard_gates


def test_pullback_boundaries_are_inclusive():
    assert evaluate_baseline_pullback_v2(value(pullback_pct=2), C).actionable
    assert evaluate_baseline_pullback_v2(value(pullback_pct=10), C).actionable


def test_pullback_outside_range_is_rejected():
    assert "pullback_out_of_range" in evaluate_baseline_pullback_v2(value(pullback_pct=1.99), C).hard_gates


def test_stale_event_is_rejected():
    result = evaluate_baseline_pullback_v2(value(decision_at=T + timedelta(minutes=26)), C)
    assert "event_stale" in result.hard_gates


def test_rejection_or_wick_can_satisfy_exhaustion_gate():
    by_rejection = evaluate_baseline_pullback_v2(value(rejection_pct=1, wick_ratio=0), C)
    by_wick = evaluate_baseline_pullback_v2(value(rejection_pct=0, wick_ratio=0.5), C)
    neither = evaluate_baseline_pullback_v2(value(rejection_pct=0, wick_ratio=0.49), C)
    assert by_rejection.actionable and by_wick.actionable and "rejection_missing" in neither.hard_gates


def test_invalid_prices_are_rejected():
    result = evaluate_baseline_pullback_v2(value(event_base=float("inf"), decision_price=0), C)
    assert "event_prices_invalid" in result.hard_gates


def test_invalid_config_is_rejected():
    result = evaluate_baseline_pullback_v2(value(), BaselineV2Config(10, 2, 1, 0.5, 1, 20))
    assert "config_invalid" in result.hard_gates


def test_zone_does_not_affect_identity_or_decision():
    left = evaluate_baseline_pullback_v2(value(), C)
    right = evaluate_baseline_pullback_v2(value(zone=(1, 2)), C)
    assert left.identity == right.identity and left.actionable == right.actionable


def test_metric_blocks_have_unique_ownership():
    result = evaluate_baseline_pullback_v2(value(), C)
    keys = [key for block in result.explanation.values() for key in block]
    assert len(keys) == len(set(keys))


def test_extreme_numeric_explanations_are_normalized():
    result = evaluate_baseline_pullback_v2(value(rejection_pct=1e9, wick_ratio=-1e9, volume_z=1e9, oi_change=-1e9, funding=1e9, spread_pct=1e9, depth=1e99), C)
    numeric = [number for block in result.explanation.values() for number in block.values() if isinstance(number, float)]
    assert all(-1 <= number <= 1 for number in numeric)


def test_missing_enrichment_is_explicitly_unknown_and_neutral():
    result = evaluate_baseline_pullback_v2(value(), C)
    assert result.actionable
    assert result.explanation["derivatives"]["derivatives_status"] == "UNKNOWN"
    assert result.explanation["liquidity"]["liquidity_status"] == "UNKNOWN"


def test_identity_is_deterministic():
    assert evaluate_baseline_pullback_v2(value(), C).identity == evaluate_baseline_pullback_v2(value(), C).identity


def test_input_is_immutable():
    candidate = value()
    evaluate_baseline_pullback_v2(candidate, C)
    assert candidate == value()
