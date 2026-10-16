from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite
from typing import Mapping


@dataclass(frozen=True, slots=True)
class BaselineV2Config:
    min_pullback: float
    max_pullback: float
    min_rejection: float
    min_wick_ratio: float
    min_volume: float
    max_age_minutes: int
    extension_scale_pct: float = 100.0
    age_scale_minutes: float = 60.0
    pullback_scale_pct: float = 100.0
    rejection_scale_pct: float = 100.0
    wick_ratio_scale: float = 1.0
    volume_z_scale: float = 5.0
    oi_change_scale_pct: float = 100.0
    funding_scale: float = 0.01
    spread_scale_pct: float = 1.0
    depth_scale: float = 100_000.0


@dataclass(frozen=True, slots=True)
class BaselineV2Input:
    symbol: str
    event_id: str
    event_started_at: datetime
    event_high_at: datetime
    event_base: float
    event_high: float
    decision_at: datetime
    decision_price: float
    pullback_pct: float
    rejection_pct: float
    wick_ratio: float
    volume_z: float | None
    oi_change: float | None
    funding: float | None
    spread_pct: float | None
    depth: float | None
    data_quality: str
    zone: tuple[float, float] | None = None


@dataclass(frozen=True, slots=True)
class BaselineV2Result:
    identity: str
    actionable: bool
    hard_gates: tuple[str, ...]
    explanation: dict[str, Mapping[str, float | str | None]]
    calibrated_probability: None = None
    expected_utility: None = None


def evaluate_baseline_pullback_v2(value: BaselineV2Input, config: BaselineV2Config) -> BaselineV2Result:
    gates: list[str] = []
    if not _config_valid(config):
        gates.append("config_invalid")
    inputs_valid = _event_inputs_valid(value)
    if not inputs_valid:
        gates.append("event_prices_invalid")
    age = (_utc(value.decision_at) - _utc(value.event_high_at)).total_seconds() / 60
    if _utc(value.event_started_at) > _utc(value.event_high_at) or age < 0:
        gates.append("causal_chronology_invalid")
    if value.data_quality != "OK":
        gates.append("data_quality_not_ok")
    if inputs_valid and _finite(value.pullback_pct) and not config.min_pullback <= value.pullback_pct <= config.max_pullback:
        gates.append("pullback_out_of_range")
    elif not _finite(value.pullback_pct):
        gates.append("pullback_out_of_range")
    if _finite(age) and age > config.max_age_minutes:
        gates.append("event_stale")
    if not (_finite(value.rejection_pct) and _finite(value.wick_ratio) and (value.rejection_pct >= config.min_rejection or value.wick_ratio >= config.min_wick_ratio)):
        gates.append("rejection_missing")
    if value.volume_z is None or not _finite(value.volume_z) or value.volume_z < config.min_volume:
        gates.append("volume_missing_or_low")
    return BaselineV2Result(_identity(value), not gates, tuple(dict.fromkeys(gates)), _explanation(value, config, age))


def _config_valid(config: BaselineV2Config) -> bool:
    finite_positive = (
        config.max_age_minutes,
        config.extension_scale_pct,
        config.age_scale_minutes,
        config.pullback_scale_pct,
        config.rejection_scale_pct,
        config.wick_ratio_scale,
        config.volume_z_scale,
        config.oi_change_scale_pct,
        config.funding_scale,
        config.spread_scale_pct,
        config.depth_scale,
    )
    return (
        all(_finite(item) and item > 0 for item in finite_positive)
        and _finite(config.min_pullback)
        and _finite(config.max_pullback)
        and _finite(config.min_rejection)
        and _finite(config.min_wick_ratio)
        and _finite(config.min_volume)
        and 0 <= config.min_pullback <= config.max_pullback
        and config.min_rejection >= 0
        and config.min_wick_ratio >= 0
    )


def _event_inputs_valid(value: BaselineV2Input) -> bool:
    return all(_finite(item) and item > 0 for item in (value.event_base, value.event_high, value.decision_price)) and value.event_high > value.event_base


def _explanation(value: BaselineV2Input, config: BaselineV2Config, age: float) -> dict[str, Mapping[str, float | str | None]]:
    return {
        "event_context": {
            "extension": _normalized((value.event_high - value.event_base) / value.event_base * 100 if _finite(value.event_base) and value.event_base > 0 and _finite(value.event_high) else None, config.extension_scale_pct),
            "age_minutes": _normalized(age, config.age_scale_minutes),
        },
        "exhaustion_rejection": {
            "rejection_pct": _normalized(value.rejection_pct, config.rejection_scale_pct),
            "wick_ratio": _normalized(value.wick_ratio, config.wick_ratio_scale),
        },
        "volume": {"zscore": _normalized(value.volume_z, config.volume_z_scale)},
        "derivatives": {
            "derivatives_status": "UNKNOWN" if value.oi_change is None or value.funding is None else "AVAILABLE",
            "oi_change": _normalized(value.oi_change, config.oi_change_scale_pct),
            "funding": _normalized(value.funding, config.funding_scale),
        },
        "liquidity": {
            "liquidity_status": "UNKNOWN" if value.spread_pct is None or value.depth is None else "AVAILABLE",
            "spread_pct": _normalized(value.spread_pct, config.spread_scale_pct),
            "depth": _normalized(value.depth, config.depth_scale),
        },
        "timing_data_quality": {
            "quality": value.data_quality,
            "pullback_pct": _normalized(value.pullback_pct, config.pullback_scale_pct),
        },
    }


def _normalized(metric: float | None, scale: float) -> float | None:
    if metric is None or not _finite(metric):
        return None
    return max(-1.0, min(1.0, float(metric) / scale))


def _identity(value: BaselineV2Input) -> str:
    return "baseline-pullback-v2:" + sha256(f"{value.symbol}:{value.event_id}".encode()).hexdigest()[:20]


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and isfinite(float(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
