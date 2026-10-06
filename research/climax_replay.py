"""Deterministic offline replay helpers for climax-short research."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from math import sqrt
from pathlib import Path
from typing import Any


def load_fixture(path: Path) -> dict[str, Any]:
    """Load a checked-in replay fixture without any network access."""

    with path.open(encoding="utf-8") as fixture_file:
        return json.load(fixture_file)


def run_replay(fixture: dict[str, Any]) -> dict[str, Any]:
    """Replay the fixture without network, DB, or live-runtime dependencies."""

    candles = _candles(fixture)
    blogger_time = _timestamp("2026-07-15T08:08:00Z")
    spike_index, spike = _find_climax(candles)

    models = {
        "M1": _oi_divergence_model(candles, fixture, spike_index, blogger_time),
        "M2": _short_covering_exhaustion_model(candles, spike_index, blogger_time),
        "M3": _taker_flow_reversal_model(fixture),
        "M4": _hybrid_climax_model(candles, spike_index, blogger_time),
    }
    confirmed = [
        (name, model)
        for name, model in models.items()
        if model["status"] == "CONFIRMED"
    ]
    first_name, first_model = min(
        confirmed,
        key=lambda item: item[1]["confirmation_timestamp_ms"],
    )

    return {
        "report_schema_version": 1,
        "symbol": fixture["symbol"],
        "replay_window": fixture["window"],
        "evidence": fixture["evidence"],
        "missing_data": fixture["missing_data"],
        "baseline": _baseline(fixture),
        "climax_features": _climax_features(candles, spike_index),
        "oi_price_observations": _oi_price_observations(candles, fixture),
        "funding_premium": _funding_premium(fixture, blogger_time),
        "models": models,
        "first_confirmed_model": first_name,
        "first_confirmation_time_utc": first_model["confirmation_time_utc"],
        "live_capture_recommendations": [
            "Persist every 1m candle used by the scanner with request/as-of timestamp.",
            "Persist OI snapshots at 1m or the finest available cadence with source timestamp.",
            "Persist funding, premium-index, mark-price, and index-price snapshots.",
            "Persist raw public trades with side, price, size, and exchange timestamp to derive CVD.",
            "Persist orderbook top levels, spread, and depth at each scan and at event transitions.",
            "Persist shortlist eligibility, fetch failures, state transitions, and rejection reasons per cycle.",
        ],
    }


def _candles(fixture: dict[str, Any]) -> list[dict[str, float | int]]:
    return [
        {
            "timestamp": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
        }
        for row in fixture["candles_1m"]
    ]


def _find_climax(candles: list[dict[str, float | int]]) -> tuple[int, dict[str, float | int]]:
    start = _timestamp("2026-07-15T07:55:00Z")
    for index in range(30, len(candles) - 2):
        candle = candles[index]
        if candle["timestamp"] < start:
            continue
        previous = candles[index - 30 : index]
        prior_high = max(row["high"] for row in previous)
        velocity_5m = _pct_change(candle["close"], candles[index - 5]["close"])
        volume_ratio = candle["volume"] / _mean(row["volume"] for row in previous)
        rejection_pct = _pct_change(candle["high"], candle["close"])
        if (
            candle["high"] >= prior_high * 1.01
            and velocity_5m >= 10
            and volume_ratio >= 3
            and rejection_pct >= 4
        ):
            return index, candle
    raise ValueError("No climax candle satisfies the deterministic research rules.")


def _short_covering_exhaustion_model(
    candles: list[dict[str, float | int]],
    spike_index: int,
    blogger_time: int,
) -> dict[str, Any]:
    spike = candles[spike_index]
    confirmation = candles[spike_index + 1]
    failed_continuation = confirmation["close"] <= spike["close"] * 0.97
    if not failed_continuation:
        return _not_confirmed("M2", "failed_continuation_not_observed")
    return _confirmed(
        "M2",
        confirmation,
        candles,
        blogger_time,
        [
            "new_high_event",
            "volume_climax",
            "upper_wick_rejection",
            "acceleration_rollover",
            "failed_continuation",
        ],
        {
            "spike_time_utc": _iso(spike["timestamp"]),
            "spike_high": spike["high"],
            "spike_close": spike["close"],
            "confirmation_close": confirmation["close"],
        },
    )


def _hybrid_climax_model(
    candles: list[dict[str, float | int]],
    spike_index: int,
    blogger_time: int,
) -> dict[str, Any]:
    spike = candles[spike_index]
    first_follow_through = candles[spike_index + 1]
    confirmation = candles[spike_index + 2]
    rollover = (
        first_follow_through["close"] < spike["close"]
        and confirmation["close"] < first_follow_through["close"]
    )
    if not rollover:
        return _not_confirmed("M4", "two_candle_rollover_not_observed")
    return _confirmed(
        "M4",
        confirmation,
        candles,
        blogger_time,
        [
            "new_high_event",
            "volume_climax",
            "upper_wick_rejection",
            "time_near_high",
            "two_candle_acceleration_rollover",
            "failed_continuation",
        ],
        {
            "spike_time_utc": _iso(spike["timestamp"]),
            "first_follow_through_close": first_follow_through["close"],
            "confirmation_close": confirmation["close"],
        },
    )


def _oi_divergence_model(
    candles: list[dict[str, float | int]],
    fixture: dict[str, Any],
    spike_index: int,
    blogger_time: int,
) -> dict[str, Any]:
    spike_time = candles[spike_index]["timestamp"]
    oi_rows = [
        {"timestamp": int(row["timestamp"]), "value": float(row["openInterest"])}
        for row in fixture["open_interest"]["5m"]
    ]
    for previous, current in zip(oi_rows, oi_rows[1:]):
        if current["timestamp"] <= spike_time:
            continue
        before = _candle_at(candles, previous["timestamp"])
        after = _candle_at(candles, current["timestamp"])
        price_change = _pct_change(after["close"], before["close"])
        oi_change = _pct_change(current["value"], previous["value"])
        if price_change >= 2 and oi_change <= -3:
            return _confirmed(
                "M1",
                after,
                candles,
                blogger_time,
                ["price_up_while_oi_down", "post_high_oi_rollover", "short_covering_exhaustion"],
                {
                    "oi_interval_start_utc": _iso(previous["timestamp"]),
                    "oi_interval_end_utc": _iso(current["timestamp"]),
                    "price_change_pct": _round(price_change),
                    "oi_change_pct": _round(oi_change),
                },
            )
    return _not_confirmed("M1", "no_5m_price_oi_divergence")


def _taker_flow_reversal_model(fixture: dict[str, Any]) -> dict[str, Any]:
    missing = fixture["missing_data"]["taker_buy_sell_pressure"]
    return {
        "name": "M3",
        "status": "INSUFFICIENT_DATA",
        "confirmation_time_utc": None,
        "confirmation_timestamp_ms": None,
        "paper_entry": None,
        "delay_vs_blogger_minutes": None,
        "criteria": [],
        "details": {"taker_buy_sell_pressure": missing, "approximate_cvd": fixture["missing_data"]["approximate_cvd"]},
        "missing_reason": missing["missing_reason"],
        "outcomes": None,
        "first_hit": None,
    }


def _confirmed(
    name: str,
    confirmation: dict[str, float | int],
    candles: list[dict[str, float | int]],
    blogger_time: int,
    criteria: list[str],
    details: dict[str, Any],
) -> dict[str, Any]:
    index = candles.index(confirmation)
    entry = float(confirmation["close"])
    return {
        "name": name,
        "status": "CONFIRMED",
        "confirmation_time_utc": _iso(confirmation["timestamp"]),
        "confirmation_timestamp_ms": confirmation["timestamp"],
        "paper_entry": {
            "time_utc": _iso(confirmation["timestamp"]),
            "price": entry,
            "basis": "confirmation_candle_close",
            "side": "SHORT",
        },
        "delay_vs_blogger_minutes": int((confirmation["timestamp"] - blogger_time) // 60_000),
        "criteria": criteria,
        "details": details,
        "missing_reason": None,
        "outcomes": _outcomes(candles, index, entry),
        "first_hit": _first_hits(candles, index, entry),
    }


def _not_confirmed(name: str, reason: str) -> dict[str, Any]:
    return {
        "name": name,
        "status": "NOT_CONFIRMED",
        "confirmation_time_utc": None,
        "confirmation_timestamp_ms": None,
        "paper_entry": None,
        "delay_vs_blogger_minutes": None,
        "criteria": [],
        "details": {},
        "missing_reason": reason,
        "outcomes": None,
        "first_hit": None,
    }


def _outcomes(candles: list[dict[str, float | int]], entry_index: int, entry: float) -> dict[str, Any]:
    outcomes: dict[str, Any] = {}
    for label, minutes in {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240}.items():
        future = candles[entry_index + 1 : entry_index + minutes + 1]
        close = candles[entry_index + minutes]["close"]
        outcomes[label] = {
            "close_time_utc": _iso(candles[entry_index + minutes]["timestamp"]),
            "close_price": close,
            "short_return_pct": _round(_pct_change(entry, close)),
            "mfe_pct": _round(_pct_change(entry, min(row["low"] for row in future))),
            "mae_pct": _round(_pct_change(max(row["high"] for row in future), entry)),
        }
    return outcomes


def _first_hits(candles: list[dict[str, float | int]], entry_index: int, entry: float) -> dict[str, Any]:
    future = candles[entry_index + 1 :]
    return {
        "favorable_5_vs_adverse_3": _first_hit(future, entry, 5, 3),
        "favorable_10_vs_adverse_5": _first_hit(future, entry, 10, 5),
    }


def _first_hit(
    candles: list[dict[str, float | int]], entry: float, favorable_pct: float, adverse_pct: float
) -> str:
    favorable_price = entry * (1 - favorable_pct / 100)
    adverse_price = entry * (1 + adverse_pct / 100)
    for candle in candles:
        favorable = candle["low"] <= favorable_price
        adverse = candle["high"] >= adverse_price
        if favorable and adverse:
            return "AMBIGUOUS_SAME_CANDLE"
        if favorable:
            return f"FAVORABLE_{favorable_pct:g}_FIRST"
        if adverse:
            return f"ADVERSE_{adverse_pct:g}_FIRST"
    return "NEITHER"


def _climax_features(candles: list[dict[str, float | int]], spike_index: int) -> dict[str, Any]:
    spike = candles[spike_index]
    prior = candles[spike_index - 30 : spike_index]
    next_candle = candles[spike_index + 1]
    return {
        "event_new_high": spike["high"] > max(row["high"] for row in prior),
        "event_high_time_utc": _iso(spike["timestamp"]),
        "event_high": spike["high"],
        "price_velocity_5m_pct": _round(_pct_change(spike["close"], candles[spike_index - 5]["close"])),
        "acceleration_rollover": next_candle["close"] < spike["close"],
        "upper_wick_pct": _round(_pct_change(spike["high"], spike["close"])),
        "rejection_from_high_pct": _round(_pct_change(spike["high"], spike["close"])),
        "failed_continuation": next_candle["close"] <= spike["close"] * 0.97,
        "time_near_high_minutes": 1,
        "volume": spike["volume"],
        "volume_ratio_to_prior_30m": _round(spike["volume"] / _mean(row["volume"] for row in prior)),
        "volume_zscore_to_prior_30m": _round(_zscore(spike["volume"], [row["volume"] for row in prior])),
    }


def _oi_price_observations(candles: list[dict[str, float | int]], fixture: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for interval, rows in fixture["open_interest"].items():
        observations = []
        parsed = [{"timestamp": int(row["timestamp"]), "value": float(row["openInterest"])} for row in rows]
        for previous, current in zip(parsed, parsed[1:]):
            before = _candle_at(candles, previous["timestamp"])
            after = _candle_at(candles, current["timestamp"])
            observations.append(
                {
                    "from_utc": _iso(previous["timestamp"]),
                    "to_utc": _iso(current["timestamp"]),
                    "price_change_pct": _round(_pct_change(after["close"], before["close"])),
                    "oi_change_pct": _round(_pct_change(current["value"], previous["value"])),
                }
            )
        result[interval] = observations
    return result


def _funding_premium(fixture: dict[str, Any], blogger_time: int) -> dict[str, Any]:
    funding = min(fixture["funding"], key=lambda row: abs(int(row["fundingRateTimestamp"]) - blogger_time))
    premium = min(fixture["premium_index_1m"], key=lambda row: abs(int(row[0]) - blogger_time))
    return {
        "funding_rate": float(funding["fundingRate"]),
        "funding_time_utc": _iso(int(funding["fundingRateTimestamp"])),
        "premium_index_close": float(premium[4]),
        "premium_time_utc": _iso(int(premium[0])),
    }


def _baseline(fixture: dict[str, Any]) -> dict[str, Any]:
    server = fixture["evidence"]["server_baseline"]
    return {
        **server,
        "lifecycle_at_0808_utc": {
            "value": "PUMP_DETECTED",
            "certainty": "inferred",
            "basis": "The 08:03 EARLY_PUMP_WATCH requires PUMP_DETECTED; the 08:07/08:10 score-0 REJECT rows are consistent with a state that never reached PULLBACK_OBSERVED. Historical event-state versions were not persisted.",
        },
        "signal_emitted": False,
    }


def _candle_at(candles: list[dict[str, float | int]], timestamp: int) -> dict[str, float | int]:
    for candle in candles:
        if candle["timestamp"] == timestamp:
            return candle
    raise ValueError(f"Fixture is missing candle at {timestamp}.")


def _timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp / 1000, UTC).isoformat().replace("+00:00", "Z")


def _pct_change(new_value: float | int, old_value: float | int) -> float:
    return ((float(new_value) / float(old_value)) - 1) * 100


def _mean(values: Any) -> float:
    values = list(values)
    return sum(values) / len(values)


def _zscore(value: float | int, values: list[float | int]) -> float:
    mean = _mean(values)
    variance = sum((float(item) - mean) ** 2 for item in values) / len(values)
    return (float(value) - mean) / sqrt(variance) if variance else 0.0


def _round(value: float) -> float:
    return round(value, 6)
