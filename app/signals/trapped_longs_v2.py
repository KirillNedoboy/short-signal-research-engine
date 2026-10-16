"""Pure shadow contract for the TRAPPED_LONGS_REVERSAL_V2 hypothesis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite

import pandas as pd

from app.signals.trapped_longs import classify_trapped_longs_oi_sequence
from app.market.candles import complete_5m_ohlcv


@dataclass(frozen=True, slots=True)
class FrozenBreakout:
    """The immutable breakout facts established by the root-event detector."""

    identity: str
    occurred_at: datetime
    reference: float | None
    revision: int
    symbol: str = ""
    horizon: str = "5m"


@dataclass(frozen=True, slots=True)
class OiObservation:
    """One timestamped open-interest observation supplied by the caller."""

    observed_at: datetime
    value: float | None


@dataclass(frozen=True, slots=True)
class ExplanationFactor:
    """A named, measurable V2 predicate result."""

    measurement: object
    passed: bool


@dataclass(frozen=True, slots=True)
class TrappedLongsV2Result:
    identity: str
    revision: int
    actionable: bool
    blockers: tuple[str, ...]
    explanation: dict[str, ExplanationFactor]


_REQUIRED_COLUMNS = frozenset({"timestamp", "open", "high", "low", "close", "volume"})
TRAPPED_LONGS_REVERSAL_V2 = "TRAPPED_LONGS_REVERSAL_V2"
TRAPPED_LONGS_V2_MODEL_VERSION = "trapped-longs-v2"


def evaluate_trapped_longs_reversal_v2(
    frame_1m: pd.DataFrame,
    breakout: FrozenBreakout,
    oi_observations: tuple[OiObservation, ...],
    decision_at: datetime,
    config: object | None = None,
    *,
    min_closed_5m_candles_after_breakout: int | None = None,
    new_high_tolerance_pct: float | None = None,
    block_price_oi_acceleration_together: bool | None = None,
    price_acceleration_veto_pct: float | None = None,
    oi_acceleration_veto_pct: float | None = None,
    require_oi_sequence: bool | None = None,
    require_breakout_oi_expansion: bool | None = None,
    require_oi_sequence_points: int | None = None,
    max_oi_age_minutes: float | None = None,
) -> TrappedLongsV2Result:
    """Evaluate frozen breakout failure without mutating caller-owned inputs.

    Candles are resampled from normalized one-minute input.  A bin is eligible
    only once its UTC close time is at or before ``decision_at``.
    """
    blockers: list[str] = []
    explanation: dict[str, ExplanationFactor] = {}
    decision_at_utc = _safe_utc(decision_at)
    decision_at = decision_at_utc or datetime.min.replace(tzinfo=UTC)
    identity = _identity(breakout)
    reference = getattr(breakout, "reference", None)
    breakout_at = _safe_utc(getattr(breakout, "occurred_at", None))
    revision = _safe_revision(getattr(breakout, "revision", None))
    breakout_identity = getattr(breakout, "identity", None)
    if decision_at_utc is None:
        blockers.append("decision_at_invalid")
    min_closed, min_closed_invalid = _validated_count(
        _cfg(config, "trapped_longs_min_closed_5m_candles_after_breakout", 2)
        if min_closed_5m_candles_after_breakout is None
        else min_closed_5m_candles_after_breakout,
        default=2,
    )
    tolerance_pct, tolerance_invalid = _validated_nonnegative_float(
        _cfg(config, "trapped_longs_new_high_tolerance_pct", 0.30)
        if new_high_tolerance_pct is None
        else new_high_tolerance_pct,
        default=0.30,
    )
    acceleration_enabled = bool(_cfg(config, "trapped_longs_block_price_oi_acceleration_together", True) if block_price_oi_acceleration_together is None else block_price_oi_acceleration_together)
    price_accel_pct, price_accel_invalid = _validated_nonnegative_float(
        _cfg(config, "trapped_longs_price_acceleration_veto_pct", 0.0)
        if price_acceleration_veto_pct is None
        else price_acceleration_veto_pct,
        default=0.0,
    )
    oi_accel_pct, oi_accel_invalid = _validated_nonnegative_float(
        _cfg(config, "trapped_longs_oi_acceleration_veto_pct", 0.0)
        if oi_acceleration_veto_pct is None
        else oi_acceleration_veto_pct,
        default=0.0,
    )
    require_sequence = bool(_cfg(config, "trapped_longs_require_oi_sequence", True) if require_oi_sequence is None else require_oi_sequence)
    require_expansion = bool(_cfg(config, "trapped_longs_require_breakout_oi_expansion", True) if require_breakout_oi_expansion is None else require_breakout_oi_expansion)
    required_points, required_points_invalid = _validated_count(
        _cfg(config, "trapped_longs_require_oi_sequence_points", 4)
        if require_oi_sequence_points is None
        else require_oi_sequence_points,
        default=4,
    )
    if required_points != 4:
        explanation["oi_sequence_contract"] = ExplanationFactor(
            {"required_points": required_points, "contract_points": 4}, False
        )
        blockers.append("oi_sequence_points_invalid")
        required_points_invalid = True
    max_oi_age, max_oi_age_invalid = _validated_nonnegative_float(
        _cfg(config, "trapped_longs_max_oi_age_minutes", 60.0)
        if max_oi_age_minutes is None
        else max_oi_age_minutes,
        default=60.0,
    )
    retained_increase_pct, retained_increase_invalid = _validated_nonnegative_float(
        _cfg(config, "trapped_longs_v2_min_oi_retained_increase_pct", 1.0),
        default=1.0,
    )
    if any((min_closed_invalid, tolerance_invalid, price_accel_invalid, oi_accel_invalid, required_points_invalid, max_oi_age_invalid, retained_increase_invalid)):
        blockers.append("config_invalid")

    reference_valid = bool(breakout_identity) and _finite_positive(reference)
    reference_valid = reference_valid and breakout_at is not None and revision > 0
    explanation["frozen_breakout"] = ExplanationFactor(
        {
            "identity": breakout_identity,
            "reference": reference,
            "occurred_at": None if breakout_at is None else breakout_at.isoformat(),
            "revision": getattr(breakout, "revision", None),
        },
        reference_valid,
    )
    if not _finite_positive(reference):
        blockers.append("frozen_breakout_reference_missing")
    if breakout_at is None:
        blockers.append("breakout_occurred_at_invalid")
    if not isinstance(breakout_identity, str) or not breakout_identity.strip():
        blockers.append("breakout_identity_missing")
    if revision <= 0:
        blockers.append("breakout_revision_invalid")
    if breakout_at is not None and decision_at_utc is not None and decision_at < breakout_at:
        blockers.append("decision_before_breakout")

    if not _valid_frame(frame_1m):
        explanation["closed_5m_candles"] = ExplanationFactor(0, False)
        blockers.append("candle_frame_invalid")
        _add_unknown_oi(explanation, blockers, oi_observations)
        return _result(identity, revision, blockers, explanation)
    try:
        closed = complete_5m_ohlcv(frame_1m, decision_at)
    except (KeyError, TypeError, ValueError, OverflowError):
        explanation["closed_5m_candles"] = ExplanationFactor(0, False)
        blockers.append("candle_frame_invalid")
        _add_unknown_oi(explanation, blockers, oi_observations)
        return _result(identity, revision, blockers, explanation)
    explanation["closed_5m_candles"] = ExplanationFactor(len(closed), not closed.empty)
    if closed.empty:
        blockers.append("closed_5m_candles_missing")

    if not reference_valid or closed.empty:
        _add_unknown_oi(explanation, blockers, oi_observations)
        return _result(identity, revision, blockers, explanation)

    reference_value = float(reference)
    breakout_bin = _breakout_bin(closed, breakout_at, reference_value) if breakout_at is not None else None
    explanation["breakout_validation"] = ExplanationFactor(
        None if breakout_bin is None else breakout_bin.isoformat(), breakout_bin is not None
    )
    if breakout_bin is None:
        blockers.append("frozen_breakout_not_observed")
        _add_unknown_oi(explanation, blockers, oi_observations)
        return _result(identity, revision, blockers, explanation)

    after_breakout = closed.loc[closed.index > breakout_bin]
    failures = after_breakout.loc[after_breakout["close"] < reference_value]
    failure_bin = failures.index[0] if not failures.empty else None
    explanation["close_below_failure"] = ExplanationFactor(
        None if failure_bin is None else failure_bin.isoformat(), failure_bin is not None
    )
    if failure_bin is None:
        blockers.append("failure_after_breakout_missing")

    retest_bin = None
    if failure_bin is not None:
        after_failure = closed.loc[closed.index > failure_bin]
        retests = after_failure.loc[
            (after_failure["high"] >= reference_value)
            & (after_failure["close"] < reference_value)
        ]
        if not retests.empty:
            retest_bin = retests.index[0]
    explanation["failed_retest"] = ExplanationFactor(
        None if retest_bin is None else retest_bin.isoformat(), retest_bin is not None
    )
    if retest_bin is None:
        blockers.append("failed_retest_missing")

    closed_after_breakout = 0 if breakout_bin is None else int((closed.index > breakout_bin).sum())
    explanation["closed_5m_candles_after_breakout"] = ExplanationFactor(
        closed_after_breakout, closed_after_breakout >= min_closed
    )
    if breakout_bin is not None and closed_after_breakout < min_closed:
        blockers.append("insufficient_closed_5m_candles")

    new_high = bool((after_breakout["high"] > reference_value * (1 + tolerance_pct / 100)).any())
    explanation["new_high_before_delivery"] = ExplanationFactor(new_high, not new_high)
    if new_high:
        blockers.append("new_high_before_delivery")

    if failure_bin is not None and failure_bin <= breakout_bin:
        blockers.append("failure_chronology_invalid")
    if retest_bin is not None and (failure_bin is None or retest_bin <= failure_bin):
        blockers.append("retest_chronology_invalid")

    _evaluate_oi(
        explanation,
        blockers,
        oi_observations,
        closed,
        breakout_bin,
        failure_bin,
        retest_bin,
        decision_at,
        min_retained_increase_pct=retained_increase_pct,
        acceleration_enabled=acceleration_enabled,
        price_acceleration_veto_pct=price_accel_pct,
        oi_acceleration_veto_pct=oi_accel_pct,
        require_oi_sequence=require_sequence,
        require_breakout_oi_expansion=require_expansion,
        require_oi_sequence_points=required_points,
        max_oi_age_minutes=max_oi_age,
    )
    return _result(identity, revision, blockers, explanation)



def _breakout_bin(candles: pd.DataFrame, breakout_at: datetime, reference: float) -> pd.Timestamp | None:
    expected = pd.Timestamp(breakout_at).floor("5min") + pd.Timedelta(minutes=5)
    candidates = candles.loc[candles.index >= expected]
    for timestamp, candle in candidates.iterrows():
        if float(candle["close"]) > reference:
            return pd.Timestamp(timestamp)
    return None


def _evaluate_oi(
    explanation: dict[str, ExplanationFactor],
    blockers: list[str],
    observations: tuple[OiObservation, ...],
    candles: pd.DataFrame,
    breakout_bin: pd.Timestamp,
    failure_bin: pd.Timestamp | None,
    retest_bin: pd.Timestamp | None,
    decision_at: datetime,
    *,
    min_retained_increase_pct: float,
    acceleration_enabled: bool,
    price_acceleration_veto_pct: float,
    oi_acceleration_veto_pct: float,
    require_oi_sequence: bool,
    require_breakout_oi_expansion: bool,
    require_oi_sequence_points: int,
    max_oi_age_minutes: float,
) -> None:
    if not require_oi_sequence:
        explanation["oi_sequence"] = ExplanationFactor({"required": False}, True)
        return
    if not observations:
        _add_unknown_oi(explanation, blockers, observations)
        return
    if require_oi_sequence_points < 1 or len(observations) < require_oi_sequence_points:
        _add_unknown_oi(explanation, blockers, observations, reason="oi_sequence_points_missing")
        return
    if failure_bin is None or retest_bin is None:
        _add_unknown_oi(explanation, blockers, observations)
        return
    ordered = tuple(sorted(observations, key=lambda item: _utc(item.observed_at)))
    input_times = tuple(_utc(item.observed_at) for item in observations)
    if len(set(input_times)) != len(input_times) or input_times != tuple(sorted(input_times)):
        _add_unknown_oi(explanation, blockers, observations)
        blockers.append("oi_sequence_chronology_invalid")
        return
    if any(not _finite_positive(item.value) for item in ordered):
        _add_unknown_oi(explanation, blockers, observations, reason="oi_sequence_invalid")
        return
    selected = _select_oi_sequence(
        ordered,
        breakout_bin.to_pydatetime(),
        failure_bin,
        retest_bin,
        decision_at,
    )
    if selected is None:
        _add_unknown_oi(explanation, blockers, observations)
        return
    if len(selected) != 4:
        _add_unknown_oi(explanation, blockers, observations, reason="oi_sequence_points_invalid")
        return
    observations = selected
    timestamps = tuple(_utc(item.observed_at) for item in observations)
    if any((decision_at - timestamp).total_seconds() > max_oi_age_minutes * 60 for timestamp in timestamps):
        _add_unknown_oi(explanation, blockers, observations, reason="oi_sequence_stale")
        explanation["oi_staleness"] = ExplanationFactor(
            {"max_age_minutes": max_oi_age_minutes, "oldest_age_minutes": (decision_at - timestamps[0]).total_seconds() / 60},
            False,
        )
        return
    if not (timestamps[0] <= breakout_bin.to_pydatetime() <= timestamps[1] <= failure_bin.to_pydatetime() <= timestamps[2] <= retest_bin.to_pydatetime() <= timestamps[3] <= decision_at):
        _add_unknown_oi(explanation, blockers, observations)
        return
    pre = candles.loc[candles.index < breakout_bin]
    if pre.empty:
        _add_unknown_oi(explanation, blockers, observations)
        return
    classification = classify_trapped_longs_oi_sequence(
        price_before=float(pre["close"].iloc[-1]),
        price_breakout=float(candles.loc[breakout_bin, "close"]),
        price_failure=float(candles.loc[failure_bin, "close"]),
        oi_before=observations[0].value,
        oi_breakout=observations[1].value,
        oi_failure=observations[2].value,
        oi_decision=observations[3].value,
    )
    strong = (
        float(observations[1].value) > float(observations[0].value)
        if require_breakout_oi_expansion
        else float(observations[0].value) > 0
    ) and float(observations[2].value) >= float(observations[0].value) * (1 + min_retained_increase_pct / 100) and float(observations[3].value) >= float(observations[0].value) * (1 + min_retained_increase_pct / 100)
    if strong and classification != "STRONG_TRAPPED_LONG_EVIDENCE":
        classification = "STRONG_TRAPPED_LONG_EVIDENCE"
    explanation["oi_sequence"] = ExplanationFactor(
        {"classification": classification, "values": tuple(item.value for item in observations)}, strong
    )
    if classification == "OI_UNKNOWN":
        blockers.append("oi_sequence_unknown")
    elif not strong:
        blockers.append("oi_sequence_not_trapped")
    retained_floor = float(observations[0].value) * (1 + min_retained_increase_pct / 100)
    retained = float(observations[2].value) >= retained_floor and float(observations[3].value) >= retained_floor
    explanation["oi_retention_after_failure"] = ExplanationFactor(
        {"floor": retained_floor, "failure": observations[2].value, "decision": observations[3].value}, retained
    )
    if not retained:
        blockers.append("oi_sequence_not_retained")
    if acceleration_enabled:
        decision_rows = candles.loc[candles.index <= pd.Timestamp(decision_at)]
        if not decision_rows.empty:
            failure_close = float(candles.loc[failure_bin, "close"])
            decision_close = float(decision_rows["close"].iloc[-1])
            price_change = (decision_close - failure_close) / failure_close * 100
            oi_change = (float(observations[3].value) - float(observations[2].value)) / float(observations[2].value) * 100
            accelerated = price_change >= price_acceleration_veto_pct and oi_change >= oi_acceleration_veto_pct
            explanation["price_oi_continuation_acceleration"] = ExplanationFactor(
                {"price_change_pct": price_change, "oi_change_pct": oi_change}, not accelerated
            )
            if accelerated:
                blockers.append("price_oi_continuation_acceleration")

def _select_oi_sequence(
    observations: tuple[OiObservation, ...],
    breakout_at: datetime,
    failure_at: pd.Timestamp,
    retest_at: pd.Timestamp,
    decision_at: datetime,
) -> tuple[OiObservation, ...] | None:
    """Select four distinct as-of observations around price anchors."""
    breakout_at = _utc(breakout_at)
    failure_at = failure_at.to_pydatetime()
    retest_at = retest_at.to_pydatetime()
    decision_at = _utc(decision_at)
    used: set[datetime] = set()

    def take(predicate, *, latest: bool = False) -> OiObservation | None:
        candidates = [
            item for item in observations
            if _utc(item.observed_at) not in used and predicate(_utc(item.observed_at))
        ]
        if not candidates:
            return None
        item = candidates[-1] if latest else candidates[0]
        used.add(_utc(item.observed_at))
        return item

    pre = take(lambda value: value < breakout_at, latest=True)
    breakout = take(lambda value: value >= breakout_at and value <= failure_at)
    failure = take(lambda value: value >= failure_at and value <= retest_at)
    decision = take(lambda value: value >= retest_at and value <= decision_at)
    if None in (pre, breakout, failure, decision):
        return None
    return (pre, breakout, failure, decision)


def _add_unknown_oi(
    explanation: dict[str, ExplanationFactor],
    blockers: list[str],
    observations: tuple[OiObservation, ...],
    *,
    reason: str = "oi_sequence_unknown",
) -> None:
    explanation["oi_sequence"] = ExplanationFactor(
        {"classification": "UNKNOWN", "observations": len(observations)}, False
    )
    blockers.append(reason)


def _finite_positive(value: object) -> bool:
    try:
        return isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError, OverflowError):
        return False


def _validated_nonnegative_float(value: object, *, default: float) -> tuple[float, bool]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return default, True
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default, True
    return (numeric, False) if isfinite(numeric) and numeric >= 0 else (default, True)


def _validated_count(value: object, *, default: int) -> tuple[int, bool]:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return default, True
    return value, False


def _safe_revision(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0


def _safe_utc(value: object) -> datetime | None:
    try:
        return _utc(value)  # type: ignore[arg-type]
    except (AttributeError, TypeError, ValueError, OverflowError, OSError):
        return None


def _valid_frame(frame: object) -> bool:
    if not isinstance(frame, pd.DataFrame) or not _REQUIRED_COLUMNS.issubset(frame.columns):
        return False
    try:
        timestamps = pd.to_datetime(
            frame["timestamp"] if "timestamp" in frame else frame.index,
            utc=True,
            errors="coerce",
        )
        if timestamps.isna().any():
            return False
        for column in _REQUIRED_COLUMNS - {"timestamp"}:
            values = pd.to_numeric(frame[column], errors="coerce")
            if values.isna().any() or not values.map(isfinite).all():
                return False
            if column == "volume" and (values < 0).any():
                return False
    except (TypeError, ValueError, OverflowError):
        return False
    return True


def _cfg(config: object | None, name: str, default: object) -> object:
    return getattr(config, name, default) if config is not None else default


def _identity(breakout: FrozenBreakout) -> str:
    identity = getattr(breakout, "identity", None)
    symbol = getattr(breakout, "symbol", None)
    horizon = getattr(breakout, "horizon", None)
    occurred_at = getattr(breakout, "occurred_at", None)
    reference = getattr(breakout, "reference", None)
    revision = getattr(breakout, "revision", None)
    material = ":".join(
        (
            "TRAPPED_LONGS_REVERSAL_V2",
            _stable_text(symbol),
            _stable_text(horizon),
            _stable_text(identity),
            _canonical_datetime(occurred_at),
            _canonical_price(reference),
            _stable_text(revision),
        )
    )
    return f"trapped-longs-v2:{sha256(material.encode()).hexdigest()[:20]}"


def _canonical_datetime(value: datetime) -> str:
    normalized = _safe_utc(value)
    return "INVALID" if normalized is None else normalized.isoformat(timespec="microseconds")


def _canonical_price(value: float | None) -> str:
    try:
        return "UNKNOWN" if value is None else format(value, ".12g")
    except (TypeError, ValueError):
        return "INVALID"


def _stable_text(value: object) -> str:
    return "NONE" if value is None else str(value)


def _result(
    identity: str,
    revision: int,
    blockers: list[str],
    explanation: dict[str, ExplanationFactor],
) -> TrappedLongsV2Result:
    return TrappedLongsV2Result(identity, revision, not blockers, tuple(dict.fromkeys(blockers)), explanation)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
