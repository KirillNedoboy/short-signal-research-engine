"""Pure shadow contract for the TRAPPED_LONGS_REVERSAL_V2 hypothesis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256

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
) -> TrappedLongsV2Result:
    """Evaluate frozen breakout failure without mutating caller-owned inputs.

    Candles are resampled from normalized one-minute input.  A bin is eligible
    only once its UTC close time is at or before ``decision_at``.
    """
    blockers: list[str] = []
    explanation: dict[str, ExplanationFactor] = {}
    decision_at = _utc(decision_at)
    identity = _identity(breakout)
    reference = breakout.reference
    breakout_at = _utc(breakout.occurred_at)

    reference_valid = (
        bool(breakout.identity)
        and reference is not None
        and float(reference) > 0
        and breakout.revision > 0
    )
    explanation["frozen_breakout"] = ExplanationFactor(
        {
            "identity": breakout.identity,
            "reference": reference,
            "occurred_at": breakout_at.isoformat(),
            "revision": breakout.revision,
        },
        reference_valid,
    )
    if reference is None or not isinstance(reference, (int, float)) or reference <= 0:
        blockers.append("frozen_breakout_reference_missing")
    if not breakout.identity:
        blockers.append("breakout_identity_missing")
    if breakout.revision <= 0:
        blockers.append("breakout_revision_invalid")
    if decision_at < breakout_at:
        blockers.append("decision_before_breakout")

    closed = complete_5m_ohlcv(frame_1m, decision_at)
    explanation["closed_5m_candles"] = ExplanationFactor(len(closed), not closed.empty)
    if closed.empty:
        blockers.append("closed_5m_candles_missing")

    if not reference_valid or closed.empty:
        _add_unknown_oi(explanation, blockers, oi_observations)
        return _result(identity, breakout.revision, blockers, explanation)

    reference_value = float(reference)
    breakout_bin = _breakout_bin(closed, breakout_at, reference_value)
    explanation["breakout_validation"] = ExplanationFactor(
        None if breakout_bin is None else breakout_bin.isoformat(), breakout_bin is not None
    )
    if breakout_bin is None:
        blockers.append("frozen_breakout_not_observed")
        _add_unknown_oi(explanation, blockers, oi_observations)
        return _result(identity, breakout.revision, blockers, explanation)

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
    )
    return _result(identity, breakout.revision, blockers, explanation)



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
) -> None:
    if failure_bin is None or retest_bin is None:
        _add_unknown_oi(explanation, blockers, observations)
        return
    ordered = tuple(sorted(observations, key=lambda item: _utc(item.observed_at)))
    if any(item.value is None or item.value <= 0 for item in ordered):
        _add_unknown_oi(explanation, blockers, observations)
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
    observations = selected
    timestamps = tuple(_utc(item.observed_at) for item in observations)
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
    strong = classification == "STRONG_TRAPPED_LONG_EVIDENCE"
    explanation["oi_sequence"] = ExplanationFactor(
        {"classification": classification, "values": tuple(item.value for item in observations)}, strong
    )
    if classification == "OI_UNKNOWN":
        blockers.append("oi_sequence_unknown")
    elif not strong:
        blockers.append("oi_sequence_not_trapped")

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
) -> None:
    explanation["oi_sequence"] = ExplanationFactor(
        {"classification": "UNKNOWN", "observations": len(observations)}, False
    )
    blockers.append("oi_sequence_unknown")


def _identity(breakout: FrozenBreakout) -> str:
    material = ":".join(
        (
            "TRAPPED_LONGS_REVERSAL_V2",
            breakout.symbol,
            breakout.horizon,
            breakout.identity,
            _canonical_datetime(breakout.occurred_at),
            _canonical_price(breakout.reference),
            str(breakout.revision),
        )
    )
    return f"trapped-longs-v2:{sha256(material.encode()).hexdigest()[:20]}"


def _canonical_datetime(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _canonical_price(value: float | None) -> str:
    return "UNKNOWN" if value is None else format(value, ".12g")


def _result(
    identity: str,
    revision: int,
    blockers: list[str],
    explanation: dict[str, ExplanationFactor],
) -> TrappedLongsV2Result:
    return TrappedLongsV2Result(identity, revision, not blockers, tuple(dict.fromkeys(blockers)), explanation)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
