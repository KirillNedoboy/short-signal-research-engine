"""Market-data correctness: invariant validation for OHLCV, timestamps, derivatives."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum

class OHLCVStatus(StrEnum):
    VALID = "VALID"
    MISSING_COLUMNS = "MISSING_COLUMNS"
    EMPTY_FRAME = "EMPTY_FRAME"
    NEGATIVE_OHLC = "NEGATIVE_OHLC"
    HIGH_BELOW_LOW = "HIGH_BELOW_LOW"
    OPEN_OUTSIDE_HL = "OPEN_OUTSIDE_HL"
    CLOSE_OUTSIDE_HL = "CLOSE_OUTSIDE_HL"
    NEGATIVE_VOLUME = "NEGATIVE_VOLUME"
    ZERO_HIGH_LOW = "ZERO_HIGH_LOW"
    NEGATIVE_TURNOVER = "NEGATIVE_TURNOVER"
    NONFINITE_VALUE = "NONFINITE_VALUE"

class TimestampStatus(StrEnum):
    VALID = "VALID"
    UNSORTED = "UNSORTED"
    DUPLICATE_TIMESTAMPS = "DUPLICATE_TIMESTAMPS"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    OPEN_MISMATCHED_INTERVAL = "OPEN_MISMATCHED_INTERVAL"
    MISSING_TIMESTAMP_COLUMN = "MISSING_TIMESTAMP_COLUMN"
    OPEN_TIME_AFTER_CLOSE = "OPEN_TIME_AFTER_CLOSE"
    INCOMPLETE_CANDLE = "INCOMPLETE_CANDLE"

class ContinuityStatus(StrEnum):
    CONTIGUOUS = "CONTIGUOUS"
    GAP_DETECTED = "GAP_DETECTED"
    OVERLAP_DETECTED = "OVERLAP_DETECTED"
    SINGLE_CANDLE = "SINGLE_CANDLE"
    EMPTY = "EMPTY"

class DerivativeFreshnessStatus(StrEnum):
    FRESH = "FRESH"
    STALE_ORDERBOOK = "STALE_ORDERBOOK"
    STALE_FUNDING = "STALE_FUNDING"
    STALE_OPEN_INTEREST = "STALE_OPEN_INTEREST"
    MISSING = "MISSING"
    DERIVATIVES_DISABLED = "DERIVATIVES_DISABLED"

class MarketDataQuality(StrEnum):
    INVALID = "INVALID"
    DATA_GAP = "DATA_GAP"
    INCOMPLETE = "INCOMPLETE"
    STALE = "STALE"
    WARMING = "WARMING"
    ACTIONABLE = "ACTIONABLE"

@dataclass(frozen=True, slots=True)
class OHLCVValidationResult:
    status: OHLCVStatus
    violations: tuple[str, ...] = ()
    violating_rows: tuple[int, ...] = ()
    @property
    def valid(self):
        return self.status == OHLCVStatus.VALID

@dataclass(frozen=True, slots=True)
class TimestampValidationResult:
    status: TimestampStatus
    violations: tuple[str, ...] = ()
    future_timestamps: tuple[datetime, ...] = ()
    gap_intervals: tuple[tuple[datetime, datetime, int], ...] = ()
    @property
    def valid(self):
        return self.status == TimestampStatus.VALID

@dataclass(frozen=True, slots=True)
class ContinuityValidationResult:
    status: ContinuityStatus
    gaps: tuple[tuple[datetime, datetime, int], ...] = ()
    overlaps: tuple[tuple[datetime, datetime], ...] = ()
    @property
    def valid(self):
        return self.status in {ContinuityStatus.CONTIGUOUS, ContinuityStatus.SINGLE_CANDLE}

@dataclass(frozen=True, slots=True)
class DerivativeFreshnessResult:
    status: DerivativeFreshnessStatus
    orderbook_age_seconds: float | None = None
    funding_age_seconds: float | None = None
    oi_age_seconds: float | None = None
    @property
    def fresh(self):
        return self.status == DerivativeFreshnessStatus.FRESH

@dataclass(frozen=True, slots=True)
class MarketDataValidationSummary:
    symbol: str
    captured_at: datetime
    ohlcv: OHLCVValidationResult
    timestamps: TimestampValidationResult
    continuity: ContinuityValidationResult
    derivatives: DerivativeFreshnessResult | None = None
    overall: MarketDataQuality = MarketDataQuality.ACTIONABLE
    evidence: tuple[str, ...] = ()
    @property
    def actionable(self):
        return self.overall == MarketDataQuality.ACTIONABLE

def _is_finite(value):
    import math
    return math.isfinite(value)

def _utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)

_REQUIRED_COLUMNS = frozenset({"open", "high", "low", "close", "volume", "timestamp"})
_OPTIONAL_NONNEGATIVE_COLUMNS = frozenset({"turnover"})

class OHLCVValidator:
    def validate(self, frame):
        if frame.empty:
            return OHLCVValidationResult(OHLCVStatus.EMPTY_FRAME, ("frame is empty",))
        missing = _REQUIRED_COLUMNS - set(frame.columns)
        if missing:
            return OHLCVValidationResult(OHLCVStatus.MISSING_COLUMNS, (f"missing columns: {sorted(missing)}",))
        violations = []
        violating_rows = []
        for idx, row in frame.iterrows():
            try:
                open_p = float(row["open"])
                high_p = float(row["high"])
                low_p = float(row["low"])
                close_p = float(row["close"])
                volume_v = float(row["volume"])
            except (ValueError, TypeError):
                violations.append(f"row {idx}: non-numeric value")
                violating_rows.append(int(str(idx)) if str(idx).isdigit() else -1)
                continue
            row_num = int(str(idx)) if str(idx).isdigit() else -1
            values = [("open", open_p), ("high", high_p), ("low", low_p), ("close", close_p), ("volume", volume_v)]
            if "turnover" in frame.columns:
                try:
                    values.append(("turnover", float(row["turnover"])))
                except (ValueError, TypeError):
                    violations.append(f"row {idx}: non-numeric turnover")
                    violating_rows.append(row_num)
                    continue
            for name, val in values:
                if not _is_finite(val):
                    violations.append(f"row {idx}: non-finite {name}={val}")
                    violating_rows.append(row_num)
                    break
            else:
                for name, val in (("open", open_p), ("high", high_p), ("low", low_p), ("close", close_p)):
                    if val <= 0:
                        violations.append(f"row {idx}: negative {name}={val}")
                        violating_rows.append(row_num)
                        break
                else:
                    if high_p < low_p:
                        violations.append(f"row {idx}: high={high_p} < low={low_p}")
                        violating_rows.append(row_num)
                    elif high_p == 0 and low_p == 0:
                        violations.append(f"row {idx}: zero high and low")
                        violating_rows.append(row_num)
                    elif open_p < low_p or open_p > high_p:
                        violations.append(f"row {idx}: open={open_p} outside [{low_p}, {high_p}]")
                        violating_rows.append(row_num)
                    elif close_p < low_p or close_p > high_p:
                        violations.append(f"row {idx}: close={close_p} outside [{low_p}, {high_p}]")
                        violating_rows.append(row_num)
                    elif volume_v < 0:
                        violations.append(f"row {idx}: negative volume={volume_v}")
                        violating_rows.append(row_num)
                    elif "turnover" in frame.columns and float(row["turnover"]) < 0:
                        violations.append(f"row {idx}: negative turnover={float(row["turnover"])}")
                        violating_rows.append(row_num)
        if violations:
            status = OHLCVStatus.VALID
            for v in violations:
                if "non-finite" in v:
                    status = OHLCVStatus.NONFINITE_VALUE
                elif "negative turnover" in v:
                    if status == OHLCVStatus.VALID:
                        status = OHLCVStatus.NEGATIVE_TURNOVER
                elif "negative" in v and "volume" in v:
                    if status == OHLCVStatus.VALID:
                        status = OHLCVStatus.NEGATIVE_VOLUME
                elif "negative" in v:
                    status = OHLCVStatus.NEGATIVE_OHLC
                elif "zero" in v:
                    status = OHLCVStatus.ZERO_HIGH_LOW
                elif "high" in v and "low" in v:
                    status = OHLCVStatus.HIGH_BELOW_LOW
                elif "open" in v:
                    status = OHLCVStatus.OPEN_OUTSIDE_HL
                elif "close" in v:
                    status = OHLCVStatus.CLOSE_OUTSIDE_HL
            return OHLCVValidationResult(status, tuple(violations), tuple(set(violating_rows)))
        return OHLCVValidationResult(OHLCVStatus.VALID)

class TimestampValidator:
    def validate(self, frame, *, now=None, interval_minutes=1):
        reference = _utc(now or datetime.now(timezone.utc))
        if frame.empty:
            return TimestampValidationResult(TimestampStatus.VALID, ("empty frame - no timestamp check needed",))
        if "timestamp" not in frame.columns:
            return TimestampValidationResult(TimestampStatus.MISSING_TIMESTAMP_COLUMN, ("no timestamp column present",))
        violations = []
        future_timestamps = []
        timestamps = frame["timestamp"].dropna()
        for ts in timestamps:
            ts_norm = _utc(ts)
            if ts_norm > reference:
                future_timestamps.append(ts_norm)
        if future_timestamps:
            violations.append(f"{len(future_timestamps)} future timestamp(s)")
        if not timestamps.is_monotonic_increasing:
            violations.append("timestamps are not monotonically increasing")
        dupes = timestamps.duplicated()
        if dupes.any():
            violations.append(f"{int(dupes.sum())} duplicate timestamp(s)")
        if "close_time" in frame.columns:
            mismatched = frame[frame["close_time"] <= frame["timestamp"]]
            if len(mismatched) > 0:
                violations.append(f"{len(mismatched)} row(s) with close_time <= open_time")
        if future_timestamps:
            return TimestampValidationResult(TimestampStatus.FUTURE_TIMESTAMP, tuple(violations), tuple(future_timestamps))
        incomplete = [ts for ts in timestamps if _utc(ts) + timedelta(minutes=interval_minutes) > reference]
        if incomplete:
            return TimestampValidationResult(TimestampStatus.INCOMPLETE_CANDLE, (f"{len(incomplete)} incomplete candle(s)",))
        if "not monotonically" in " ".join(violations):
            return TimestampValidationResult(TimestampStatus.UNSORTED, tuple(violations))
        elif "duplicate" in " ".join(violations):
            return TimestampValidationResult(TimestampStatus.DUPLICATE_TIMESTAMPS, tuple(violations))
        elif "close_time" in " ".join(violations):
            return TimestampValidationResult(TimestampStatus.OPEN_TIME_AFTER_CLOSE, tuple(violations))
        return TimestampValidationResult(TimestampStatus.VALID)

class ContinuityValidator:
    def validate(self, frame, *, interval_minutes=1):
        if frame.empty:
            return ContinuityValidationResult(ContinuityStatus.EMPTY)
        if "timestamp" not in frame.columns:
            return ContinuityValidationResult(ContinuityStatus.EMPTY)
        timestamps = sorted(frame["timestamp"].dropna().unique())
        if len(timestamps) <= 1:
            return ContinuityValidationResult(ContinuityStatus.SINGLE_CANDLE)
        step = timedelta(minutes=interval_minutes)
        gaps = []
        overlaps = []
        for i in range(len(timestamps) - 1):
            current = _utc(timestamps[i])
            next_ts = _utc(timestamps[i + 1])
            diff = (next_ts - current).total_seconds()
            expected = step.total_seconds()
            if diff > expected * 1.1:
                missing_count = int((diff / expected) - 1)
                gaps.append((current + step, next_ts, max(1, missing_count)))
            elif diff < expected * 0.5:
                overlaps.append((current, next_ts))
        if gaps:
            return ContinuityValidationResult(ContinuityStatus.GAP_DETECTED, gaps=tuple(gaps))
        if overlaps:
            return ContinuityValidationResult(ContinuityStatus.OVERLAP_DETECTED, overlaps=tuple(overlaps))
        return ContinuityValidationResult(ContinuityStatus.CONTIGUOUS)

class DerivativeFreshnessValidator:
    def __init__(self, *, max_orderbook_age_seconds=60.0, max_funding_age_seconds=300.0, max_oi_age_seconds=120.0):
        self._max_orderbook_age = max_orderbook_age_seconds
        self._max_funding_age = max_funding_age_seconds
        self._max_oi_age = max_oi_age_seconds

    def validate(self, *, derivatives_enabled, captured_at=None, orderbook_observed_at=None, funding_observed_at=None, oi_observed_at=None, required_sources=(), orderbook_freshness_age_seconds=None, funding_rate_age_seconds=None, oi_age_seconds=None):
        if captured_at is not None:
            captured = _utc(captured_at)
            def age(observed):
                return None if observed is None else max(0.0, (captured - _utc(observed)).total_seconds())
            orderbook_freshness_age_seconds = age(orderbook_observed_at)
            funding_rate_age_seconds = age(funding_observed_at)
            oi_age_seconds = age(oi_observed_at)
        if not derivatives_enabled:
            return DerivativeFreshnessResult(DerivativeFreshnessStatus.DERIVATIVES_DISABLED)
        required_ages = {"oi": oi_age_seconds, "funding": funding_rate_age_seconds, "orderbook": orderbook_freshness_age_seconds}
        if all(age is None for age in required_ages.values()) or any(required_ages.get(source) is None for source in required_sources):
            return DerivativeFreshnessResult(DerivativeFreshnessStatus.MISSING, orderbook_freshness_age_seconds, funding_rate_age_seconds, oi_age_seconds)
        worst = DerivativeFreshnessStatus.FRESH
        if orderbook_freshness_age_seconds is not None and orderbook_freshness_age_seconds > self._max_orderbook_age:
            worst = DerivativeFreshnessStatus.STALE_ORDERBOOK
        if funding_rate_age_seconds is not None and funding_rate_age_seconds > self._max_funding_age:
            if worst == DerivativeFreshnessStatus.FRESH:
                worst = DerivativeFreshnessStatus.STALE_FUNDING
        if oi_age_seconds is not None and oi_age_seconds > self._max_oi_age:
            if worst == DerivativeFreshnessStatus.FRESH:
                worst = DerivativeFreshnessStatus.STALE_OPEN_INTEREST
        return DerivativeFreshnessResult(status=worst, orderbook_age_seconds=orderbook_freshness_age_seconds, funding_age_seconds=funding_rate_age_seconds, oi_age_seconds=oi_age_seconds)

class MarketDataValidator:
    def __init__(self, *, derivatives_enabled=False, now=None):
        self._ohlcv = OHLCVValidator()
        self._timestamps = TimestampValidator()
        self._continuity = ContinuityValidator()
        self._derivatives = DerivativeFreshnessValidator()
        self._derivatives_enabled = derivatives_enabled
        self._now = now

    def validate(self, symbol, frame_1m, *, captured_at=None, orderbook_observed_at=None, funding_observed_at=None, oi_observed_at=None, required_sources=("oi", "funding"), orderbook_age_seconds=None, funding_rate_age_seconds=None, oi_age_seconds=None):
        now = captured_at or self._now or datetime.now(timezone.utc)
        ohlcv = self._ohlcv.validate(frame_1m)
        ts = self._timestamps.validate(frame_1m, now=now)
        continuity = self._continuity.validate(frame_1m)
        derivatives = self._derivatives.validate(
            derivatives_enabled=self._derivatives_enabled,
            captured_at=now,
            orderbook_observed_at=orderbook_observed_at,
            funding_observed_at=funding_observed_at,
            oi_observed_at=oi_observed_at,
            required_sources=required_sources,
            orderbook_freshness_age_seconds=orderbook_age_seconds,
            funding_rate_age_seconds=funding_rate_age_seconds,
            oi_age_seconds=oi_age_seconds,
        )
        evidence = []
        if ohlcv.status != OHLCVStatus.VALID:
            overall = MarketDataQuality.INVALID
            evidence.append(f"OHLCV: {ohlcv.status.value}")
            for v in ohlcv.violations:
                evidence.append(f"  - {v}")
        elif ts.status == TimestampStatus.FUTURE_TIMESTAMP:
            overall = MarketDataQuality.INVALID
            evidence.append(f"Timestamp: {ts.status.value} (future candles)")
        elif ts.status == TimestampStatus.MISSING_TIMESTAMP_COLUMN:
            overall = MarketDataQuality.INVALID
            evidence.append("Timestamp: missing column")
        elif ts.status not in {TimestampStatus.VALID}:
            overall = MarketDataQuality.INCOMPLETE
            evidence.append(f"Timestamp: {ts.status.value}")
        elif continuity.status == ContinuityStatus.GAP_DETECTED:
            overall = MarketDataQuality.DATA_GAP
            for gap in continuity.gaps:
                evidence.append(f"Gap: {gap[0].isoformat()} -> {gap[1].isoformat()} ({gap[2]} missing)")
        elif continuity.status == ContinuityStatus.OVERLAP_DETECTED:
            overall = MarketDataQuality.INCOMPLETE
            evidence.append("Continuity: overlapping candles detected")
        elif continuity.status == ContinuityStatus.SINGLE_CANDLE:
            overall = MarketDataQuality.WARMING
            evidence.append("Continuity: single candle, still warming")
        elif continuity.status == ContinuityStatus.EMPTY:
            overall = MarketDataQuality.INVALID
            evidence.append("Continuity: empty frame")
        else:
            overall = MarketDataQuality.ACTIONABLE
        if (derivatives and derivatives.status != DerivativeFreshnessStatus.FRESH and derivatives.status != DerivativeFreshnessStatus.DERIVATIVES_DISABLED):
            if overall == MarketDataQuality.ACTIONABLE:
                overall = MarketDataQuality.STALE
            evidence.append(f"Derivatives: {derivatives.status.value}")
        return MarketDataValidationSummary(
            symbol=symbol, captured_at=now, ohlcv=ohlcv, timestamps=ts,
            continuity=continuity, derivatives=derivatives,
            overall=overall, evidence=tuple(evidence),
        )

def validate_decision_frame(symbol, frame_1m, *, now=None, derivatives_enabled=False, orderbook_age=None, funding_age=None, oi_age=None):
    validator = MarketDataValidator(derivatives_enabled=derivatives_enabled, now=now)
    return validator.validate(symbol, frame_1m, captured_at=now, orderbook_age_seconds=orderbook_age, funding_rate_age_seconds=funding_age, oi_age_seconds=oi_age)
