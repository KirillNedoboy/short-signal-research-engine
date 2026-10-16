'''Tests for market-data correctness validation.'''
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import pandas as pd
from app.market_data.validation import (
    OHLCVValidator, OHLCVStatus, TimestampValidator, TimestampStatus,
    ContinuityValidator, ContinuityStatus, DerivativeFreshnessValidator,
    DerivativeFreshnessStatus, MarketDataValidator, MarketDataQuality,
    validate_decision_frame,
)

def _ts(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

def _frame(data):
    return pd.DataFrame(data)

class TestOHLCVValidator:
    def test_valid_frame_passes(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.VALID
        assert result.valid is True

    def test_empty_frame(self):
        result = OHLCVValidator().validate(_frame([]))
        assert result.status == OHLCVStatus.EMPTY_FRAME

    def test_missing_columns(self):
        frame = _frame([{'timestamp': _ts('2026-01-01T00:00:00+00:00')}])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.MISSING_COLUMNS

    def test_high_below_low(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 99, 'low': 101, 'close': 100, 'volume': 1000},
        ])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.HIGH_BELOW_LOW

    def test_negative_volume(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': -100},
        ])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.NEGATIVE_VOLUME

    def test_close_outside_hl(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 102, 'volume': 1000},
        ])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.CLOSE_OUTSIDE_HL

    def test_nonfinite_value(self):
        import math
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': float('nan'), 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.NONFINITE_VALUE

    def test_multiple_rows_valid(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 100, 'high': 102, 'low': 99, 'close': 101, 'volume': 500},
        ])
        result = OHLCVValidator().validate(frame)
        assert result.status == OHLCVStatus.VALID


class TestTimestampValidator:
    def test_valid_timestamps(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 101, 'high': 102, 'low': 100, 'close': 101, 'volume': 500},
        ])
        result = TimestampValidator().validate(frame, now=_ts('2026-01-01T00:02:00+00:00'))
        assert result.status == TimestampStatus.VALID

    def test_future_timestamps_rejected(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-02T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = TimestampValidator().validate(frame, now=_ts('2026-01-01T00:02:00+00:00'))
        assert result.status == TimestampStatus.FUTURE_TIMESTAMP

    def test_unsorted_rejected(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = TimestampValidator().validate(frame, now=_ts('2026-01-01T00:10:00+00:00'))
        assert result.status == TimestampStatus.UNSORTED

    def test_missing_timestamp_column(self):
        frame = _frame([{'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000}])
        result = TimestampValidator().validate(frame, now=_ts('2026-01-01T00:02:00+00:00'))
        assert result.status == TimestampStatus.MISSING_TIMESTAMP_COLUMN


class TestContinuityValidator:
    def test_contiguous(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 101, 'high': 102, 'low': 100, 'close': 101, 'volume': 500},
        ])
        result = ContinuityValidator().validate(frame)
        assert result.status == ContinuityStatus.CONTIGUOUS

    def test_gap_detected(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:10:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = ContinuityValidator().validate(frame)
        assert result.status == ContinuityStatus.GAP_DETECTED
        assert len(result.gaps) == 1
        assert result.gaps[0][2] == 9  # 9 missing candles

    def test_single_candle(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = ContinuityValidator().validate(frame)
        assert result.status == ContinuityStatus.SINGLE_CANDLE

    def test_empty(self):
        result = ContinuityValidator().validate(_frame([]))
        assert result.status == ContinuityStatus.EMPTY


class TestDerivativeFreshnessValidator:
    def test_fresh(self):
        result = DerivativeFreshnessValidator().validate(
            derivatives_enabled=True,
            orderbook_freshness_age_seconds=10.0,
            funding_rate_age_seconds=30.0,
            oi_age_seconds=5.0,
        )
        assert result.status == DerivativeFreshnessStatus.FRESH

    def test_stale_orderbook(self):
        result = DerivativeFreshnessValidator(max_orderbook_age_seconds=30).validate(
            derivatives_enabled=True,
            orderbook_freshness_age_seconds=120.0,
        )
        assert result.status == DerivativeFreshnessStatus.STALE_ORDERBOOK

    def test_derivatives_disabled(self):
        result = DerivativeFreshnessValidator().validate(
            derivatives_enabled=False,
            orderbook_freshness_age_seconds=120.0,
        )
        assert result.status == DerivativeFreshnessStatus.DERIVATIVES_DISABLED

    def test_missing(self):
        result = DerivativeFreshnessValidator().validate(
            derivatives_enabled=True,
            orderbook_freshness_age_seconds=None,
            funding_rate_age_seconds=None,
            oi_age_seconds=None,
        )
        assert result.status == DerivativeFreshnessStatus.MISSING


class TestMarketDataValidator:
    def test_aggregate_valid(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 100, 'high': 102, 'low': 99, 'close': 101, 'volume': 500},
        ])
        result = validate_decision_frame('AAAUSDT', frame, now=_ts('2026-01-01T00:02:00+00:00'))
        assert result.overall == MarketDataQuality.ACTIONABLE
        assert result.actionable is True

    def test_aggregate_ohlcv_invalid(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 99, 'low': 101, 'close': 100, 'volume': 1000},
        ])
        result = validate_decision_frame('AAAUSDT', frame, now=_ts('2026-01-01T00:02:00+00:00'))
        assert result.overall == MarketDataQuality.INVALID
        assert result.actionable is False

    def test_aggregate_data_gap(self):
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
        ])
        result = validate_decision_frame('AAAUSDT', frame, now=_ts('2026-01-01T00:06:00+00:00'))
        assert result.overall == MarketDataQuality.DATA_GAP
        assert result.actionable is False

    def test_validate_returns_explicit_status_never_none(self):
        frame = _frame([])
        result = validate_decision_frame('AAAUSDT', frame, now=_ts('2026-01-01T00:02:00+00:00'))
        assert result.overall is not None
        assert result.overall != ''
        # Empty frame with no columns
        assert result.ohlcv.status != OHLCVStatus.VALID
