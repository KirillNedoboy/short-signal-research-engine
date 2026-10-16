'''Tests for causal pump-root V2 shadow detection.'''
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from app.observability.causal_pump_v2 import (
    CausalPumpV2Comparator,
    CausalPumpV2Config,
    CausalPumpV2Detector,
    CausalPumpV2Tracker,
    CausalV2RootDecision,
    V2ComparisonVerdict,
    V2PumpStatus,
)


def _frame_1m(data):
    return pd.DataFrame(data)

def _ts(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

def _features(**overrides):
    return {'symbol': 'AAAUSDT', 'price': 105.0, 'ret_5m': 3.0, 'ret_15m': 5.0, 'ret_1h': 8.0, 'ret_4h': 12.0, **overrides}

class TestCausalPumpV2Detector:
    def test_detects_ordered_max_drawup(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000, 'confirmed': True},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 100, 'high': 102, 'low': 99, 'close': 101, 'volume': 1000, 'confirmed': True},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 104, 'high': 110, 'low': 103, 'close': 108, 'volume': 2000, 'confirmed': True},
        ])
        detector = CausalPumpV2Detector()
        result = detector.detect('AAAUSDT', frame, _features(), now)
        assert result is not None
        assert result.state == V2PumpStatus.PUMP_TRACKING
        assert result.root_id.startswith('CAUSAL_PUMP_V2:AAAUSDT:')
        assert result.event_low_price == 99.0
        assert result.event_high_price == 110.0
        assert result.event_low_time < result.event_high_time

    def test_rejects_reversed_chronology(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 105, 'high': 110, 'low': 104, 'close': 108, 'volume': 1000, 'confirmed': True},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 100, 'high': 101, 'low': 95, 'close': 100, 'volume': 1000, 'confirmed': True},
        ])
        detector = CausalPumpV2Detector()
        result = detector.detect('AAAUSDT', frame, _features(), now)
        # No ordered low-before-high pair exists; V2 must not invent a root.
        assert result is None

    def test_returns_none_for_empty_frame(self):
        detector = CausalPumpV2Detector()
        result = detector.detect('AAAUSDT', _frame_1m([]), _features(), _ts('2026-01-01T00:00:00+00:00'))
        assert result is None

    def test_thresholds_not_met(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 100, 'confirmed': True},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 100, 'high': 100.5, 'low': 99.5, 'close': 100.2, 'volume': 100, 'confirmed': True},
        ])
        detector = CausalPumpV2Detector()
        result = detector.detect('AAAUSDT', frame, _features(ret_5m=0.1, ret_15m=0.2, ret_1h=0.3, ret_4h=0.4), now)
        assert result.state == V2PumpStatus.IDLE
        assert result.reason == 'pump_thresholds_not_met'

    def test_root_id_deterministic(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000, 'confirmed': True},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 100, 'high': 110, 'low': 100, 'close': 108, 'volume': 2000, 'confirmed': True},
        ])
        detector = CausalPumpV2Detector()
        r1 = detector.detect('AAAUSDT', frame, _features(), now)
        r2 = detector.detect('AAAUSDT', frame, _features(), now)
        assert r1.root_id == r2.root_id
        assert r1.root_id.endswith(':110')

    def test_forming_row_without_confirmation_is_not_a_closed_high(self):
        now = _ts('2026-01-01T00:05:00+00:00')
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 104, 'high': 110, 'low': 103, 'close': 108, 'volume': 2000},
        ])
        result = CausalPumpV2Detector().detect('AAAUSDT', frame, _features(), now)
        assert result is None

class TestCausalPumpV2Tracker:
    def test_high_can_only_increase(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        current = CausalV2RootDecision(
            state=V2PumpStatus.PUMP_TRACKING, root_id='test-root', reason='initial',
            event_low_price=99.0, event_low_time=_ts('2026-01-01T00:00:00+00:00'),
            event_high_price=105.0, event_high_time=_ts('2026-01-01T00:05:00+00:00'),
            event_range_pct=6.06,
        )
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:09:00+00:00'), 'open': 110, 'high': 113, 'low': 108, 'close': 111, 'volume': 2000, 'confirmed': True},
        ])
        tracker = CausalPumpV2Tracker()
        result = tracker.advance(current, frame, _features(price=110.0), now)
        assert result.event_high_price == 113.0
        assert 'causal_high_reset_upward' in result.reason

    def test_high_never_decreases(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        current = CausalV2RootDecision(
            state=V2PumpStatus.PUMP_TRACKING, root_id='test-root', reason='initial',
            event_low_price=99.0, event_low_time=_ts('2026-01-01T00:00:00+00:00'),
            event_high_price=115.0, event_high_time=_ts('2026-01-01T00:05:00+00:00'),
            event_range_pct=16.16,
        )
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:07:00+00:00'), 'open': 105, 'high': 108, 'low': 104, 'close': 106, 'volume': 1000, 'confirmed': True},
        ])
        tracker = CausalPumpV2Tracker()
        result = tracker.advance(current, frame, _features(price=106.0), now)
        assert result.event_high_price == 115.0

    def test_transitions_to_pullback(self):
        now = _ts('2026-01-01T00:31:00+00:00')
        current = CausalV2RootDecision(
            state=V2PumpStatus.PUMP_TRACKING, root_id='test-root', reason='initial',
            event_low_price=99.0, event_low_time=_ts('2026-01-01T00:00:00+00:00'),
            event_high_price=110.0, event_high_time=_ts('2026-01-01T00:05:00+00:00'),
            event_range_pct=11.11,
        )
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:30:00+00:00'), 'open': 107, 'high': 108, 'low': 105, 'close': 106, 'volume': 1000, 'confirmed': True},
        ])
        tracker = CausalPumpV2Tracker(CausalPumpV2Config(pullback_min_pct=2.0, pullback_max_pct=8.0))
        result = tracker.advance(current, frame, _features(price=106.0), now)
        assert result.state == V2PumpStatus.PULLBACK_TRACKING

    def test_expires_after_timeout(self):
        now = _ts('2026-01-01T02:00:00+00:00')
        current = CausalV2RootDecision(
            state=V2PumpStatus.PUMP_TRACKING, root_id='test-root', reason='initial',
            event_low_price=99.0, event_low_time=_ts('2026-01-01T00:00:00+00:00'),
            event_high_price=110.0, event_high_time=_ts('2026-01-01T00:00:00+00:00'),
            event_range_pct=11.11,
        )
        tracker = CausalPumpV2Tracker(CausalPumpV2Config(expiry_minutes=90))
        result = tracker.advance(current, _frame_1m([]), _features(), now)
        assert result.state == V2PumpStatus.EXPIRED

class TestCausalPumpV2Comparator:
    def test_v2_earlier(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        comp = CausalPumpV2Comparator()
        row = comp.compare(
            symbol='AAAUSDT', now=now,
            v1_event_id='ev1', v1_state='pump_detected',
            v1_event_high_time=_ts('2026-01-01T00:05:00+00:00'), v1_event_high=110.0, v1_trigger_window='15m',
            v2_root_id='v2-1', v2_state='PUMP_TRACKING',
            v2_event_low_time=_ts('2026-01-01T00:00:00+00:00'), v2_event_low_price=99.0,
            v2_event_high_time=_ts('2026-01-01T00:03:00+00:00'), v2_event_high_price=105.0, v2_trigger_window='5m',
            price=108.0,
        )
        assert row.verdict == V2ComparisonVerdict.V2_EARLIER
        assert row.v2_ahead_by_seconds == 120.0

    def test_v1_only(self):
        now = _ts('2026-01-01T00:10:00+00:00')
        comp = CausalPumpV2Comparator()
        row = comp.compare(
            symbol='AAAUSDT', now=now,
            v1_event_id='ev1', v1_state='pump_detected',
            v1_event_high_time=_ts('2026-01-01T00:05:00+00:00'), v1_event_high=110.0, v1_trigger_window='15m',
            v2_root_id=None, v2_state=None,
            v2_event_low_time=None, v2_event_low_price=None,
            v2_event_high_time=None, v2_event_high_price=None, v2_trigger_window=None,
            price=108.0,
        )
        assert row.verdict == V2ComparisonVerdict.V1_ONLY

class TestPersistence:
    def test_causal_pump_v2_root_persists(self, tmp_path):
        from app.storage.db import Database
        from app.storage.repository import BotRepository
        db = Database(f'sqlite:///{tmp_path}/causal.sqlite')
        db.create_all()
        repo = BotRepository(db)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        root = {
            'root_id': 'CAUSAL_PUMP_V2:AAAUSDT:1000:1200:110000000',
            'symbol': 'AAAUSDT', 'state': 'PUMP_TRACKING', 'reason': 'ordered_max_drawup_detected',
            'event_low_price': 99.0, 'event_low_time': now,
            'event_high_price': 110.0, 'event_high_time': now + timedelta(minutes=5),
            'event_range_pct': 11.11, 'trigger_window': '5m',
            'condition_snapshot': {'pump_5m': 'PASS:3.00'},
            'created_at': now, 'method_version': 'CAUSAL_PUMP_V2_V1',
        }
        assert repo.record_causal_pump_v2_root(root) is True
        assert repo.record_causal_pump_v2_root(root) is False

    def test_causal_pump_v2_comparison_persists(self, tmp_path):
        from app.storage.db import Database
        from app.storage.repository import BotRepository
        db = Database(f'sqlite:///{tmp_path}/comp.sqlite')
        db.create_all()
        repo = BotRepository(db)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        row = {
            'symbol': 'AAAUSDT', 'compared_at': now,
            'v1_event_id': 'ev1', 'v1_state': 'pump_detected',
            'v1_event_high_time': now + timedelta(minutes=5), 'v1_event_high': 110.0, 'v1_trigger_window': '15m',
            'v2_root_id': 'v2-1', 'v2_state': 'PUMP_TRACKING',
            'v2_event_low_time': now, 'v2_event_low_price': 99.0,
            'v2_event_high_time': now + timedelta(minutes=3), 'v2_event_high_price': 105.0, 'v2_trigger_window': '5m',
            'verdict': 'V2_EARLIER', 'price_at_comparison': 108.0, 'v2_ahead_by_seconds': 120.0,
        }
        assert repo.record_causal_pump_v2_comparison(row) is True

    def test_v2_tables_appear_in_schema(self, tmp_path):
        from sqlalchemy import inspect

        from app.storage.db import Database
        db = Database(f'sqlite:///{tmp_path}/schema.sqlite')
        db.create_all()
        tables = inspect(db.engine).get_table_names()
        assert 'causal_pump_v2_roots' in tables
        assert 'causal_pump_v2_comparisons' in tables
        assert 'outcome_contract_v2_results' in tables

    def test_newer_high_is_not_masked_by_older_window_high(self):
        current = CausalV2RootDecision(V2PumpStatus.PUMP_TRACKING, 'r', 'x', 90, _ts('2026-01-01T00:00:00+00:00'), 100, _ts('2026-01-01T00:05:00+00:00'), 11)
        frame = _frame_1m([
            {'timestamp': _ts('2026-01-01T00:04:00+00:00'), 'open': 100, 'high': 150, 'low': 99, 'close': 100, 'volume': 1, 'confirmed': True},
            {'timestamp': _ts('2026-01-01T00:06:00+00:00'), 'open': 100, 'high': 110, 'low': 99, 'close': 105, 'volume': 1, 'confirmed': True},
        ])
        result = CausalPumpV2Tracker().advance(current, frame, _features(price=105), _ts('2026-01-01T00:07:00+00:00'))
        assert result.event_high_price == 110
        assert result.event_high_time == _ts('2026-01-01T00:06:00+00:00')
