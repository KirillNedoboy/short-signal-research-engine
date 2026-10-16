'''Tests for Outcome Contract V2.'''
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from app.outcomes.contract_v2 import (
    ContractV2Resolution,
    EntryFillStatus,
    ExecutionObservation,
    OutcomeContractV2Config,
    OutcomeContractV2Evaluator,
)


def _ts(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

def _frame(data):
    return pd.DataFrame(data)


class TestOutcomeContractV2:
    def test_strict_post_entry_executable_fill(self):
        '''Fill must be after decision_time + latency, not at decision_time.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:00:30+00:00'), 'open': 100, 'high': 101, 'low': 97, 'close': 98, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 98, 'high': 99, 'low': 96, 'close': 97, 'volume': 2000},
        ])
        config = OutcomeContractV2Config(latency_seconds=10)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=101.0, frame_1m=frame,
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        # Entry fills when price drops below 100 — at 00:00:30 (97 < 100)
        assert result.entry_fill_status == EntryFillStatus.FILLED
        assert result.entry_fill_time is not None
        assert result.entry_fill_time >= decision_time + timedelta(seconds=10)

    def test_no_fill_when_price_never_drops(self):
        '''Short entry requires price to drop. Never drops = NO_FILL.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 101, 'high': 102, 'low': 100.5, 'close': 101, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 101, 'high': 103, 'low': 101, 'close': 102, 'volume': 1000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=101.0, frame_1m=frame,
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        assert result.entry_fill_status == EntryFillStatus.NO_FILL
        assert result.first_passage == ContractV2Resolution.NO_FILL

    def test_invalid_entry_price(self):
        '''Zero or negative entry price = INVALID_ENTRY.'''
        evaluator = OutcomeContractV2Evaluator()
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=_ts('2026-01-01T00:00:00+00:00'), decision_price=0.0,
            event_high=None, frame_1m=_frame([]),
        )
        assert result.first_passage == ContractV2Resolution.INVALID_ENTRY

    def test_first_passage_tp_before_sl(self):
        '''TP hit chronologically before SL = TP_FIRST.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 99, 'close': 99, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 99, 'high': 99, 'low': 96, 'close': 97, 'volume': 2000},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 97, 'high': 103, 'low': 96, 'close': 102, 'volume': 3000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, tp_mult=0.97, sl_mult=1.03, use_event_high_as_sl=False)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=None, frame_1m=frame,
            now=_ts('2026-01-01T00:06:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.TP_FIRST

    def test_first_passage_sl_before_tp(self):
        '''SL hit chronologically before TP = SL_FIRST.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 99, 'close': 99, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 99, 'high': 104, 'low': 98, 'close': 103, 'volume': 2000},
            {'timestamp': _ts('2026-01-01T00:05:00+00:00'), 'open': 103, 'high': 103, 'low': 96, 'close': 97, 'volume': 3000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, tp_mult=0.97, sl_mult=1.03, use_event_high_as_sl=False)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=None, frame_1m=frame,
            now=_ts('2026-01-01T00:06:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.SL_FIRST

    def test_ambiguous_when_both_in_same_candle(self):
        '''TP and SL hit in same candle = AMBIGUOUS.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 99, 'close': 99, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 99, 'high': 104, 'low': 96, 'close': 103, 'volume': 2000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, tp_mult=0.97, sl_mult=1.03, use_event_high_as_sl=False)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=None, frame_1m=frame,
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.AMBIGUOUS

    def test_data_gap_on_empty_frame(self):
        '''Empty frame after min_data = DATA_GAP.'''
        evaluator = OutcomeContractV2Evaluator()
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=_ts('2026-01-01T00:00:00+00:00'), decision_price=100.0,
            event_high=None, frame_1m=_frame([]),
            now=_ts('2026-01-01T00:10:00+00:00'),
        )
        assert result.evaluation_status == 'DATA_GAP'

    def test_censored_when_not_enough_time(self):
        '''Not enough time elapsed from decision = CENSORED.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 99, 'close': 99, 'volume': 1000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, min_data_minutes=5)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=None, frame_1m=frame,
            now=_ts('2026-01-01T00:01:00+00:00'),
        )
        assert result.evaluation_status == 'CENSORED'

    def test_configurable_costs_affect_net_return(self):
        '''Spread and funding costs reduce net return.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 96, 'close': 97, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 97, 'high': 97, 'low': 96.5, 'close': 96.5, 'volume': 2000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, tp_mult=0.97, sl_mult=1.03,
                                          spread_pct=0.20, funding_cost_annual_pct=10.0,
                                          use_event_high_as_sl=False)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=None, frame_1m=frame,
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.TP_FIRST
        assert result.total_cost_pct is not None
        assert result.total_cost_pct >= 0.20  # at least spread cost
        assert result.net_return_pct is not None

    def test_event_high_as_sl(self):
        '''When use_event_high_as_sl, SL is max of sl_mult*entry and event_high.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 99, 'close': 99, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 99, 'high': 105, 'low': 98, 'close': 104, 'volume': 2000},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, sl_mult=1.03, use_event_high_as_sl=True)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=104.0, frame_1m=frame,
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        # SL = max(100*1.03=103, 104) = 104
        assert result.sl_price == 104.0
        assert result.first_passage == ContractV2Resolution.SL_FIRST

    def test_expired_no_hit(self):
        '''After max_evaluation_minutes without TP/SL = EXPIRED_NO_HIT.'''
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 100, 'high': 100, 'low': 99, 'close': 99, 'volume': 1000},
            {'timestamp': _ts('2026-01-01T00:30:00+00:00'), 'open': 99, 'high': 100, 'low': 98, 'close': 99.5, 'volume': 500},
        ])
        config = OutcomeContractV2Config(latency_seconds=5, tp_mult=0.97, sl_mult=1.03,
                                          max_evaluation_minutes=25, use_event_high_as_sl=False)
        evaluator = OutcomeContractV2Evaluator(config)
        result = evaluator.evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='ev1',
            decision_time=decision_time, decision_price=100.0,
            event_high=None, frame_1m=frame,
            now=_ts('2026-01-01T00:31:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.EXPIRED_NO_HIT

    def test_persistence(self, tmp_path):
        from app.storage.db import Database
        from app.storage.repository import BotRepository
        db = Database(f'sqlite:///{tmp_path}/ocv2.sqlite')
        db.create_all()
        repo = BotRepository(db)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = {
            'signal_id': 1, 'symbol': 'AAAUSDT', 'event_id': 'ev1',
            'decision_time': now,
            'entry_fill_time': now + timedelta(seconds=30),
            'entry_fill_price': 99.0,
            'entry_fill_status': 'FILLED',
            'tp_price': 96.03, 'sl_price': 101.97,
            'tp_hit_time': now + timedelta(minutes=5),
            'sl_hit_time': None,
            'first_passage': 'TP_FIRST',
            'resolution_detail': 'TP before SL',
            'mfe_pct': 3.0, 'mae_pct': 1.0,
            'return_at_expiry_pct': 1.5,
            'total_cost_pct': 0.22, 'net_return_pct': 1.28,
            'evaluated_at': now + timedelta(minutes=10),
            'evaluation_status': 'MATURE',
            'horizons': {}, 'evidence': {},
            'method_version': 'OUTCOME_CONTRACT_V2_V1',
        }
        assert repo.record_outcome_contract_v2_result(result) is True

        due = repo.list_outcome_contract_v2_results_due(now=now + timedelta(hours=1))
        assert len(due) == 0  # MATURE results are not due

    def test_results_due_for_censored(self, tmp_path):
        from app.storage.db import Database
        from app.storage.repository import BotRepository
        db = Database(f'sqlite:///{tmp_path}/ocv2due.sqlite')
        db.create_all()
        repo = BotRepository(db)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = {
            'signal_id': 1, 'symbol': 'AAAUSDT', 'event_id': 'ev1',
            'decision_time': now,
            'entry_fill_time': None, 'entry_fill_price': None,
            'entry_fill_status': 'NO_FILL',
            'tp_price': None, 'sl_price': None,
            'tp_hit_time': None, 'sl_hit_time': None,
            'first_passage': 'NO_FILL',
            'resolution_detail': 'no fill',
            'mfe_pct': None, 'mae_pct': None,
            'return_at_expiry_pct': None, 'total_cost_pct': None, 'net_return_pct': None,
            'evaluated_at': now,
            'evaluation_status': 'CENSORED',
            'horizons': {}, 'evidence': {},
            'method_version': 'OUTCOME_CONTRACT_V2_V1',
        }
        assert repo.record_outcome_contract_v2_result(result) is True
        due = repo.list_outcome_contract_v2_results_due(now=now + timedelta(hours=1), limit=10)
        assert len(due) >= 1
        assert due[0]['evaluation_status'] == 'CENSORED'

    def test_gap_before_fill_is_explicit_data_gap(self):
        decision_time = _ts('2026-01-01T00:00:00+00:00')
        frame = _frame([
            {'timestamp': _ts('2026-01-01T00:00:10+00:00'), 'open': 101, 'high': 102, 'low': 101, 'close': 101, 'volume': 1},
            {'timestamp': _ts('2026-01-01T00:02:00+00:00'), 'open': 101, 'high': 102, 'low': 101, 'close': 101, 'volume': 1},
        ])
        result = OutcomeContractV2Evaluator(OutcomeContractV2Config(latency_seconds=5)).evaluate(
            signal_id=1, symbol='AAAUSDT', event_id='e', decision_time=decision_time,
            decision_price=100, event_high=None, frame_1m=frame, now=_ts('2026-01-01T00:03:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.DATA_GAP
        assert result.entry_fill_status == EntryFillStatus.GAP_BEFORE_FILL

    def test_strict_mode_marks_missing_execution_evidence_unobservable(self):
        result = OutcomeContractV2Evaluator(
            OutcomeContractV2Config(require_execution_observation=True, latency_seconds=5)
        ).evaluate(
            signal_id=1,
            symbol='AAAUSDT',
            event_id='e',
            decision_time=_ts('2026-01-01T00:00:00+00:00'),
            decision_price=100,
            event_high=None,
            frame_1m=_frame([
                {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1},
            ]),
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        assert result.first_passage == ContractV2Resolution.EXECUTION_UNOBSERVABLE
        assert result.entry_fill_status == EntryFillStatus.GAP_BEFORE_FILL
        assert result.evaluation_status == 'UNKNOWN'

    def test_strict_mode_uses_first_executable_bid_after_latency(self):
        decision = _ts('2026-01-01T00:00:00+00:00')
        result = OutcomeContractV2Evaluator(
            OutcomeContractV2Config(require_execution_observation=True, latency_seconds=10)
        ).evaluate(
            signal_id=1,
            symbol='AAAUSDT',
            event_id='e',
            decision_time=decision,
            decision_price=100,
            event_high=None,
            execution_observations=(
                ExecutionObservation(_ts('2026-01-01T00:00:15+00:00'), bid=101, ask=101.2),
                ExecutionObservation(_ts('2026-01-01T00:00:20+00:00'), bid=99.5, ask=99.7),
            ),
            frame_1m=_frame([
                {'timestamp': _ts('2026-01-01T00:01:00+00:00'), 'open': 99.5, 'high': 100, 'low': 97, 'close': 98, 'volume': 1},
            ]),
            now=_ts('2026-01-01T00:02:00+00:00'),
        )
        assert result.entry_fill_status == EntryFillStatus.FILLED
        assert result.entry_fill_time == _ts('2026-01-01T00:00:20+00:00')
        assert result.entry_fill_price == 99.5


def test_outcome_result_duplicate_is_idempotent(tmp_path):
    from app.storage.db import Database
    from app.storage.repository import BotRepository
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    result = {
        "signal_id": 7, "symbol": "AAAUSDT", "event_id": "event", "decision_time": now,
        "entry_fill_status": "NO_FILL", "first_passage": "NO_FILL", "resolution_detail": "none",
        "evaluated_at": now, "evaluation_status": "CENSORED", "method_version": "OUTCOME_CONTRACT_V2_V1",
    }
    repository = BotRepository(Database(f"sqlite:///{tmp_path / 'outcome-duplicate.sqlite'}"))
    repository._db.create_all()
    assert repository.record_outcome_contract_v2_result(result) is True
    assert repository.record_outcome_contract_v2_result(result) is False


def test_outcome_non_duplicate_database_error_propagates(tmp_path):
    import pytest

    from app.storage.db import Database
    from app.storage.repository import BotRepository
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repository = BotRepository(Database(f"sqlite:///{tmp_path / 'outcome-error.sqlite'}"))
    repository._db.create_all()
    with pytest.raises(Exception):
        repository.record_outcome_contract_v2_result({
            "signal_id": 9, "symbol": "AAAUSDT", "event_id": "event", "decision_time": now,
            "entry_fill_status": "NO_FILL", "first_passage": "NO_FILL", "resolution_detail": "none",
            "evaluated_at": now, "horizons": object(), "method_version": "OUTCOME_CONTRACT_V2_V1",
        })
