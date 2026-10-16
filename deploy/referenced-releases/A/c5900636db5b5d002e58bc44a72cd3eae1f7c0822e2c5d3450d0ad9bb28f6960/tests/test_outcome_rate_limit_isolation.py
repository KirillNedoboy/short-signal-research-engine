from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from app.config import AppConfig
from app.infra.provider_errors import OutcomeBudgetUnavailable, ProviderRateLimited
from app.outcomes.fetch import OutcomeCandleFetcher
from app.outcomes.tracker import OutcomeTracker
from app.storage.db import Database
from app.storage.models import OutcomeFetchRetryModel, SignalModel, StrategyObservationModel, TelegramDeliveryOutboxModel
from app.storage.repository import BotRepository

UTC = timezone.utc
START = datetime(2026, 10, 3, 14, 0, tzinfo=UTC)


class Clock:
    def __init__(self):
        self.now = START + timedelta(hours=1)
        self.tick = 0.0

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)
        self.tick += seconds


class Client:
    def __init__(self, failures=0):
        self.calls = []
        self.failures = failures
        self.gate = None

    async def fetch_klines(self, symbol, interval, **kwargs):
        self.calls.append((symbol, interval, kwargs))
        if self.gate is not None:
            await self.gate.wait()
        if self.failures:
            self.failures -= 1
            raise ProviderRateLimited(retry_after=4)
        return [[str(int((START+timedelta(minutes=i)).timestamp()*1000)), '100', '101', '98', '99', '10', '1000'] for i in range(20)]


def setup(tmp_path, *, failures=0, config=None):
    db = Database(f"sqlite:///{tmp_path/'outcomes.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    client, clock = Client(failures), Clock()
    fetcher = OutcomeCandleFetcher(client, repo, config or AppConfig(), clock=lambda: clock.now, monotonic=lambda: clock.tick)
    return db, repo, client, clock, fetcher


def observation(db, identity='ark-1', **kwargs):
    fields = dict(observation_id=identity, idempotency_key=identity, run_id='run', runtime_instance_id='instance',
                  strategy_family='VOLUME_CLIMAX', strategy='VOLUME_CLIMAX_UNWIND', evaluation_phase='scan',
                  symbol='ARKUSDT', observed_at=START, live_decision='NO_SIGNAL', shadow_decision='OBSERVE',
                  market_price=100, event_high=110, model_version='v1', config_hash='fixture', input_fingerprint='fixture')
    fields.update(kwargs)
    with db.session() as session:
        session.add(StrategyObservationModel(**fields))


def test_ark_rate_limit_is_deferred_then_completes_without_signals(tmp_path, caplog):
    db, repo, client, clock, fetcher = setup(tmp_path, failures=1)
    observation(db, outcome_json={'horizons': {'1m': {'price': 99}}}, outcome_mfe_pct=2)
    tracker = OutcomeTracker(client, repo, fetcher=fetcher)
    async def run():
        assert await tracker.update_due_outcomes(now=clock.now) == 1
        with db.session() as session:
            row = session.get(StrategyObservationModel, 'ark-1')
            assert row.outcome_status == 'deferred'
            assert row.outcome_json['data_reason'] == 'provider_rate_limited'
            assert row.outcome_json['retryable'] is True
            assert row.outcome_json['horizons']['1m']['price'] == 99
            assert row.outcome_mfe_pct == 2
            assert row.outcome_next_attempt_at.replace(tzinfo=UTC) > clock.now
            assert 'SL_OR_BAD' not in json.dumps(row.outcome_json)
        assert repo.list_strategy_observations_due_outcomes(now=clock.now) == []
        clock.advance(200)
        assert await tracker.update_due_outcomes(now=clock.now) == 1
        with db.session() as session:
            row = session.get(StrategyObservationModel, 'ark-1')
            assert row.outcome_status == 'complete'
            assert row.outcome_json['horizons']['15m']['price'] == 99
            assert row.outcome_next_attempt_at is None
            assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
            assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0
            assert session.scalar(select(func.count()).select_from(OutcomeFetchRetryModel)) == 0
        assert fetcher.metrics['outcome_retry_success_total'] == 1
    asyncio.run(run())
    event = next(json.loads(record.message) for record in caplog.records if 'provider_rate_limit_deferred' in record.message)
    assert event['symbol'] == 'ARKUSDT'
    assert event['provider_code'] == 10006
    assert event['request_class'] == 'OUTCOME_REPLAY'
    assert {'attempt','max_attempts','retry_at','scheduler_queue_depth','live_scan_in_flight','outcome_in_flight'} <= event.keys()


def test_exhausted_retry_survives_worker_recreation_and_stays_technical(tmp_path):
    config = AppConfig(outcome_rate_limit_max_attempts=1, outcome_rate_limit_retry_windows_sec=(), outcome_max_total_attempts=2)
    db, repo, client, clock, fetcher = setup(tmp_path, failures=20, config=config)
    observation(db)
    async def run():
        for _ in range(2):
            # A new process/worker must not get a fresh task retry allowance.
            f = OutcomeCandleFetcher(client, repo, config, clock=lambda: clock.now, monotonic=lambda: clock.tick)
            await OutcomeTracker(client, repo, fetcher=f).update_due_outcomes(now=clock.now)
            clock.advance(3600)
        assert len(client.calls) == 2
        assert repo.list_strategy_observations_due_outcomes(now=clock.now) == []
        with db.session() as session:
            row = session.get(StrategyObservationModel, 'ark-1')
            assert row.outcome_status == 'deferred'
            assert row.outcome_json['automatic_retry_exhausted'] is True
            assert row.outcome_json['retryable'] is True
            assert row.outcome_next_attempt_at is None
            retry = session.get(OutcomeFetchRetryModel, 'observation:ark-1')
            assert retry.attempt_count == 2
            assert retry.exhausted
        backlog = repo.outcome_tracker_backlog(now=clock.now)
        assert backlog['exhausted_total'] == 1
        assert backlog['due_total'] == 0
    asyncio.run(run())


def test_explicit_requeue_resets_only_selected_exhausted_observation(tmp_path):
    config = AppConfig(outcome_rate_limit_max_attempts=1, outcome_rate_limit_retry_windows_sec=(), outcome_max_total_attempts=1)
    db, repo, client, clock, fetcher = setup(tmp_path, failures=10, config=config)
    observation(db)

    async def run():
        await OutcomeTracker(client, repo, fetcher=fetcher).update_due_outcomes(now=clock.now)
        retry_at = clock.now + timedelta(hours=1)
        assert repo.requeue_outcome_fetch("observation:ark-1", now=retry_at)
        retry = repo.outcome_fetch_retry("observation:ark-1")
        assert retry["provider_attempt_count"] == 0
        assert retry["automatic_retry_exhausted"] is False
        assert repo.list_strategy_observations_due_outcomes(now=retry_at)
        assert not repo.requeue_outcome_fetch("observation:missing", now=retry_at)

    asyncio.run(run())


def test_runtime_worker_recovers_after_ark_rate_limit_without_blocking_live_or_fast(tmp_path):
    from app.infra.provider_errors import RateLimitPolicy
    from app.infra.request_scheduler import RequestPriority, RequestScheduler
    from app.market.bybit_client import BybitClient
    from app.observability.outcome_scheduler import ShadowOutcomeScheduler
    from app.runtime.outcome_worker import OutcomeWorker

    async def run():
        db, repo, _, clock, _ = setup(tmp_path)
        observation(db)
        scheduler = RequestScheduler(
            2, 0, 0, 0,
            rate_limit_policy=RateLimitPolicy(windows=((0, 0),), max_attempts=2, max_total_attempts=8),
        )
        client = BybitClient(scheduler)
        sends = []

        def response(**params):
            symbol = params["symbol"]
            sends.append(symbol)
            if symbol == "ARKUSDT" and sends.count(symbol) == 1:
                return {"retCode": 10006, "retMsg": "rate limited", "result": {"list": []}}
            return {"retCode": 0, "result": {"list": [[str(int((START+timedelta(minutes=i)).timestamp()*1000)), '100', '101', '98', '99', '10', '1000'] for i in range(20)]}}

        client._client.get_kline = response
        config = AppConfig()
        fetcher = OutcomeCandleFetcher(client, repo, config, clock=lambda: clock.now, monotonic=lambda: clock.tick)
        tracker = OutcomeTracker(client, repo, fetcher=fetcher)
        shadow = ShadowOutcomeScheduler(client=client, repository=repo, v2_enabled=False, fetcher=fetcher)
        worker = OutcomeWorker(shadow_scheduler=shadow, outcome_tracker=tracker, fetcher=fetcher, idle_poll_sec=1, clock=lambda: clock.now)

        async def wait_for(predicate):
            for _ in range(300):
                if predicate():
                    return
                await asyncio.sleep(0.01)
            raise AssertionError("runtime outcome worker did not reach expected state")

        try:
            await worker.start()
            await wait_for(lambda: "ARKUSDT" in sends and _outcome_status(db, "ark-1") == "deferred")
            with db.session() as session:
                deferred = session.get(StrategyObservationModel, "ark-1")
                assert deferred.outcome_status == "deferred"
                assert deferred.outcome_next_attempt_at.replace(tzinfo=UTC) > clock.now

            live, fast = await asyncio.gather(
                client.fetch_klines("LIVEUSDT", "1", priority=RequestPriority.LIVE_SCAN),
                client.fetch_klines("FASTUSDT", "1", priority=RequestPriority.FAST_MONITOR),
            )
            assert live and fast
            assert sends.index("LIVEUSDT") < sends.index("FASTUSDT")
            assert scheduler.stats_snapshot()["metrics"]["bybit_rate_limit_total:OUTCOME_REPLAY"] == 1

            clock.advance(200)
            await wait_for(lambda: (
                _outcome_status(db, "ark-1") == "complete"
                and worker.stats()["backlog"].get("due_total") == 0
            ))
            stats = worker.stats()
            assert stats["rate_limit"]["outcome_retry_success_total"] == 1
            assert stats["backlog"]["exhausted_total"] == 0
            assert scheduler.stats_snapshot()["metrics"]["bybit_rate_limit_total:LIVE_SCAN"] == 0
            with db.session() as session:
                assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
                assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0
        finally:
            await worker.stop()
            await fetcher.close()

    asyncio.run(run())


def _outcome_status(db, identity):
    with db.session() as session:
        row = session.get(StrategyObservationModel, identity)
        return row.outcome_status
def test_identical_observations_share_one_request_and_both_complete(tmp_path):
    db, repo, client, clock, fetcher = setup(tmp_path)
    observation(db, 'ark-1')
    observation(db, 'ark-2')
    asyncio.run(OutcomeTracker(client, repo, fetcher=fetcher).update_due_outcomes(now=clock.now))
    assert len(client.calls) == 1
    with db.session() as session:
        rows = session.scalars(select(StrategyObservationModel)).all()
        assert {r.observation_id: r.outcome_status for r in rows} == {'ark-1':'complete','ark-2':'complete'}


def test_concurrent_dedup_survives_waiter_cancellation(tmp_path):
    _, _, client, _, fetcher = setup(tmp_path)
    async def run():
        client.gate = asyncio.Event()
        first = asyncio.create_task(fetcher.fetch_klines('ARKUSDT','1',start_ms=1,end_ms=2,limit=100))
        second = asyncio.create_task(fetcher.fetch_klines('ARKUSDT','1',start_ms=1,end_ms=2,limit=100))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        client.gate.set()
        assert (await second)[0][4] == '99'
        assert len(client.calls) == 1
        assert not fetcher._inflight
    asyncio.run(run())


@pytest.mark.parametrize('change', [{'symbol':'BTCUSDT'}, {'interval':'5'}, {'start_ms':2}, {'end_ms':4}, {'limit':99}])
def test_different_request_keys_do_not_mix(tmp_path, change):
    _, _, client, _, fetcher = setup(tmp_path)
    async def run():
        key = dict(symbol='ARKUSDT', interval='1', start_ms=1,end_ms=3,limit=100)
        await fetcher.fetch_klines(**key)
        await fetcher.fetch_klines(**{**key, **change})
        assert len(client.calls) == 2
    asyncio.run(run())


def test_cache_ttl_lru_and_mutation_isolation(tmp_path):
    _, _, client, clock, fetcher = setup(tmp_path, config=AppConfig(outcome_cache_max_entries=2, outcome_cache_ttl_sec=5))
    async def run():
        result = await fetcher.fetch_klines('ARKUSDT','1')
        result[0][4] = 'bad mutation'
        assert (await fetcher.fetch_klines('ARKUSDT','1'))[0][4] == '99'
        clock.advance(6)
        await fetcher.fetch_klines('ARKUSDT','1')
        await fetcher.fetch_klines('BTCUSDT','1')
        await fetcher.fetch_klines('ETHUSDT','1')
        assert len(fetcher._cache) == 2
        await fetcher.fetch_klines('ARKUSDT','1')
        assert len(client.calls) == 5
    asyncio.run(run())


def test_cache_failures_are_not_successes_and_inflight_is_bounded(tmp_path):
    _, _, client, clock, fetcher = setup(tmp_path, failures=1, config=AppConfig(outcome_max_inflight_keys=1))
    async def run():
        client.gate = asyncio.Event()
        task = asyncio.create_task(fetcher.fetch_klines('ARKUSDT','1'))
        await asyncio.sleep(0)
        with pytest.raises(OutcomeBudgetUnavailable):
            await fetcher.fetch_klines('BTCUSDT','1')
        client.gate.set()
        with pytest.raises(ProviderRateLimited):
            await task
        assert not fetcher._cache
        assert not fetcher._inflight
        clock.advance(10)
        assert (await fetcher.fetch_klines('ARKUSDT','1'))[0][4] == '99'
        assert len(client.calls) == 2
    asyncio.run(run())


def test_nested_cycles_share_physical_request_cap(tmp_path):
    _, _, client, _, fetcher = setup(tmp_path, config=AppConfig(outcome_max_requests_per_cycle=1))
    async def run():
        with fetcher.cycle():
            with fetcher.cycle():
                await fetcher.fetch_klines('ARKUSDT','1')
            with fetcher.cycle():
                await fetcher.fetch_klines('ARKUSDT','1')
                with pytest.raises(OutcomeBudgetUnavailable):
                    await fetcher.fetch_klines('BTCUSDT','1')
        assert len(client.calls) == 1
    asyncio.run(run())
