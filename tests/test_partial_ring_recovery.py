"""Partial historical rings request earlier slots, without lifting data gates."""
import asyncio
from datetime import timedelta

from app.infra.request_scheduler import RequestPriority
from app.market_data.store import CanonicalMarketStore, STALE_INCOMPLETE_RING
from test_market_data_store import _START, _candle, _clock, _ticker


def test_partial_ring_backfill_fetches_older_missing_closed_slot():
    now = _START + timedelta(minutes=10)
    store = CanonicalMarketStore(ring_size=3, clock=_clock(now))
    store.update_symbols(['TESTUSDT'])
    for minute in (8, 9):
        store.observe_closed_candle(_candle('TESTUSDT', minute))
    store.observe_ticker(_ticker('TESTUSDT', now))
    assert store.peek_snapshot('TESTUSDT', now).stale_reason == STALE_INCOMPLETE_RING

    class Client:
        called = []

        async def fetch_klines(self, symbol, interval, *, limit, start_ms, end_ms, **kwargs):
            self.called.append((symbol, start_ms, end_ms))
            earliest = int((_START + timedelta(minutes=7)).timestamp() * 1000)
            return [[str(earliest), '1', '2', '0.5', '1.5', '1000', '100000']] if start_ms <= earliest else []

    client = Client()
    assert asyncio.run(store.backfill_missing(client, 'TESTUSDT', priority=RequestPriority.RECOVERY))
    assert client.called[0][1] <= int((_START + timedelta(minutes=7)).timestamp() * 1000)
    assert store.peek_snapshot('TESTUSDT', now).stale_reason != STALE_INCOMPLETE_RING


def test_partial_ring_insufficient_exchange_history_stays_blocked():
    now = _START + timedelta(minutes=10)
    store = CanonicalMarketStore(ring_size=3, clock=_clock(now))
    store.update_symbols(['TESTUSDT'])
    store.observe_closed_candle(_candle('TESTUSDT', 9))

    class Client:
        async def fetch_klines(self, symbol, interval, **kwargs):
            return []

    assert not asyncio.run(store.backfill_missing(Client(), 'TESTUSDT', priority=RequestPriority.RECOVERY))
    assert store.peek_snapshot('TESTUSDT', now).stale_reason == STALE_INCOMPLETE_RING
