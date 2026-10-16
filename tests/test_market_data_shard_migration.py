"""Topic migration across planned WS shards must rebind store continuity."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.market_data.hub import MarketDataHub
from app.market_data.models import MarketCandle
from app.market_data.planning import plan_topics
from app.market_data.store import CanonicalMarketStore
from test_market_data_planned_rotation import connection_factory, eventually


def _candle(symbol, epoch, minute):
    opened = datetime(2026, 9, 5, 12, minute, tzinfo=UTC)
    return MarketCandle(
        symbol=symbol, open_time=opened, close_time=opened + timedelta(minutes=1),
        interval="1", open=Decimal(1), high=Decimal(1), low=Decimal(1),
        close=Decimal(1), volume=Decimal(1), turnover=Decimal(1),
        confirmed=True, exchange_timestamp=opened, received_at=opened,
        connection_epoch=epoch,
    )


@pytest.mark.asyncio
async def test_planned_top100_shard_migration_rebinds_kline_epoch(monkeypatch):
    monkeypatch.delenv("MARKET_DATA_KEEP_STATE_ON_PLANNED_ROTATION", raising=False)
    old_symbols = [f"S{i:03}USDT" for i in range(100)]
    new_symbols = old_symbols[1:] + ["S999USDT"]
    old_plan, new_plan = plan_topics(old_symbols), plan_topics(new_symbols)
    shard = lambda plan, topic: next(i for i, conn in enumerate(plan.connections) if topic in conn.topics)
    moved = "S093USDT"
    topic = f"kline.1.{moved}"
    assert shard(old_plan, topic) == 1
    assert shard(new_plan, topic) == 0
    connections, connector = connection_factory()
    now = datetime(2026, 9, 5, 12, 3, tzinfo=UTC)
    hub = MarketDataHub(old_symbols, connector=connector, clock=lambda: now, ping_interval=100)
    store = CanonicalMarketStore(ring_size=2, clock=lambda: now, hub=hub)
    store.update_symbols(old_symbols)
    hub.add_observer(store)
    try:
        await hub.start()
        await eventually(lambda: len(connections) == len(old_plan.connections) and all(c.sent for c in connections))
        old_epoch = store._states[moved].required_epoch
        assert old_epoch is not None and old_epoch.shard_id == 1
        store.observe_closed_candle(_candle(moved, old_epoch, 0))
        store.observe_closed_candle(_candle(moved, old_epoch, 1))
        assert store._states[moved].continuity_reestablished
        stable = "S050USDT"
        stable_epoch = store._states[stable].required_epoch
        invalidations_before = store.counters["epoch_invalidations"]

        hub.update_universe(new_symbols)
        store.update_symbols(new_symbols)
        await eventually(lambda: len(connections) == len(old_plan.connections) + len(new_plan.connections)
                         and all(c.sent for c in connections[len(old_plan.connections):]))
        new_epoch = store._states[moved].required_epoch
        assert new_epoch is not None and new_epoch.shard_id == 0
        assert new_epoch != old_epoch
        assert not store._states[moved].continuity_reestablished
        assert store._states[stable].required_epoch == stable_epoch
        # One migrated kline plus the newly subscribed symbol; no broad wave.
        assert store.counters["epoch_invalidations"] == invalidations_before + 2
        store.observe_closed_candle(_candle(moved, new_epoch, 2))
        assert store._states[moved].continuity_reestablished
    finally:
        await hub.stop()
