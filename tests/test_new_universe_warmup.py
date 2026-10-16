"""Newly eligible symbols must receive bounded frame warmup after refresh."""
import asyncio
from types import SimpleNamespace

from app.main import ShortSignalBot


def test_new_eligible_symbol_is_warmed_even_if_provider_publishes_during_fetch():
    class Store:
        symbols_seen = {'OLDUSDT'}

        def symbols(self):
            return tuple(self.symbols_seen)

    class Provider:
        def __init__(self, store):
            self.frame_store = store
            self.last_universe_telemetry = None
            self.published = []

        async def fetch_market_snapshots(self):
            self.frame_store.symbols_seen.add('NEWUSDT')
            self.last_universe_telemetry = SimpleNamespace(
                eligible_symbols=('OLDUSDT', 'NEWUSDT'), exchange_symbols=('OLDUSDT', 'NEWUSDT')
            )
            return []

        def update_universe(self, symbols):
            self.published.append(tuple(symbols))
            self.frame_store.symbols_seen.update(symbols)

    async def run():
        store = Store()
        bot = ShortSignalBot.__new__(ShortSignalBot)
        bot._market_data_provider = Provider(store)
        bot._state_store = SimpleNamespace(load_active=lambda now: {})
        bot._universe_refreshed_at_monotonic = 0
        bot._event_universe_snapshots = []
        bot._event_eligible_symbols = ()
        warmed = []
        bot._schedule_frame_store_warmup = lambda symbols: warmed.extend(symbols)
        await bot._refresh_event_universe(startup=False)
        assert warmed == ['NEWUSDT']
        assert set(bot._market_data_provider.published[-1]) == {'OLDUSDT', 'NEWUSDT'}

    asyncio.run(run())
