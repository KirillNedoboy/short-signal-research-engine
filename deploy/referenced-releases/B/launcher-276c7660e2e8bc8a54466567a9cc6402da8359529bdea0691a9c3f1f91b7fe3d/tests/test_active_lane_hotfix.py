from __future__ import annotations

import pytest

from app.config import AppConfig, CanonicalMarketDataProviderMode, MarketFrameStoreMode
from app.market_data.hub import MarketDataHub
from app.market_data.provider import CanonicalMarketDataProvider


def _oversized_symbols() -> list[str]:
    return [f"S{index:04d}USDT" for index in range(400)]


def test_hub_rejects_over_capacity_before_mutating_universe() -> None:
    hub = MarketDataHub(["BTCUSDT"], max_connections=1)
    before = (hub._symbols, hub._expected, hub._active_topics)

    with pytest.raises(ValueError, match="capacity"):
        hub.update_universe(_oversized_symbols())

    assert (hub._symbols, hub._expected, hub._active_topics) == before


class _Client:
    pass


class _Scanner:
    client = _Client()


class _Hub:
    _max_connections = 1

    def __init__(self) -> None:
        self.updates: list[tuple[str, ...]] = []

    def update_universe(self, symbols: tuple[str, ...]) -> None:
        self.updates.append(tuple(symbols))


def test_provider_rejects_over_capacity_without_partial_store_mutation() -> None:
    hub = _Hub()
    provider = CanonicalMarketDataProvider(
        scanner=_Scanner(),
        hub=hub,
        config=AppConfig(
            canonical_market_data_provider_mode=CanonicalMarketDataProviderMode.HUB_PREFERRED,
            market_frame_store_mode=MarketFrameStoreMode.PREFERRED,
        ),
    )
    provider.update_universe(["BTCUSDT"])
    before = provider.frame_store.symbols()

    with pytest.raises(ValueError, match="capacity"):
        provider.update_universe(_oversized_symbols())

    assert provider.frame_store.symbols() == before
    assert hub.updates == [("BTCUSDT",)]
