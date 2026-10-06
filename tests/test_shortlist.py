from app.config import AppConfig
from app.domain import MarketSnapshot
from app.market.shortlist import build_shortlist, filter_universe, round_robin_slice


def test_shortlist_union_and_filtering() -> None:
    config = AppConfig(
        min_24h_volume=5_000_000,
        exclude_symbols=["SKIPUSDT"],
        exclude_btc_eth=True,
    )
    snapshots = [
        MarketSnapshot("BTCUSDT", 100.0, 15.0, 10_000_000, 1_000_000),
        MarketSnapshot("AAAUSDT", 10.0, 12.0, 8_000_000, 400_000),
        MarketSnapshot("BBBUSDT", 9.0, 3.0, 9_000_000, 450_000),
        MarketSnapshot("SKIPUSDT", 8.0, 20.0, 10_000_000, 400_000),
        MarketSnapshot("CCCUSDT", 7.0, 5.0, 4_000_000, 300_000),
    ]
    filtered = filter_universe(snapshots, config)

    previous = {
        "AAAUSDT": MarketSnapshot("AAAUSDT", 8.0, 0.0, 8_000_000, 400_000),
        "BBBUSDT": MarketSnapshot("BBBUSDT", 6.0, 0.0, 9_000_000, 450_000),
    }
    shortlist = build_shortlist(filtered, previous_snapshots=previous, shortlist_size=2)

    assert [item.symbol for item in filtered] == ["AAAUSDT", "BBBUSDT"]
    assert [item.symbol for item in shortlist] == ["BBBUSDT", "AAAUSDT"]


def test_shadow_round_robin_covers_all_eligible_symbols():
    snapshots = [
        MarketSnapshot(symbol, 1.0, 0.0, 1.0, 1.0)
        for symbol in ("CCCUSDT", "AAAUSDT", "BBBUSDT", "DDDUSDT", "EEEUSDT")
    ]
    first, cursor = round_robin_slice(snapshots, cursor=0, batch_size=2)
    second, cursor = round_robin_slice(snapshots, cursor=cursor, batch_size=2)
    third, cursor = round_robin_slice(snapshots, cursor=cursor, batch_size=2)

    assert [item.symbol for item in first + second + third][:5] == [
        "AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EEEUSDT"
    ]
    assert cursor == 1
