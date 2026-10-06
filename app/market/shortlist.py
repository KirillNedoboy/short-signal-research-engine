"""Shortlist ranking helpers."""

from __future__ import annotations

from collections.abc import Iterable

from app.config import AppConfig
from app.domain import MarketSnapshot


def filter_universe(
    snapshots: Iterable[MarketSnapshot],
    config: AppConfig,
) -> list[MarketSnapshot]:
    """Filter only liquid, allowed USDT perpetual instruments."""

    excluded = {symbol.upper() for symbol in config.exclude_symbols}
    if config.exclude_btc_eth:
        excluded.update({"BTCUSDT", "ETHUSDT"})

    filtered: list[MarketSnapshot] = []
    for snapshot in snapshots:
        if snapshot.turnover_24h < config.min_24h_volume:
            continue
        if snapshot.symbol.upper() in excluded:
            continue
        filtered.append(snapshot)
    return filtered


def build_shortlist(
    snapshots: Iterable[MarketSnapshot],
    previous_snapshots: dict[str, MarketSnapshot],
    shortlist_size: int,
) -> list[MarketSnapshot]:
    """Build a union shortlist from daily movers and scan-to-scan velocity."""

    snapshot_list = list(snapshots)
    if not snapshot_list:
        return []

    velocity_ranked = sorted(
        snapshot_list,
        key=lambda item: _velocity_score(item, previous_snapshots.get(item.symbol)),
        reverse=True,
    )
    price_ranked = sorted(snapshot_list, key=lambda item: item.price_24h_pct, reverse=True)

    shortlisted: dict[str, MarketSnapshot] = {}
    for candidate in price_ranked[: shortlist_size * 2]:
        shortlisted[candidate.symbol] = candidate
    for candidate in velocity_ranked[: shortlist_size * 2]:
        shortlisted[candidate.symbol] = candidate

    ranked = sorted(shortlisted.values(), key=lambda item: item.turnover_24h, reverse=True)
    return ranked[:shortlist_size]


def round_robin_slice(
    snapshots: Iterable[MarketSnapshot],
    cursor: int,
    batch_size: int,
) -> tuple[list[MarketSnapshot], int]:
    """Return a deterministic shadow-only slice covering every eligible symbol."""
    ordered = sorted(snapshots, key=lambda item: item.symbol)
    if not ordered or batch_size <= 0:
        return [], 0
    start = cursor % len(ordered)
    selected = [ordered[(start + offset) % len(ordered)] for offset in range(min(batch_size, len(ordered)))]
    next_cursor = (start + len(selected)) % len(ordered)
    return selected, next_cursor


def _velocity_score(current: MarketSnapshot, previous: MarketSnapshot | None) -> float:
    if previous is None or previous.last_price <= 0:
        return 0.0
    return ((current.last_price / previous.last_price) - 1) * 100
