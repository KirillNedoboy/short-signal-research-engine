"""Deterministic topic construction and character-budget batching."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

DEFAULT_CONNECTION_TOPIC_BUDGET = 18_000
DEFAULT_REQUEST_BATCH_BUDGET = 8_000
DEFAULT_MAX_CONNECTIONS = 4


@dataclass(frozen=True, slots=True)
class TopicBatch:
    topics: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ConnectionTopics:
    topics: tuple[str, ...]
    batches: tuple[TopicBatch, ...]


@dataclass(frozen=True, slots=True)
class TopicPlan:
    topics: tuple[str, ...]
    connections: tuple[ConnectionTopics, ...]


def normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()}
        )
    )


def _partition(topics: tuple[str, ...], budget: int) -> tuple[tuple[str, ...], ...]:
    if budget <= 0:
        raise ValueError("topic budget must be positive")
    result: list[tuple[str, ...]] = []
    current: list[str] = []
    current_size = 0
    for topic in topics:
        size = len(topic)
        if size > budget:
            raise ValueError(f"topic exceeds character budget: {topic}")
        if current and current_size + size > budget:
            result.append(tuple(current))
            current = []
            current_size = 0
        current.append(topic)
        current_size += size
    if current:
        result.append(tuple(current))
    return tuple(result)


def plan_topics(
    symbols: Iterable[str],
    *,
    connection_budget: int = DEFAULT_CONNECTION_TOPIC_BUDGET,
    request_budget: int = DEFAULT_REQUEST_BATCH_BUDGET,
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
) -> TopicPlan:
    """Return a sorted plan without randomisation or size-dependent ordering."""
    normalized = normalize_symbols(symbols)
    topics = tuple(
        sorted(
            [f"tickers.{symbol}" for symbol in normalized]
            + [f"kline.1.{symbol}" for symbol in normalized]
        )
    )
    groups = _partition(topics, connection_budget)
    if len(groups) > max_connections:
        raise ValueError("universe exceeds the configured connection capacity")
    connections = tuple(
        ConnectionTopics(
            group,
            tuple(TopicBatch(batch) for batch in _partition(group, request_budget)),
        )
        for group in groups
    )
    return TopicPlan(topics, connections)
