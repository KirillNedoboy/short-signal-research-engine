"""Deterministic historical replay clock."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable


class HistoricalReplayClock:
    def __init__(self, instants: Iterable[datetime]) -> None:
        self._instants = tuple(_utc(value) for value in instants)
        if any(right < left for left, right in zip(self._instants, self._instants[1:])):
            raise ValueError("historical clock instants must be ordered")
        self._cursor = 0

    def next(self) -> datetime:
        if self._cursor >= len(self._instants):
            raise StopIteration
        value = self._instants[self._cursor]
        self._cursor += 1
        return value

    @property
    def exhausted(self) -> bool:
        return self._cursor >= len(self._instants)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
