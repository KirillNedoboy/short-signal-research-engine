"""Deterministic dataset coverage and chronological split primitives.

This module is deliberately data-source agnostic. It never fabricates outcomes;
rows with unknown quality remain visible and prevent a frozen-OOS verdict.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence


_REQUIRED = frozenset(
    {
        "root_id",
        "attempt_id",
        "decision_time",
        "outcome_end_time",
        "symbol",
        "market_regime",
        "outcome_status",
        "data_quality_status",
    }
)


@dataclass(frozen=True, slots=True)
class DatasetAssessment:
    rows: int
    unique_roots: int
    unknown_or_gap: int
    duplicate_root_rows: int
    status: str
    promotion_eligible: bool


@dataclass(frozen=True, slots=True)
class ChronologicalSplit:
    train: tuple[Mapping[str, Any], ...]
    validation: tuple[Mapping[str, Any], ...]
    holdout: tuple[Mapping[str, Any], ...]
    status: str
    purge: timedelta
    embargo: timedelta


def assess_dataset(rows: Sequence[Mapping[str, Any]]) -> DatasetAssessment:
    missing = sorted({field for row in rows for field in _REQUIRED - row.keys()})
    if missing:
        raise ValueError(f"dataset rows missing required fields: {', '.join(missing)}")
    roots = [str(row["root_id"]) for row in rows]
    unique = len(set(roots))
    unknown = sum(
        1
        for row in rows
        if str(row["outcome_status"]).upper() in {"UNKNOWN", "DATA_GAP", "INCOMPLETE"}
        or str(row["data_quality_status"]).upper() in {"UNKNOWN", "DATA_GAP", "INCOMPLETE"}
    )
    return DatasetAssessment(
        rows=len(rows),
        unique_roots=unique,
        unknown_or_gap=unknown,
        duplicate_root_rows=len(rows) - unique,
        status="PRELIMINARY",
        promotion_eligible=False,
    )


def chronological_split(
    rows: Sequence[Mapping[str, Any]],
    *,
    train_end: datetime,
    validation_end: datetime,
    max_horizon: timedelta,
    embargo: timedelta,
    holdout_symbols: set[str] | frozenset[str] = frozenset(),
) -> ChronologicalSplit:
    if train_end >= validation_end:
        raise ValueError("train_end must be before validation_end")
    if max_horizon < timedelta(0) or embargo < timedelta(0):
        raise ValueError("purge and embargo must be non-negative")
    train_end = _utc(train_end)
    validation_end = _utc(validation_end)
    purge_start = train_end - max_horizon
    validation_start = train_end + embargo
    holdout_start = validation_end + embargo
    train: list[Mapping[str, Any]] = []
    validation: list[Mapping[str, Any]] = []
    holdout: list[Mapping[str, Any]] = []
    for row in sorted(rows, key=lambda item: (_utc(item["decision_time"]), str(item["root_id"]))):
        decision = _utc(row["decision_time"])
        symbol = str(row["symbol"])
        if symbol in holdout_symbols and decision >= holdout_start:
            holdout.append(row)
        elif validation_start <= decision < validation_end and symbol not in holdout_symbols:
            validation.append(row)
        elif decision < purge_start and symbol not in holdout_symbols:
            train.append(row)
    return ChronologicalSplit(tuple(train), tuple(validation), tuple(holdout), "VALID", max_horizon, embargo)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("dataset timestamps must be datetime values")
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
