"""Observability-only market coverage lifecycle helpers."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable


def universe_fingerprint(symbols: Iterable[str]) -> str:
    """Return an order-independent SHA-256 fingerprint of unique symbols."""
    normalized = sorted({str(symbol).upper() for symbol in symbols if symbol})
    payload = json.dumps(normalized, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ScanUniverseTelemetry:
    exchange_symbols: tuple[str, ...]
    eligible_symbols: tuple[str, ...]
    excluded: tuple[tuple[str, str], ...]
    observed_at: datetime

    @property
    def exchange_fingerprint(self) -> str:
        return universe_fingerprint(self.exchange_symbols)

    @property
    def eligible_fingerprint(self) -> str:
        return universe_fingerprint(self.eligible_symbols)


def build_coverage_rows(*, rotation_id: str, observed_at: datetime, exchange_symbols: Iterable[str], eligible_symbols: Iterable[str], excluded: Iterable[tuple[str, str]], scheduled_symbols: Iterable[str], symbol_results: Iterable[dict]) -> list[dict]:
    exchange = sorted({str(x).upper() for x in exchange_symbols})
    eligible = {str(x).upper() for x in eligible_symbols}
    scheduled = {str(x).upper() for x in scheduled_symbols}
    exclusions = {str(symbol).upper(): reason for symbol, reason in excluded}
    results = {str(row.get("symbol", "")).upper(): row for row in symbol_results}
    rows = []
    for symbol in exchange:
        row = results.get(symbol, {})
        excluded_symbol = symbol not in eligible
        unexpected_result_present = excluded_symbol and bool(row)
        # EXCLUDED is canonical. A stray scanner result is retained as evidence,
        # but never allowed to contaminate the funnel's scanned state.
        status = "EXCLUDED" if excluded_symbol else str(row.get("terminal_status") or "SCAN_SKIPPED")
        rows.append({
            "rotation_id": rotation_id, "observed_at": observed_at, "symbol": symbol,
            "exchange_present": True, "eligible": not excluded_symbol,
            "exclusion_reason": exclusions.get(symbol), "scheduled": symbol in scheduled if not excluded_symbol else False,
            "scanned": status in {"SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"} if not excluded_symbol else False,
            "scan_status": status,
            "unexpected_result_present": unexpected_result_present,
            "evidence_json": {"reason_code": row.get("reason_code") or exclusions.get(symbol), "observed_terminal_status": row.get("terminal_status") if unexpected_result_present else None},
        })
    return rows


TERMINAL_STATUSES = {"EXCLUDED", "SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
ROTATION_STATUSES = {"OPEN", "COMPLETED", "INCOMPLETE", "ABORTED_RESTART", "FAILED"}


def select_rotation_batch(
    *,
    eligible_symbols: Iterable[str],
    already_scheduled: Iterable[str],
    preferred_symbols: Iterable[str],
    batch_size: int,
) -> list[str]:
    """Select an uncovered batch while preserving ranked preference order."""
    eligible = sorted({str(symbol).upper() for symbol in eligible_symbols if symbol})
    if batch_size <= 0 or not eligible:
        return []
    scheduled = {str(symbol).upper() for symbol in already_scheduled if symbol}
    uncovered = set(eligible) - scheduled
    preferred = list(
        dict.fromkeys(
            str(symbol).upper()
            for symbol in preferred_symbols
            if str(symbol).upper() in uncovered
        )
    )
    ordered = preferred + [
        symbol
        for symbol in eligible
        if symbol in uncovered and symbol not in preferred
    ]
    return ordered[:batch_size]


def coverage_percent(covered: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(min(100.0, max(0.0, covered * 100.0 / denominator)), 2)
