"""Observability-only market coverage lifecycle helpers."""
from __future__ import annotations

import hashlib
import json
import re
from enum import StrEnum
from dataclasses import dataclass
from datetime import datetime
from collections import Counter
from typing import Any, Iterable


class ScanFailureReasonCode(StrEnum):
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    STALE_MARKET_DATA = "STALE_MARKET_DATA"
    MARKET_DATA_INCOMPLETE = "MARKET_DATA_INCOMPLETE"
    MARKET_DATA_DISCONTINUITY = "MARKET_DATA_DISCONTINUITY"
    EMPTY_RESPONSE = "EMPTY_RESPONSE"
    PROVIDER_ERROR = "PROVIDER_ERROR"


FAILURE_REASON_CODES = {code.value for code in ScanFailureReasonCode}

_SAFE_EVIDENCE_TEXT = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_TOKEN_LIKE = re.compile(r"(?:sk[-_]|token[-_=]|secret[-_=]|bearer\s+)[A-Za-z0-9._:-]+", re.IGNORECASE)
_SAFE_SYMBOL = re.compile(r"^[A-Z0-9_.:-]{1,32}$")
_SAFE_BOUNDED_TEXT = re.compile(r"^[\x20-\x7e]{1,255}$")
MAX_SAFE_CYCLE_COUNTER = 1_000_000_000


def sanitize_evidence_value(value: object, *, kind: str) -> str | None:
    """Keep ledger evidence scalar, bounded, and free of token-like data."""
    if value is None:
        return None
    if not isinstance(value, str):
        return f"REDACTED_NON_SCALAR_{kind.upper()}"
    if _TOKEN_LIKE.search(value) or not _SAFE_EVIDENCE_TEXT.fullmatch(value):
        return f"REDACTED_INVALID_{kind.upper()}"
    return value


def _sanitize_bounded_text(value: object, *, kind: str, limit: int = 255) -> str:
    if not isinstance(value, str):
        return f"REDACTED_NON_SCALAR_{kind.upper()}"
    if len(value) > limit or _TOKEN_LIKE.search(value) or not _SAFE_BOUNDED_TEXT.fullmatch(value):
        return f"REDACTED_INVALID_{kind.upper()}"
    return value


def sanitize_json_value(value: object, *, depth: int = 0) -> object:
    if depth > 2:
        return "REDACTED_MAX_DEPTH"
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value if abs(value) <= 1_000_000_000 else "REDACTED_OVERSIZED_NUMBER"
    if isinstance(value, str):
        return _sanitize_bounded_text(value, kind="detail")
    if isinstance(value, dict):
        return {str(key)[:64]: sanitize_json_value(item, depth=depth + 1) for key, item in list(value.items())[:16] if not any(token in str(key).lower() for token in ("secret", "token", "password", "private"))}
    if isinstance(value, list):
        return [sanitize_json_value(item, depth=depth + 1) for item in value[:16]]
    return "REDACTED_NON_SCALAR_DETAIL"


def sanitize_symbol(value: object) -> str:
    if not isinstance(value, str):
        return "REDACTED_INVALID_SYMBOL"
    normalized = value.upper()
    return normalized if _SAFE_SYMBOL.fullmatch(normalized) and not _TOKEN_LIKE.search(normalized) else "REDACTED_INVALID_SYMBOL"


def sanitize_excluded_with_diagnostics(
    excluded: Any,
) -> tuple[list[tuple[str, str]], list[str]]:
    """Return only bounded ``(symbol, reason)`` exclusion evidence."""
    if excluded is None or isinstance(excluded, (str, bytes, dict)):
        return [], []
    try:
        items = iter(excluded)
    except Exception:
        return [], ["excluded_iterator_conversion_failed"]
    result: list[tuple[str, str]] = []
    try:
        for item in items:
            if isinstance(item, (tuple, list)) and len(item) == 2:
                result.append(
                    (
                        sanitize_symbol(item[0]),
                        sanitize_evidence_value(item[1], kind="reason")
                        or "REDACTED_INVALID_REASON",
                    )
                )
    except Exception:
        return [], ["excluded_iterator_iteration_failed"]
    return result, []


def sanitize_excluded(excluded: Any) -> list[tuple[str, str]]:
    return sanitize_excluded_with_diagnostics(excluded)[0]


def sanitize_cycle_counter(value: object) -> tuple[int, bool]:
    """Return a SQLite-safe count and whether the input was sanitized."""
    if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_SAFE_CYCLE_COUNTER:
        return value, False
    return 0, True


def sanitize_symbol_result(row: dict) -> dict:
    """Return the bounded symbol-result contract used by spool and SQLite."""
    raw_status = row.get("terminal_status")
    result = {"symbol": sanitize_symbol(row.get("symbol"))}
    result["terminal_status"] = raw_status if isinstance(raw_status, str) and raw_status in SCHEDULED_TERMINAL_STATUSES | {"EXCLUDED"} else "SCAN_FAILED"
    safe_reason = sanitize_evidence_value(row.get("reason_code"), kind="reason")
    result["reason_code"] = safe_reason or "PROVIDER_ERROR"
    if "duration_ms" in row:
        duration = row.get("duration_ms")
        result["duration_ms"] = duration if isinstance(duration, int) and 0 <= duration <= 86_400_000 else None
    if "details" in row or "details_json" in row:
        result["details"] = sanitize_json_value(row.get("details", row.get("details_json")))
    return result


def sanitize_cycle_payload(payload: dict) -> dict:
    """Sanitize raw cycle fields before canonical spool serialization."""
    safe = {field: payload.get(field) for field in ("cycle_identity", "cycle_started_at", "cycle_completed_at")}
    for field in ("exchange_symbols", "eligible_symbols", "scheduled_symbols"):
        values = payload.get(field, [])
        safe[field] = [sanitize_symbol(value) for value in values] if isinstance(values, list) else []
    safe["excluded"], exclusion_mismatch = sanitize_excluded_with_diagnostics(payload.get("excluded", []))
    if exclusion_mismatch:
        safe["exclusion_sanitization_mismatch"] = exclusion_mismatch
    invalid_counters = []
    for field in ("candidate_symbols", "evaluated_symbols"):
        safe[field], sanitized = sanitize_cycle_counter(payload.get(field))
        if sanitized:
            invalid_counters.append(field)
    if invalid_counters:
        safe["counter_sanitization_mismatch"] = invalid_counters
    safe["last_error"] = None if payload.get("last_error") is None else _sanitize_bounded_text(payload.get("last_error"), kind="error")
    return safe


def normalize_scan_failure(
    reason: object,
    *,
    exception: Exception | None = None,
) -> dict[str, object]:
    """Return a terminal scan failure with a bounded, stable reason code."""
    text = str(reason).upper()
    if any(token in text for token in ("DEADLINE", "TIMEOUT", "TIMED OUT")):
        code = ScanFailureReasonCode.DEADLINE_EXCEEDED
    elif any(token in text for token in ("STALE", "OLD DATA")):
        code = ScanFailureReasonCode.STALE_MARKET_DATA
    elif any(token in text for token in ("DISCONTINU", "GAP", "DUPLICATE")):
        code = ScanFailureReasonCode.MARKET_DATA_DISCONTINUITY
    elif any(token in text for token in ("EMPTY", "NO DATA", "NO RESPONSE")):
        code = ScanFailureReasonCode.EMPTY_RESPONSE
    elif any(token in text for token in ("INCOMPLETE", "MISSING", "REQUIRED")):
        code = ScanFailureReasonCode.MARKET_DATA_INCOMPLETE
    else:
        code = ScanFailureReasonCode.PROVIDER_ERROR
    result: dict[str, object] = {"terminal_status": "SCAN_FAILED", "reason_code": code.value}
    if exception is not None:
        result["details"] = {"exception_type": type(exception).__name__}
    return result


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
    exclusions = dict(sanitize_excluded(excluded))
    results: dict[str, dict] = {}
    for raw_row in symbol_results:
        if isinstance(raw_row, dict):
            results[str(raw_row.get("symbol", "")).upper()] = raw_row
    rows = []
    for symbol in exchange:
        row = results.get(symbol, {})
        excluded_symbol = symbol not in eligible
        raw_status = row.get("terminal_status") if row else None
        invalid_status = bool(row) and (
            not isinstance(raw_status, str) or raw_status not in TERMINAL_STATUSES
        )
        unexpected_result_present = bool(row) and (
            excluded_symbol or symbol not in scheduled
        )
        # EXCLUDED is canonical. A stray scanner result is retained as evidence,
        # but never allowed to contaminate the funnel's scanned state.
        if excluded_symbol:
            status = "EXCLUDED"
        elif unexpected_result_present:
            status = "SCAN_FAILED"
        elif invalid_status:
            status = "SCAN_FAILED"
        elif row:
            status = str(raw_status)
        elif symbol in scheduled:
            status = "SCAN_FAILED"
        else:
            status = "SCAN_SKIPPED"
        reason_code = row.get("reason_code") or exclusions.get(symbol)
        if status == "SCAN_FAILED":
            normalized = normalize_scan_failure(
                "INVALID_TERMINAL_STATUS" if invalid_status else "INCOMPLETE"
                if reason_code in (None, "NO_SETUP") else reason_code
            )
            reason_code = normalized["reason_code"]
        elif status == "SCAN_SKIPPED" and reason_code is None:
            reason_code = "NOT_SCHEDULED"
        reason_code = sanitize_evidence_value(reason_code, kind="reason") or "REDACTED_INVALID_REASON"
        observed_status = (
            sanitize_evidence_value(raw_status, kind="status")
            if (unexpected_result_present or invalid_status)
            else None
        )
        rows.append({
            "rotation_id": rotation_id, "observed_at": observed_at, "symbol": symbol,
            "exchange_present": True, "eligible": not excluded_symbol,
            "exclusion_reason": exclusions.get(symbol), "scheduled": symbol in scheduled if not excluded_symbol else False,
            "scanned": status in {"SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"} if not excluded_symbol else False,
            "scan_status": status,
            "unexpected_result_present": unexpected_result_present,
            "evidence_json": {"reason_code": reason_code, "observed_terminal_status": observed_status},
        })
    return rows


TERMINAL_STATUSES = {"EXCLUDED", "SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
SCHEDULED_TERMINAL_STATUSES = {"SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
ROTATION_STATUSES = {"OPEN", "COMPLETED", "INCOMPLETE", "ABORTED_RESTART", "FAILED"}


def _is_scheduled_terminal(value: object) -> bool:
    return isinstance(value, str) and value in SCHEDULED_TERMINAL_STATUSES


def _status_description(value: object) -> str:
    if isinstance(value, str):
        return value[:64]
    return f"<invalid_{type(value).__name__}>"


def validate_scan_accounting(
    *, scheduled_symbols: Iterable[str], symbol_results: Iterable[dict]
) -> dict[str, object]:
    """Validate the one-terminal-result conservation boundary for one batch."""
    scheduled_values = [str(symbol).upper() for symbol in scheduled_symbols]
    scheduled = {symbol for symbol in scheduled_values if symbol}
    blank_scheduled = [
        index for index, symbol in enumerate(scheduled_values) if not symbol.strip()
    ]
    result_rows = list(symbol_results)
    malformed_rows = [index for index, row in enumerate(result_rows) if not isinstance(row, dict)]
    missing_symbol_rows = [
        index for index, row in enumerate(result_rows)
        if isinstance(row, dict) and not str(row.get("symbol", "")).strip()
    ]
    result_symbols = [
        str(row.get("symbol", "")).upper() if isinstance(row, dict) else ""
        for row in result_rows
    ]
    result_counts = Counter(result_symbols)
    result_map = {
        symbol: row for symbol, row in zip(result_symbols, result_rows)
        if symbol and isinstance(row, dict)
    }
    missing = sorted(scheduled - set(result_symbols))
    duplicates = sorted(symbol for symbol, count in result_counts.items() if symbol and count > 1)
    unexpected = sorted(set(result_symbols) - scheduled - {""})
    invalid = sorted(
        {
            f"{symbol}:{_status_description(row.get('terminal_status'))}"
            for symbol, row in zip(result_symbols, result_rows)
            if symbol and isinstance(row, dict)
            and not _is_scheduled_terminal(row.get("terminal_status"))
        }
    )
    duplicate_scheduled = sorted(
        symbol for symbol, count in Counter(scheduled_values).items() if count > 1
    )
    mismatch = {
        key: values
        for key, values in {
            "missing_symbols": missing,
            "duplicate_result_symbols": duplicates,
            "unexpected_result_symbols": unexpected,
            "invalid_terminal_statuses": invalid,
            "duplicate_scheduled_symbols": duplicate_scheduled,
            "missing_symbol_rows": missing_symbol_rows,
            "malformed_result_rows": malformed_rows,
            "blank_scheduled_symbols": blank_scheduled,
        }.items()
        if values
    }
    terminal_counts = Counter(
        row.get("terminal_status")
        for symbol, row in result_map.items()
        if symbol in scheduled and _is_scheduled_terminal(row.get("terminal_status"))
    )
    unfinished = sorted(
        scheduled
        - {
            symbol
            for symbol, row in result_map.items()
            if _is_scheduled_terminal(row.get("terminal_status"))
        }
    )
    return {
        "valid": not mismatch and len(scheduled) == sum(terminal_counts.values()),
        "mismatch": mismatch,
        "started_symbols": len(scheduled),
        "unfinished_symbols": unfinished,
        "terminal_counts": {
            "SCANNED_OK": terminal_counts["SCANNED_OK"],
            "SCAN_FAILED": terminal_counts["SCAN_FAILED"],
            "SCAN_SKIPPED": terminal_counts["SCAN_SKIPPED"],
        },
        "result_map": result_map,
    }


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
