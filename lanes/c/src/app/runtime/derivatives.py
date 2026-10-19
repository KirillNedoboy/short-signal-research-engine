"""Bounded background cache for C's historical derivative inputs.

The C feature builder consumes Bybit historical open-interest rows and funding
history.  This cache keeps those inputs off the per-symbol scan await path. A
scan may use only a complete, fresh cache entry; missing or stale data remains
explicitly unknown and is surfaced as a terminal incomplete/stale status by
the composition root.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Iterable, Mapping

from app.infra.request_scheduler import RequestPriority, request_class

LOGGER = logging.getLogger(__name__)

# C's scanner treats a successful derivatives response as a bounded snapshot:
# the timestamps on historical rows describe the exchange measurement, while
# response arrival controls whether that response can still be used.  A failed
# refresh retries sooner but does not erase the last successful response.
SUCCESS_TTL_SEC = 180.0
FAILURE_RETRY_SEC = 60.0

# Kept as compatibility aliases for callers that imported the old names.  They
# are diagnostic-only now; row timestamps never become a second trade gate.
OI_FRESHNESS_SEC = SUCCESS_TTL_SEC
FUNDING_FRESHNESS_SEC = SUCCESS_TTL_SEC


@dataclass(slots=True)
class _Entry:
    payload: dict[str, Any]
    fetched_at: datetime | None
    oi_observed_at: datetime | None
    funding_observed_at: datetime | None
    next_retry_at: datetime
    refresh_failed: bool = False


class DerivativeRefreshCache:
    """Keep a bounded, asynchronously refreshed derivative context cache."""

    def __init__(
        self,
        scanner: Any,
        config: Any,
        *,
        clock: Callable[[], datetime] | None = None,
        max_entries: int | None = None,
    ) -> None:
        self._scanner = scanner
        self._config = config
        self._clock = clock or (lambda: datetime.now(UTC))
        configured = int(getattr(config, "shortlist_size", 100) or 100)
        self._max_entries = max(1, min(256, int(max_entries or configured)))
        self._max_concurrency = max(
            1, min(16, int(getattr(config, "max_request_concurrency", 4) or 4))
        )
        self._refresh_interval_sec = max(
            1.0, min(60.0, float(getattr(config, "scan_interval_sec", 60) or 60))
        )
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self._symbols: tuple[str, ...] = ()
        self._refresh_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def update_symbols(self, symbols: Iterable[str]) -> None:
        """Set the bounded refresh universe and evict symbols outside it."""

        # Preserve caller order: ``frames`` supplies active symbols before the
        # broader shortlist, so truncating after a lexical sort could evict a
        # live event and spend the bounded cache on cold tail symbols.
        normalized = tuple(
            dict.fromkeys(str(item).upper() for item in symbols if item)
        )
        self._symbols = normalized[: self._max_entries]
        keep = set(self._symbols)
        for symbol in tuple(self._entries):
            if symbol not in keep:
                self._entries.pop(symbol, None)
        if self._task is not None and not self._task.done():
            self._wake.set()

    def seed(
        self,
        symbol: str,
        payload: Mapping[str, Any],
        *,
        observed_at: datetime | None = None,
        fetched_at: datetime | None = None,
    ) -> None:
        """Seed deterministic/restored data with explicit response timing.

        A restored payload must carry either ``fetched_at`` or the historical
        ``observed_at`` argument.  Treating a checkpoint as freshly fetched at
        first read would silently extend an outage indefinitely.
        """

        arrival = fetched_at or observed_at
        if arrival is None:
            raise ValueError("derivative seed requires fetched_at or observed_at")
        now = _aware(arrival)
        normalized = deepcopy(dict(payload))
        normalized["derivatives_fetched_at"] = now.isoformat()
        oi_at = _field_time(normalized.get("oi_observed_at")) or _latest_row_time(
            normalized.get("open_interest"), now
        )
        funding_at = _field_time(normalized.get("funding_observed_at")) or _latest_row_time(
            normalized.get("funding"), now
        )
        self._store(
            str(symbol).upper(),
            _Entry(
                payload=normalized,
                fetched_at=now,
                oi_observed_at=oi_at,
                funding_observed_at=funding_at,
                next_retry_at=now + timedelta(seconds=SUCCESS_TTL_SEC),
            ),
        )

    def get(self, symbol: str, *, now: datetime | None = None) -> dict[str, Any]:
        """Return a C-shaped mapping with explicit freshness/unknown status."""

        current = _aware(now or self._clock())
        key = str(symbol).upper()
        entry = self._entries.get(key)
        if entry is None:
            return _unknown_payload(key, "derivatives_cache_miss")
        self._entries.move_to_end(key)
        return _freshness_payload(key, entry, current)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(
            self._run(), name="lane-c-derivatives-refresh"
        )

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def kick(self) -> None:
        """Wake the background loop after a new universe is available."""

        if self._task is not None and not self._task.done():
            self._wake.set()

    async def refresh_now(self, symbols: Iterable[str] | None = None) -> dict[str, int]:
        """Refresh due symbols with bounded concurrency; no caller-side fanout."""

        requested = tuple(
            dict.fromkeys(str(item).upper() for item in (symbols or self._symbols) if item)
        )[: self._max_entries]
        if not requested:
            return {"requested": 0, "refreshed": 0, "failed": 0}
        async with self._refresh_lock:
            semaphore = asyncio.Semaphore(self._max_concurrency)

            async def one(symbol: str) -> bool:
                async with semaphore:
                    return await self._refresh_one(symbol)

            results = await asyncio.gather(*(one(symbol) for symbol in requested))
        return {
            "requested": len(requested),
            "refreshed": sum(results),
            "failed": len(results) - sum(results),
        }

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self.refresh_now(self._due_symbols())
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - refresh failures stay explicit in cache
                LOGGER.exception("C derivative refresh loop failed")
            try:
                await asyncio.wait_for(
                    self._wake.wait(), timeout=self._next_wakeup_delay()
                )
                self._wake.clear()
            except asyncio.TimeoutError:
                pass

    def _due_symbols(self) -> tuple[str, ...]:
        now = _aware(self._clock())
        due: list[str] = []
        for symbol in self._symbols:
            entry = self._entries.get(symbol)
            if entry is None or now >= entry.next_retry_at:
                if entry is None or _entry_needs_refresh(entry, now):
                    due.append(symbol)
        return tuple(due)

    def _next_wakeup_delay(self) -> float:
        """Sleep until the nearest retry/TTL deadline, bounded by cadence."""

        now = _aware(self._clock())
        deadlines = [
            entry.next_retry_at
            for symbol in self._symbols
            if (entry := self._entries.get(symbol)) is not None
        ]
        if not deadlines:
            return self._refresh_interval_sec
        seconds = max(0.0, (min(deadlines) - now).total_seconds())
        return min(self._refresh_interval_sec, seconds)

    async def _refresh_one(self, symbol: str) -> bool:
        try:
            result = await _fetch_fresh(self._scanner, symbol)
            # The TTL starts when the provider response arrives, not when the
            # background task was admitted.  Slow REST calls therefore do not
            # shorten an otherwise successful 180-second snapshot.
            fetched_at = _aware(self._clock())
            payload = deepcopy(dict(result or {}))
            oi_at = _field_time(payload.get("oi_observed_at")) or _latest_row_time(
                payload.get("open_interest"), fetched_at
            )
            funding_at = _field_time(
                payload.get("funding_observed_at")
            ) or _latest_row_time(payload.get("funding"), fetched_at)
            # Missing provider timestamps mean the response arrival is the
            # strongest available observation time; never invent a value for
            # absent rows themselves.
            if payload.get("open_interest") and oi_at is None:
                oi_at = fetched_at
            if payload.get("funding") and funding_at is None:
                funding_at = fetched_at
            self._store(
                symbol,
                _Entry(
                    payload=payload,
                    fetched_at=fetched_at,
                    oi_observed_at=oi_at,
                    funding_observed_at=funding_at,
                    next_retry_at=fetched_at + timedelta(seconds=SUCCESS_TTL_SEC),
                    refresh_failed=False,
                ),
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one symbol stays isolated
            failed_at = _aware(self._clock())
            LOGGER.warning(
                "C derivative refresh unavailable | symbol=%s error=%s",
                symbol,
                type(exc).__name__,
            )
            previous = self._entries.get(symbol)
            if previous is None:
                self._store(
                    symbol,
                    _Entry(
                        payload=_unknown_payload(symbol, "derivatives_refresh_error"),
                        fetched_at=failed_at,
                        oi_observed_at=None,
                        funding_observed_at=None,
                        next_retry_at=failed_at + timedelta(seconds=FAILURE_RETRY_SEC),
                        refresh_failed=True,
                    ),
                )
            else:
                previous.next_retry_at = failed_at + timedelta(seconds=FAILURE_RETRY_SEC)
                previous.refresh_failed = True
            return False

    def _store(self, symbol: str, entry: _Entry) -> None:
        self._entries[symbol] = entry
        self._entries.move_to_end(symbol)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


async def _fetch_fresh(scanner: Any, symbol: str) -> Mapping[str, Any]:
    fetcher = getattr(scanner, "fetch_optional_derivatives", None)
    if fetcher is None:
        raise AttributeError("scanner has no derivative fetcher")
    try:
        signature = inspect.signature(fetcher)
    except (TypeError, ValueError):
        signature = None
    # Scanner implementations delegate to the shared Bybit client.  Carry the
    # recovery priority through the context so those requests cannot consume
    # live scan admission even when the legacy scanner signature has no
    # explicit priority keyword.
    token = request_class.set(RequestPriority.RECOVERY)
    try:
        if signature is not None and "force_refresh" in signature.parameters:
            return await fetcher(symbol, force_refresh=True)
        return await fetcher(symbol)
    finally:
        request_class.reset(token)


def _entry_needs_refresh(entry: _Entry, now: datetime) -> bool:
    if entry.refresh_failed:
        return True
    payload = entry.payload
    if str(payload.get("derivatives_status") or "") != "OK":
        return True
    if entry.fetched_at is None:
        return True
    return (now - entry.fetched_at).total_seconds() >= SUCCESS_TTL_SEC


def _freshness_payload(symbol: str, entry: _Entry, now: datetime) -> dict[str, Any]:
    payload = deepcopy(entry.payload)
    oi_rows = list(payload.get("open_interest") or [])
    funding_rows = list(payload.get("funding") or [])
    reasons = [str(item) for item in (payload.get("derivatives_reasons") or [])]
    warnings = [str(item) for item in (payload.get("data_quality_warnings") or [])]
    response_age = _age(now, entry.fetched_at)
    oi_age = _age(now, entry.oi_observed_at)
    funding_age = _age(now, entry.funding_observed_at)
    if not oi_rows:
        reasons.append("open_interest_missing")
        warnings.append("oi_missing")
    elif response_age is None:
        oi_rows = []
        reasons.append("derivatives_fetched_at_missing")
        warnings.append("derivatives_expiry_unknown")
    if not funding_rows:
        reasons.append("funding_missing")
        warnings.append("derivatives_missing")
    elif response_age is None:
        funding_rows = []
        reasons.append("derivatives_fetched_at_missing")
        warnings.append("derivatives_expiry_unknown")
    original_status = str(payload.get("derivatives_status") or "UNKNOWN")
    status = original_status
    if response_age is None:
        status = "UNKNOWN"
    elif response_age >= SUCCESS_TTL_SEC and original_status == "OK":
        status = "STALE"
        oi_rows = []
        funding_rows = []
        reasons.append("derivatives_cache_expired")
        warnings.append("derivatives_stale")
    elif not oi_rows or not funding_rows:
        if original_status == "OK":
            status = "STALE"
        elif not payload.get("derivatives_status"):
            status = "UNKNOWN"
    payload.update(
        {
            "symbol": symbol,
            "open_interest": oi_rows,
            "funding": funding_rows,
            "derivatives_status": status,
            "derivatives_reasons": _dedupe(reasons),
            "data_quality_warnings": _dedupe(warnings),
            "oi_observed_at": entry.oi_observed_at.isoformat() if entry.oi_observed_at else None,
            "funding_observed_at": entry.funding_observed_at.isoformat() if entry.funding_observed_at else None,
            "derivatives_fetched_at": entry.fetched_at.isoformat() if entry.fetched_at else None,
            "derivatives_cache_age_sec": (
                max(0.0, (now - entry.fetched_at).total_seconds())
                if entry.fetched_at is not None
                else None
            ),
            "oi_measurement_age_sec": oi_age,
            "funding_measurement_age_sec": funding_age,
            "scan_failure": (
                None
                if status == "OK"
                else {"code": _failure_code(status)}
            ),
        }
    )
    return payload


def _unknown_payload(symbol: str, reason: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "open_interest": [],
        "funding": [],
        "derivatives_status": "UNKNOWN",
        "derivatives_reasons": [reason],
        "data_quality_warnings": ["oi_missing", "derivatives_missing"],
        "oi_observed_at": None,
        "funding_observed_at": None,
        "derivatives_fetched_at": None,
        "scan_failure": {"code": "DERIVATIVES_UNKNOWN"},
    }


def _failure_code(status: str) -> str:
    if status == "STALE":
        return "DERIVATIVES_STALE"
    if status in {"UNKNOWN", "MISSING"}:
        return "DERIVATIVES_UNKNOWN"
    return f"DERIVATIVES_{status}"


def classify_payload(payload: Mapping[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Apply the same freshness gates to a store-provided C payload."""

    current = _aware(now or datetime.now(UTC))
    # Store payloads normally carry explicit observation fields.  Derive them
    # from timestamped rows when an older checkpoint lacks those fields, while
    # leaving genuinely untimestamped data unknown so freshness is never
    # silently reset by the classification call.
    explicit_fetched_at = (
        _field_time(payload.get("derivatives_fetched_at"))
        or _field_time(payload.get("fetched_at"))
        or _field_time(payload.get("response_at"))
        or max(
            (
                value
                for value in (
                    _field_time(payload.get("oi_observed_at")),
                    _field_time(payload.get("funding_observed_at")),
                )
                if value is not None
            ),
            default=None,
        )
    )
    entry = _Entry(
        payload=deepcopy(dict(payload)),
        fetched_at=explicit_fetched_at,
        oi_observed_at=_field_time(payload.get("oi_observed_at"))
        or _latest_row_time(payload.get("open_interest")),
        funding_observed_at=_field_time(payload.get("funding_observed_at"))
        or _latest_row_time(payload.get("funding")),
        next_retry_at=current,
        refresh_failed=False,
    )
    return _freshness_payload(str(payload.get("symbol") or "").upper(), entry, current)


def _latest_row_time(rows: Any, fallback: datetime | None = None) -> datetime | None:
    if not isinstance(rows, (list, tuple)):
        return None
    candidates = [_row_time(row) for row in rows]
    candidates = [item for item in candidates if item is not None]
    return max(candidates) if candidates else (fallback if rows else None)


def _row_time(row: Any) -> datetime | None:
    if not isinstance(row, Mapping):
        return None
    for key in ("timestamp", "time", "fundingRateTimestamp", "funding_rate_timestamp"):
        if row.get(key) in (None, ""):
            continue
        value = row[key]
        try:
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000.0
            return datetime.fromtimestamp(number, tz=UTC)
        except (TypeError, ValueError, OverflowError, OSError):
            try:
                parsed = datetime.fromisoformat(str(value))
            except ValueError:
                continue
            return _aware(parsed)
    return None


def _field_time(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _aware(value)
    try:
        return _aware(datetime.fromisoformat(str(value)))
    except ValueError:
        return None


def _age(now: datetime, observed: datetime | None) -> float | None:
    if observed is None:
        return None
    return max(0.0, (now - _aware(observed)).total_seconds())


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


__all__ = [
    "DerivativeRefreshCache",
    "FAILURE_RETRY_SEC",
    "FUNDING_FRESHNESS_SEC",
    "OI_FRESHNESS_SEC",
    "SUCCESS_TTL_SEC",
    "classify_payload",
]
