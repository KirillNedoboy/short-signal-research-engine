"""Bounded 20-second climax monitoring for Lane C.

The historical C runtime kept the fast path inside ``app.main``.  This module
preserves the small, stateful part of that implementation while leaving market
acquisition, strategy evaluation, persistence, and delivery to the composition
root.  The monitor only receives the active climax candidates and asks the
caller for frames for those candidates; it never discovers a universe or
performs an unbounded REST request.

The two seams used by the composition root are deliberately explicit:

* :class:`CandidatePool` owns event identity, replacement/removal, TTL, and
  fair ten-symbol selection.  ``track`` is called by the minute scan and
  ``remove(..., reason="signal_created")`` is called when a signal is
  persisted.
* :class:`BoundedFastMonitor` owns the independent 20-second cadence.  The
  ``frames_callback`` reads the current market store/cache for the selected
  symbols, while ``evaluate_callback`` invokes the climax-only engine.  The
  main runtime supplies its per-symbol mutation lock and state-version guard.

Research and heartbeat callbacks are best-effort queue boundaries.  They are
called without awaiting and queues are written with ``put_nowait`` so a
telemetry consumer cannot extend the fast evaluation path.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


LOGGER = logging.getLogger(__name__)


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _utc(value: datetime | Any | None) -> datetime | None:
    """Normalize supported timestamps without importing pandas at runtime."""

    if value is None:
        return None
    if not isinstance(value, datetime):
        converter = getattr(value, "to_pydatetime", None)
        if callable(converter):
            value = converter()
    if not isinstance(value, datetime):
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        else:
            return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _symbol(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("climax candidate symbol is required")
    return normalized


def _event_id(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("climax candidate event_id is required")
    return normalized


def _close_awaitable(value: Any) -> None:
    """Avoid leaking a coroutine when a synchronous callback is required."""

    if inspect.iscoroutine(value):
        value.close()


def _call_sync(callback: Callable[..., Any], *args: Any) -> Any:
    """Call a small callback with the positional arity it declares.

    This keeps the public seam convenient for the main runtime: callbacks may
    accept the full documented context or only the values they need.  A
    callback's own ``TypeError`` is not retried, so side effects cannot happen
    twice.
    """

    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args)
    parameters = tuple(signature.parameters.values())
    if any(item.kind is inspect.Parameter.VAR_POSITIONAL for item in parameters):
        return callback(*args)
    positional = tuple(
        item
        for item in parameters
        if item.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    )
    return callback(*args[: len(positional)])


async def _call_async(callback: Callable[..., Any], *args: Any) -> Any:
    result = _call_sync(callback, *args)
    if inspect.isawaitable(result):
        return await result
    return result


@dataclass(frozen=True, slots=True)
class ClimaxCandidate:
    """The event identity and fixed TTL anchor copied from frozen C."""

    symbol: str
    event_id: str
    event_high_time: datetime | None
    candidate_added_at: datetime
    pool_added_at: datetime
    generation: Any = None
    root_event_revision: int | None = None

    @property
    def key(self) -> tuple[str, str]:
        return self.symbol, self.event_id


# Keep the frozen C record name available to parity-oriented callers.
_ClimaxCandidate = ClimaxCandidate


@dataclass(frozen=True, slots=True)
class SnapshotVersion:
    """Comparable freshness metadata supplied by a frame/state callback."""

    asof: datetime | None = None
    generation: Any = None


@dataclass(frozen=True, slots=True)
class FastMonitorPoll:
    """Bounded result/telemetry summary for one fast poll."""

    poll_sequence: int
    started_at: datetime
    completed_at: datetime
    pool_size: int
    selected: tuple[str, ...]
    processed: tuple[str, ...]
    skipped: Mapping[str, str]
    expired: tuple[str, ...] = ()
    event_loop_lag_ms: float = 0.0

    @property
    def selected_count(self) -> int:
        return len(self.selected)


class CandidatePool:
    """Bounded active climax candidates with frozen-C lifecycle semantics."""

    def __init__(
        self,
        *,
        ttl_minutes: float = 30.0,
        max_active_symbols: int = 10,
        ttl: timedelta | None = None,
        now_fn: Callable[[], datetime] | None = None,
        event_sink: Callable[[Mapping[str, Any]], Any] | Any | None = None,
        root_event_tracking_enabled: bool = False,
        root_event_preparer: Callable[..., Any] | None = None,
        root_event_closer: Callable[..., Any] | None = None,
    ) -> None:
        self._ttl = ttl or timedelta(minutes=float(ttl_minutes))
        if self._ttl <= timedelta(0):
            raise ValueError("candidate TTL must be positive")
        if int(max_active_symbols) < 1:
            raise ValueError("max_active_symbols must be positive")
        self._max_active_symbols = int(max_active_symbols)
        self._now_fn = now_fn or (lambda: datetime.now(UTC))
        self._event_sink = event_sink
        self._root_event_tracking_enabled = bool(root_event_tracking_enabled)
        self._root_event_preparer = root_event_preparer
        self._root_event_closer = root_event_closer
        self._items: OrderedDict[tuple[str, str], ClimaxCandidate] = OrderedDict()
        self._cursor = 0

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    @property
    def ttl_minutes(self) -> float:
        return self._ttl.total_seconds() / 60.0

    @property
    def max_active_symbols(self) -> int:
        return self._max_active_symbols

    @property
    def cursor(self) -> int:
        return self._cursor

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self):
        return iter(self._items.values())

    def snapshot(self) -> tuple[ClimaxCandidate, ...]:
        return tuple(self._items.values())

    @property
    def candidates(self) -> tuple[ClimaxCandidate, ...]:
        return self.snapshot()

    def get(self, symbol: str, event_id: str) -> ClimaxCandidate | None:
        return self._items.get((_symbol(symbol), _event_id(event_id)))

    def _event(
        self,
        *,
        action: str,
        candidate: ClimaxCandidate | None = None,
        symbol: str | None = None,
        event_id: str | None = None,
        event_high_time: datetime | None = None,
        reason: str | None = None,
        observed_at: datetime | None = None,
        **details: Any,
    ) -> None:
        payload: dict[str, Any] = {
            "action": action,
            "created_at": _utc(observed_at) or datetime.now(UTC),
            "symbol": candidate.symbol if candidate else symbol,
            "event_id": candidate.event_id if candidate else event_id,
            "event_high_time": (
                candidate.event_high_time if candidate else _utc(event_high_time)
            ),
            "reason": reason,
            "pool_size": len(self._items),
        }
        if candidate is not None:
            payload.update(
                {
                    "root_event_id": candidate.event_id,
                    "pool_added_at": candidate.pool_added_at,
                    "generation": candidate.generation,
                    "event_revision": candidate.root_event_revision,
                }
            )
        payload.update(details)
        sink = self._event_sink
        if sink is None:
            return
        try:
            if hasattr(sink, "put_nowait"):
                sink.put_nowait(payload)
                return
            result = _call_sync(sink, payload)
            # Event sinks are intentionally synchronous/nonblocking.  Do not
            # await an accidental async sink on the fast path.
            _close_awaitable(result)
        except Exception:  # noqa: BLE001 - telemetry cannot break state
            LOGGER.debug("fast monitor event sink failed", exc_info=True)

    @staticmethod
    def _state_snapshot(state: Any) -> Mapping[str, Any]:
        snapshot = _value(state, "event_features_snapshot", {})
        return snapshot if isinstance(snapshot, Mapping) else {}

    def _root_revision(self, state: Any, result: Any) -> int | None:
        if isinstance(result, Mapping):
            value = result.get("event_revision", result.get("revision"))
        elif isinstance(result, tuple) and result:
            value = result[0]
        else:
            value = result
        if value is None:
            value = self._state_snapshot(state).get("root_event_revision")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _prepare_root(
        self,
        state: Any,
        observed_at: datetime,
        *,
        enabled: bool,
        preparer: Callable[..., Any] | None,
    ) -> int | None:
        snapshot_revision = self._state_snapshot(state).get("root_event_revision")
        if not enabled:
            try:
                return int(snapshot_revision or 1)
            except (TypeError, ValueError):
                return 1
        if preparer is None:
            raise RuntimeError(
                "climax_root_event_tracking_enabled requires root_event_preparer"
            )
        result = _call_sync(preparer, state, observed_at)
        if inspect.isawaitable(result):
            _close_awaitable(result)
            raise TypeError("root_event_preparer must be synchronous")
        return self._root_revision(state, result)

    def _remove_key(
        self,
        key: tuple[str, str],
        *,
        reason: str,
        observed_at: datetime,
    ) -> ClimaxCandidate | None:
        candidate = self._items.pop(key, None)
        if candidate is not None:
            self._event(
                action="candidate_removed",
                candidate=candidate,
                reason=reason,
                observed_at=observed_at,
            )
        return candidate

    def track(
        self,
        state_or_symbol: Any,
        event_id: str | datetime | None = None,
        event_high_time: datetime | None = None,
        now: datetime | None = None,
        *,
        symbol: str | None = None,
        generation: Any = None,
        root_event_preparer: Callable[..., Any] | None = None,
        root_event_tracking_enabled: bool | None = None,
        root_event_closer: Callable[..., Any] | None = None,
    ) -> ClimaxCandidate | None:
        """Track a candidate from an ``EventState``-like value.

        ``track(state, now=...)`` is the preferred form.  For small harnesses
        the equivalent scalar form is ``track(symbol, event_id,
        event_high_time, now)``.  Existing event identities are returned as-is
        and never receive a new TTL anchor.
        """

        state = None
        if isinstance(state_or_symbol, str):
            resolved_symbol = symbol or state_or_symbol
            if isinstance(event_id, datetime):
                if now is None:
                    now = event_id
                event_id = None
            if event_id is None:
                raise ValueError("event_id is required for scalar candidate tracking")
            resolved_event_id = event_id
        else:
            state = state_or_symbol
            # ``track(state, now)`` is a useful positional shorthand.
            if isinstance(event_id, datetime) and now is None:
                now = event_id
                event_id = None
            resolved_symbol = symbol or _value(state, "symbol")
            resolved_event_id = event_id or _value(state, "event_id")
            event_high_time = (
                event_high_time
                if event_high_time is not None
                else _value(state, "event_high_time")
            )
            if generation is None:
                generation = _value(state, "generation")

        if resolved_event_id is None:
            return None
        candidate_symbol = _symbol(resolved_symbol)
        candidate_event_id = _event_id(resolved_event_id)
        observed_at = _utc(now) or _utc(self._now_fn()) or datetime.now(UTC)
        high_time = _utc(event_high_time)
        ttl_anchor = high_time or observed_at
        enabled = (
            self._root_event_tracking_enabled
            if root_event_tracking_enabled is None
            else bool(root_event_tracking_enabled)
        )
        preparer = root_event_preparer or self._root_event_preparer
        root_revision = None
        if state is not None:
            # Frozen C prepares root evidence before checking whether the event
            # is already in the pool.  Keep that ordering and its side effect.
            root_revision = self._prepare_root(
                state,
                observed_at,
                enabled=enabled,
                preparer=preparer,
            )
        key = (candidate_symbol, candidate_event_id)

        for old_key in list(self._items):
            if old_key[0] != candidate_symbol or old_key == key:
                continue
            old = self._remove_key(
                old_key,
                reason="replaced_by_new_event",
                observed_at=observed_at,
            )
            closer = root_event_closer or self._root_event_closer
            if enabled and old is not None and closer is not None:
                result = _call_sync(closer, old, state or key, observed_at)
                if inspect.isawaitable(result):
                    _close_awaitable(result)

        current = self._items.get(key)
        if current is not None:
            return current
        if observed_at - ttl_anchor >= self._ttl:
            self._event(
                action="candidate_rejected",
                symbol=candidate_symbol,
                event_id=candidate_event_id,
                event_high_time=high_time,
                reason="ttl_expired_before_pool_add",
                observed_at=observed_at,
            )
            return None

        candidate = ClimaxCandidate(
            symbol=candidate_symbol,
            event_id=candidate_event_id,
            event_high_time=high_time,
            candidate_added_at=ttl_anchor,
            pool_added_at=observed_at,
            generation=generation,
            root_event_revision=root_revision,
        )
        self._items[key] = candidate
        self._event(
            action="pool_add",
            candidate=candidate,
            reason="new_event_id",
            observed_at=observed_at,
            observed_at_utc=observed_at,
        )
        return candidate

    # Explicit aliases make the handoff from the frozen method names obvious.
    track_climax_candidate = track

    def remove(
        self,
        symbol: str,
        event_id: str,
        *,
        reason: str = "removed",
        now: datetime | None = None,
    ) -> ClimaxCandidate | None:
        observed_at = _utc(now) or _utc(self._now_fn()) or datetime.now(UTC)
        return self._remove_key(
            (_symbol(symbol), _event_id(event_id)),
            reason=reason,
            observed_at=observed_at,
        )

    remove_candidate = remove
    remove_climax_candidate = remove

    def expire(self, *, now: datetime | None = None) -> tuple[ClimaxCandidate, ...]:
        observed_at = _utc(now) or _utc(self._now_fn()) or datetime.now(UTC)
        expired: list[ClimaxCandidate] = []
        for key, candidate in list(self._items.items()):
            if observed_at - candidate.candidate_added_at < self._ttl:
                continue
            removed = self._remove_key(key, reason="ttl", observed_at=observed_at)
            if removed is not None:
                expired.append(removed)
        return tuple(expired)

    def select(
        self,
        *,
        now: datetime | None = None,
        limit: int | None = None,
    ) -> list[ClimaxCandidate]:
        """Return the next fair bounded slice and advance the cursor."""

        self.expire(now=now)
        keys = list(self._items)
        if not keys:
            return []
        start = self._cursor % len(keys)
        ordered = keys[start:] + keys[:start]
        bound = self._max_active_symbols if limit is None else min(
            self._max_active_symbols, max(0, int(limit))
        )
        selected_keys = ordered[:bound]
        self._cursor = (start + len(selected_keys)) % len(keys)
        return [self._items[key] for key in selected_keys]


@asynccontextmanager
async def _empty_mutation(_: str) -> AsyncIterator[None]:
    yield


def _version(value: Any) -> SnapshotVersion:
    if isinstance(value, SnapshotVersion):
        return value
    nested = _value(value, "snapshot", None)
    if nested is not None and nested is not value:
        value = nested
    asof = _value(value, "asof")
    if asof is None:
        asof = _value(value, "market_asof")
    if asof is None:
        asof = _value(value, "captured_at_utc")
    if asof is None:
        asof = _value(value, "captured_at")
    if asof is None:
        asof = _value(value, "received_at")
    if asof is None:
        asof = _value(value, "updated_at")
    generation = _value(value, "generation")
    if generation is None:
        generation = _value(value, "revision")
    if generation is None:
        generation = _value(value, "version")
    if generation is None:
        generation = _value(value, "sequence")
    return SnapshotVersion(asof=_utc(asof), generation=generation)


def _compare(left: Any, right: Any) -> int:
    if left is None or right is None:
        return 0
    if left == right:
        return 0
    try:
        return 1 if left > right else -1
    except TypeError:
        left_text, right_text = str(left), str(right)
        if left_text == right_text:
            return 0
        return 1 if left_text > right_text else -1


def _older(candidate: SnapshotVersion, reference: SnapshotVersion) -> bool:
    if (
        candidate.generation is not None
        and reference.generation is not None
        and _compare(candidate.generation, reference.generation) < 0
    ):
        return True
    if (
        candidate.asof is not None
        and reference.asof is not None
        and candidate.asof < reference.asof
    ):
        return True
    return False


def _empty_snapshot(snapshot: Any) -> bool:
    if snapshot is None:
        return True
    nested = _value(snapshot, "frame_1m", None)
    if nested is not None:
        snapshot = _value(nested, "frame", nested)
    if bool(_value(snapshot, "empty", False)):
        return True
    try:
        return len(snapshot) == 0 if not isinstance(snapshot, (str, bytes, Mapping)) else False
    except TypeError:
        return False


class BoundedFastMonitor:
    """A separate bounded 20-second task for the climax contour only.

    ``evaluate_callback(candidate, snapshot, poll_sequence)`` must call the
    C climax-only engine.  The monitor intentionally has no baseline, trapped
    pipeline, notifier, database, or provider dependency.
    """

    def __init__(
        self,
        pool: CandidatePool,
        *,
        frames_callback: Callable[..., Any] | None = None,
        snapshot_callback: Callable[..., Any] | None = None,
        evaluate_callback: Callable[..., Any] | None = None,
        climax_evaluator: Callable[..., Any] | None = None,
        on_result: Callable[..., Any] | None = None,
        result_callback: Callable[..., Any] | None = None,
        mutation_lock: Callable[[str], Any] | None = None,
        state_version_callback: Callable[..., Any] | None = None,
        event_sink: Callable[[Mapping[str, Any]], Any] | Any | None = None,
        event_queue: Any | None = None,
        heartbeat_callback: Callable[[Mapping[str, Any]], Any] | None = None,
        heartbeat_queue: Any | None = None,
        poll_interval_sec: float = 20.0,
        max_active_symbols: int | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if float(poll_interval_sec) <= 0:
            raise ValueError("poll_interval_sec must be positive")
        self.pool = pool
        self.frames_callback = frames_callback
        self.snapshot_callback = snapshot_callback
        self.evaluate_callback = evaluate_callback or climax_evaluator
        self.on_result = on_result or result_callback
        self.mutation_lock = mutation_lock
        self.state_version_callback = state_version_callback
        self.event_sink = event_sink
        self.event_queue = event_queue
        self.heartbeat_callback = heartbeat_callback
        self.heartbeat_queue = heartbeat_queue
        self.poll_interval_sec = float(poll_interval_sec)
        self.max_active_symbols = int(max_active_symbols or pool.max_active_symbols)
        if self.max_active_symbols < 1:
            raise ValueError("max_active_symbols must be positive")
        self._clock = clock or (lambda: datetime.now(UTC))
        self._watermarks: dict[str, SnapshotVersion] = {}
        self._poll_sequence = 0
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @property
    def poll_sequence(self) -> int:
        return self._poll_sequence

    @property
    def running(self) -> bool:
        return self._running

    def _emit(self, payload: Mapping[str, Any]) -> None:
        for queue in (self.event_queue,):
            if queue is None:
                continue
            try:
                queue.put_nowait(dict(payload))
            except Exception:  # noqa: BLE001 - telemetry must not block
                LOGGER.debug("fast monitor event queue unavailable", exc_info=True)
        sink = self.event_sink
        if sink is None:
            return
        try:
            if hasattr(sink, "put_nowait"):
                sink.put_nowait(dict(payload))
            else:
                result = _call_sync(sink, payload)
                _close_awaitable(result)
        except Exception:  # noqa: BLE001 - telemetry must not block
            LOGGER.debug("fast monitor event sink failed", exc_info=True)

    def _heartbeat(self, payload: Mapping[str, Any]) -> None:
        for queue in (self.heartbeat_queue,):
            if queue is None:
                continue
            try:
                queue.put_nowait(dict(payload))
            except Exception:  # noqa: BLE001 - telemetry must not block
                LOGGER.debug("fast monitor heartbeat queue unavailable", exc_info=True)
        callback = self.heartbeat_callback
        if callback is None:
            return
        try:
            result = _call_sync(callback, payload)
            _close_awaitable(result)
        except Exception:  # noqa: BLE001 - telemetry must not block
            LOGGER.debug("fast monitor heartbeat callback failed", exc_info=True)

    async def _frames(self, symbols: tuple[str, ...]) -> Mapping[str, Any]:
        if not symbols:
            return {}
        callback = self.frames_callback
        if callback is not None:
            result = await _call_async(callback, symbols)
            return result if isinstance(result, Mapping) else {}
        callback = self.snapshot_callback
        if callback is None:
            return {}
        values: dict[str, Any] = {}
        for symbol in symbols:
            values[symbol] = await _call_async(callback, symbol)
        return values

    @staticmethod
    def _frame_for(frames: Mapping[str, Any], symbol: str) -> Any:
        if symbol in frames:
            return frames[symbol]
        upper = symbol.upper()
        if upper in frames:
            return frames[upper]
        for key, value in frames.items():
            if str(key).upper() == upper:
                return value
        return None

    def _lock(self, symbol: str):
        callback = self.mutation_lock
        if callback is None:
            return _empty_mutation(symbol)
        result = callback(symbol)
        if inspect.isawaitable(result):
            _close_awaitable(result)
            raise TypeError("mutation_lock must return an async context manager")
        if not hasattr(result, "__aenter__"):
            raise TypeError("mutation_lock must return an async context manager")
        return result

    async def _state_version(self, symbol: str) -> SnapshotVersion:
        callback = self.state_version_callback
        if callback is None:
            return SnapshotVersion()
        value = await _call_async(callback, symbol)
        return _version(value)

    async def _evaluate(self, candidate: ClimaxCandidate, snapshot: Any) -> Any:
        callback = self.evaluate_callback
        if callback is None:
            return None
        return await _call_async(callback, candidate, snapshot, self._poll_sequence)

    async def _publish_result(
        self,
        candidate: ClimaxCandidate,
        snapshot: Any,
        result: Any,
    ) -> None:
        callback = self.on_result
        if callback is None:
            return
        await _call_async(callback, candidate, snapshot, result, self._poll_sequence)

    async def poll_once(self, *, now: datetime | None = None) -> FastMonitorPoll:
        started_at = _utc(now) or _utc(self._clock()) or datetime.now(UTC)
        self._poll_sequence += 1
        sequence = self._poll_sequence
        selected_candidates = self.pool.select(
            now=started_at,
            limit=self.max_active_symbols,
        )
        selected_symbols = tuple(candidate.symbol for candidate in selected_candidates)
        self._heartbeat(
            {
                "kind": "fast_monitor",
                "action": "poll_start",
                "poll_sequence": sequence,
                "pool_size": len(self.pool),
                "selected_count": len(selected_candidates),
                "checked_at": started_at,
            }
        )
        self._emit(
            {
                "kind": "fast_monitor",
                "action": "poll_start",
                "poll_sequence": sequence,
                "pool_size": len(self.pool),
                "selected_count": len(selected_candidates),
            }
        )
        processed: list[str] = []
        skipped: dict[str, str] = {}
        frames: Mapping[str, Any] = {}
        if selected_symbols:
            try:
                frames = await self._frames(selected_symbols)
            except Exception as exc:  # noqa: BLE001 - bound one provider failure
                for candidate in selected_candidates:
                    skipped[candidate.symbol] = "frames_error"
                    self._emit(
                        {
                            "kind": "fast_monitor",
                            "action": "poll_skip",
                            "symbol": candidate.symbol,
                            "event_id": candidate.event_id,
                            "reason": "frames_error",
                            "error_code": type(exc).__name__,
                            "poll_sequence": sequence,
                        }
                    )
                frames = {}

        for selected in selected_candidates:
            current = self.pool.get(selected.symbol, selected.event_id)
            if current is None:
                skipped[selected.symbol] = "candidate_removed"
                continue
            snapshot = self._frame_for(frames, selected.symbol)
            if _empty_snapshot(snapshot):
                skipped[selected.symbol] = "empty_frame_or_snapshot"
                self._emit(
                    {
                        "kind": "fast_monitor",
                        "action": "poll_skip",
                        "symbol": selected.symbol,
                        "event_id": selected.event_id,
                        "reason": "empty_frame_or_snapshot",
                        "poll_sequence": sequence,
                    }
                )
                continue
            snapshot_version = _version(snapshot)
            candidate_generation = SnapshotVersion(generation=current.generation)
            if _older(snapshot_version, candidate_generation):
                skipped[selected.symbol] = "stale_snapshot"
                continue
            try:
                async with self._lock(selected.symbol):
                    current = self.pool.get(selected.symbol, selected.event_id)
                    if current is None:
                        skipped[selected.symbol] = "candidate_removed"
                        continue
                    state_version = await self._state_version(selected.symbol)
                    watermark = self._watermarks.get(selected.symbol, SnapshotVersion())
                    if _older(snapshot_version, state_version) or _older(
                        snapshot_version, watermark
                    ):
                        skipped[selected.symbol] = "stale_snapshot"
                        self._emit(
                            {
                                "kind": "fast_monitor",
                                "action": "poll_skip",
                                "symbol": selected.symbol,
                                "event_id": selected.event_id,
                                "reason": "stale_snapshot",
                                "poll_sequence": sequence,
                            }
                        )
                        continue
                    result = await self._evaluate(current, snapshot)
                    # A full scan may have committed a newer snapshot while an
                    # injected evaluator was running.  The same lock plus this
                    # post-evaluation check prevents the old result from being
                    # persisted over fresh state.
                    latest_state_version = await self._state_version(selected.symbol)
                    if _older(snapshot_version, latest_state_version):
                        skipped[selected.symbol] = "stale_snapshot"
                        continue
                    self._watermarks[selected.symbol] = snapshot_version
                    await self._publish_result(current, snapshot, result)
                    processed.append(current.symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one candidate cannot stop poll
                skipped[selected.symbol] = "candidate_error"
                self._emit(
                    {
                        "kind": "fast_monitor",
                        "action": "poll_skip",
                        "symbol": selected.symbol,
                        "event_id": selected.event_id,
                        "reason": "candidate_error",
                        "error_code": type(exc).__name__,
                        "poll_sequence": sequence,
                    }
                )

        completed_at = _utc(self._clock()) or datetime.now(UTC)
        expired = self.pool.expire(now=completed_at)
        report = FastMonitorPoll(
            poll_sequence=sequence,
            started_at=started_at,
            completed_at=completed_at,
            pool_size=len(self.pool),
            selected=selected_symbols,
            processed=tuple(processed),
            skipped=dict(skipped),
            expired=tuple(candidate.symbol for candidate in expired),
        )
        heartbeat = {
            "kind": "fast_monitor",
            "action": "poll_complete",
            "poll_sequence": sequence,
            "pool_size": len(self.pool),
            "selected_count": len(selected_candidates),
            "processed_count": len(processed),
            "completed_at": completed_at,
        }
        self._heartbeat(heartbeat)
        self._emit(heartbeat)
        return report

    run_once = poll_once

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            await asyncio.sleep(self.poll_interval_sec)
            if not self._running:
                break
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep the bounded task alive
                LOGGER.exception("climax fast monitor poll failed")

    def start(self) -> asyncio.Task[None]:
        if self._task is not None and not self._task.done():
            return self._task
        self._running = True
        self._task = asyncio.create_task(self.run_forever(), name="climax-fast-monitor")
        return self._task

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


__all__ = [
    "BoundedFastMonitor",
    "CandidatePool",
    "ClimaxCandidate",
    "FastMonitorPoll",
    "SnapshotVersion",
    "_ClimaxCandidate",
]
