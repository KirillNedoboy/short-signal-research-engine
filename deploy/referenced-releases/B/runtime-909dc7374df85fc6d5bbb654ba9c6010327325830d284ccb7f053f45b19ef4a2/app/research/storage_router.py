"""Runtime routing for bounded research telemetry.

The router owns storage-mode decisions and counters.  It deliberately does not
know about strategies, signal delivery, or SQLAlchemy models; callers decide
whether a SQLite operation is still required from ``ResearchWriteResult``.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import is_dataclass, asdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping

from app.config import ResearchTelemetryStorageMode
from app.infra.disk_capacity import DiskCapacitySnapshot
from app.research.spool import DRecordLifecycle, ResearchSpool, make_spool_record

logger = logging.getLogger(__name__)


RESEARCH_DATASET_CLASS: dict[str, str] = {
    "__db_heartbeat": "A",
    "event_states": "A",
    "signals": "A",
    "signal_provenance": "A",
    "signal_outcomes": "A",
    "telegram_delivery_outbox": "A",
    "watch_candidates": "A",
    "climax_root_events": "A",
    "climax_entry_attempts": "A",
    "runtime_heartbeats": "A",
    "runtime_heartbeat_history": "B",
    "market_coverage_ledger": "C",
    "market_scan_symbol_results": "C",
    "climax_monitor_events": "C",
    "reject_stats": "C",
    "root_detector_shadow_legacy_mappings": "C",
    "strategy_observations": "D",
    "volume_climax_observations": "D",
    "climax_entry_attempt_events": "D",
    "market_scan_cycles": "D",
    "market_scan_rotations": "D",
    "root_detector_shadow_candidates": "D",
    "root_detector_shadow_episodes": "D",
    "root_detector_shadow_episode_outcomes": "D",
    "root_detector_shadow_v2_roots": "D",
    "root_detector_shadow_v2_outcomes": "D",
    "current_root_outcomes": "D",
    "climax_evaluations": "E",
    "root_detector_shadow_observations": "E",
    "root_detector_shadow_episode_root_links": "E",
    "current_root_predicate_snapshots": "E",
}


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(child) for child in value]
    return value


@dataclass(frozen=True, slots=True)
class ResearchWriteResult:
    record_id: str | None
    sqlite_required: bool
    spooled: bool
    paused: bool = False
    error: str | None = None


class ResearchStorageRouter:
    """Bounded, fail-safe routing decision for research telemetry."""

    def __init__(
        self,
        *,
        mode: ResearchTelemetryStorageMode,
        spool: ResearchSpool,
        capacity_provider: Callable[[], DiskCapacitySnapshot],
        reserve_bytes: int,
        code_sha: str,
        shadow_headroom_bytes: int = 1 * 1024 * 1024 * 1024,
    ) -> None:
        self.mode = ResearchTelemetryStorageMode(mode)
        self.spool = spool
        self.capacity_provider = capacity_provider
        self.reserve_bytes = reserve_bytes
        self.code_sha = code_sha
        self.shadow_headroom_bytes = shadow_headroom_bytes
        self._tokens: dict[str, DRecordLifecycle] = {}
        self._counters: dict[str, int] = {
            "append_total": 0,
            "sealed_total": 0,
            "compaction_total": 0,
            "class_c_sqlite_writes": 0,
            "class_c_spool_writes": 0,
            "paused_total": 0,
            "dropped_total": 0,
            "errors_total": 0,
        }
        self._last_error: str | None = None
        self._last_complete_batch: str | None = None

    def write(
        self,
        dataset: str,
        payload: Mapping[str, Any],
        *,
        identity: str,
        observed_at_utc: datetime,
        _terminal: bool = False,
    ) -> ResearchWriteResult:
        classification = RESEARCH_DATASET_CLASS.get(dataset, "A")
        if (
            classification not in {"C"} and not (classification == "D" and _terminal)
        ) or self.mode is ResearchTelemetryStorageMode.SQLITE_ONLY:
            if classification == "C":
                self._counters["class_c_sqlite_writes"] += 1
            return ResearchWriteResult(None, True, False)

        record_payload = _json_safe(dict(payload))
        record_payload.setdefault("identity", identity)
        try:
            record = make_spool_record(
                dataset=dataset,
                payload=record_payload,
                code_sha=self.code_sha,
                occurred_at_utc=observed_at_utc,
                max_bytes=self.spool.record_max_bytes,
            )
            self.spool.append(
                record,
                capacity=self.capacity_provider(),
                reserve_bytes=self.reserve_bytes,
                shadow=self.mode is ResearchTelemetryStorageMode.SPOOL_SHADOW,
                shadow_headroom_bytes=self.shadow_headroom_bytes,
            )
        except Exception as exc:  # noqa: BLE001 - research failures are isolated
            self._counters["errors_total"] += 1
            self._last_error = type(exc).__name__
            paused = "disk" in str(exc).lower() or "budget" in str(exc).lower()
            if paused:
                self._counters["paused_total"] += 1
            else:
                self._counters["dropped_total"] += 1
            sqlite_required = self.mode is ResearchTelemetryStorageMode.SPOOL_SHADOW
            if classification == "C" and sqlite_required:
                self._counters["class_c_sqlite_writes"] += 1
            return ResearchWriteResult(
                None,
                sqlite_required,
                False,
                paused=paused,
                error=type(exc).__name__,
            )

        self._counters["append_total"] += 1
        if classification == "C":
            self._counters["class_c_spool_writes"] += 1
            if self.mode is ResearchTelemetryStorageMode.SPOOL_SHADOW:
                self._counters["class_c_sqlite_writes"] += 1
        return ResearchWriteResult(
            record.record_id,
            self.mode is ResearchTelemetryStorageMode.SPOOL_SHADOW,
            True,
        )

    def write_terminal(
        self,
        dataset: str,
        payload: Mapping[str, Any],
        *,
        identity: str,
        observed_at_utc: datetime,
    ) -> DRecordLifecycle | None:
        result = self.write(
            dataset,
            payload,
            identity=identity,
            observed_at_utc=observed_at_utc,
            _terminal=True,
        )
        if not result.spooled or result.record_id is None:
            return None
        token = DRecordLifecycle()
        token.mark_terminal(result.record_id)
        self._tokens[result.record_id] = token
        return token

    def mark_sealed(self, token: DRecordLifecycle, segment_id: str) -> None:
        token.mark_spooled(segment_id)
        token.mark_sealed()
        self._counters["sealed_total"] += 1

    def seal_open_segments(self) -> tuple[str, ...]:
        sealed: list[str] = []
        for path in self.spool.recover_open():
            sealed.append(str(self.spool.seal(path)))
            self._counters["sealed_total"] += 1
        return tuple(sealed)

    def mark_complete(self, token: DRecordLifecycle, manifest_id: str) -> None:
        token.mark_complete(manifest_id)
        self._last_complete_batch = manifest_id
        self._counters["compaction_total"] += 1

    def verify_identity(self, token: DRecordLifecycle, *, verified: bool) -> None:
        token.verify_identity(verified=verified)

    def removal_eligibility(self, token: DRecordLifecycle) -> bool:
        return token.sqlite_removal_eligible

    def health_snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "storage_mode": self.mode.value,
            "spool_bytes": self.spool._size(),
            "spool_open_segments": len(self.spool.recover_open()),
            "last_error": self._last_error,
            "last_complete_batch": self._last_complete_batch,
            **self._counters,
        }

    def wrap_repository(self, repository: Any) -> "ResearchRepositoryProxy":
        return ResearchRepositoryProxy(repository, self)


class ResearchRepositoryProxy:
    """Compatibility proxy that intercepts only Class C append operations."""

    def __init__(self, repository: Any, router: ResearchStorageRouter) -> None:
        self._repository = repository
        self._router = router

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repository, name)

    def record_market_scan_cycle(self, **kwargs: Any) -> dict[str, Any] | None:
        completed = kwargs.get("cycle_completed_at") or datetime.now().astimezone()
        cycle_identity = uuid.uuid4().hex
        payload = {
            "cycle_identity": cycle_identity,
            "cycle_started_at": kwargs.get("cycle_started_at"),
            "cycle_completed_at": completed,
            "exchange_symbols": kwargs.get("exchange_symbols", []),
            "eligible_symbols": kwargs.get("eligible_symbols", []),
            "excluded": kwargs.get("excluded", []),
            "scheduled_symbols": kwargs.get("scheduled_symbols", []),
            "candidate_symbols": kwargs.get("candidate_symbols", 0),
            "evaluated_symbols": kwargs.get("evaluated_symbols", 0),
            "last_error": kwargs.get("last_error"),
        }
        result = self._router.write(
            "market_scan_cycles",
            payload,
            identity=cycle_identity,
            observed_at_utc=completed,
        )
        for row in kwargs.get("symbol_results", []):
            symbol = str(row.get("symbol", "UNKNOWN"))
            self._router.write(
                "market_scan_symbol_results",
                {"cycle_identity": cycle_identity, **dict(row)},
                identity=f"{cycle_identity}:{symbol}",
                observed_at_utc=completed,
            )
        self._router.write(
            "market_coverage_ledger",
            {
                "cycle_identity": cycle_identity,
                "exchange_symbols": kwargs.get("exchange_symbols", []),
                "eligible_symbols": kwargs.get("eligible_symbols", []),
                "excluded": kwargs.get("excluded", []),
            },
            identity=f"coverage:{cycle_identity}",
            observed_at_utc=completed,
        )
        if self._router.mode is ResearchTelemetryStorageMode.SPOOL_CANONICAL:
            # The composite legacy repository method writes both Class C and D
            # rows in one transaction.  Canonical mode uses its operational-
            # only companion so Class C cannot leak back into SQLite.
            operational = getattr(
                self._repository, "record_market_scan_cycle_operational", None
            )
            if callable(operational):
                operational_kwargs = dict(kwargs)
                operational_kwargs.pop("excluded", None)
                return operational(**operational_kwargs)
            return {"cycle_id": cycle_identity, "rotation_id": "spool-only"}
        if result.sqlite_required:
            return self._repository.record_market_scan_cycle(**kwargs)
        return {"cycle_id": cycle_identity, "rotation_id": "spool-only"}

    def record_climax_monitor_event(self, **kwargs: Any) -> None:
        observed = kwargs.get("observed_at") or kwargs.get("created_at") or datetime.now().astimezone()
        identity = f"{kwargs.get('event_id', 'unknown')}:{kwargs.get('action', 'event')}:{observed.isoformat()}"
        result = self._router.write(
            "climax_monitor_events",
            dict(kwargs),
            identity=identity,
            observed_at_utc=observed,
        )
        if result.sqlite_required:
            self._repository.record_climax_monitor_event(**kwargs)

    def record_reject_stat(self, **kwargs: Any) -> None:
        observed = kwargs.get("logged_at") or datetime.now().astimezone()
        identity = f"{kwargs.get('symbol', 'unknown')}:{observed.isoformat()}:{kwargs.get('decision_type', '')}"
        result = self._router.write(
            "reject_stats",
            dict(kwargs),
            identity=identity,
            observed_at_utc=observed,
        )
        if result.sqlite_required:
            self._repository.record_reject_stat(**kwargs)
