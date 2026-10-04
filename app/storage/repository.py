"""DTO-oriented repository methods."""

from __future__ import annotations

import logging
import math
import os
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import selectinload

from app.domain import (
    EventState,
    EventStatus,
    SignalDecision,
    SignalOutcome,
    SignalProvenanceInput,
    SignalRecord,
    WatchCandidateRecord,
)
from app.infra.disk_capacity import is_sqlite_full
from app.market.coverage import (
    build_coverage_rows,
    coverage_percent,
    universe_fingerprint,
)
from app.observability.delivery_funnel import audit_delivery_funnel
from app.observability.strategy_observations import (
    ObservationWriteResult,
    ObservationWriteStatus,
    StrategyObservation,
    canonicalize_json_payload,
)
from app.storage.db import Database
from app.storage.models import (
    ClimaxEntryAttemptEventModel,
    ClimaxEntryAttemptModel,
    ClimaxEvaluationModel,
    ClimaxMonitorEventModel,
    ClimaxRootEventModel,
    CurrentRootOutcomeModel,
    CurrentRootPredicateSnapshotModel,
    EventStateModel,
    MarketCoverageLedgerModel,
    MarketScanCycleModel,
    MarketScanRotationModel,
    MarketScanSymbolResultModel,
    RejectStatModel,
    RootDetectorShadowCandidateModel,
    RootDetectorShadowEpisodeModel,
    RootDetectorShadowEpisodeOutcomeModel,
    RootDetectorShadowEpisodeRootLinkModel,
    RootDetectorShadowLegacyMappingModel,
    RootDetectorShadowObservationModel,
    RootDetectorShadowV2OutcomeModel,
    RootDetectorShadowV2RootModel,
    RuntimeHeartbeatHistoryModel,
    RuntimeHeartbeatModel,
    SignalModel,
    SignalOutcomeModel,
    SignalProvenanceModel,
    StrategyObservationModel,
    TelegramDeliveryOutboxModel,
    VolumeClimaxObservationModel,
    WatchCandidateModel,
)

logger = logging.getLogger(__name__)

_TERMINAL_ATTEMPT_STATES = {
    "SHADOW_ACTIONABLE",
    "EXPIRED",
    "INVALIDATED",
    "ROOT_REPLACED",
}


def _is_valid_lifecycle_attempt_values(
    *,
    attempt_created_at: datetime | None,
    confirmation_expires_at: datetime | None,
) -> bool:
    """Born-expired rows remain history, never valid lifecycle attempts."""
    if attempt_created_at is None or confirmation_expires_at is None:
        return True
    return confirmation_expires_at > attempt_created_at


class BotRepository:
    """Persist and restore event, signal, and analytics data."""

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def db_url(self) -> str:
        return self._db.db_url

    def set_runtime_metadata(
        self,
        *,
        runtime_instance_id: str,
        config_fingerprint: str,
        model_version: str = "climax-v1",
    ) -> None:
        """Set non-secret process metadata attached to subsequent telemetry rows."""
        self._runtime_instance_id = runtime_instance_id
        self._config_fingerprint = config_fingerprint
        self._model_version = model_version

    def check_storage_health(self) -> dict[str, int | str]:
        return self._db.write_heartbeat()

    def validate_storage_schema(
        self, *, include_shadow_v2: bool = True
    ) -> dict[str, object]:
        """Validate schema without issuing DDL or changing schema metadata."""
        from app.storage.migrations import assert_schema_current

        result = assert_schema_current(
            self._db.engine,
            include_shadow_v2=include_shadow_v2,
        )
        return result.as_dict()

    def sqlite_pragmas(self) -> dict[str, int | str] | None:
        return self._db.get_sqlite_pragmas()

    def record_market_scan_cycle(
        self,
        *,
        cycle_started_at: datetime,
        cycle_completed_at: datetime,
        exchange_symbols: list[str],
        eligible_symbols: list[str],
        excluded: list[tuple[str, str]],
        scheduled_symbols: list[str],
        symbol_results: list[dict[str, Any]],
        candidate_symbols: int = 0,
        evaluated_symbols: int = 0,
        last_error: str | None = None,
    ) -> dict[str, Any] | None:
        """Persist bounded coverage telemetry; never raises into scanner runtime."""
        try:
            runtime_id = getattr(self, "_runtime_instance_id", "unknown")
            model_version = getattr(self, "_model_version", "unknown")
            config_fingerprint = getattr(self, "_config_fingerprint", "unknown")
            exchange = sorted({str(s).upper() for s in exchange_symbols})
            eligible = sorted({str(s).upper() for s in eligible_symbols})
            eligible_set = set(eligible)
            exchange_fp = universe_fingerprint(exchange)
            eligible_fp = universe_fingerprint(eligible)
            now = cycle_completed_at
            with self._db.session() as session:
                rotation = session.scalars(
                    select(MarketScanRotationModel)
                    .where(MarketScanRotationModel.status == "OPEN")
                    .order_by(MarketScanRotationModel.rotation_started_at.desc())
                ).first()
                if (
                    rotation is None
                    or rotation.runtime_instance_id != runtime_id
                    or rotation.eligible_universe_fingerprint != eligible_fp
                    or rotation.exchange_universe_fingerprint != exchange_fp
                ):
                    if rotation is not None:
                        rotation.status = (
                            "ABORTED_RESTART"
                            if rotation.runtime_instance_id != runtime_id
                            else "INCOMPLETE"
                        )
                        rotation.rotation_completed_at = now
                        rotation.last_error = "universe_fingerprint_changed"
                    rotation = MarketScanRotationModel(
                        rotation_id=uuid.uuid4().hex,
                        runtime_instance_id=runtime_id,
                        model_version=model_version,
                        config_fingerprint=config_fingerprint,
                        rotation_started_at=cycle_started_at,
                        status="OPEN",
                        exchange_universe_size=len(exchange),
                        eligible_universe_size=len(eligible),
                        exchange_universe_fingerprint=exchange_fp,
                        eligible_universe_fingerprint=eligible_fp,
                        details_json={
                            "excluded_reason_codes": {},
                            "rotation_exchange_symbols": exchange,
                            "rotation_eligible_symbols": eligible,
                        },
                    )
                    session.add(rotation)
                    session.flush()
                cycle_sequence = (
                    session.scalar(
                        select(func.max(MarketScanCycleModel.cycle_sequence))
                    )
                    or 0
                ) + 1
                cycle_id = uuid.uuid4().hex
                scheduled = sorted(
                    {
                        str(s).upper()
                        for s in scheduled_symbols
                        if str(s).upper() in eligible_set
                    }
                )
                result_map = {str(row["symbol"]).upper(): row for row in symbol_results}
                for symbol, reason in excluded:
                    symbol = str(symbol).upper()
                    if symbol in eligible_set:
                        continue
                    if (
                        session.scalar(
                            select(MarketScanSymbolResultModel.id).where(
                                MarketScanSymbolResultModel.rotation_id
                                == rotation.rotation_id,
                                MarketScanSymbolResultModel.symbol == symbol,
                            )
                        )
                        is None
                    ):
                        session.add(
                            MarketScanSymbolResultModel(
                                rotation_id=rotation.rotation_id,
                                symbol=symbol,
                                terminal_status="EXCLUDED",
                                reason_code=reason,
                                completed_at=now,
                                runtime_instance_id=runtime_id,
                                details_json={},
                            )
                        )
                for coverage in build_coverage_rows(
                    rotation_id=rotation.rotation_id,
                    observed_at=now,
                    exchange_symbols=exchange,
                    eligible_symbols=eligible,
                    excluded=excluded,
                    scheduled_symbols=scheduled,
                    symbol_results=symbol_results,
                ):
                    session.add(MarketCoverageLedgerModel(**coverage))
                for symbol in scheduled:
                    row = result_map.get(
                        symbol,
                        {
                            "terminal_status": "SCAN_SKIPPED",
                            "reason_code": "NOT_SCHEDULED_IN_BATCH",
                        },
                    )
                    existing = session.scalar(
                        select(MarketScanSymbolResultModel.id).where(
                            MarketScanSymbolResultModel.rotation_id
                            == rotation.rotation_id,
                            MarketScanSymbolResultModel.symbol == symbol,
                        )
                    )
                    if existing is None:
                        session.add(
                            MarketScanSymbolResultModel(
                                rotation_id=rotation.rotation_id,
                                cycle_id=cycle_id,
                                symbol=symbol,
                                terminal_status=row.get(
                                    "terminal_status", "SCAN_SKIPPED"
                                ),
                                reason_code=row.get("reason_code", "UNKNOWN"),
                                scheduled_at=cycle_started_at,
                                completed_at=now,
                                duration_ms=row.get("duration_ms"),
                                runtime_instance_id=runtime_id,
                                details_json=row.get("details", {}),
                            )
                        )
                session.add(
                    MarketScanCycleModel(
                        cycle_id=cycle_id,
                        rotation_id=rotation.rotation_id,
                        runtime_instance_id=runtime_id,
                        cycle_sequence=cycle_sequence,
                        batch_index=cycle_sequence,
                        batch_count_expected=max(
                            1,
                            (len(eligible) + max(1, len(scheduled)) - 1)
                            // max(1, len(scheduled)),
                        ),
                        cycle_started_at=cycle_started_at,
                        cycle_completed_at=now,
                        status="COMPLETED" if not last_error else "PARTIAL",
                        exchange_universe_size=len(exchange),
                        eligible_universe_size=len(eligible),
                        scheduled_symbols=len(scheduled),
                        scanned_ok_symbols=sum(
                            1
                            for s in scheduled
                            if result_map.get(s, {}).get("terminal_status")
                            == "SCANNED_OK"
                        ),
                        failed_symbols=sum(
                            1
                            for s in scheduled
                            if result_map.get(s, {}).get("terminal_status")
                            == "SCAN_FAILED"
                        ),
                        skipped_symbols=sum(
                            1
                            for s in scheduled
                            if result_map.get(s, {}).get("terminal_status")
                            == "SCAN_SKIPPED"
                        ),
                        candidate_symbols=candidate_symbols,
                        evaluated_symbols=evaluated_symbols,
                        scheduled_fingerprint=universe_fingerprint(scheduled),
                        scanned_ok_fingerprint=universe_fingerprint(
                            [
                                s
                                for s in scheduled
                                if result_map.get(s, {}).get("terminal_status")
                                == "SCANNED_OK"
                            ]
                        ),
                        duration_ms=max(
                            0, int((now - cycle_started_at).total_seconds() * 1000)
                        ),
                        last_error=last_error,
                        details_json={
                            "historical_100_100_semantics": "scheduled_batch"
                        },
                    )
                )
                session.flush()
                result_rows = session.scalars(
                    select(MarketScanSymbolResultModel).where(
                        MarketScanSymbolResultModel.rotation_id == rotation.rotation_id
                    )
                ).all()
                covered = {
                    row.symbol
                    for row in result_rows
                    if row.terminal_status
                    in {"SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
                    and row.symbol in eligible_set
                }
                ok = {
                    row.symbol
                    for row in result_rows
                    if row.terminal_status == "SCANNED_OK"
                    and row.symbol in eligible_set
                }
                failed = {
                    row.symbol
                    for row in result_rows
                    if row.terminal_status == "SCAN_FAILED"
                    and row.symbol in eligible_set
                }
                skipped = {
                    row.symbol
                    for row in result_rows
                    if row.terminal_status == "SCAN_SKIPPED"
                    and row.symbol in eligible_set
                }
                scheduled_all = {
                    row.symbol for row in result_rows if row.symbol in eligible_set
                }
                excluded_count = sum(
                    1 for row in result_rows if row.terminal_status == "EXCLUDED"
                )
                reason_counts: dict[str, int] = {}
                for row in result_rows:
                    reason_counts[row.reason_code] = (
                        reason_counts.get(row.reason_code, 0) + 1
                    )
                rotation.exchange_universe_size = len(exchange)
                rotation.eligible_universe_size = len(eligible)
                rotation.scheduled_unique_symbols = len(scheduled_all)
                rotation.scanned_ok_unique_symbols = len(ok)
                rotation.failed_unique_symbols = len(failed)
                rotation.skipped_unique_symbols = len(skipped)
                rotation.excluded_symbols = excluded_count
                rotation.scheduled_universe_fingerprint = universe_fingerprint(
                    scheduled_all
                )
                rotation.scanned_ok_universe_fingerprint = universe_fingerprint(ok)
                rotation.eligible_coverage_pct = coverage_percent(
                    len(covered), len(eligible)
                )
                rotation.exchange_coverage_pct = coverage_percent(
                    len(covered) + excluded_count, len(exchange)
                )
                rotation.batch_count = (
                    session.scalar(
                        select(func.count(MarketScanCycleModel.cycle_id)).where(
                            MarketScanCycleModel.rotation_id == rotation.rotation_id
                        )
                    )
                    or 0
                )
                rotation.last_batch_sequence = cycle_sequence
                rotation.last_error = last_error
                rotation.details_json = {
                    "reason_counts": reason_counts,
                    "failed_symbols": sorted(failed),
                    "skipped_symbols": sorted(skipped),
                    "rotation_exchange_symbols": exchange,
                    "rotation_eligible_symbols": eligible,
                }
                if (
                    len(covered) == len(eligible)
                    and len(scheduled_all) == len(eligible)
                    and len(covered) <= len(eligible)
                ):
                    rotation.status = "COMPLETED"
                    rotation.rotation_completed_at = now
                session.flush()
                return {
                    "rotation_id": rotation.rotation_id,
                    "cycle_id": cycle_id,
                    "status": rotation.status,
                    "eligible_coverage_pct": rotation.eligible_coverage_pct,
                    "exchange_coverage_pct": rotation.exchange_coverage_pct,
                    "eligible_universe_size": len(eligible),
                    "scheduled_unique_symbols": len(scheduled_all),
                    "scanned_ok_unique_symbols": len(ok),
                    "failed_unique_symbols": len(failed),
                    "skipped_unique_symbols": len(skipped),
                    "reason_counts": reason_counts,
                    "failed_symbols": sorted(failed),
                    "excluded_symbols": excluded_count,
                }
        except Exception:
            logger.exception("market coverage telemetry write failed")
            return None

    def prepare_market_scan_rotation(
        self,
        *,
        rotation_started_at: datetime,
        exchange_symbols: list[str],
        eligible_symbols: list[str],
    ) -> str | None:
        """Open or reuse the exact rotation that the current scan will record."""
        try:
            runtime_id = getattr(self, "_runtime_instance_id", "unknown")
            model_version = getattr(self, "_model_version", "unknown")
            config_fingerprint = getattr(self, "_config_fingerprint", "unknown")
            exchange = sorted({str(s).upper() for s in exchange_symbols})
            eligible = sorted({str(s).upper() for s in eligible_symbols})
            exchange_fp = universe_fingerprint(exchange)
            eligible_fp = universe_fingerprint(eligible)
            with self._db.session() as session:
                rotation = session.scalars(
                    select(MarketScanRotationModel)
                    .where(MarketScanRotationModel.status == "OPEN")
                    .order_by(MarketScanRotationModel.rotation_started_at.desc())
                ).first()
                if (
                    rotation is not None
                    and rotation.runtime_instance_id == runtime_id
                ):
                    return rotation.rotation_id
                if rotation is not None:
                    rotation.status = (
                        "ABORTED_RESTART"
                        if rotation.runtime_instance_id != runtime_id
                        else "INCOMPLETE"
                    )
                    rotation.rotation_completed_at = rotation_started_at
                rotation = MarketScanRotationModel(
                    rotation_id=uuid.uuid4().hex,
                    runtime_instance_id=runtime_id,
                    model_version=model_version,
                    config_fingerprint=config_fingerprint,
                    rotation_started_at=rotation_started_at,
                    status="OPEN",
                    exchange_universe_size=len(exchange),
                    eligible_universe_size=len(eligible),
                    exchange_universe_fingerprint=exchange_fp,
                    eligible_universe_fingerprint=eligible_fp,
                    details_json={
                        "excluded_reason_codes": {},
                        "rotation_exchange_symbols": exchange,
                        "rotation_eligible_symbols": eligible,
                    },
                )
                session.add(rotation)
                session.flush()
                return rotation.rotation_id
        except Exception:
            logger.exception("market coverage rotation preparation failed")
            return None

    def rotation_scheduled_symbols(self, rotation_id: str) -> set[str] | None:
        """Return eligible symbols already assigned a terminal result in a rotation."""
        try:
            with self._db.session() as session:
                rows = session.scalars(
                    select(MarketScanSymbolResultModel.symbol).where(
                        MarketScanSymbolResultModel.rotation_id == rotation_id,
                        MarketScanSymbolResultModel.terminal_status.in_(
                            {
                                "EXCLUDED",
                                "SCANNED_OK",
                                "SCAN_FAILED",
                                "SCAN_SKIPPED",
                            }
                        ),
                    )
                ).all()
                return {str(symbol).upper() for symbol in rows}
        except Exception:
            logger.exception("market coverage rotation state read failed")
            return None

    def rotation_universe(
        self, rotation_id: str
    ) -> tuple[list[str], list[str]] | None:
        """Return the immutable exchange/eligible snapshot for an open rotation."""
        try:
            with self._db.session() as session:
                rotation = session.get(MarketScanRotationModel, rotation_id)
                if rotation is None:
                    return None
                details = rotation.details_json or {}
                exchange = details.get("rotation_exchange_symbols")
                eligible = details.get("rotation_eligible_symbols")
                if not isinstance(exchange, list) or not isinstance(eligible, list):
                    return None
                return (
                    sorted({str(symbol).upper() for symbol in exchange if symbol}),
                    sorted({str(symbol).upper() for symbol in eligible if symbol}),
                )
        except Exception:
            logger.exception("market coverage rotation universe read failed")
            return None

    def record_market_scan_cycle_operational(
        self,
        *,
        cycle_started_at: datetime,
        cycle_completed_at: datetime,
        exchange_symbols: list[str],
        eligible_symbols: list[str],
        scheduled_symbols: list[str],
        symbol_results: list[dict[str, Any]],
        candidate_symbols: int = 0,
        evaluated_symbols: int = 0,
        last_error: str | None = None,
    ) -> dict[str, Any] | None:
        """Persist only the operational scan-cycle row.

        Class C coverage and symbol-result rows are routed by the research
        storage proxy; this method intentionally touches only Class D.
        """

        try:
            runtime_id = getattr(self, "_runtime_instance_id", "unknown")
            exchange = sorted({str(s).upper() for s in exchange_symbols})
            eligible = sorted({str(s).upper() for s in eligible_symbols})
            scheduled = sorted(
                {
                    str(s).upper()
                    for s in scheduled_symbols
                    if str(s).upper() in set(eligible)
                }
            )
            result_map = {str(row["symbol"]).upper(): row for row in symbol_results}
            now = cycle_completed_at
            with self._db.session() as session:
                rotation = session.scalars(
                    select(MarketScanRotationModel)
                    .where(MarketScanRotationModel.runtime_instance_id == runtime_id)
                    .order_by(MarketScanRotationModel.rotation_started_at.desc())
                ).first()
                if rotation is None:
                    return None
                cycle_sequence = (
                    session.scalar(select(func.max(MarketScanCycleModel.cycle_sequence)))
                    or 0
                ) + 1
                cycle_id = uuid.uuid4().hex
                session.add(
                    MarketScanCycleModel(
                        cycle_id=cycle_id,
                        rotation_id=rotation.rotation_id,
                        runtime_instance_id=runtime_id,
                        cycle_sequence=cycle_sequence,
                        batch_index=cycle_sequence,
                        batch_count_expected=max(
                            1,
                            (len(eligible) + max(1, len(scheduled)) - 1)
                            // max(1, len(scheduled)),
                        ),
                        cycle_started_at=cycle_started_at,
                        cycle_completed_at=now,
                        status="COMPLETED" if not last_error else "PARTIAL",
                        exchange_universe_size=len(exchange),
                        eligible_universe_size=len(eligible),
                        scheduled_symbols=len(scheduled),
                        scanned_ok_symbols=sum(
                            1
                            for symbol in scheduled
                            if result_map.get(symbol, {}).get("terminal_status")
                            == "SCANNED_OK"
                        ),
                        failed_symbols=sum(
                            1
                            for symbol in scheduled
                            if result_map.get(symbol, {}).get("terminal_status")
                            == "SCAN_FAILED"
                        ),
                        skipped_symbols=sum(
                            1
                            for symbol in scheduled
                            if result_map.get(symbol, {}).get("terminal_status")
                            == "SCAN_SKIPPED"
                        ),
                        candidate_symbols=candidate_symbols,
                        evaluated_symbols=evaluated_symbols,
                        scheduled_fingerprint=universe_fingerprint(scheduled),
                        scanned_ok_fingerprint=universe_fingerprint(
                            [
                                symbol
                                for symbol in scheduled
                                if result_map.get(symbol, {}).get("terminal_status")
                                == "SCANNED_OK"
                            ]
                        ),
                        duration_ms=max(
                            0, int((now - cycle_started_at).total_seconds() * 1000)
                        ),
                        last_error=last_error,
                        details_json={"historical_100_100_semantics": "scheduled_batch"},
                    )
                )
                session.flush()
                return {"rotation_id": rotation.rotation_id, "cycle_id": cycle_id}
        except Exception:
            logger.exception("operational market scan cycle write failed")
            return None

    def record_root_detector_shadow_candidate(self, candidate: dict[str, Any]) -> bool:
        """Persist bounded shadow telemetry without allowing failures into live flow."""
        try:
            with self._db.session() as session:
                if (
                    session.scalar(
                        select(RootDetectorShadowCandidateModel.id).where(
                            RootDetectorShadowCandidateModel.candidate_id
                            == candidate["candidate_id"]
                        )
                    )
                    is not None
                ):
                    return False
                session.add(RootDetectorShadowCandidateModel(**candidate))
            return True
        except Exception:
            logger.exception("root detector shadow telemetry write failed")
            return False

    def record_root_detector_shadow_v2_root(self, root: dict[str, Any]) -> bool:
        """Insert one immutable V2 root and its independent outcome row."""
        try:
            with self._db.session() as session:
                if (
                    session.get(
                        RootDetectorShadowV2RootModel, root["shadow_v2_root_id"]
                    )
                    is not None
                ):
                    return False
                session.add(RootDetectorShadowV2RootModel(**root))
                session.add(
                    RootDetectorShadowV2OutcomeModel(
                        shadow_v2_root_id=root["shadow_v2_root_id"],
                        episode_id=root["episode_id"],
                        outcome_status="CENSORED",
                        outcome_next_due_at=_ensure_utc(root["created_at"])
                        + timedelta(minutes=15),
                        outcome_attempt_count=0,
                    )
                )
            return True
        except Exception:
            logger.exception("root detector shadow V2 telemetry write failed")
            return False

    def link_root_detector_shadow_v2_live_root(
        self, *, episode_id: str, root_event_id: str, live_root_created_at: datetime
    ) -> bool:
        try:
            with self._db.session() as session:
                row = session.scalar(
                    select(RootDetectorShadowV2RootModel).where(
                        RootDetectorShadowV2RootModel.episode_id == episode_id,
                        RootDetectorShadowV2RootModel.root_type
                        == "FIRST_V2_EARLY_ROOT",
                    )
                )
                if row is None:
                    return False
                created = _ensure_utc(live_root_created_at)
                row.live_root_event_id = root_event_id
                row.live_root_created_at = created
                row.v2_to_live_root_latency_seconds = max(
                    0.0, (created - _ensure_utc(row.created_at)).total_seconds()
                )
                row.state = "LIVE_ROOT_SEEN"
                live_outcome = session.get(CurrentRootOutcomeModel, root_event_id)
                if live_outcome is not None:
                    live_outcome.episode_id = episode_id
            return True
        except Exception:
            logger.exception("root detector shadow V2 live-root linkage failed")
            return False

    def list_root_detector_shadow_v2_outcomes_due(
        self, *, now: datetime, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._db.session() as session:
            rows = session.execute(
                select(RootDetectorShadowV2OutcomeModel, RootDetectorShadowV2RootModel)
                .join(
                    RootDetectorShadowV2RootModel,
                    RootDetectorShadowV2RootModel.shadow_v2_root_id
                    == RootDetectorShadowV2OutcomeModel.shadow_v2_root_id,
                )
                .where(
                    RootDetectorShadowV2OutcomeModel.outcome_next_due_at.is_not(None),
                    RootDetectorShadowV2OutcomeModel.outcome_next_due_at <= now,
                    RootDetectorShadowV2OutcomeModel.outcome_status != "MATURE",
                )
                .order_by(RootDetectorShadowV2OutcomeModel.outcome_next_due_at)
                .limit(limit)
            ).all()
            return [
                {
                    "shadow_v2_root_id": outcome.shadow_v2_root_id,
                    "episode_id": outcome.episode_id,
                    "symbol": root.symbol,
                    "anchor_time": root.created_at,
                    "anchor_price": root.reference_price,
                    "event_high": root.event_high_at_decision,
                    "next_due_at": outcome.outcome_next_due_at,
                    "outcome_attempt_count": outcome.outcome_attempt_count,
                }
                for outcome, root in rows
            ]

    def record_root_detector_shadow_v2_outcome_attempt(
        self,
        shadow_v2_root_id: str,
        *,
        outcome: dict[str, Any] | None = None,
        error: str | None = None,
        next_due_at: datetime | None = None,
        computed_at: datetime | None = None,
    ) -> bool:
        safe_outcome = (
            canonicalize_json_payload(outcome) if outcome is not None else None
        )
        try:
            now = _ensure_utc(computed_at or datetime.now(timezone.utc))
            with self._db.session() as session:
                row = session.get(RootDetectorShadowV2OutcomeModel, shadow_v2_root_id)
                if row is None:
                    return False
                row.outcome_last_attempt_at = now
                row.outcome_attempt_count = int(row.outcome_attempt_count or 0) + 1
                row.outcome_next_due_at = next_due_at
                row.outcome_last_error = str(error)[:255] if error else None
                row.outcome_updated_at = now
                if safe_outcome is not None:
                    row.outcome_status = str(
                        safe_outcome.get("outcome_status") or row.outcome_status
                    )
                    row.outcome_json = safe_outcome
                    row.outcome_code_version = safe_outcome.get("outcome_code_version")
                    row.outcome_method_version = safe_outcome.get(
                        "outcome_method_version"
                    )
                    row.outcome_computed_at = now
            return True
        except Exception:
            logger.exception("root detector shadow V2 outcome write failed")
            return False

    def record_root_detector_shadow_episode_observation(
        self, candidate: dict[str, Any]
    ) -> str | None:
        """Open/continue a 30-minute gap episode and append one observation."""
        try:
            observed_at = _ensure_utc(candidate["first_seen_at"])
            symbol = str(candidate["symbol"]).upper()
            with self._db.session() as session:
                episode = session.scalars(
                    select(RootDetectorShadowEpisodeModel)
                    .where(
                        RootDetectorShadowEpisodeModel.symbol == symbol,
                        RootDetectorShadowEpisodeModel.episode_status == "OPEN",
                    )
                    .order_by(RootDetectorShadowEpisodeModel.last_trigger_at.desc())
                ).first()
                if episode is None or observed_at - _ensure_utc(
                    episode.last_trigger_at
                ) > timedelta(minutes=30):
                    if episode is not None:
                        episode.episode_status = "CLOSED"
                        episode.closed_at = observed_at
                    episode_id = uuid.uuid4().hex
                    episode = RootDetectorShadowEpisodeModel(
                        episode_id=episode_id,
                        symbol=symbol,
                        opened_at=observed_at,
                        last_trigger_at=observed_at,
                        last_high=candidate.get("event_high"),
                        episode_status="OPEN",
                        episode_mapping_version="ROOT_EPISODE_MAP_V1",
                        code_version=candidate["code_version"],
                        runtime_instance_id=candidate["runtime_instance_id"],
                        runtime_started_at=candidate["runtime_started_at"],
                    )
                    session.add(episode)
                else:
                    episode_id = episode.episode_id
                    episode.last_trigger_at = observed_at
                    episode.last_high = candidate.get("event_high")
                session.add(
                    RootDetectorShadowObservationModel(
                        episode_id=episode_id,
                        legacy_candidate_id=candidate.get("candidate_id"),
                        symbol=symbol,
                        observed_at=observed_at,
                        price=candidate["price"],
                        event_high=candidate.get("event_high"),
                        high_time=candidate.get("high_time"),
                        pump_5m=candidate.get("pump_5m"),
                        pump_15m=candidate.get("pump_15m"),
                        pump_1h=candidate.get("pump_1h"),
                        pump_4h=candidate.get("pump_4h"),
                        volume_ratio=candidate.get("volume_ratio"),
                        volume_z=candidate.get("volume_z"),
                        oi_5m=candidate.get("oi_5m"),
                        oi_15m=candidate.get("oi_15m"),
                        oi_1h=candidate.get("oi_1h"),
                        distance_from_high=candidate.get("distance_from_high"),
                        conditions_json=candidate.get("conditions_json") or {},
                        code_version=candidate["code_version"],
                        runtime_instance_id=candidate["runtime_instance_id"],
                        runtime_started_at=candidate["runtime_started_at"],
                    )
                )
                outcome = session.get(RootDetectorShadowEpisodeOutcomeModel, episode_id)
                if outcome is None:
                    session.add(
                        RootDetectorShadowEpisodeOutcomeModel(
                            episode_id=episode_id,
                            outcome_status="CENSORED",
                            outcome_next_due_at=observed_at + timedelta(minutes=15),
                            outcome_attempt_count=0,
                        )
                    )
            return episode_id
        except Exception:
            logger.exception("root detector shadow episode telemetry write failed")
            return None

    def link_root_detector_shadow_episode(
        self,
        episode_id: str,
        *,
        root_event_id: str,
        linked_at: datetime,
        link_type: str = "FIRST_ROOT",
        event_revision: int | None = None,
    ) -> bool:
        try:
            with self._db.session() as session:
                exists = session.scalar(
                    select(RootDetectorShadowEpisodeRootLinkModel.id).where(
                        RootDetectorShadowEpisodeRootLinkModel.episode_id == episode_id,
                        RootDetectorShadowEpisodeRootLinkModel.root_event_id
                        == root_event_id,
                        RootDetectorShadowEpisodeRootLinkModel.link_type == link_type,
                    )
                )
                if exists is None:
                    session.add(
                        RootDetectorShadowEpisodeRootLinkModel(
                            episode_id=episode_id,
                            root_event_id=root_event_id,
                            linked_at=linked_at,
                            link_type=link_type,
                            event_revision=event_revision,
                        )
                    )
            return True
        except Exception:
            logger.exception("root detector shadow episode link failed")
            return False

    def map_legacy_root_detector_shadow_candidates(
        self, *, mapping_version: str = "ROOT_EPISODE_MAP_V1"
    ) -> int:
        """Create deterministic legacy mappings without modifying raw candidate rows."""
        try:
            with self._db.session() as session:
                candidates = session.scalars(
                    select(RootDetectorShadowCandidateModel).order_by(
                        RootDetectorShadowCandidateModel.symbol,
                        RootDetectorShadowCandidateModel.first_seen_at,
                        RootDetectorShadowCandidateModel.id,
                    )
                ).all()
                active: dict[str, tuple[str, datetime]] = {}
                created = 0
                for candidate in candidates:
                    existing = session.get(
                        RootDetectorShadowLegacyMappingModel, candidate.candidate_id
                    )
                    if existing is not None:
                        active[candidate.symbol] = (
                            existing.episode_id,
                            _ensure_utc(candidate.first_seen_at),
                        )
                        continue
                    last = active.get(candidate.symbol)
                    if last is None or _ensure_utc(candidate.first_seen_at) - last[
                        1
                    ] > timedelta(minutes=30):
                        episode_id = uuid.uuid4().hex
                        active[candidate.symbol] = (
                            episode_id,
                            _ensure_utc(candidate.first_seen_at),
                        )
                        session.add(
                            RootDetectorShadowEpisodeModel(
                                episode_id=episode_id,
                                symbol=candidate.symbol,
                                opened_at=candidate.first_seen_at,
                                last_trigger_at=candidate.first_seen_at,
                                last_high=candidate.event_high,
                                episode_status="CLOSED",
                                closed_at=candidate.first_seen_at,
                                episode_mapping_version=mapping_version,
                                code_version=candidate.code_version,
                                runtime_instance_id=candidate.runtime_instance_id,
                                runtime_started_at=candidate.runtime_started_at,
                            )
                        )
                    else:
                        episode_id = last[0]
                        active[candidate.symbol] = (
                            episode_id,
                            _ensure_utc(candidate.first_seen_at),
                        )
                    session.add(
                        RootDetectorShadowLegacyMappingModel(
                            legacy_candidate_id=candidate.candidate_id,
                            episode_id=episode_id,
                            episode_mapping_version=mapping_version,
                            mapping_reason="GAP_ONLY_30M_LEGACY_ORDERED",
                        )
                    )
                    created += 1
                return created
        except Exception:
            logger.exception("root detector shadow legacy mapping failed")
            return 0

    def list_root_detector_shadow_episode_outcomes_due(
        self, *, due_at: datetime, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._db.session() as session:
            rows = session.scalars(
                select(RootDetectorShadowEpisodeOutcomeModel)
                .where(
                    RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at.is_not(
                        None
                    ),
                    RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at <= due_at,
                )
                .order_by(RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at)
                .limit(limit)
            ).all()
            return [
                {
                    "episode_id": row.episode_id,
                    "outcome_status": row.outcome_status,
                    "outcome_next_due_at": row.outcome_next_due_at,
                }
                for row in rows
            ]

    def list_shadow_episode_outcomes_due(
        self,
        *,
        now: datetime,
        runtime_limit: int,
        legacy_limit: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return disjoint runtime and legacy due batches with runtime priority."""
        with self._db.session() as session:
            rows = session.execute(
                select(
                    RootDetectorShadowEpisodeOutcomeModel,
                    RootDetectorShadowEpisodeModel,
                )
                .join(
                    RootDetectorShadowEpisodeModel,
                    RootDetectorShadowEpisodeModel.episode_id
                    == RootDetectorShadowEpisodeOutcomeModel.episode_id,
                )
                .where(
                    RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at.is_not(
                        None
                    ),
                    RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at <= now,
                    RootDetectorShadowEpisodeOutcomeModel.outcome_status != "MATURE",
                )
                .order_by(
                    RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at,
                    RootDetectorShadowEpisodeOutcomeModel.episode_id,
                )
            ).all()
            batches = {"runtime": [], "legacy": []}
            for outcome, episode in rows:
                observation = session.scalars(
                    select(RootDetectorShadowObservationModel)
                    .where(
                        RootDetectorShadowObservationModel.episode_id
                        == episode.episode_id
                    )
                    .order_by(
                        RootDetectorShadowObservationModel.observed_at,
                        RootDetectorShadowObservationModel.observation_id,
                    )
                ).first()
                if observation is None:
                    continue
                cohort = (
                    "legacy"
                    if session.scalar(
                        select(RootDetectorShadowLegacyMappingModel.legacy_candidate_id)
                        .where(
                            RootDetectorShadowLegacyMappingModel.episode_id
                            == episode.episode_id
                        )
                        .limit(1)
                    )
                    is not None
                    else "runtime"
                )
                if len(batches[cohort]) >= (
                    legacy_limit if cohort == "legacy" else runtime_limit
                ):
                    continue
                batches[cohort].append(
                    {
                        "episode_id": episode.episode_id,
                        "symbol": episode.symbol,
                        "anchor_time": episode.opened_at,
                        "anchor_price": observation.price,
                        "event_high": observation.event_high or episode.last_high,
                        "next_due_at": outcome.outcome_next_due_at,
                        "attempt_count": outcome.outcome_attempt_count,
                        "outcome_status": outcome.outcome_status,
                    }
                )
            return batches

    def count_shadow_episode_outcomes_due(self, *, now: datetime) -> int:
        with self._db.session() as session:
            return int(
                session.scalar(
                    select(func.count())
                    .select_from(RootDetectorShadowEpisodeOutcomeModel)
                    .where(
                        RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at.is_not(
                            None
                        ),
                        RootDetectorShadowEpisodeOutcomeModel.outcome_next_due_at
                        <= now,
                        RootDetectorShadowEpisodeOutcomeModel.outcome_status
                        != "MATURE",
                    )
                )
                or 0
            )

    def estimate_shadow_episode_rate(
        self, *, now: datetime, window_hours: int = 24
    ) -> float:
        """Estimate new runtime episodes/hour for bounded capacity sizing."""
        cutoff = now - timedelta(hours=max(1, window_hours))
        with self._db.session() as session:
            count = (
                session.scalar(
                    select(func.count())
                    .select_from(RootDetectorShadowEpisodeModel)
                    .where(
                        RootDetectorShadowEpisodeModel.opened_at >= cutoff,
                        RootDetectorShadowEpisodeModel.opened_at <= now,
                        ~select(RootDetectorShadowLegacyMappingModel.episode_id)
                        .where(
                            RootDetectorShadowLegacyMappingModel.episode_id
                            == RootDetectorShadowEpisodeModel.episode_id
                        )
                        .exists(),
                    )
                )
                or 0
            )
        return float(count) / max(1, window_hours)

    def record_root_detector_shadow_episode_outcome_attempt(
        self,
        episode_id: str,
        *,
        outcome: dict[str, Any] | None = None,
        error: str | None = None,
        next_due_at: datetime | None = None,
        computed_at: datetime | None = None,
    ) -> bool:
        try:
            now = _ensure_utc(computed_at or datetime.now(timezone.utc))
            with self._db.session() as session:
                row = session.get(RootDetectorShadowEpisodeOutcomeModel, episode_id)
                if row is None:
                    return False
                row.outcome_last_attempt_at = now
                row.outcome_attempt_count = int(row.outcome_attempt_count or 0) + 1
                row.outcome_next_due_at = next_due_at
                row.outcome_updated_at = now
                row.outcome_last_error = str(error)[:255] if error else None
                if outcome:
                    row.outcome_status = outcome.get(
                        "outcome_status", row.outcome_status
                    )
                    row.outcome_computed_at = now
                    row.outcome_code_version = outcome.get(
                        "outcome_code_version", row.outcome_code_version
                    )
                    row.outcome_method_version = outcome.get(
                        "outcome_method_version", "ROOT_SHADOW_OUTCOME_V2"
                    )
                    for label, field in (
                        ("15m", "return_15m"),
                        ("30m", "return_30m"),
                        ("1h", "return_1h"),
                        ("4h", "return_4h"),
                        ("12h", "return_12h"),
                        ("24h", "return_24h"),
                        ("48h", "return_48h"),
                    ):
                        value = (outcome.get("horizons", {}).get(label) or {}).get(
                            "short_return_pct"
                        )
                        if value is not None and getattr(row, field) is None:
                            setattr(row, field, value)
                    for label, field in (
                        ("1h", "mfe_1h"),
                        ("4h", "mfe_4h"),
                        ("12h", "mfe_12h"),
                        ("24h", "mfe_24h"),
                    ):
                        value = (outcome.get("mfe_by_horizon") or {}).get(label)
                        if value is not None and getattr(row, field) is None:
                            setattr(row, field, value)
                    for label, field in (
                        ("1h", "mae_1h"),
                        ("4h", "mae_4h"),
                        ("12h", "mae_12h"),
                        ("24h", "mae_24h"),
                    ):
                        value = (outcome.get("mae_by_horizon") or {}).get(label)
                        if value is not None and getattr(row, field) is None:
                            setattr(row, field, value)
                    row.new_high_after_episode = outcome.get(
                        "new_high_after_candidate", row.new_high_after_episode
                    )
                    row.coverage_end_at = outcome.get(
                        "coverage_end_at", row.coverage_end_at
                    )
            return True
        except Exception:
            logger.exception("root detector shadow episode outcome write failed")
            return False

    def append_root_detector_shadow_observation(
        self, candidate_id: str, observation: dict[str, Any]
    ) -> bool:
        try:
            with self._db.session() as session:
                row = session.scalar(
                    select(RootDetectorShadowCandidateModel).where(
                        RootDetectorShadowCandidateModel.candidate_id == candidate_id
                    )
                )
                if row is None:
                    return False
                rows = list(row.observations_json or [])
                if len(rows) < 96:
                    rows.append(_json_ready(observation))
                    row.observations_json = rows
            return True
        except Exception:
            logger.exception("root detector shadow observation write failed")
            return False

    def mature_root_detector_shadow_candidate(
        self, candidate_id: str, outcome: dict[str, Any]
    ) -> bool:
        try:
            with self._db.session() as session:
                row = session.scalar(
                    select(RootDetectorShadowCandidateModel).where(
                        RootDetectorShadowCandidateModel.candidate_id == candidate_id
                    )
                )
                if row is None:
                    return False
                row.outcome_json = _json_ready(outcome)
                row.outcome_status = outcome.get("outcome_status")
                row.outcome_mfe_pct = outcome.get("mfe_pct")
                row.outcome_mae_pct = outcome.get("mae_pct")
                row.new_high_after_candidate = outcome.get("new_high_after_candidate")
                row.coverage_end_at = outcome.get("coverage_end_at")
            return True
        except Exception:
            logger.exception("root detector shadow outcome write failed")
            return False

    def link_root_detector_shadow_candidate(
        self,
        candidate_id: str,
        *,
        root_event_id: str,
        root_created_at: datetime,
        peak_time: datetime | None = None,
    ) -> bool:
        try:
            with self._db.session() as session:
                row = session.scalar(
                    select(RootDetectorShadowCandidateModel).where(
                        RootDetectorShadowCandidateModel.candidate_id == candidate_id
                    )
                )
                if row is None:
                    return False
                row.live_root_created = True
                row.live_root_event_id = root_event_id
                row.root_created_at = root_created_at
                created = _ensure_utc(root_created_at)
                first_seen = _ensure_utc(row.first_seen_at)
                peak = _ensure_utc(peak_time)
                row.candidate_to_root_latency = (
                    max(0.0, (created - first_seen).total_seconds())
                    if created and first_seen
                    else None
                )
                row.peak_to_root_latency = (
                    max(0.0, (created - peak).total_seconds())
                    if created and peak
                    else None
                )
                if (
                    session.get(RootDetectorShadowEpisodeModel, candidate_id)
                    is not None
                ):
                    link_exists = session.scalar(
                        select(RootDetectorShadowEpisodeRootLinkModel.id).where(
                            RootDetectorShadowEpisodeRootLinkModel.episode_id
                            == candidate_id,
                            RootDetectorShadowEpisodeRootLinkModel.root_event_id
                            == root_event_id,
                            RootDetectorShadowEpisodeRootLinkModel.link_type
                            == "FIRST_ROOT",
                        )
                    )
                    if link_exists is None:
                        session.add(
                            RootDetectorShadowEpisodeRootLinkModel(
                                episode_id=candidate_id,
                                root_event_id=root_event_id,
                                linked_at=root_created_at,
                                link_type="FIRST_ROOT",
                                event_revision=None,
                            )
                        )
            return True
        except Exception:
            logger.exception("root detector shadow linkage write failed")
            return False

    def latest_market_coverage(self) -> dict[str, Any] | None:
        """Return the latest persisted rotation aggregate for read-only ops."""
        try:
            with self._db.session() as session:
                row = session.scalars(
                    select(MarketScanRotationModel).order_by(
                        MarketScanRotationModel.rotation_started_at.desc()
                    )
                ).first()
                if row is None:
                    return None
                return {
                    "rotation_id": row.rotation_id,
                    "runtime_instance_id": row.runtime_instance_id,
                    "status": row.status,
                    "rotation_started_at": row.rotation_started_at,
                    "rotation_completed_at": row.rotation_completed_at,
                    "exchange_universe_size": row.exchange_universe_size,
                    "eligible_universe_size": row.eligible_universe_size,
                    "scheduled_unique_symbols": row.scheduled_unique_symbols,
                    "scanned_ok_unique_symbols": row.scanned_ok_unique_symbols,
                    "failed_unique_symbols": row.failed_unique_symbols,
                    "skipped_unique_symbols": row.skipped_unique_symbols,
                    "excluded_symbols": row.excluded_symbols,
                    "eligible_coverage_pct": row.eligible_coverage_pct,
                    "exchange_coverage_pct": row.exchange_coverage_pct,
                    "exchange_universe_fingerprint": row.exchange_universe_fingerprint,
                    "eligible_universe_fingerprint": row.eligible_universe_fingerprint,
                    "scheduled_universe_fingerprint": row.scheduled_universe_fingerprint,
                    "scanned_ok_universe_fingerprint": row.scanned_ok_universe_fingerprint,
                    "details_json": row.details_json,
                }
        except Exception:
            logger.exception("market coverage telemetry read failed")
            return None

    def upsert_event_state(self, state: EventState) -> EventState:
        with self._db.session() as session:
            model = session.get(EventStateModel, state.symbol)
            if model is None:
                model = EventStateModel(
                    symbol=state.symbol,
                    event_id=state.event_id,
                    state=state.state.value,
                )
                session.add(model)
            model.event_id = state.event_id
            model.state = state.state.value
            model.event_start_time = state.event_start_time
            model.event_high = state.event_high
            model.event_high_time = state.event_high_time
            model.event_base_price = state.event_base_price
            model.event_range_pct = state.event_range_pct
            model.event_features_snapshot = _json_ready(state.event_features_snapshot)
            model.trigger_window = state.trigger_window
            model.pullback_detected_at = state.pullback_detected_at
            model.pullback_depth_pct = state.pullback_depth_pct
            model.pullback_low_price = state.pullback_low_price
            model.zone_low = state.zone_low
            model.zone_high = state.zone_high
            model.signal_sent_at = state.signal_sent_at
            model.signal_id = state.signal_id
            model.expires_at = state.expires_at
            session.flush()
            session.refresh(model)
            return _event_from_model(model)

    def get_event_state(self, symbol: str) -> EventState | None:
        with self._db.session() as session:
            model = session.get(EventStateModel, symbol)
            return None if model is None else _event_from_model(model)

    def list_active_event_states(self, now: datetime | None = None) -> list[EventState]:
        now = now or datetime.now(timezone.utc)
        with self._db.session() as session:
            stmt = select(EventStateModel).where(
                ~EventStateModel.state.in_(
                    [EventStatus.IDLE.value, EventStatus.EXPIRED.value]
                ),
            )
            models = session.scalars(stmt).all()
            states = [_event_from_model(model) for model in models]
            return [
                state
                for state in states
                if state.expires_at is None or _ensure_utc(state.expires_at) > now
            ]

    def expire_symbol(
        self, symbol: str, when: datetime | None = None
    ) -> EventState | None:
        when = when or datetime.now(timezone.utc)
        state = self.get_event_state(symbol)
        if state is None:
            return None
        state.state = EventStatus.EXPIRED
        state.expires_at = when
        return self.upsert_event_state(state)

    def save_signal(
        self,
        decision: SignalDecision,
        event_state: EventState,
        telegram_sent: bool,
        delivery_payload: str | None = None,
        *,
        provenance: SignalProvenanceInput,
    ) -> SignalRecord:
        if decision.signal_type.value == "Watch":
            raise ValueError("WATCH decisions must be stored via save_watch_candidate")
        features = decision.features_snapshot
        with self._db.session() as session:
            decision_event_high = provenance.decision_event_high
            model = SignalModel(
                symbol=decision.symbol,
                signal_time=decision.signal_time,
                signal_type=decision.signal_type.value,
                grade=decision.grade,
                score=decision.score,
                market_price=decision.market_price,
                short_zone_low=decision.short_zone_low,
                short_zone_high=decision.short_zone_high,
                event_id=decision.event_id,
                event_high=decision_event_high
                or event_state.event_high
                or decision.market_price,
                event_base_price=event_state.event_base_price or decision.market_price,
                event_range_pct=event_state.event_range_pct or 0.0,
                pullback_from_high_pct=_required_float(
                    features.get("pullback_from_high_pct"),
                    field_name="pullback_from_high_pct",
                ),
                dist_to_vwap_pct=_required_float(
                    features.get("dist_to_vwap_pct"), field_name="dist_to_vwap_pct"
                ),
                upper_wick_ratio=_required_float(
                    features.get("upper_wick_ratio"), field_name="upper_wick_ratio"
                ),
                rejection_from_high_pct=_required_float(
                    features.get("rejection_from_high_pct"),
                    field_name="rejection_from_high_pct",
                ),
                vol_zscore_30m=_required_float(
                    features.get("vol_zscore_30m"), field_name="vol_zscore_30m"
                ),
                dist_to_ema20_atr=_required_float(
                    features.get("dist_to_ema20_atr"), field_name="dist_to_ema20_atr"
                ),
                rsi_15m=_required_float(features.get("rsi_15m"), field_name="rsi_15m"),
                ret_1h=_required_float(features.get("ret_1h"), field_name="ret_1h"),
                ret_4h=_required_float(features.get("ret_4h"), field_name="ret_4h"),
                range_atr_ratio=_required_float(
                    features.get("range_atr_ratio"), field_name="range_atr_ratio"
                ),
                oi_change_15m=_nullable_float(features.get("oi_change_15m")),
                oi_change_1h=_nullable_float(features.get("oi_change_1h")),
                funding_rate=_nullable_float(features.get("funding_rate")),
                strategy_type=decision.strategy_type,
                strategy_subtype=decision.strategy_subtype,
                model_version=decision.model_version,
                context_json=_json_ready(
                    {
                        **decision.features_snapshot,
                        **decision.strategy_metadata,
                        "strategy_type": decision.strategy_type,
                        "strategy_subtype": decision.strategy_subtype,
                        "model_version": decision.model_version,
                        "reasons": decision.reasons,
                        "risk_flags": decision.risk_flags,
                        "score_breakdown": decision.score_breakdown,
                        "decision_type": decision.decision_type,
                        "actionable": decision.actionable,
                        "blockers": decision.blockers,
                        "squeeze_risk_score": decision.squeeze_risk_score,
                        "squeeze_risk_level": decision.squeeze_risk_level,
                        "squeeze_risk_reasons": decision.squeeze_risk_reasons,
                        "squeeze_guard_action": decision.squeeze_guard_action,
                        "data_quality_warnings": decision.data_quality_warnings,
                    }
                ),
                telegram_sent=telegram_sent,
            )
            session.add(model)
            session.flush()
            if provenance.event_id != model.event_id:
                raise ValueError(
                    "signal provenance event_id must match signal event_id"
                )
            session.add(
                SignalProvenanceModel(
                    signal_id=model.id,
                    strategy_family=provenance.strategy_family,
                    strategy_branch=provenance.strategy_branch,
                    event_id=provenance.event_id,
                    root_event_id=provenance.root_event_id,
                    decision_evaluation_id=provenance.decision_evaluation_id,
                    admission_evaluation_id=provenance.admission_evaluation_id,
                    code_version=provenance.code_version,
                    config_hash=provenance.config_hash,
                    runtime_instance_id=provenance.runtime_instance_id,
                    runtime_started_at=provenance.runtime_started_at,
                    decision_at=provenance.decision_at,
                    signal_created_at=model.created_at,
                    decision_entry_price=provenance.decision_entry_price,
                    decision_event_high=provenance.decision_event_high,
                    decision_distance_from_high=provenance.decision_distance_from_high,
                    provenance_anomaly=provenance.provenance_anomaly,
                )
            )
            if delivery_payload is not None:
                session.add(
                    TelegramDeliveryOutboxModel(
                        entity_type="SIGNAL",
                        entity_id=model.id,
                        payload=delivery_payload,
                        idempotency_key=f"telegram:signal:{model.id}",
                    )
                )
            session.refresh(model)
            return _signal_from_model(model)

    def update_signal_telegram_status(
        self, signal_id: int, telegram_sent: bool
    ) -> None:
        """Persist delivery result after a durable signal row already exists."""
        with self._db.session() as session:
            model = session.get(SignalModel, signal_id)
            if model is None:
                raise LookupError(f"signal not found: {signal_id}")
            model.telegram_sent = telegram_sent

    def update_watch_telegram_status(self, watch_id: int, telegram_sent: bool) -> None:
        """Persist WATCH delivery result after its durable row already exists."""
        with self._db.session() as session:
            model = session.get(WatchCandidateModel, watch_id)
            if model is None:
                raise LookupError(f"watch candidate not found: {watch_id}")
            model.telegram_sent = telegram_sent

    def has_signal_for_event(
        self, symbol: str, event_id: str, strategy_subtype: str, model_version: str
    ) -> bool:
        """Idempotent dedupe check for the main and fast-monitor paths."""
        with self._db.session() as session:
            stmt = (
                select(SignalModel.id)
                .where(
                    SignalModel.symbol == symbol,
                    SignalModel.event_id == event_id,
                    SignalModel.strategy_subtype == strategy_subtype,
                    SignalModel.model_version == model_version,
                )
                .limit(1)
            )
            return session.scalar(stmt) is not None

    def save_watch_candidate(
        self,
        decision: SignalDecision,
        event_state: EventState,
        telegram_sent: bool,
        delivery_payload: str | None = None,
    ) -> WatchCandidateRecord:
        features = decision.features_snapshot
        with self._db.session() as session:
            model = WatchCandidateModel(
                symbol=decision.symbol,
                timeframe=event_state.trigger_window or "15m",
                signal_time=decision.signal_time,
                score=decision.score,
                base_grade=decision.grade,
                signal_type=decision.signal_type.value,
                actionable=False,
                blockers_json=_json_ready(decision.blockers),
                risk_flags_json=_json_ready(decision.risk_flags),
                squeeze_risk_level=decision.squeeze_risk_level,
                squeeze_risk_score=decision.squeeze_risk_score,
                squeeze_risk_reasons_json=_json_ready(decision.squeeze_risk_reasons),
                data_quality_warnings_json=_json_ready(decision.data_quality_warnings),
                context_json=_json_ready(
                    {
                        **decision.features_snapshot,
                        "reasons": decision.reasons,
                        "risk_flags": decision.risk_flags,
                        "score_breakdown": decision.score_breakdown,
                        "decision_type": decision.decision_type,
                        "actionable": False,
                        "blockers": decision.blockers,
                        "squeeze_risk_score": decision.squeeze_risk_score,
                        "squeeze_risk_level": decision.squeeze_risk_level,
                        "squeeze_risk_reasons": decision.squeeze_risk_reasons,
                        "squeeze_guard_action": decision.squeeze_guard_action,
                        "data_quality_warnings": decision.data_quality_warnings,
                        "event_id": decision.event_id,
                        "event_high": event_state.event_high,
                        "event_base_price": event_state.event_base_price,
                        "event_range_pct": event_state.event_range_pct,
                    }
                ),
                dist_to_vwap_pct=_nullable_float(features.get("dist_to_vwap_pct")),
                upper_wick_ratio=_nullable_float(features.get("upper_wick_ratio")),
                rejection_from_high_pct=_nullable_float(
                    features.get("rejection_from_high_pct")
                ),
                volume_zscore_30m=_nullable_float(features.get("vol_zscore_30m")),
                pullback_from_event_high_pct=_nullable_float(
                    features.get("pullback_from_high_pct")
                ),
                dist_to_ema20_atr=_nullable_float(features.get("dist_to_ema20_atr")),
                rsi_15m=_nullable_float(features.get("rsi_15m")),
                spread_pct=_nullable_float(features.get("spread_pct")),
                orderbook_depth_1pct=_nullable_float(
                    features.get("orderbook_depth_usdt_1pct")
                ),
                telegram_sent=telegram_sent,
            )
            session.add(model)
            session.flush()
            if delivery_payload is not None:
                session.add(
                    TelegramDeliveryOutboxModel(
                        entity_type="WATCH",
                        entity_id=model.id,
                        payload=delivery_payload,
                        idempotency_key=f"telegram:watch:{model.id}",
                    )
                )
            session.refresh(model)
            return _watch_candidate_from_model(model)

    def claim_due_deliveries(
        self,
        now: datetime,
        *,
        limit: int,
        lease_seconds: int,
        entity_type: str | None = None,
        entity_id: int | None = None,
    ) -> list[dict[str, object]]:
        """Claim a bounded batch of pending/retry deliveries for at-least-once send."""
        with self._db.session() as session:
            session.query(TelegramDeliveryOutboxModel).filter(
                TelegramDeliveryOutboxModel.status == "IN_FLIGHT",
                TelegramDeliveryOutboxModel.lease_until <= now,
                TelegramDeliveryOutboxModel.attempt_count >= 5,
            ).update(
                {
                    TelegramDeliveryOutboxModel.status: "DEAD",
                    TelegramDeliveryOutboxModel.lease_until: None,
                    TelegramDeliveryOutboxModel.last_error: "delivery_lease_expired_after_max_attempts",
                },
                synchronize_session=False,
            )
            session.query(TelegramDeliveryOutboxModel).filter(
                TelegramDeliveryOutboxModel.status == "IN_FLIGHT",
                TelegramDeliveryOutboxModel.lease_until <= now,
                TelegramDeliveryOutboxModel.attempt_count < 5,
            ).update(
                {
                    TelegramDeliveryOutboxModel.status: "RETRY",
                    TelegramDeliveryOutboxModel.lease_until: None,
                    TelegramDeliveryOutboxModel.next_attempt_at: now,
                },
                synchronize_session=False,
            )
            conditions = [
                TelegramDeliveryOutboxModel.status.in_(["PENDING", "RETRY"]),
                TelegramDeliveryOutboxModel.next_attempt_at <= now,
            ]
            if entity_type is not None:
                conditions.append(
                    TelegramDeliveryOutboxModel.entity_type == entity_type
                )
            if entity_id is not None:
                conditions.append(TelegramDeliveryOutboxModel.entity_id == entity_id)
            stmt = (
                select(TelegramDeliveryOutboxModel)
                .where(*conditions)
                .order_by(TelegramDeliveryOutboxModel.id)
                .limit(limit)
            )
            claimed: list[dict[str, object]] = []
            lease_until = now + timedelta(seconds=lease_seconds)
            for model in session.scalars(stmt).all():
                model.status = "IN_FLIGHT"
                model.attempt_count += 1
                model.last_attempt_at = now
                model.lease_until = lease_until
                claimed.append(
                    {
                        "id": model.id,
                        "entity_type": model.entity_type,
                        "entity_id": model.entity_id,
                        "payload": model.payload,
                        "attempt_count": model.attempt_count,
                    }
                )
            return claimed

    def reconcile_delivery_leases(self, now: datetime) -> dict[str, int]:
        """Release stale delivery leases without claiming or sending anything."""
        with self._db.session() as session:
            dead = (
                session.query(TelegramDeliveryOutboxModel)
                .filter(
                    TelegramDeliveryOutboxModel.status == "IN_FLIGHT",
                    TelegramDeliveryOutboxModel.lease_until <= now,
                    TelegramDeliveryOutboxModel.attempt_count >= 5,
                )
                .update(
                    {
                        TelegramDeliveryOutboxModel.status: "DEAD",
                        TelegramDeliveryOutboxModel.lease_until: None,
                        TelegramDeliveryOutboxModel.last_error: "delivery_lease_expired_after_max_attempts",
                    },
                    synchronize_session=False,
                )
            )
            retry = (
                session.query(TelegramDeliveryOutboxModel)
                .filter(
                    TelegramDeliveryOutboxModel.status == "IN_FLIGHT",
                    TelegramDeliveryOutboxModel.lease_until <= now,
                    TelegramDeliveryOutboxModel.attempt_count < 5,
                )
                .update(
                    {
                        TelegramDeliveryOutboxModel.status: "RETRY",
                        TelegramDeliveryOutboxModel.lease_until: None,
                        TelegramDeliveryOutboxModel.next_attempt_at: now,
                    },
                    synchronize_session=False,
                )
            )
            return {"dead": int(dead), "retry": int(retry)}

    def audit_delivery_funnel(self, now: datetime | None = None) -> list[dict[str, Any]]:
        """Read-only audit of signal/provenance/outbox delivery invariants."""
        observed_at = now or datetime.now(timezone.utc)
        with self._db.session() as session:
            signals = [
                {"id": int(signal_id), "telegram_sent": bool(telegram_sent)}
                for signal_id, telegram_sent in session.execute(
                    select(SignalModel.id, SignalModel.telegram_sent)
                )
            ]
            provenances = [
                {"signal_id": int(signal_id)}
                for (signal_id,) in session.execute(
                    select(SignalProvenanceModel.signal_id)
                )
            ]
            outbox = [
                {
                    "id": int(outbox_id),
                    "entity_type": entity_type,
                    "entity_id": int(entity_id),
                    "status": status,
                    "lease_until": lease_until,
                }
                for outbox_id, entity_type, entity_id, status, lease_until in session.execute(
                    select(
                        TelegramDeliveryOutboxModel.id,
                        TelegramDeliveryOutboxModel.entity_type,
                        TelegramDeliveryOutboxModel.entity_id,
                        TelegramDeliveryOutboxModel.status,
                        TelegramDeliveryOutboxModel.lease_until,
                    )
                )
            ]
        return audit_delivery_funnel(
            signals=signals,
            provenances=provenances,
            outbox=outbox,
            now=observed_at,
        )

    def delivery_id_for_entity(self, entity_type: str, entity_id: int) -> int:
        with self._db.session() as session:
            stmt = select(TelegramDeliveryOutboxModel.id).where(
                TelegramDeliveryOutboxModel.entity_type == entity_type,
                TelegramDeliveryOutboxModel.entity_id == entity_id,
                TelegramDeliveryOutboxModel.status != "SENT",
            )
            delivery_id = session.scalar(stmt)
            if delivery_id is None:
                raise LookupError(f"delivery not found: {entity_type}:{entity_id}")
            return int(delivery_id)

    def mark_delivery_sent(self, outbox_id: int) -> None:
        """Atomically mark outbox and its source entity as delivered."""
        with self._db.session() as session:
            outbox = session.get(TelegramDeliveryOutboxModel, outbox_id)
            if outbox is None:
                raise LookupError(f"delivery outbox not found: {outbox_id}")
            now = datetime.now(timezone.utc)
            outbox.status = "SENT"
            outbox.sent_at = now
            outbox.lease_until = None
            if outbox.entity_type == "SIGNAL":
                source = session.get(SignalModel, outbox.entity_id)
            elif outbox.entity_type == "WATCH":
                source = session.get(WatchCandidateModel, outbox.entity_id)
            else:
                raise ValueError(f"unknown delivery entity type: {outbox.entity_type}")
            if source is None:
                raise LookupError(
                    f"delivery source not found: {outbox.entity_type}:{outbox.entity_id}"
                )
            source.telegram_sent = True

    def mark_delivery_retry(
        self, outbox_id: int, *, error: str, next_attempt_at: datetime
    ) -> None:
        """Persist a bounded retry or dead-letter a delivery after five attempts."""
        with self._db.session() as session:
            outbox = session.get(TelegramDeliveryOutboxModel, outbox_id)
            if outbox is None:
                raise LookupError(f"delivery outbox not found: {outbox_id}")
            outbox.status = "DEAD" if outbox.attempt_count >= 5 else "RETRY"
            outbox.next_attempt_at = next_attempt_at
            outbox.lease_until = None
            outbox.last_error = error[:1000]

    def count_legacy_unsent_signals(self) -> int:
        """Count historical unsent rows intentionally excluded from the new outbox."""
        with self._db.session() as session:
            stmt = select(func.count(SignalModel.id)).where(
                SignalModel.telegram_sent.is_(False),
                SignalModel.strategy_subtype.is_(None),
                SignalModel.model_version.is_(None),
            )
            return int(session.scalar(stmt) or 0)

    def list_signals_missing_outcomes(
        self,
        now: datetime | None = None,
        lookback_hours: int = 48,
        limit: int = 200,
    ) -> list[SignalRecord]:
        now = now or datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=lookback_hours)
        with self._db.session() as session:
            stmt = (
                select(SignalModel)
                .where(SignalModel.signal_time >= cutoff)
                .where(SignalModel.signal_type != "Watch")
                .order_by(SignalModel.signal_time.asc())
                .limit(limit)
            )
            models = session.scalars(stmt).all()
            results: list[SignalRecord] = []
            for model in models:
                if model.outcome is None or model.outcome.price_after_4h is None:
                    results.append(_signal_from_model(model))
            return results

    def upsert_signal_outcome(self, outcome: SignalOutcome) -> SignalOutcome:
        with self._db.session() as session:
            model = session.get(SignalOutcomeModel, outcome.signal_id)
            if model is None:
                model = SignalOutcomeModel(signal_id=outcome.signal_id)
                session.add(model)
            model.price_after_15m = outcome.price_after_15m
            model.price_after_1h = outcome.price_after_1h
            model.price_after_4h = outcome.price_after_4h
            model.mfe_pct = outcome.mfe_pct
            model.mae_pct = outcome.mae_pct
            model.reached_vwap = outcome.reached_vwap
            model.time_to_vwap_minutes = outcome.time_to_vwap_minutes
            model.tp1_hit = outcome.tp1_hit
            model.stopped_virtual = outcome.stopped_virtual
            model.risk_adjusted_status = outcome.risk_adjusted_status
            model.squeeze_extension_pct = outcome.squeeze_extension_pct
            model.is_clean_short = outcome.is_clean_short
            model.is_squeeze_before_tp = outcome.is_squeeze_before_tp
            model.updated_at = outcome.updated_at or datetime.now(timezone.utc)
            session.flush()
            session.refresh(model)
            return _outcome_from_model(model)

    def record_strategy_observation(
        self, observation: StrategyObservation
    ) -> ObservationWriteResult:
        """Append one research observation without interrupting the scanner."""

        try:
            values = {
                "observation_id": observation.observation_id,
                "idempotency_key": observation.idempotency_key,
                "run_id": observation.run_id,
                "runtime_instance_id": observation.runtime_instance_id,
                "runtime_started_at": observation.runtime_started_at,
                "code_version": observation.code_version,
                "strategy_family": observation.strategy_family,
                "strategy": observation.strategy,
                "evaluation_phase": observation.evaluation_phase,
                "symbol": observation.symbol,
                "root_event_id": observation.root_event_id,
                "event_revision": observation.event_revision,
                "attempt_id": observation.attempt_id,
                "evaluation_id": observation.evaluation_id,
                "signal_id": observation.signal_id,
                "observed_at": observation.observed_at,
                "exchange_time": observation.exchange_time,
                "market_asof": observation.market_asof,
                "live_decision": observation.live_decision,
                "shadow_decision": observation.shadow_decision,
                "score": observation.score,
                "blockers_json": _json_ready(observation.blockers),
                "warnings_json": _json_ready(observation.warnings),
                "market_price": _nullable_float(observation.market_price),
                "event_high": _nullable_float(observation.event_high),
                "model_version": observation.model_version,
                "config_hash": observation.config_hash,
                "input_fingerprint": observation.input_fingerprint,
                "input_snapshot_json": _json_ready(observation.input_snapshot),
            }
            with self._db.session() as session:
                statement = (
                    sqlite_insert(StrategyObservationModel)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=["idempotency_key"])
                )
                result = session.execute(statement)
                if result.rowcount:
                    return ObservationWriteResult(
                        ObservationWriteStatus.INSERTED, observation.observation_id
                    )
                return ObservationWriteResult(ObservationWriteStatus.DUPLICATE)
        except Exception as exc:
            logger.exception(
                "strategy observation ledger write failed family=%s strategy=%s symbol=%s root_event_id=%s",
                observation.strategy_family,
                observation.strategy,
                observation.symbol,
                observation.root_event_id,
            )
            return ObservationWriteResult(
                ObservationWriteStatus.FAILED,
                failure_classification="SQLITE_FULL"
                if is_sqlite_full(exc)
                else "ERROR",
            )

    def list_strategy_observations_due_outcomes(
        self,
        *,
        limit: int = 25,
        now: datetime | None = None,
        exclude_observation_ids: set[str] | tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        """Return target climax observations whose outcome is not complete."""

        due_at = now or datetime.now(timezone.utc)
        with self._db.session() as session:
            statement = (
                select(StrategyObservationModel)
                .where(
                    StrategyObservationModel.strategy.in_(
                        ("VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE")
                    ),
                    or_(
                        StrategyObservationModel.outcome_status.is_(None),
                        and_(
                            StrategyObservationModel.outcome_status == "incomplete",
                            or_(
                                StrategyObservationModel.outcome_next_attempt_at.is_(
                                    None
                                ),
                                StrategyObservationModel.outcome_next_attempt_at
                                <= due_at,
                            ),
                        ),
                        and_(
                            StrategyObservationModel.outcome_status == "unknown",
                            StrategyObservationModel.outcome_next_attempt_at.is_not(
                                None
                            ),
                            StrategyObservationModel.outcome_next_attempt_at <= due_at,
                        ),
                    ),
                )
                .order_by(
                    func.coalesce(
                        StrategyObservationModel.outcome_next_attempt_at,
                        StrategyObservationModel.observed_at,
                    ).asc(),
                    StrategyObservationModel.observed_at.asc(),
                    StrategyObservationModel.observation_id.asc(),
                )
                .limit(limit)
            )
            if exclude_observation_ids:
                statement = statement.where(
                    StrategyObservationModel.observation_id.not_in(
                        exclude_observation_ids
                    )
                )
            models = session.scalars(statement).all()
            return [
                {
                    "observation_id": model.observation_id,
                    "symbol": model.symbol,
                    "observed_at": model.observed_at,
                    "market_price": model.market_price,
                    "event_high": model.event_high,
                    "outcome_attempt_count": model.outcome_attempt_count or 0,
                }
                for model in models
            ]

    def update_strategy_observation_outcome(
        self,
        observation_id: str,
        outcome: dict[str, Any],
        *,
        updated_at: datetime,
        next_attempt_at: datetime | None = None,
    ) -> bool:
        """Persist one research outcome without changing observation identity."""

        status = str(outcome.get("data_status") or "unknown")
        if status not in {"complete", "incomplete", "unknown"}:
            raise ValueError(
                f"unsupported strategy observation outcome status: {status}"
            )
        with self._db.session() as session:
            model = session.get(StrategyObservationModel, observation_id)
            if model is None:
                return False
            model.outcome_status = status
            model.outcome_json = _json_ready(outcome)
            model.outcome_mfe_pct = _nullable_float(outcome.get("mfe_pct"))
            model.outcome_mae_pct = _nullable_float(outcome.get("mae_pct"))
            model.outcome_time_to_mfe_minutes = _nullable_float(
                outcome.get("time_to_mfe_minutes")
            )
            model.outcome_time_to_mae_minutes = _nullable_float(
                outcome.get("time_to_mae_minutes")
            )
            model.outcome_new_high_after_observation = outcome.get(
                "new_high_after_observation"
            )
            model.outcome_updated_at = updated_at
            model.outcome_next_attempt_at = next_attempt_at
            model.outcome_attempt_count = (model.outcome_attempt_count or 0) + 1
            session.flush()
            return True

    def record_volume_climax_observation(
        self,
        *,
        observed_at: datetime,
        market_asof: datetime | None,
        symbol: str,
        event_id: str,
        root_event_id: str | None,
        event_revision: int | None,
        runtime_instance_id: str | None,
        model_version: str | None,
        subtype: str,
        stage: str,
        score: int,
        grade: str,
        veto_reasons: list[str],
        data_quality: list[str],
        metadata: dict[str, Any],
        source_evaluation_id: int | None,
        attempt_id: str | None,
    ) -> int | None:
        """Append every volume-climax family observation independently of selection."""
        try:
            with self._db.session() as session:
                model = VolumeClimaxObservationModel(
                    observed_at=observed_at,
                    market_asof=market_asof,
                    symbol=symbol,
                    event_id=event_id,
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    runtime_instance_id=runtime_instance_id
                    or getattr(self, "_runtime_instance_id", None),
                    model_version=model_version,
                    subtype=subtype,
                    stage=stage,
                    score=score,
                    grade=grade,
                    veto_reasons_json=_json_ready(veto_reasons),
                    data_quality_json=_json_ready(data_quality),
                    metadata_json=_json_ready(metadata),
                    source_evaluation_id=source_evaluation_id,
                    attempt_id=attempt_id,
                )
                session.add(model)
                session.flush()
                return model.id
        except Exception:
            logger.exception(
                "volume observation ledger write failed symbol=%s event_id=%s",
                symbol,
                event_id,
            )
            return None

    def record_climax_evaluation(
        self,
        *,
        evaluation_time: datetime,
        symbol: str,
        strategy: str,
        subtype_candidate: str | None,
        model_version: str | None,
        event_id: str,
        event_high: float | None,
        event_high_time: datetime | None,
        event_detected_at: datetime | None,
        candidate_added_at: datetime | None,
        candidate_age_sec: float | None,
        fast_monitor: bool,
        poll_sequence: int | None,
        frame_asof: datetime | None,
        candles_asof: datetime | None,
        oi_asof: datetime | None,
        orderbook_asof: datetime | None,
        score: int,
        grade: str,
        actionable: bool,
        admission_passed: bool,
        veto_reasons: list[str],
        passed_conditions: list[str],
        data_quality: list[str],
        liquidity: dict[str, Any],
        oi: dict[str, Any],
        features: dict[str, Any],
        lifecycle_state: str,
        removal_reason: str | None = None,
        telegram_eligible: bool = False,
        runtime_instance_id: str | None = None,
        root_event_id: str | None = None,
        event_revision: int | None = None,
        attempt_id: str | None = None,
        observed_at: datetime | None = None,
        market_asof: datetime | None = None,
        pool_added_at: datetime | None = None,
        event_age_sec: float | None = None,
        pool_age_sec: float | None = None,
        evaluation_completed_at: datetime | None = None,
        live_decision: str | None = None,
        live_veto_reasons: list[str] | None = None,
        shadow_decision: str | None = None,
        shadow_veto_reasons: list[str] | None = None,
        decision_delta: str | None = None,
        shadow_hypothetical_entry_price: float | None = None,
        shadow_hypothetical_grade: str | None = None,
        shadow_hypothetical_score: int | None = None,
        shadow_removed_vetoes: list[str] | None = None,
    ) -> int | None:
        """Append climax evidence without allowing telemetry to break runtime."""
        try:
            with self._db.session() as session:
                model = ClimaxEvaluationModel(
                    evaluation_time=evaluation_time,
                    symbol=symbol,
                    strategy=strategy,
                    subtype_candidate=subtype_candidate,
                    model_version=model_version,
                    event_id=event_id,
                    event_high=_nullable_float(event_high),
                    event_high_time=event_high_time,
                    event_detected_at=event_detected_at,
                    candidate_added_at=candidate_added_at,
                    candidate_age_sec=_nullable_float(candidate_age_sec),
                    fast_monitor=fast_monitor,
                    poll_sequence=poll_sequence,
                    frame_asof=frame_asof,
                    candles_asof=candles_asof,
                    oi_asof=oi_asof,
                    orderbook_asof=orderbook_asof,
                    score=score,
                    grade=grade,
                    actionable=actionable,
                    admission_passed=admission_passed,
                    veto_reasons_json=_json_ready(veto_reasons),
                    passed_conditions_json=_json_ready(passed_conditions),
                    data_quality_json=_json_ready(data_quality),
                    liquidity_json=_json_ready(liquidity),
                    oi_json=_json_ready(oi),
                    features_json=_json_ready(features),
                    lifecycle_state=lifecycle_state,
                    removal_reason=removal_reason,
                    telegram_eligible=telegram_eligible,
                    runtime_instance_id=runtime_instance_id
                    or getattr(self, "_runtime_instance_id", None),
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    attempt_id=attempt_id,
                    observed_at=observed_at or evaluation_time,
                    market_asof=market_asof or frame_asof,
                    pool_added_at=pool_added_at,
                    event_age_sec=_nullable_float(event_age_sec),
                    pool_age_sec=_nullable_float(pool_age_sec),
                    evaluation_completed_at=evaluation_completed_at or evaluation_time,
                    live_decision=live_decision,
                    live_veto_reasons_json=_json_ready(live_veto_reasons or []),
                    shadow_decision=shadow_decision,
                    shadow_veto_reasons_json=_json_ready(shadow_veto_reasons or []),
                    decision_delta=decision_delta,
                    shadow_hypothetical_entry_price=_nullable_float(
                        shadow_hypothetical_entry_price
                    ),
                    shadow_hypothetical_grade=shadow_hypothetical_grade,
                    shadow_hypothetical_score=shadow_hypothetical_score,
                    shadow_removed_vetoes_json=_json_ready(shadow_removed_vetoes or []),
                )
                session.add(model)
                session.flush()
                return model.id
        except Exception:
            logger.exception(
                "climax telemetry write failed symbol=%s event_id=%s", symbol, event_id
            )

    def upsert_shadow_root_event(
        self,
        *,
        root_event_id: str,
        symbol: str,
        event_started_at: datetime | None,
        event_base_price: float | None,
        peak_high: float | None,
        peak_high_time: datetime | None,
        initial_extension_pct: float | None,
        initial_extension_source: str | None,
        observed_at: datetime,
    ) -> tuple[int, float | None]:
        """Upsert immutable root identity and return (revision, frozen extension)."""
        try:
            with self._db.session() as session:
                model = session.get(ClimaxRootEventModel, root_event_id)
                if model is None:
                    model = ClimaxRootEventModel(
                        root_event_id=root_event_id,
                        symbol=symbol,
                        event_started_at=event_started_at,
                        event_base_price=_nullable_float(event_base_price),
                        peak_high=_nullable_float(peak_high),
                        peak_high_time=peak_high_time,
                        peak_revision=1,
                        initial_extension_pct=_nullable_float(initial_extension_pct),
                        initial_extension_source=initial_extension_source,
                        initial_extension_confirmed_at=observed_at
                        if initial_extension_pct is not None
                        else None,
                        last_observed_at=observed_at,
                    )
                    session.add(model)
                else:
                    if peak_high is not None and (
                        model.peak_high is None or peak_high > model.peak_high
                    ):
                        model.peak_high = _nullable_float(peak_high)
                        model.peak_high_time = peak_high_time
                        model.peak_revision += 1
                    if (
                        model.initial_extension_pct is None
                        and initial_extension_pct is not None
                    ):
                        model.initial_extension_pct = _nullable_float(
                            initial_extension_pct
                        )
                        model.initial_extension_source = initial_extension_source
                        model.initial_extension_confirmed_at = observed_at
                    model.last_observed_at = observed_at
                outcome = session.get(CurrentRootOutcomeModel, root_event_id)
                if outcome is None:
                    anchor_price = _nullable_float(event_base_price)
                    if anchor_price is None:
                        anchor_price = _nullable_float(peak_high)
                    if anchor_price is not None:
                        session.add(
                            CurrentRootOutcomeModel(
                                root_event_id=root_event_id,
                                symbol=symbol.upper(),
                                anchor_type="LIVE_ROOT",
                                anchor_time=observed_at,
                                anchor_price=anchor_price,
                                event_high=_nullable_float(peak_high),
                                outcome_status="CENSORED",
                                outcome_next_due_at=_ensure_utc(observed_at)
                                + timedelta(minutes=15),
                                outcome_attempt_count=0,
                            )
                        )
                session.flush()
                return model.peak_revision, model.initial_extension_pct
        except Exception:
            logger.exception(
                "shadow root-event write failed symbol=%s root_event_id=%s",
                symbol,
                root_event_id,
            )
            # Shadow telemetry must not veto the live evaluation/sender path.
            return 1, None

    def record_live_root_outcome_attempt(
        self,
        *,
        root_event_id: str,
        episode_id: str | None = None,
        symbol: str | None = None,
        anchor_time: datetime | None = None,
        anchor_price: float | None = None,
        event_high: float | None = None,
        outcome: dict[str, Any] | None = None,
        error: str | None = None,
        next_due_at: datetime | None = None,
        computed_at: datetime | None = None,
    ) -> bool:
        """Persist outcomes without ever changing the LIVE_ROOT anchor."""
        safe_outcome = (
            canonicalize_json_payload(outcome) if outcome is not None else None
        )
        try:
            now = _ensure_utc(computed_at or datetime.now(timezone.utc))
            with self._db.session() as session:
                row = session.get(CurrentRootOutcomeModel, root_event_id)
                if row is None:
                    if symbol is None or anchor_time is None or anchor_price is None:
                        return False
                    row = CurrentRootOutcomeModel(
                        root_event_id=root_event_id,
                        episode_id=episode_id,
                        symbol=str(symbol).upper(),
                        anchor_type="LIVE_ROOT",
                        anchor_time=_ensure_utc(anchor_time),
                        anchor_price=float(anchor_price),
                        event_high=_nullable_float(event_high),
                        outcome_status="CENSORED",
                        outcome_attempt_count=0,
                    )
                    session.add(row)
                if row.anchor_type != "LIVE_ROOT":
                    return False
                if episode_id is not None:
                    row.episode_id = episode_id
                row.outcome_last_attempt_at = now
                row.outcome_attempt_count = int(row.outcome_attempt_count or 0) + 1
                row.outcome_next_due_at = next_due_at
                row.outcome_last_error = str(error)[:255] if error else None
                row.outcome_updated_at = now
                if safe_outcome is not None:
                    row.outcome_status = str(
                        safe_outcome.get("outcome_status") or row.outcome_status
                    )
                    row.outcome_json = safe_outcome
                    row.outcome_code_version = safe_outcome.get("outcome_code_version")
                    row.outcome_method_version = safe_outcome.get(
                        "outcome_method_version"
                    )
                    row.outcome_computed_at = now
                    for label, field in (
                        ("1h", "return_1h"),
                        ("4h", "return_4h"),
                        ("12h", "return_12h"),
                        ("24h", "return_24h"),
                    ):
                        value = (safe_outcome.get("horizons", {}).get(label) or {}).get(
                            "short_return_pct"
                        )
                        if value is not None:
                            setattr(row, field, value)
                    for label, field in (
                        ("1h", "mfe_1h"),
                        ("4h", "mfe_4h"),
                        ("12h", "mfe_12h"),
                        ("24h", "mfe_24h"),
                    ):
                        value = (safe_outcome.get("mfe_by_horizon") or {}).get(label)
                        if value is not None:
                            setattr(row, field, value)
                    for label, field in (
                        ("1h", "mae_1h"),
                        ("4h", "mae_4h"),
                        ("12h", "mae_12h"),
                        ("24h", "mae_24h"),
                    ):
                        value = (safe_outcome.get("mae_by_horizon") or {}).get(label)
                        if value is not None:
                            setattr(row, field, value)
                    row.new_high_after_anchor = safe_outcome.get(
                        "new_high_after_candidate", row.new_high_after_anchor
                    )
                    coverage_end_at = (outcome or {}).get("coverage_end_at")
                    if isinstance(coverage_end_at, str):
                        coverage_end_at = datetime.fromisoformat(
                            coverage_end_at.replace("Z", "+00:00")
                        )
                    row.coverage_end_at = coverage_end_at or row.coverage_end_at
            return True
        except Exception:
            logger.exception(
                "current live-root outcome write failed root_event_id=%s", root_event_id
            )
            return False

    def list_current_root_outcomes_due(
        self, *, now: datetime, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._db.session() as session:
            rows = session.scalars(
                select(CurrentRootOutcomeModel)
                .where(
                    CurrentRootOutcomeModel.outcome_next_due_at.is_not(None),
                    CurrentRootOutcomeModel.outcome_next_due_at <= now,
                    CurrentRootOutcomeModel.outcome_status != "MATURE",
                )
                .order_by(CurrentRootOutcomeModel.outcome_next_due_at)
                .limit(limit)
            ).all()
            return [
                {
                    "root_event_id": row.root_event_id,
                    "episode_id": row.episode_id,
                    "symbol": row.symbol,
                    "anchor_time": row.anchor_time,
                    "anchor_price": row.anchor_price,
                    "event_high": row.event_high,
                    "next_due_at": row.outcome_next_due_at,
                    "outcome_attempt_count": row.outcome_attempt_count,
                }
                for row in rows
            ]

    def count_current_root_outcomes_due(self, *, now: datetime) -> int:
        with self._db.session() as session:
            return int(
                session.scalar(
                    select(func.count())
                    .select_from(CurrentRootOutcomeModel)
                    .where(
                        CurrentRootOutcomeModel.outcome_next_due_at.is_not(None),
                        CurrentRootOutcomeModel.outcome_next_due_at <= now,
                        CurrentRootOutcomeModel.outcome_status != "MATURE",
                    )
                )
                or 0
            )

    def record_current_root_predicate_snapshot(
        self,
        *,
        root_event_id: str,
        episode_id: str | None,
        evaluation_id: int | None,
        observed_at: datetime,
        snapshot: dict[str, Any],
    ) -> bool:
        try:
            with self._db.session() as session:
                if episode_id is None:
                    episode_id = session.scalar(
                        select(RootDetectorShadowV2RootModel.episode_id).where(
                            RootDetectorShadowV2RootModel.live_root_event_id
                            == root_event_id
                        )
                    )
                session.add(
                    CurrentRootPredicateSnapshotModel(
                        root_event_id=root_event_id,
                        episode_id=episode_id,
                        evaluation_id=evaluation_id,
                        observed_at=_ensure_utc(observed_at),
                        snapshot_json=canonicalize_json_payload(snapshot),
                    )
                )
            return True
        except Exception:
            logger.exception(
                "current root predicate snapshot write failed root_event_id=%s",
                root_event_id,
            )
            return False

    def upsert_shadow_entry_attempt(
        self,
        *,
        attempt_id: str,
        root_event_id: str,
        observed_at: datetime,
        local_retest_high: float | None,
        breakdown_level: float | None,
        attempt_state: str,
        attempt_trigger: str = "structure_observed",
        confirmation_expires_at: datetime | None = None,
        close_reason: str | None = None,
        event_revision: int | None = None,
        runtime_instance_id: str | None = None,
        model_version: str | None = None,
        max_attempts_per_root_event: int | None = None,
    ) -> bool:
        """Persist or reuse a deterministic shadow attempt without reopening terminals."""
        try:
            with self._db.session() as session:
                if max_attempts_per_root_event is not None and getattr(
                    self._db, "is_sqlite", False
                ):
                    # Serialize count+insert admission across concurrent SQLite workers.
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                model = session.get(ClimaxEntryAttemptModel, attempt_id)
                if model is None:
                    if (
                        attempt_state not in _TERMINAL_ATTEMPT_STATES
                        and not _is_valid_lifecycle_attempt_values(
                            attempt_created_at=observed_at,
                            confirmation_expires_at=confirmation_expires_at,
                        )
                    ):
                        return False
                    if max_attempts_per_root_event is not None:
                        existing_count = (
                            session.scalar(
                                select(
                                    func.count(ClimaxEntryAttemptModel.attempt_id)
                                ).where(
                                    ClimaxEntryAttemptModel.root_event_id
                                    == root_event_id,
                                    or_(
                                        ClimaxEntryAttemptModel.confirmation_expires_at.is_(None),
                                        ClimaxEntryAttemptModel.confirmation_expires_at
                                        > ClimaxEntryAttemptModel.attempt_created_at,
                                    ),
                                )
                            )
                            or 0
                        )
                        if existing_count >= max_attempts_per_root_event:
                            self._append_attempt_event(
                                session,
                                attempt_id=None,
                                root_event_id=root_event_id,
                                event_revision=event_revision,
                                evaluation_id=None,
                                event_type="attempt_limit_reached",
                                previous_state=None,
                                new_state=None,
                                reason="max_attempts_per_root_event_reached",
                                observed_at=observed_at,
                                market_asof=None,
                                runtime_instance_id=runtime_instance_id,
                                model_version=model_version,
                                details={
                                    "existing_attempt_count": int(existing_count),
                                    "max_attempts_per_root_event": max_attempts_per_root_event,
                                    "requested_attempt_id": attempt_id,
                                },
                                idempotency_key=f"{root_event_id}:attempt-limit:{max_attempts_per_root_event}:{attempt_id}",
                            )
                            return False
                    model = ClimaxEntryAttemptModel(
                        attempt_id=attempt_id,
                        root_event_id=root_event_id,
                        attempt_created_at=observed_at,
                        attempt_trigger=attempt_trigger,
                        local_retest_high=_nullable_float(local_retest_high),
                        breakdown_level=_nullable_float(breakdown_level),
                        confirmation_started_at=observed_at,
                        confirmation_expires_at=confirmation_expires_at,
                        attempt_state=attempt_state,
                        attempt_close_reason=close_reason,
                        attempt_closed_at=(
                            observed_at
                            if close_reason or attempt_state in _TERMINAL_ATTEMPT_STATES
                            else None
                        ),
                        last_observed_at=observed_at,
                    )
                    session.add(model)
                    session.flush()
                    self._append_attempt_event(
                        session,
                        attempt_id=attempt_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        evaluation_id=None,
                        event_type="attempt_created",
                        previous_state=None,
                        new_state=attempt_state,
                        reason=attempt_trigger,
                        observed_at=observed_at,
                        market_asof=None,
                        runtime_instance_id=runtime_instance_id,
                        model_version=model_version,
                        details={},
                        idempotency_key=f"{attempt_id}:attempt_created",
                    )
                    return True
                if (
                    model.attempt_closed_at is not None
                    or model.attempt_state in _TERMINAL_ATTEMPT_STATES
                ):
                    return True
                previous_state = model.attempt_state
                model.local_retest_high = (
                    _nullable_float(local_retest_high) or model.local_retest_high
                )
                model.breakdown_level = (
                    _nullable_float(breakdown_level) or model.breakdown_level
                )
                # The TTL anchor is immutable; never refresh confirmation_expires_at on repeated evaluation.
                if (
                    model.confirmation_expires_at is None
                    and confirmation_expires_at is not None
                ):
                    model.confirmation_expires_at = confirmation_expires_at
                model.attempt_state = attempt_state
                model.last_observed_at = observed_at
                if close_reason:
                    model.attempt_state = attempt_state
                    model.attempt_close_reason = (
                        model.attempt_close_reason or close_reason
                    )
                    model.attempt_closed_at = model.attempt_closed_at or observed_at
                if previous_state != model.attempt_state:
                    self._append_attempt_event(
                        session,
                        attempt_id=attempt_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        evaluation_id=None,
                        event_type="attempt_state_changed",
                        previous_state=previous_state,
                        new_state=model.attempt_state,
                        reason=close_reason or "observation",
                        observed_at=observed_at,
                        market_asof=None,
                        runtime_instance_id=runtime_instance_id,
                        model_version=model_version,
                        details={},
                        idempotency_key=f"{attempt_id}:state:{model.attempt_state}:{observed_at.isoformat()}",
                    )
                latest = session.scalars(
                    select(ClimaxEntryAttemptEventModel)
                    .where(ClimaxEntryAttemptEventModel.attempt_id == attempt_id)
                    .order_by(ClimaxEntryAttemptEventModel.id.desc())
                    .limit(1)
                ).first()
                if (
                    runtime_instance_id
                    and latest
                    and latest.runtime_instance_id
                    and latest.runtime_instance_id != runtime_instance_id
                ):
                    self._append_attempt_event(
                        session,
                        attempt_id=attempt_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        evaluation_id=None,
                        event_type="attempt_reused_after_restart",
                        previous_state=model.attempt_state,
                        new_state=model.attempt_state,
                        reason="existing_nonterminal_attempt",
                        observed_at=observed_at,
                        market_asof=None,
                        runtime_instance_id=runtime_instance_id,
                        model_version=model_version,
                        details={},
                        idempotency_key=f"{attempt_id}:restart:{runtime_instance_id}",
                    )
                return True
        except Exception:
            logger.exception(
                "shadow entry-attempt write failed root_event_id=%s attempt_id=%s",
                root_event_id,
                attempt_id,
            )
            return False

    def transition_shadow_entry_attempt(
        self,
        *,
        attempt_id: str,
        root_event_id: str,
        event_revision: int | None,
        evaluation_id: int | None,
        new_state: str,
        reason: str,
        observed_at: datetime,
        market_asof: datetime | None,
        runtime_instance_id: str | None,
        model_version: str | None,
        details: dict[str, Any] | None = None,
    ) -> bool:
        """Apply an idempotent shadow lifecycle transition; never creates delivery state."""
        try:
            with self._db.session() as session:
                if getattr(self._db, "is_sqlite", False):
                    # Serialize read-check-update-event for terminal transitions.
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                model = session.get(ClimaxEntryAttemptModel, attempt_id)
                if model is None or model.attempt_closed_at is not None:
                    return False
                previous = model.attempt_state
                if new_state not in _TERMINAL_ATTEMPT_STATES and previous == new_state:
                    return False
                if previous != new_state:
                    model.attempt_state = new_state
                    self._append_attempt_event(
                        session,
                        attempt_id=attempt_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        evaluation_id=evaluation_id,
                        event_type="attempt_state_changed",
                        previous_state=previous,
                        new_state=new_state,
                        reason=reason,
                        observed_at=observed_at,
                        market_asof=market_asof,
                        runtime_instance_id=runtime_instance_id,
                        model_version=model_version,
                        details=details or {},
                        idempotency_key=f"{attempt_id}:state:{evaluation_id}:{new_state}",
                    )
                if new_state in _TERMINAL_ATTEMPT_STATES:
                    model.attempt_closed_at = model.attempt_closed_at or observed_at
                    model.attempt_close_reason = model.attempt_close_reason or reason
                    self._append_attempt_event(
                        session,
                        attempt_id=attempt_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        evaluation_id=evaluation_id,
                        event_type="attempt_closed",
                        previous_state=previous,
                        new_state=new_state,
                        reason=reason,
                        observed_at=observed_at,
                        market_asof=market_asof,
                        runtime_instance_id=runtime_instance_id,
                        model_version=model_version,
                        details=details or {},
                        idempotency_key=f"{attempt_id}:closed:{evaluation_id}:{new_state}",
                    )
                model.last_observed_at = observed_at
                return True
        except Exception:
            logger.exception(
                "shadow attempt transition failed attempt_id=%s", attempt_id
            )
            return False

    def close_open_shadow_attempts_for_root(
        self,
        *,
        root_event_id: str,
        new_state: str,
        reason: str,
        observed_at: datetime,
        runtime_instance_id: str | None,
        model_version: str | None,
    ) -> int:
        """Close all currently open attempts for a replaced root without touching signals."""
        with self._db.session() as session:
            if getattr(self._db, "is_sqlite", False):
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            models = session.scalars(
                select(ClimaxEntryAttemptModel).where(
                    ClimaxEntryAttemptModel.root_event_id == root_event_id,
                    ClimaxEntryAttemptModel.attempt_closed_at.is_(None),
                )
            ).all()
            count = 0
            for model in models:
                previous = model.attempt_state
                model.attempt_state = new_state
                model.attempt_closed_at = observed_at
                model.attempt_close_reason = reason
                model.last_observed_at = observed_at
                self._append_attempt_event(
                    session,
                    attempt_id=model.attempt_id,
                    root_event_id=root_event_id,
                    event_revision=None,
                    evaluation_id=None,
                    event_type="attempt_state_changed",
                    previous_state=previous,
                    new_state=new_state,
                    reason=reason,
                    observed_at=observed_at,
                    market_asof=None,
                    runtime_instance_id=runtime_instance_id,
                    model_version=model_version,
                    details={},
                    idempotency_key=f"{model.attempt_id}:state:root-replaced",
                )
                self._append_attempt_event(
                    session,
                    attempt_id=model.attempt_id,
                    root_event_id=root_event_id,
                    event_revision=None,
                    evaluation_id=None,
                    event_type="attempt_closed",
                    previous_state=previous,
                    new_state=new_state,
                    reason=reason,
                    observed_at=observed_at,
                    market_asof=None,
                    runtime_instance_id=runtime_instance_id,
                    model_version=model_version,
                    details={},
                    idempotency_key=f"{model.attempt_id}:closed:root-replaced",
                )
                count += 1
            return count

    def get_open_shadow_attempt_id(self, *, root_event_id: str) -> str | None:
        """Return the deterministic currently open attempt for correlation."""
        with self._db.session() as session:
            model = session.scalars(
                select(ClimaxEntryAttemptModel)
                .where(
                    ClimaxEntryAttemptModel.root_event_id == root_event_id,
                    ClimaxEntryAttemptModel.attempt_closed_at.is_(None),
                    or_(
                        ClimaxEntryAttemptModel.confirmation_expires_at.is_(None),
                        ClimaxEntryAttemptModel.confirmation_expires_at
                        > ClimaxEntryAttemptModel.attempt_created_at,
                    ),
                )
                .order_by(ClimaxEntryAttemptModel.attempt_created_at.desc())
                .limit(1)
            ).first()
            return model.attempt_id if model else None

    def get_shadow_entry_attempt(self, *, attempt_id: str) -> dict[str, datetime | str | None] | None:
        """Read immutable Strategy-4 attempt timing for admission checks."""
        with self._db.session() as session:
            model = session.get(ClimaxEntryAttemptModel, attempt_id)
            if model is None:
                return None
            return {
                "attempt_id": model.attempt_id,
                "attempt_created_at": _ensure_utc(model.attempt_created_at),
                "confirmation_expires_at": _ensure_utc(model.confirmation_expires_at),
                "attempt_state": model.attempt_state,
                "attempt_closed_at": model.attempt_closed_at,
            }

    def expire_shadow_attempt_if_due(
        self,
        *,
        attempt_id: str,
        root_event_id: str,
        event_revision: int | None,
        evaluation_id: int | None,
        observed_at: datetime,
        market_asof: datetime | None,
        runtime_instance_id: str | None,
        model_version: str | None,
    ) -> bool:
        """Close an open attempt once its immutable confirmation TTL has elapsed."""
        with self._db.session() as session:
            if getattr(self._db, "is_sqlite", False):
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            model = session.get(ClimaxEntryAttemptModel, attempt_id)
            if (
                not model
                or model.attempt_closed_at is not None
                or not model.confirmation_expires_at
            ):
                return False
            expires_at = model.confirmation_expires_at
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            observed_utc = (
                observed_at
                if observed_at.tzinfo is not None
                else observed_at.replace(tzinfo=timezone.utc)
            )
            if observed_utc < expires_at:
                return False
            previous = model.attempt_state
            model.attempt_state = "EXPIRED"
            model.attempt_closed_at = observed_at
            model.attempt_close_reason = (
                model.attempt_close_reason or "confirmation_ttl_expired"
            )
            model.last_observed_at = observed_at
            self._append_attempt_event(
                session,
                attempt_id=attempt_id,
                root_event_id=root_event_id,
                event_revision=event_revision,
                evaluation_id=evaluation_id,
                event_type="attempt_state_changed",
                previous_state=previous,
                new_state="EXPIRED",
                reason="confirmation_ttl_expired",
                observed_at=observed_at,
                market_asof=market_asof,
                runtime_instance_id=runtime_instance_id,
                model_version=model_version,
                details={},
                idempotency_key=f"{attempt_id}:state:{evaluation_id}:EXPIRED",
            )
            self._append_attempt_event(
                session,
                attempt_id=attempt_id,
                root_event_id=root_event_id,
                event_revision=event_revision,
                evaluation_id=evaluation_id,
                event_type="attempt_closed",
                previous_state=previous,
                new_state="EXPIRED",
                reason="confirmation_ttl_expired",
                observed_at=observed_at,
                market_asof=market_asof,
                runtime_instance_id=runtime_instance_id,
                model_version=model_version,
                details={},
                idempotency_key=f"{attempt_id}:closed:{evaluation_id}:EXPIRED",
            )
            return True

    def record_attempt_reused_after_restart(
        self,
        *,
        attempt_id: str,
        root_event_id: str,
        event_revision: int | None,
        observed_at: datetime,
        runtime_instance_id: str | None,
        model_version: str | None,
    ) -> None:
        """Record restart reuse once per attempt and runtime instance."""
        with self._db.session() as session:
            model = session.get(ClimaxEntryAttemptModel, attempt_id)
            if not model or model.attempt_closed_at is not None:
                return
            self._append_attempt_event(
                session,
                attempt_id=attempt_id,
                root_event_id=root_event_id,
                event_revision=event_revision,
                evaluation_id=None,
                event_type="attempt_reused_after_restart",
                previous_state=model.attempt_state,
                new_state=model.attempt_state,
                reason="existing_attempt_after_runtime_restart",
                observed_at=observed_at,
                market_asof=None,
                runtime_instance_id=runtime_instance_id,
                model_version=model_version,
                details={},
                idempotency_key=f"{attempt_id}:restart:{runtime_instance_id or 'unknown'}",
            )

    def reconcile_shadow_lifecycle(
        self,
        *,
        observed_at: datetime,
        runtime_instance_id: str | None,
        model_version: str,
    ) -> dict[str, int]:
        """Reconcile expired/orphan shadow attempts without fabricating live state."""
        reconciled_expired = 0
        orphan_attempts = 0
        duplicate_terminal_events = 0
        try:
            with self._db.session() as session:
                if getattr(self._db, "is_sqlite", False):
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                root_ids = set(
                    session.scalars(select(ClimaxRootEventModel.root_event_id)).all()
                )
                attempts = session.scalars(select(ClimaxEntryAttemptModel)).all()
                for model in attempts:
                    if model.root_event_id not in root_ids:
                        orphan_detected = self._append_attempt_event(
                            session,
                            attempt_id=model.attempt_id,
                            root_event_id=model.root_event_id,
                            event_revision=None,
                            evaluation_id=None,
                            event_type="orphan_attempt_detected",
                            previous_state=model.attempt_state,
                            new_state=model.attempt_state,
                            reason="root_event_missing",
                            observed_at=observed_at,
                            market_asof=None,
                            runtime_instance_id=runtime_instance_id,
                            model_version=model_version,
                            details={},
                            idempotency_key=f"{model.attempt_id}:orphan-root",
                        )
                        orphan_attempts += int(orphan_detected)
                    if (
                        model.attempt_closed_at is None
                        and model.confirmation_expires_at is not None
                        and (
                            model.confirmation_expires_at.replace(tzinfo=timezone.utc)
                            if model.confirmation_expires_at.tzinfo is None
                            else model.confirmation_expires_at
                        )
                        <= observed_at
                    ):
                        previous = model.attempt_state
                        model.attempt_state = "EXPIRED"
                        model.attempt_closed_at = observed_at
                        model.attempt_close_reason = (
                            model.attempt_close_reason
                            or "startup_reconciliation_ttl_expired"
                        )
                        model.last_observed_at = observed_at
                        self._append_attempt_event(
                            session,
                            attempt_id=model.attempt_id,
                            root_event_id=model.root_event_id,
                            event_revision=None,
                            evaluation_id=None,
                            event_type="attempt_state_changed",
                            previous_state=previous,
                            new_state="EXPIRED",
                            reason="startup_reconciliation_ttl_expired",
                            observed_at=observed_at,
                            market_asof=None,
                            runtime_instance_id=runtime_instance_id,
                            model_version=model_version,
                            details={},
                            idempotency_key=f"{model.attempt_id}:reconcile-state:EXPIRED",
                        )
                        self._append_attempt_event(
                            session,
                            attempt_id=model.attempt_id,
                            root_event_id=model.root_event_id,
                            event_revision=None,
                            evaluation_id=None,
                            event_type="attempt_closed",
                            previous_state=previous,
                            new_state="EXPIRED",
                            reason="startup_reconciliation_ttl_expired",
                            observed_at=observed_at,
                            market_asof=None,
                            runtime_instance_id=runtime_instance_id,
                            model_version=model_version,
                            details={},
                            idempotency_key=f"{model.attempt_id}:reconcile-closed:EXPIRED",
                        )
                        reconciled_expired += 1
                duplicate_rows = session.execute(
                    select(
                        ClimaxEntryAttemptEventModel.attempt_id,
                        ClimaxEntryAttemptEventModel.event_type,
                        func.count(ClimaxEntryAttemptEventModel.id),
                    )
                    .where(ClimaxEntryAttemptEventModel.event_type == "attempt_closed")
                    .group_by(
                        ClimaxEntryAttemptEventModel.attempt_id,
                        ClimaxEntryAttemptEventModel.event_type,
                    )
                    .having(func.count(ClimaxEntryAttemptEventModel.id) > 1)
                ).all()
                duplicate_terminal_events = len(duplicate_rows)
            return {
                "expired_reconciled": reconciled_expired,
                "orphan_attempts": orphan_attempts,
                "duplicate_terminal_event_groups": duplicate_terminal_events,
                "reconciliation_failed": 0,
            }
        except Exception:
            logger.exception("shadow lifecycle reconciliation failed")
            return {
                "expired_reconciled": 0,
                "orphan_attempts": 0,
                "duplicate_terminal_event_groups": 0,
                "reconciliation_failed": 1,
            }

    def record_attempt_correlation_missing(
        self,
        *,
        root_event_id: str,
        event_revision: int | None,
        attempt_id: str | None,
        evaluation_id: int | None,
        observed_at: datetime,
        market_asof: datetime | None,
        runtime_instance_id: str | None,
        model_version: str | None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record a correlation gap without converting it into a live veto."""
        with self._db.session() as session:
            key = f"{root_event_id}:correlation-missing:{evaluation_id or observed_at.isoformat()}"
            self._append_attempt_event(
                session,
                attempt_id=attempt_id,
                root_event_id=root_event_id,
                event_revision=event_revision,
                evaluation_id=evaluation_id,
                event_type="attempt_correlation_missing",
                previous_state=None,
                new_state=None,
                reason="attempt_correlation_missing",
                observed_at=observed_at,
                market_asof=market_asof,
                runtime_instance_id=runtime_instance_id,
                model_version=model_version,
                details=details or {},
                idempotency_key=key,
            )

    @staticmethod
    def _append_attempt_event(
        session: Any,
        *,
        attempt_id: str | None,
        root_event_id: str,
        event_revision: int | None,
        evaluation_id: int | None,
        event_type: str,
        previous_state: str | None,
        new_state: str | None,
        reason: str | None,
        observed_at: datetime,
        market_asof: datetime | None,
        runtime_instance_id: str | None,
        model_version: str | None,
        details: dict[str, Any],
        idempotency_key: str,
    ) -> bool:
        existing = session.scalar(
            select(ClimaxEntryAttemptEventModel.id).where(
                ClimaxEntryAttemptEventModel.idempotency_key == idempotency_key
            )
        )
        if existing is not None:
            return False
        session.add(
            ClimaxEntryAttemptEventModel(
                attempt_id=attempt_id,
                root_event_id=root_event_id,
                event_revision=event_revision,
                evaluation_id=evaluation_id,
                event_type=event_type,
                previous_state=previous_state,
                new_state=new_state,
                reason=reason,
                observed_at=observed_at,
                market_asof=market_asof,
                runtime_instance_id=runtime_instance_id,
                model_version=model_version,
                details_json=_json_ready(details)
                if len(str(details)) <= 4096
                else {"status": "TRUNCATED"},
                idempotency_key=idempotency_key,
            )
        )
        return True

    def record_climax_monitor_event(
        self,
        *,
        created_at: datetime,
        symbol: str,
        event_id: str,
        event_high_time: datetime | None,
        action: str,
        reason: str | None,
        pool_size: int,
        poll_sequence: int | None,
        worker_id: str,
        details: dict[str, Any] | None = None,
        runtime_instance_id: str | None = None,
        root_event_id: str | None = None,
        event_revision: int | None = None,
        attempt_id: str | None = None,
        observed_at: datetime | None = None,
        market_asof: datetime | None = None,
        pool_added_at: datetime | None = None,
        event_age_sec: float | None = None,
        pool_age_sec: float | None = None,
    ) -> None:
        """Append a bounded fast-monitor lifecycle event."""
        try:
            with self._db.session() as session:
                session.add(
                    ClimaxMonitorEventModel(
                        created_at=created_at,
                        symbol=symbol,
                        event_id=event_id,
                        event_high_time=event_high_time,
                        action=action,
                        reason=reason,
                        pool_size=pool_size,
                        poll_sequence=poll_sequence,
                        worker_id=worker_id,
                        details_json=_json_ready(details or {}),
                        runtime_instance_id=runtime_instance_id
                        or getattr(self, "_runtime_instance_id", None),
                        root_event_id=root_event_id or event_id,
                        event_revision=event_revision or 1,
                        attempt_id=attempt_id,
                        observed_at=observed_at or created_at,
                        market_asof=market_asof or event_high_time,
                        pool_added_at=pool_added_at,
                        event_age_sec=_nullable_float(event_age_sec),
                        pool_age_sec=_nullable_float(pool_age_sec),
                    )
                )
        except Exception:
            logger.exception(
                "climax monitor telemetry write failed symbol=%s action=%s",
                symbol,
                action,
            )

    def update_fast_monitor_heartbeat(
        self,
        *,
        checked_at: datetime,
        pool_size: int,
        poll_sequence: int,
        last_poll_at: datetime | None = None,
        last_complete_at: datetime | None = None,
        last_error: str | None = None,
        runtime_instance_id: str | None = None,
        model_version: str | None = None,
        config_fingerprint: str | None = None,
        full_scan_running: bool = False,
        fast_monitor_running: bool = True,
        event_loop_lag_ms: float | None = None,
        poll_duration_ms: float | None = None,
    ) -> None:
        """Update the singleton fast-monitor heartbeat."""
        try:
            with self._db.session() as session:
                heartbeat = session.get(RuntimeHeartbeatModel, 1)
                if heartbeat is None:
                    heartbeat = RuntimeHeartbeatModel(id=1)
                    session.add(heartbeat)
                heartbeat.checked_at = checked_at
                heartbeat.fast_monitor_pool_size = pool_size
                heartbeat.fast_monitor_poll_sequence = poll_sequence
                if last_poll_at is not None:
                    heartbeat.fast_monitor_last_poll_at = last_poll_at
                if last_complete_at is not None:
                    heartbeat.fast_monitor_last_complete_at = last_complete_at
                if last_error is not None:
                    heartbeat.fast_monitor_last_error = last_error[:255]
                heartbeat.runtime_instance_id = (
                    runtime_instance_id
                    or getattr(self, "_runtime_instance_id", None)
                    or heartbeat.runtime_instance_id
                )
                heartbeat.model_version = (
                    model_version
                    or getattr(self, "_model_version", None)
                    or heartbeat.model_version
                )
                heartbeat.config_fingerprint = (
                    config_fingerprint
                    or getattr(self, "_config_fingerprint", None)
                    or heartbeat.config_fingerprint
                )
                session.add(
                    RuntimeHeartbeatHistoryModel(
                        created_at=checked_at,
                        runtime_instance_id=runtime_instance_id
                        or getattr(self, "_runtime_instance_id", None),
                        main_pid=os.getpid(),
                        poll_sequence=poll_sequence,
                        pool_size=pool_size,
                        last_poll_started_at=last_poll_at,
                        last_poll_completed_at=last_complete_at,
                        last_error_code=(last_error[:255] if last_error else None),
                        last_error_at=checked_at if last_error else None,
                        full_scan_running=full_scan_running,
                        fast_monitor_running=fast_monitor_running,
                        event_loop_lag_ms=_nullable_float(event_loop_lag_ms),
                        poll_duration_ms=_nullable_float(poll_duration_ms),
                    )
                )
        except Exception:
            logger.exception("fast-monitor heartbeat write failed")

    def record_reject_stat(
        self,
        *,
        symbol: str,
        timeframe: str,
        decision_type: str,
        score: int,
        reasons: list[str],
        blockers: list[str],
        risk_flags: list[str],
        close_to_watch: bool,
        squeeze_risk_level: str,
        derivatives_status: str | None = None,
        derivatives_reasons: list[str] | None = None,
        data_quality_warnings: list[str] | None = None,
        logged_at: datetime | None = None,
    ) -> None:
        with self._db.session() as session:
            session.add(
                RejectStatModel(
                    symbol=symbol,
                    timeframe=timeframe,
                    decision_type=decision_type,
                    score=score,
                    reasons_json=_json_ready(reasons),
                    blockers_json=_json_ready(blockers),
                    risk_flags_json=_json_ready(risk_flags),
                    close_to_watch=close_to_watch,
                    squeeze_risk_level=squeeze_risk_level,
                    derivatives_status=derivatives_status,
                    derivatives_reasons_json=_json_ready(derivatives_reasons or []),
                    data_quality_warnings_json=_json_ready(data_quality_warnings or []),
                    logged_at=logged_at or datetime.now(timezone.utc),
                )
            )

    def reject_reason_summary(
        self, hours: int = 24, since: datetime | None = None
    ) -> dict[str, Any]:
        with self._db.session() as session:
            latest = session.scalar(
                select(RejectStatModel.logged_at)
                .order_by(RejectStatModel.logged_at.desc())
                .limit(1)
            )
            cutoff = _resolve_since_window(
                hours=hours, since=since, anchor=_ensure_utc(latest)
            )
            rows = session.scalars(
                select(RejectStatModel)
                .where(RejectStatModel.logged_at >= cutoff)
                .order_by(RejectStatModel.logged_at.asc())
            ).all()

        by_reason: Counter[str] = Counter()
        by_symbol: Counter[str] = Counter()
        by_timeframe: Counter[str] = Counter()
        blockers: Counter[str] = Counter()
        by_derivatives_status: Counter[str] = Counter()
        derivatives_reason_counts: Counter[str] = Counter()
        data_quality_counts: Counter[str] = Counter()
        squeeze_risk_by_reason: Counter[str] = Counter()
        funding_negative_count = 0
        oi_rising_count = 0
        symbols_missing_derivatives: Counter[str] = Counter()
        symbols_high_squeeze_risk: Counter[str] = Counter()
        close_to_watch = 0
        blocked_by_squeeze_risk = 0
        for row in rows:
            by_symbol[row.symbol] += 1
            by_timeframe[row.timeframe] += 1
            if row.close_to_watch:
                close_to_watch += 1
            if row.squeeze_risk_level in {"HIGH", "EXTREME"}:
                blocked_by_squeeze_risk += 1
                symbols_high_squeeze_risk[row.symbol] += 1
            if row.derivatives_status:
                by_derivatives_status[str(row.derivatives_status)] += 1
            for reason in row.reasons_json or []:
                reason_text = str(reason)
                by_reason[reason_text] += 1
                if reason_text == "funding_negative_trap":
                    funding_negative_count += 1
                    squeeze_risk_by_reason[reason_text] += 1
                if reason_text == "oi_rising_with_price":
                    oi_rising_count += 1
                    squeeze_risk_by_reason[reason_text] += 1
                if reason_text == "squeeze_risk":
                    squeeze_risk_by_reason[reason_text] += 1
            for blocker in row.blockers_json or []:
                blocker_text = str(blocker)
                blockers[blocker_text] += 1
                if blocker_text == "squeeze_risk":
                    squeeze_risk_by_reason[blocker_text] += 1
            for derivative_reason in row.derivatives_reasons_json or []:
                derivatives_reason_counts[str(derivative_reason)] += 1
            for warning in row.data_quality_warnings_json or []:
                warning_text = str(warning)
                data_quality_counts[warning_text] += 1
                if warning_text in {"derivatives_missing", "oi_missing"}:
                    symbols_missing_derivatives[row.symbol] += 1
        return {
            "since": cutoff.isoformat(),
            "rows_in_window": len(rows),
            "checked_candidates": len(rows),
            "by_reason": dict(by_reason),
            "by_symbol": dict(by_symbol),
            "by_timeframe": dict(by_timeframe),
            "top_blockers": blockers.most_common(10),
            "close_to_watch": close_to_watch,
            "blocked_by_squeeze_risk": blocked_by_squeeze_risk,
            "by_derivatives_status": dict(by_derivatives_status),
            "derivatives_reason_counts": dict(derivatives_reason_counts),
            "data_quality_counts": dict(data_quality_counts),
            "funding_negative_count": funding_negative_count,
            "oi_rising_count": oi_rising_count,
            "squeeze_risk_by_reason": dict(squeeze_risk_by_reason),
            "symbols_missing_derivatives": dict(symbols_missing_derivatives),
            "symbols_high_squeeze_risk": dict(symbols_high_squeeze_risk),
        }

    def outcome_quality_summary(
        self, hours: int = 24, since: datetime | None = None
    ) -> dict[str, Any]:
        cutoff = _resolve_since_window(hours=hours, since=since)
        with self._db.session() as session:
            stmt = (
                select(SignalModel)
                .options(selectinload(SignalModel.outcome))
                .where(SignalModel.signal_type != "Watch")
            )
            if since is None:
                stmt = stmt.where(SignalModel.signal_time >= cutoff).order_by(
                    SignalModel.signal_time.desc()
                )
            else:
                stmt = (
                    stmt.join(SignalModel.outcome)
                    .where(SignalOutcomeModel.updated_at >= cutoff)
                    .order_by(SignalOutcomeModel.updated_at.desc())
                )
            models = session.scalars(stmt).all()

        raw_summary: Counter[str] = Counter()
        risk_summary: Counter[str] = Counter()
        worst_squeeze: list[dict[str, Any]] = []
        dirty_tps: list[dict[str, Any]] = []
        clean_tps: list[dict[str, Any]] = []
        by_symbol: Counter[str] = Counter()
        by_timeframe: Counter[str] = Counter()
        rows_in_window = 0
        for model in models:
            if model.outcome is None:
                continue
            rows_in_window += 1
            outcome = model.outcome
            by_symbol[model.symbol] += 1
            by_timeframe["15m"] += 1
            raw_key = _raw_outcome_label(outcome)
            raw_summary[raw_key] += 1
            risk_key = outcome.risk_adjusted_status or "INVALID_OR_MISSING"
            risk_summary[risk_key] += 1
            item = {
                "symbol": model.symbol,
                "signal_time": _ensure_utc(model.signal_time).isoformat(),
                "mae_pct": outcome.mae_pct,
                "mfe_pct": outcome.mfe_pct,
                "squeeze_extension_pct": outcome.squeeze_extension_pct,
                "risk_adjusted_status": outcome.risk_adjusted_status,
            }
            if risk_key == "SQUEEZE_BEFORE_TP":
                worst_squeeze.append(item)
            elif risk_key == "DIRTY_TP_HIGH_MAE":
                dirty_tps.append(item)
            elif risk_key == "CLEAN_TP":
                clean_tps.append(item)
        worst_squeeze.sort(
            key=lambda item: item.get("squeeze_extension_pct") or 0, reverse=True
        )
        dirty_tps.sort(key=lambda item: item.get("mae_pct") or 0, reverse=True)
        clean_tps.sort(key=lambda item: item.get("mfe_pct") or 0, reverse=True)
        return {
            "since": cutoff.isoformat(),
            "rows_in_window": rows_in_window,
            "raw_summary": dict(raw_summary),
            "risk_adjusted_summary": dict(risk_summary),
            "worst_squeeze_before_tp": worst_squeeze[:10],
            "dirty_tp_cases": dirty_tps[:10],
            "clean_tp_cases": clean_tps[:10],
            "by_symbol": dict(by_symbol),
            "by_timeframe": dict(by_timeframe),
        }


def _raw_outcome_label(model: SignalOutcomeModel) -> str:
    if model.tp1_hit:
        return "TP"
    if model.stopped_virtual:
        return "SL"
    return "PENDING_OR_OTHER"


def _event_from_model(model: EventStateModel) -> EventState:
    return EventState(
        symbol=model.symbol,
        event_id=model.event_id,
        state=EventStatus(model.state),
        event_start_time=_ensure_utc(model.event_start_time),
        event_high=model.event_high,
        event_high_time=_ensure_utc(model.event_high_time),
        event_base_price=model.event_base_price,
        event_range_pct=model.event_range_pct,
        event_features_snapshot=model.event_features_snapshot or {},
        trigger_window=model.trigger_window,
        pullback_detected_at=_ensure_utc(model.pullback_detected_at),
        pullback_depth_pct=model.pullback_depth_pct,
        pullback_low_price=model.pullback_low_price,
        zone_low=model.zone_low,
        zone_high=model.zone_high,
        signal_sent_at=_ensure_utc(model.signal_sent_at),
        signal_id=model.signal_id,
        expires_at=_ensure_utc(model.expires_at),
        updated_at=_ensure_utc(model.updated_at),
    )


def _signal_from_model(model: SignalModel) -> SignalRecord:
    return SignalRecord(
        id=model.id,
        symbol=model.symbol,
        signal_time=_ensure_utc(model.signal_time),
        signal_type=model.signal_type,
        grade=model.grade,
        score=model.score,
        market_price=model.market_price,
        short_zone_low=model.short_zone_low,
        short_zone_high=model.short_zone_high,
        event_high=model.event_high,
        event_base_price=model.event_base_price,
        event_range_pct=model.event_range_pct,
        pullback_from_high_pct=model.pullback_from_high_pct,
        dist_to_vwap_pct=model.dist_to_vwap_pct,
        upper_wick_ratio=model.upper_wick_ratio,
        rejection_from_high_pct=model.rejection_from_high_pct,
        vol_zscore_30m=model.vol_zscore_30m,
        dist_to_ema20_atr=model.dist_to_ema20_atr,
        rsi_15m=model.rsi_15m,
        ret_1h=model.ret_1h,
        ret_4h=model.ret_4h,
        range_atr_ratio=model.range_atr_ratio,
        oi_change_15m=model.oi_change_15m,
        oi_change_1h=model.oi_change_1h,
        funding_rate=model.funding_rate,
        context_json=model.context_json or {},
        telegram_sent=model.telegram_sent,
        created_at=_ensure_utc(model.created_at),
    )


def _watch_candidate_from_model(model: WatchCandidateModel) -> WatchCandidateRecord:
    return WatchCandidateRecord(
        id=model.id,
        symbol=model.symbol,
        timeframe=model.timeframe,
        signal_time=_ensure_utc(model.signal_time),
        score=model.score,
        base_grade=model.base_grade,
        actionable=model.actionable,
        blockers_json=model.blockers_json or [],
        risk_flags_json=model.risk_flags_json or [],
        squeeze_risk_level=model.squeeze_risk_level,
        squeeze_risk_score=model.squeeze_risk_score,
        squeeze_risk_reasons_json=model.squeeze_risk_reasons_json or [],
        data_quality_warnings_json=model.data_quality_warnings_json or [],
        context_json=model.context_json or {},
        dist_to_vwap_pct=model.dist_to_vwap_pct,
        upper_wick_ratio=model.upper_wick_ratio,
        rejection_from_high_pct=model.rejection_from_high_pct,
        volume_zscore_30m=model.volume_zscore_30m,
        pullback_from_event_high_pct=model.pullback_from_event_high_pct,
        dist_to_ema20_atr=model.dist_to_ema20_atr,
        rsi_15m=model.rsi_15m,
        spread_pct=model.spread_pct,
        orderbook_depth_1pct=model.orderbook_depth_1pct,
        telegram_sent=model.telegram_sent,
        created_at=_ensure_utc(model.created_at),
    )


def _outcome_from_model(model: SignalOutcomeModel) -> SignalOutcome:
    return SignalOutcome(
        signal_id=model.signal_id,
        price_after_15m=model.price_after_15m,
        price_after_1h=model.price_after_1h,
        price_after_4h=model.price_after_4h,
        mfe_pct=model.mfe_pct,
        mae_pct=model.mae_pct,
        reached_vwap=model.reached_vwap,
        time_to_vwap_minutes=model.time_to_vwap_minutes,
        tp1_hit=model.tp1_hit,
        stopped_virtual=model.stopped_virtual,
        risk_adjusted_status=model.risk_adjusted_status,
        squeeze_extension_pct=model.squeeze_extension_pct,
        is_clean_short=model.is_clean_short,
        is_squeeze_before_tp=model.is_squeeze_before_tp,
        updated_at=_ensure_utc(model.updated_at),
    )


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _resolve_since_window(
    *, hours: int, since: datetime | None, anchor: datetime | None = None
) -> datetime:
    if since is not None:
        return _ensure_utc(since) or datetime.now(timezone.utc)
    base = anchor or datetime.now(timezone.utc)
    return base - timedelta(hours=hours)


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    return numeric


def _required_float(value: Any, *, field_name: str) -> float:
    numeric = _nullable_float(value)
    if numeric is None:
        raise ValueError(f"{field_name} is required for actionable signal persistence")
    return numeric


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value
