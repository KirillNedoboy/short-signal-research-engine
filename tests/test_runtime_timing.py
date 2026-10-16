from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone

import pytest

from app.config import AppConfig
from app.infra.runtime_timing import (
    METRIC_KEYS,
    CycleTiming,
    EventLoopLagMonitor,
    attach_cycle_timing,
    build_evidence_document,
    detach_cycle_timing,
    publish_evidence,
)
from app.main import ShortSignalBot
from app.storage.db import Database
from app.storage.models import TelegramDeliveryOutboxModel
from app.storage.repository import BotRepository


class _FakeScanner:
    client = None


class _FakeNotifier:
    async def send_signal(self, _message: str) -> bool:
        return True


def test_cycle_metrics_publish_all_eight_keys(tmp_path) -> None:
    evidence = tmp_path / "runtime-cycle-timing.json"
    database = Database(f"sqlite:///{tmp_path / 'timing.db'}")
    database.create_all()
    bot = ShortSignalBot(
        config=AppConfig(runtime_timing_evidence_path=str(evidence)),
        repository=BotRepository(database),
        scanner=_FakeScanner(),
        notifier=_FakeNotifier(),
    )

    timing = CycleTiming.begin(cycle_sequence=1)
    token = attach_cycle_timing(timing)
    timing.components_ns["market_fetch"] = 12_000_000
    timing.components_ns["strategy"] = 30_000_000
    timing.components_ns["outcomes"] = 4_000_000
    timing.components_ns["research"] = 2_000_000
    timing.finish(status="COMPLETED")
    detach_cycle_timing(token)
    bot._finalize_cycle_timing(timing, cycle_overrun_ms=1_000.0, skipped_ticks=3)

    payload = json.loads(evidence.read_text(encoding="utf-8"))
    metrics = payload["metrics_ms"]
    assert set(metrics) == set(METRIC_KEYS)
    assert metrics["market_fetch_ms"] == 12.0
    assert metrics["cycle_overrun_ms"] == 1_000.0
    assert metrics["storage_ms"] == 0.0
    assert metrics["cycle_total_ms"] >= 0.0
    assert payload["skipped_ticks"] == 3
    assert payload["status"] == "COMPLETED"
    assert payload["timing_semantics"] == "component_inclusive_v1"


def test_database_session_contributes_to_storage_ms(tmp_path) -> None:
    async def scenario() -> float:
        database = Database(f"sqlite:///{tmp_path / 'storage.db'}")
        database.create_all()
        timing = CycleTiming.begin(cycle_sequence=1)
        token = attach_cycle_timing(timing)
        with database.session():
            pass
        timing.finish(status="COMPLETED")
        detach_cycle_timing(token)
        return timing.metrics_ms()["storage_ms"]

    assert asyncio.run(scenario()) > 0.0


def test_task_local_timing_excludes_concurrent_delivery_storage(tmp_path) -> None:
    """A 50 ms worker-task session must never land in the cycle's storage_ms."""

    async def scenario() -> tuple[float, float]:
        database = Database(f"sqlite:///{tmp_path / 'exclusion.db'}")
        database.create_all()
        cycle = CycleTiming.begin(cycle_sequence=1)
        worker = CycleTiming.begin(cycle_sequence=2)

        async def worker_task() -> None:
            token = attach_cycle_timing(worker)
            try:
                with database.session():
                    await asyncio.sleep(0.05)
            finally:
                detach_cycle_timing(token)

        token = attach_cycle_timing(cycle)
        drain = asyncio.create_task(worker_task())
        with database.session():
            pass
        await drain
        cycle.finish(status="COMPLETED")
        detach_cycle_timing(token)
        return cycle.metrics_ms()["storage_ms"], worker.metrics_ms()["storage_ms"]

    cycle_storage, worker_storage = asyncio.run(scenario())
    assert worker_storage >= 45.0
    assert cycle_storage < 20.0


def test_lag_monitor_records_positive_max() -> None:
    async def scenario() -> float:
        monitor = EventLoopLagMonitor(probe_interval_sec=0.02)
        await monitor.start()
        monitor.begin_window()
        deadline = time.perf_counter() + 0.12
        while time.perf_counter() < deadline:
            pass
        await asyncio.sleep(0.05)
        lag = monitor.latest_lag_ms()
        await monitor.stop()
        return lag

    assert asyncio.run(scenario()) > 0.0


def test_repeated_publication_overwrites_one_document_without_db_rows(tmp_path) -> None:
    evidence = tmp_path / "runtime-cycle-timing.json"
    database = Database(f"sqlite:///{tmp_path / 'evidence.db'}")
    database.create_all()
    from sqlalchemy import func, select

    with database.session() as session:
        count_before = session.scalar(
            select(func.count()).select_from(TelegramDeliveryOutboxModel)
        )

    timing = CycleTiming.begin(cycle_sequence=1)
    timing.finish(status="COMPLETED")
    document = build_evidence_document(
        timing, runtime_instance_id="inst-1", code_version="a" * 64
    )
    publish_evidence(evidence, document)
    publish_evidence(evidence, {**document, "cycle_sequence": 2})

    assert sorted(tmp_path.glob("runtime-cycle-timing.json*")) == [evidence]
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["cycle_sequence"] == 2
    assert payload["schema_version"] == 1
    with database.session() as session:
        assert (
            session.scalar(
                select(func.count()).select_from(TelegramDeliveryOutboxModel)
            )
            == count_before
        )


def test_publish_rejects_non_finite_metrics() -> None:
    timing = CycleTiming.begin(cycle_sequence=1)
    timing.components_ns["market_fetch"] = -10**9
    timing.finish(status="COMPLETED")
    with pytest.raises(ValueError):
        build_evidence_document(timing, runtime_instance_id="i", code_version="c")
