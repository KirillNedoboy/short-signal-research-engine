from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.config import AppConfig
from app.runtime.contracts import MarketSnapshot, SnapshotQuality, SnapshotStatus
from app.runtime.pipeline import LaneCStrategyPipeline


def test_empty_frame_fails_closed_without_state_or_decision() -> None:
    result = LaneCStrategyPipeline(AppConfig(autoexecution="OFF")).evaluate(
        "SOLUSDT",
        __import__("pandas").DataFrame(),
        None,
    )
    assert result.decision is None
    assert result.blocked_reason == "EMPTY_FRAME"


def test_future_or_stale_snapshot_is_not_actionable() -> None:
    now = datetime.now(UTC)
    snapshot = MarketSnapshot(
        symbol="SOLUSDT",
        asof=now - timedelta(minutes=5),
        received_at=now,
        quality=SnapshotQuality.COMPLETE,
        status=SnapshotStatus.READY,
        last_price=100.0,
    )
    assert not snapshot.actionable(max_age=timedelta(seconds=90))
    assert not snapshot.with_status(SnapshotStatus.DISCONNECTED).actionable(
        max_age=timedelta(hours=1)
    )

