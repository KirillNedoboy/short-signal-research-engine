from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.signals.climax import advance_climax_lifecycle

UTC = timezone.utc


def _refresh(branch: str):
    root = datetime(2026, 7, 22, 8, 4, tzinfo=UTC)
    return advance_climax_lifecycle(
        branch=branch,
        root_event_id="root-1",
        root_created_at=root,
        latest_high=100.0,
        latest_high_at=root,
        confirmation_started_at=root,
        last_observed_at=root,
        event_revision=1,
        current_high=101.0,
        observed_at=root + timedelta(minutes=4),
        closed_candles_after_high=0,
        max_lifetime_minutes=15,
        confirmation_window_minutes=3,
    )


def test_volume_branch_refresh_emits_peak_updated_payload():
    result = _refresh("VOLUME_CLIMAX_UNWIND")

    assert result.lifecycle.event_revision == 2
    assert result.lifecycle.latest_high == 101.0
    assert result.lifecycle.root_created_at == datetime(2026, 7, 22, 8, 4, tzinfo=UTC)
    assert result.telemetry == {
        "event_type": "PEAK_UPDATED",
        "root_event_id": "root-1",
        "branch": "VOLUME_CLIMAX_UNWIND",
        "event_revision": 2,
        "previous_peak_high": 100.0,
        "previous_peak_high_at": (datetime(2026, 7, 22, 8, 4, tzinfo=UTC)).isoformat(),
        "peak_high": 101.0,
        "peak_high_at": (datetime(2026, 7, 22, 8, 8, tzinfo=UTC)).isoformat(),
    }


def test_low_volume_branch_refresh_is_independent_and_emits_same_contract():
    result = _refresh("LOW_VOLUME_EXTENSION_FAILURE")

    assert result.lifecycle.event_revision == 2
    assert result.telemetry["event_type"] == "PEAK_UPDATED"
    assert result.telemetry["branch"] == "LOW_VOLUME_EXTENSION_FAILURE"
    assert result.telemetry["root_event_id"] == "root-1"
