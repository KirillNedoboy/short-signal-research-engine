from datetime import datetime, timedelta, timezone

from app.observability.outcome_scheduler import terminalize_due_horizons
from app.storage.db import Database
from app.storage.models import (
    CurrentRootOutcomeModel,
    CurrentRootPredicateSnapshotModel,
)
from app.storage.repository import BotRepository

UTC = timezone.utc


def test_live_root_outcome_ledger_uses_live_root_anchor_and_persists_horizons(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'telemetry.sqlite'}")
    db.create_all()
    repository = BotRepository(db)
    anchor = datetime(2026, 1, 1, tzinfo=UTC)

    assert repository.record_live_root_outcome_attempt(
        root_event_id="root-live-1",
        episode_id="episode-1",
        symbol="AAAUSDT",
        anchor_time=anchor,
        anchor_price=100.0,
        outcome={
            "anchor_type": "LIVE_ROOT",
            "outcome_status": "MATURE",
            "horizons": {
                "1h": {"short_return_pct": 2.0, "price": 98.0},
                "4h": {"short_return_pct": 5.0, "price": 95.0},
                "12h": {"short_return_pct": 7.0, "price": 93.0},
                "24h": {"short_return_pct": 9.0, "price": 91.0},
            },
            "mfe_by_horizon": {"1h": 3.0, "4h": 6.0, "12h": 8.0, "24h": 10.0},
            "mae_by_horizon": {"1h": 1.0, "4h": 2.0, "12h": 3.0, "24h": 4.0},
            "new_high_after_candidate": False,
            "coverage_end_at": anchor + timedelta(hours=24),
        },
        next_due_at=None,
        computed_at=anchor + timedelta(hours=24),
    )

    with db.session() as session:
        row = session.get(CurrentRootOutcomeModel, "root-live-1")
        assert row is not None
        assert row.anchor_type == "LIVE_ROOT"
        assert row.anchor_time.replace(tzinfo=UTC) == anchor
        assert row.anchor_price == 100.0
        assert row.outcome_status == "MATURE"
        assert row.return_1h == 2.0
        assert row.return_4h == 5.0
        assert row.return_12h == 7.0
        assert row.return_24h == 9.0
        assert row.mfe_24h == 10.0
        assert row.mae_24h == 4.0
        assert row.new_high_after_anchor is False


def test_due_missing_horizon_terminalizes_partial_as_explicit_data_gap():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    outcome = {
        "outcome_status": "PARTIAL",
        "horizons": {
            "1h": {"price": 98.0},
            "4h": {"price": None},
            "12h": {"price": None},
            "24h": {"price": None},
        },
    }

    terminalized = terminalize_due_horizons(
        anchor_time=anchor,
        market_asof=anchor + timedelta(hours=5),
        outcome=outcome,
    )

    assert terminalized["outcome_status"] == "DATA_GAP"
    assert terminalized["horizons"]["4h"]["status"] == "DATA_GAP"
    assert terminalized["horizons"]["4h"]["price"] is None


def test_current_root_predicate_snapshot_persists_grouped_pass_fail_missing(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'telemetry.sqlite'}")
    db.create_all()
    repository = BotRepository(db)
    observed_at = datetime(2026, 1, 1, tzinfo=UTC)

    assert repository.record_current_root_predicate_snapshot(
        root_event_id="root-live-1",
        episode_id="episode-1",
        evaluation_id=42,
        observed_at=observed_at,
        snapshot={
            "temporal": {"maturity": "PASS"},
            "structural": {"retest": "FAIL"},
            "safety": {"new_high": "MISSING"},
            "liquidity": {"spread": "PASS"},
            "oi_squeeze": {"oi_change": "FAIL"},
        },
    )

    with db.session() as session:
        row = session.query(CurrentRootPredicateSnapshotModel).one()
        assert row.root_event_id == "root-live-1"
        assert row.evaluation_id == 42
        assert row.snapshot_json["temporal"]["maturity"] == "PASS"
        assert row.snapshot_json["structural"]["retest"] == "FAIL"
        assert row.snapshot_json["safety"]["new_high"] == "MISSING"
        assert row.snapshot_json["liquidity"]["spread"] == "PASS"
        assert row.snapshot_json["oi_squeeze"]["oi_change"] == "FAIL"
