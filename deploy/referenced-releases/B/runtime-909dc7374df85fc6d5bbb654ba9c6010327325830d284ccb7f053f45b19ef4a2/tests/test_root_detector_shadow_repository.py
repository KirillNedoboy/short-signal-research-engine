from datetime import datetime, timezone

from app.storage.db import Database
from app.storage.models import RootDetectorShadowCandidateModel, RootDetectorShadowEpisodeModel, RootDetectorShadowObservationModel, RootDetectorShadowEpisodeRootLinkModel, RootDetectorShadowEpisodeOutcomeModel, RootDetectorShadowLegacyMappingModel, SignalModel, TelegramDeliveryOutboxModel
from app.storage.repository import BotRepository


def _candidate():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return {
        "candidate_id": "candidate-1", "symbol": "AAAUSDT", "first_seen_at": now, "rotation_id": "rotation-1",
        "price": 110.0, "event_high": 110.0, "high_time": now, "pump_5m": 3.0, "pump_15m": 6.0,
        "pump_1h": 9.0, "pump_4h": 12.0, "volume_ratio": None, "volume_z": 1.0,
        "oi_5m": None, "oi_15m": None, "oi_1h": None, "distance_from_high": 0.0,
        "conditions_json": {"pump_5m": "PASS"}, "live_root_created": False,
        "code_version": "test", "config_hash": "c" * 64, "runtime_instance_id": "runtime", "runtime_started_at": now,
        "observations_json": [], "outcome_json": {},
    }


def test_shadow_candidate_dedupe_linkage_and_outcome_do_not_create_signal_or_outbox(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'shadow.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    assert repo.record_root_detector_shadow_candidate(_candidate()) is True
    assert repo.record_root_detector_shadow_candidate(_candidate()) is False
    assert repo.append_root_detector_shadow_observation("candidate-1", {"price": 111}) is True
    assert repo.link_root_detector_shadow_candidate("candidate-1", root_event_id="root-1", root_created_at=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc), peak_time=datetime(2026, 1, 1, tzinfo=timezone.utc)) is True
    assert repo.mature_root_detector_shadow_candidate("candidate-1", {"outcome_status": "CENSORED", "mfe_pct": 2.0, "mae_pct": 1.0, "horizons": {"15m": {"price": None}}}) is True
    with db.session() as session:
        row = session.query(RootDetectorShadowCandidateModel).one()
        assert row.live_root_created is True
        assert row.candidate_to_root_latency == 60.0
        assert session.query(SignalModel).count() == 0
        assert session.query(TelegramDeliveryOutboxModel).count() == 0


def test_episode_identity_merges_within_gap_and_reopens_after_gap(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'episodes.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    first = _candidate()
    first["candidate_id"] = "legacy-1"
    episode_1 = repo.record_root_detector_shadow_episode_observation(first)
    second = dict(first, candidate_id="legacy-2", first_seen_at=first["first_seen_at"].replace(minute=20), price=111.0)
    episode_2 = repo.record_root_detector_shadow_episode_observation(second)
    third = dict(first, candidate_id="legacy-3", first_seen_at=first["first_seen_at"].replace(minute=55), price=112.0)
    episode_3 = repo.record_root_detector_shadow_episode_observation(third)
    assert episode_1 == episode_2
    assert episode_3 != episode_1
    with db.session() as session:
        assert session.query(RootDetectorShadowEpisodeModel).count() == 2
        assert session.query(RootDetectorShadowObservationModel).count() == 3
        assert session.query(RootDetectorShadowEpisodeModel).filter_by(episode_status="CLOSED").count() == 1


def test_episode_root_links_are_append_only_and_allow_revisions(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'episode-links.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    episode_id = repo.record_root_detector_shadow_episode_observation(_candidate())
    assert episode_id
    when = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert repo.link_root_detector_shadow_episode(episode_id, root_event_id="root-1", linked_at=when)
    assert repo.link_root_detector_shadow_episode(episode_id, root_event_id="root-2", linked_at=when, link_type="REVISION", event_revision=2)
    with db.session() as session:
        assert session.query(RootDetectorShadowEpisodeRootLinkModel).count() == 2


def test_episode_outcome_scheduler_is_bounded_and_horizon_fill_is_idempotent(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'episode-outcomes.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    episode_id = repo.record_root_detector_shadow_episode_observation(_candidate())
    due = repo.list_root_detector_shadow_episode_outcomes_due(due_at=datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc), limit=1)
    assert [row["episode_id"] for row in due] == [episode_id]
    outcome = {"outcome_status": "PARTIAL", "outcome_method_version": "ROOT_SHADOW_OUTCOME_V2", "horizons": {"15m": {"short_return_pct": 2.0}}}
    assert repo.record_root_detector_shadow_episode_outcome_attempt(episode_id, outcome=outcome, next_due_at=None, computed_at=datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc))
    assert repo.record_root_detector_shadow_episode_outcome_attempt(episode_id, outcome={"outcome_status": "MATURE", "horizons": {"15m": {"short_return_pct": 9.0}}}, next_due_at=None, computed_at=datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc))
    from app.storage.models import RootDetectorShadowEpisodeOutcomeModel
    with db.session() as session:
        row = session.get(RootDetectorShadowEpisodeOutcomeModel, episode_id)
        assert row.return_15m == 2.0
        assert row.outcome_attempt_count == 2


def test_due_episode_selection_separates_runtime_and_legacy_batches(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'due-priority.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    runtime = _candidate()
    runtime["candidate_id"] = "runtime-candidate"
    runtime["first_seen_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runtime_episode = repo.record_root_detector_shadow_episode_observation(runtime)
    legacy = _candidate()
    legacy["candidate_id"] = "legacy-candidate"
    legacy["first_seen_at"] = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)
    legacy_episode = repo.record_root_detector_shadow_episode_observation(legacy)
    with db.session() as session:
        legacy_outcome = session.get(RootDetectorShadowEpisodeOutcomeModel, legacy_episode)
        legacy_outcome.outcome_status = "PARTIAL"
        legacy_outcome.outcome_next_due_at = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
        session.add(RootDetectorShadowLegacyMappingModel(
            legacy_candidate_id="legacy-candidate", episode_id=legacy_episode,
            episode_mapping_version="ROOT_EPISODE_MAP_V1", mapping_reason="TEST",
        ))
    batches = repo.list_shadow_episode_outcomes_due(
        now=datetime(2026, 1, 1, 2, tzinfo=timezone.utc), runtime_limit=10, legacy_limit=10,
    )
    assert [row["episode_id"] for row in batches["runtime"]] == [runtime_episode]
    assert [row["episode_id"] for row in batches["legacy"]] == [legacy_episode]
    assert repo.estimate_shadow_episode_rate(now=datetime(2026, 1, 1, 2, tzinfo=timezone.utc)) == 1 / 24
