from datetime import datetime, timedelta, timezone

from app.storage.db import Database
from app.storage.models import ClimaxEntryAttemptModel
from app.storage.repository import BotRepository


def _repo(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'lifecycle.sqlite'}")
    db.create_all()
    return db, BotRepository(db)


def _attempt(repo, now, attempt_id, expires, state="RETEST_IN_PROGRESS"):
    return repo.upsert_shadow_entry_attempt(
        attempt_id=attempt_id,
        root_event_id="root-1",
        observed_at=now,
        local_retest_high=10.0,
        breakdown_level=9.0,
        attempt_state=state,
        confirmation_expires_at=expires,
        max_attempts_per_root_event=1,
    )


def test_born_expired_active_attempt_is_not_persisted(tmp_path):
    _, repo = _repo(tmp_path)
    now = datetime.now(timezone.utc)
    assert _attempt(repo, now, "expired", now - timedelta(seconds=1)) is False


def test_equal_expiry_is_not_persisted_as_valid_attempt(tmp_path):
    _, repo = _repo(tmp_path)
    now = datetime.now(timezone.utc)
    assert _attempt(repo, now, "equal", now) is False


def test_historical_born_expired_row_does_not_consume_valid_attempt_budget(tmp_path):
    db, repo = _repo(tmp_path)
    now = datetime.now(timezone.utc)
    with db.session() as session:
        session.add(ClimaxEntryAttemptModel(
            attempt_id="historical",
            root_event_id="root-1",
            attempt_created_at=now,
            attempt_trigger="volume_climax_lifecycle",
            confirmation_started_at=now,
            confirmation_expires_at=now - timedelta(seconds=1),
            attempt_state="EXPIRED",
            attempt_closed_at=now,
            attempt_close_reason="root_lifetime_expired",
            last_observed_at=now,
        ))
    assert _attempt(repo, now, "valid", now + timedelta(minutes=5)) is True
    assert repo.get_open_shadow_attempt_id(root_event_id="root-1") == "valid"


def test_valid_attempt_still_persists_and_has_forward_expiry(tmp_path):
    db, repo = _repo(tmp_path)
    now = datetime.now(timezone.utc)
    assert _attempt(repo, now, "valid", now + timedelta(minutes=5)) is True
    with db.engine.connect() as connection:
        row = connection.exec_driver_sql(
            "select attempt_created_at, confirmation_expires_at, attempt_state "
            "from climax_entry_attempts where attempt_id='valid'"
        ).one()
    assert row[1] > row[0]
    assert row[2] == "RETEST_IN_PROGRESS"
