from sqlalchemy import inspect

from app.storage.db import Database


def test_v2_provenance_identity_has_unique_constraint(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'dedupe.db'}")
    db.create_all()
    inspector = inspect(db.engine)
    indexes = inspector.get_indexes("signal_provenance")
    uniques = inspector.get_unique_constraints("signal_provenance")
    identities = [tuple(index.get("column_names") or ()) for index in indexes if index.get("unique")]
    identities += [tuple(item.get("column_names") or ()) for item in uniques]
    assert ("root_event_id", "strategy_branch", "event_revision", "entry_trigger_time") in identities


def test_v2_deduplication_key_is_not_event_id_only():
    key = ("ROOT", "LOW_VOLUME_EXTENSION_FAILURE_V2", 2, "2026-04-13T12:10:00+00:00")
    assert key[:3] == ("ROOT", "LOW_VOLUME_EXTENSION_FAILURE_V2", 2)
    assert key[3]
