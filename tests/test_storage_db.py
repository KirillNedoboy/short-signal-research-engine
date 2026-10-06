from pathlib import Path

from app.storage.db import (
    DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    DEFAULT_SQLITE_JOURNAL_MODE,
    Database,
    _normalize_sqlite_url,
)


def test_normalize_sqlite_url_makes_relative_path_absolute(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    normalized = _normalize_sqlite_url("sqlite:///./data/bot.sqlite")

    assert normalized.startswith("sqlite:///")
    assert "data" in normalized
    assert (tmp_path / "data").exists()
    assert Path(normalized.removeprefix("sqlite:///")).is_absolute()


def test_database_sets_pragmas_and_write_heartbeat(tmp_path) -> None:
    db = Database(f"sqlite:///{tmp_path / 'bot.sqlite'}")
    db.create_all()

    health = db.write_heartbeat()
    pragmas = db.get_sqlite_pragmas()

    assert health["db_url"].endswith("bot.sqlite")
    assert health["checked_at"]
    assert pragmas is not None
    assert pragmas["journal_mode"].lower() == DEFAULT_SQLITE_JOURNAL_MODE.lower()
    assert pragmas["busy_timeout"] == DEFAULT_SQLITE_BUSY_TIMEOUT_MS
