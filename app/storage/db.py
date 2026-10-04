"""SQLAlchemy database bootstrap."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

DEFAULT_SQLITE_JOURNAL_MODE = "WAL"
DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5_000


class Database:
    """Thin wrapper around a SQLAlchemy engine and session factory."""

    def __init__(self, db_url: str) -> None:
        connect_args: dict[str, object] = {}
        db_url = _normalize_sqlite_url(db_url)
        self.db_url = db_url
        self.is_sqlite = db_url.startswith("sqlite:///")
        if self.is_sqlite:
            connect_args["check_same_thread"] = False
        self.engine = create_engine(db_url, future=True, connect_args=connect_args)
        if self.is_sqlite:
            _configure_sqlite_engine(
                self.engine,
                journal_mode=DEFAULT_SQLITE_JOURNAL_MODE,
                busy_timeout_ms=DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
            )
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            expire_on_commit=False,
            class_=Session,
        )

    def create_all(self, *, include_shadow_v2: bool = True) -> None:
        """Compatibility helper for tests that historically called create_all.

        Live entrypoints must run the explicit migration command before
        constructing a runtime repository. Keeping this method preserves the
        existing unit-test fixture API without making schema repair part of
        ``Database.__init__`` or ``write_heartbeat``.
        """

        from app.storage.migrations import bootstrap_test_schema

        bootstrap_test_schema(self.engine, include_shadow_v2=include_shadow_v2)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_sqlite_pragmas(self) -> dict[str, int | str] | None:
        """Return the effective SQLite pragma values for the current engine."""

        if not self.is_sqlite:
            return None
        with self.engine.connect() as connection:
            journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar_one()
            busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one()
        return {
            "journal_mode": str(journal_mode),
            "busy_timeout": int(busy_timeout),
        }

    def write_heartbeat(self) -> dict[str, int | str]:
        """Perform a lightweight write probe against an existing schema.

        The heartbeat table is created by the explicit schema bootstrap. A
        runtime health check may update or insert the singleton row, but it
        never repairs schema with DDL.
        """

        checked_at = datetime.now(timezone.utc).isoformat()
        with self.engine.begin() as connection:
            updated = connection.execute(
                text("UPDATE __db_heartbeat SET checked_at = :checked_at WHERE id = 1"),
                {"checked_at": checked_at},
            )
            if updated.rowcount == 0:
                connection.execute(
                    text("INSERT INTO __db_heartbeat (id, checked_at) VALUES (1, :checked_at)"),
                    {"checked_at": checked_at},
                )
            stored_value = connection.execute(
                text("SELECT checked_at FROM __db_heartbeat WHERE id = 1")
            ).scalar_one()

        pragmas = self.get_sqlite_pragmas() or {}
        return {
            "db_url": self.db_url,
            "checked_at": str(stored_value),
            **pragmas,
        }


def _normalize_sqlite_url(db_url: str) -> str:
    """Resolve SQLite file URLs to absolute writable filesystem paths."""

    url = make_url(db_url)
    if url.get_backend_name() != "sqlite":
        return db_url

    database = url.database
    if not database or database == ":memory:":
        return db_url

    db_path = Path(database).expanduser()
    if not db_path.is_absolute():
        db_path = db_path.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    normalized = url.set(database=str(db_path))
    return normalized.render_as_string(hide_password=False)


def _configure_sqlite_engine(engine: object, journal_mode: str, busy_timeout_ms: int) -> None:
    """Attach connect-time SQLite pragmas for long-running service behavior."""

    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_connection: object, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute(f"PRAGMA journal_mode={journal_mode}")
        cursor.fetchone()
        cursor.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        cursor.close()
