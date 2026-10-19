"""Fail-closed isolation and control primitives for the Lane C shadow run.

The launcher owns :class:`ShadowPaths`; the composition root owns one
:class:`ShadowControl` for the lifetime of the process.  The control stream is
deliberately small JSON Lines metadata.  It is consumed by ``tools/shadow_guard``
and is never a dump of provider responses, raw market data, or exception text.

The module only uses the standard library at import time so the launcher can
validate a shadow path before importing configuration, opening SQLite, or
constructing a provider client.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote


class ShadowIsolationError(RuntimeError):
    """Raised when a shadow path or configuration could escape its root."""


class ShadowSafetyError(RuntimeError):
    """Raised when a safety event or bounded artifact cannot be persisted."""


_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_PRODUCTION_PARTS = frozenset(
    {
        "short-telegram-bot-lite",
        "short-telegram-bot-lite-admin",
        "short-bot-lane-c",
        "short-bot-lane-a",
        "short-bot-lane-b",
    }
)
_SENSITIVE_CONFIG_FIELDS = frozenset(
    {"telegram_token", "signal_chat_id", "alerts_chat_id", "telegram_chat_id"}
)
_DEFAULT_DB_URL = "sqlite:///./data/bot.sqlite"
_DEFAULT_MARKET_STORE = "./data/lane-c-market-store.json.gz"
_DEFAULT_RESEARCH_SPOOL = "/opt/short-telegram-bot-lite-admin/shared/research_spool"
_DEFAULT_RESEARCH_HEALTH = (
    "/opt/short-telegram-bot-lite-admin/shared/runtime/research-storage-health.json"
)
_SAFE_REASON_RE = re.compile(r"^[A-Za-z0-9_.:/ -]{0,96}$")


def shadow_mode_enabled(value: str | None = None) -> bool:
    """Return whether the explicit shadow mode flag is enabled."""

    value = os.environ.get("SHADOW_MODE", "") if value is None else value
    return str(value).strip().casefold() in _TRUE_VALUES


is_shadow_mode = shadow_mode_enabled


def _absolute(value: str | Path, *, base: Path | None = None) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (base or Path.cwd()) / path
    return path


def _path_parts(path: Path) -> set[str]:
    try:
        resolved = path.resolve(strict=False)
    except OSError as exc:
        raise ShadowIsolationError(f"cannot resolve shadow path: {path}") from exc
    return {part.casefold() for part in resolved.parts}


def _looks_like_production(path: Path) -> bool:
    parts = _path_parts(path)
    if parts & _PRODUCTION_PARTS:
        return True
    for part in parts:
        if "shadow" in part or "test" in part:
            continue
        if part.startswith("short-telegram-bot-lite") or part.startswith("short-bot-lane-"):
            return True
    return False


def _reject_symlink_chain(path: Path) -> None:
    """Reject symlinks in the existing portion of a path before resolving it."""

    candidate = path
    while True:
        try:
            if candidate.is_symlink():
                raise ShadowIsolationError(
                    f"shadow path or parent is a symlink: {path}"
                )
        except OSError as exc:
            raise ShadowIsolationError(f"cannot inspect shadow path: {path}") from exc
        if candidate.parent == candidate:
            return
        candidate = candidate.parent


def _resolved_under(path: Path, root: Path, *, label: str) -> Path:
    _reject_symlink_chain(path)
    try:
        resolved = path.resolve(strict=False)
        root_resolved = root.resolve(strict=False)
        if _looks_like_production(resolved):
            raise ShadowIsolationError(f"{label} resolves into a production path")
        resolved.relative_to(root_resolved)
    except ShadowIsolationError:
        raise
    except (OSError, ValueError) as exc:
        raise ShadowIsolationError(f"{label} must be under SHADOW_ROOT") from exc
    return resolved


def _sqlite_path(value: str | Path, *, base: Path) -> Path:
    """Extract a local SQLite path from a supported SQLAlchemy URL."""

    text = str(value).strip()
    lowered = text.casefold()
    if lowered.startswith("sqlite:///"):
        raw = text.split("///", 1)[1]
        if raw.startswith("/"):
            return _absolute(unquote(raw))
        return _absolute(unquote(raw), base=base)
    if lowered.startswith("sqlite+pysqlite:///"):
        raw = text.split("///", 1)[1]
        if raw.startswith("/"):
            return _absolute(unquote(raw))
        return _absolute(unquote(raw), base=base)
    raise ShadowIsolationError("shadow DB must use a local sqlite URL")


def _shadow_db_url(path: Path) -> str:
    # SQLAlchemy accepts three slashes for an absolute POSIX path.  On Windows
    # keep the drive letter in the URL without exposing another environment
    # value that could be mistaken for a production database.
    return f"sqlite:///{path.as_posix()}"


@dataclass(frozen=True, slots=True)
class ShadowPaths:
    """All application-owned shadow paths, rooted below one private directory."""

    root: Path
    config_path: Path
    env_path: Path
    db_path: Path
    runtime_root: Path
    lock_path: Path
    checkpoint_root: Path
    comparison_root: Path
    raw_root: Path
    log_root: Path
    event_path: Path
    raw_control_path: Path
    delivery_gate_path: Path
    shadow_identity: str

    @classmethod
    def for_root(cls, root: str | Path) -> "ShadowPaths":
        """Create and validate the standard layout below ``root``."""

        requested = _absolute(root)
        _reject_symlink_chain(requested)
        if _looks_like_production(requested):
            raise ShadowIsolationError("shadow root resolves into a production path")
        try:
            requested.mkdir(parents=True, exist_ok=True)
            root_path = requested.resolve(strict=True)
        except OSError as exc:
            raise ShadowIsolationError("shadow root cannot be created") from exc
        _reject_symlink_chain(root_path)
        if _looks_like_production(root_path):
            raise ShadowIsolationError("shadow root resolves into a production path")
        try:
            root_path.chmod(0o700)
        except OSError as exc:
            raise ShadowIsolationError("shadow root permissions cannot be restricted") from exc

        directories = {
            "runtime_root": root_path / "runtime",
            "checkpoint_root": root_path / "checkpoints",
            "comparison_root": root_path / "comparisons",
            "raw_root": root_path / "raw",
            "log_root": root_path / "logs",
            "db_parent": root_path / "db",
            "lock_parent": root_path / "locks",
        }
        for directory in directories.values():
            _reject_symlink_chain(directory)
            if directory.exists() and not directory.is_dir():
                raise ShadowIsolationError(f"shadow path is not a directory: {directory}")
            try:
                directory.mkdir(parents=True, exist_ok=True)
                directory.chmod(0o700)
            except OSError as exc:
                raise ShadowIsolationError(
                    f"shadow directory cannot be created or restricted: {directory}"
                ) from exc

        values = {
            "config_path": root_path / "config.yaml",
            "env_path": root_path / "shadow.env",
            "db_path": directories["db_parent"] / "shadow.sqlite",
            "lock_path": directories["lock_parent"] / "shadow.lock",
            "event_path": directories["runtime_root"] / "shadow-events.jsonl",
            "raw_control_path": directories["runtime_root"] / "raw-capture.control.json",
            "delivery_gate_path": directories["runtime_root"] / "delivery-gate.json",
        }
        for label, path in values.items():
            _resolved_under(path, root_path, label=label)
            if path.exists() and path.is_dir():
                raise ShadowIsolationError(f"shadow path is not a file: {path}")
        identity = f"lane-c-shadow-{_stable_identity(root_path)}"
        return cls(
            root=root_path,
            config_path=values["config_path"],
            env_path=values["env_path"],
            db_path=values["db_path"],
            runtime_root=directories["runtime_root"],
            lock_path=values["lock_path"],
            checkpoint_root=directories["checkpoint_root"],
            comparison_root=directories["comparison_root"],
            raw_root=directories["raw_root"],
            log_root=directories["log_root"],
            event_path=values["event_path"],
            raw_control_path=values["raw_control_path"],
            delivery_gate_path=values["delivery_gate_path"],
            shadow_identity=identity,
        )

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "ShadowPaths":
        """Resolve explicit shadow environment paths and reject path escapes.

        Missing optional runtime directories use safe children of
        ``SHADOW_ROOT``.  Configuration and database paths never fall back to
        the release directory or its ``.env`` file.
        """

        env = dict(os.environ if environment is None else environment)
        if not shadow_mode_enabled(env.get("SHADOW_MODE")):
            raise ShadowIsolationError("SHADOW_MODE=1 is required for shadow paths")
        root_value = env.get("SHADOW_ROOT") or env.get("ROOT")
        if not root_value:
            raise ShadowIsolationError("SHADOW_ROOT is required in shadow mode")
        standard = cls.for_root(root_value)
        root = standard.root

        def explicit(name: str, *aliases: str, default: Path | None = None) -> Path:
            value = next((env.get(key) for key in (name, *aliases) if env.get(key)), None)
            return _absolute(value, base=root) if value else (default or root)

        config_value = env.get("SHADOW_CONFIG_PATH")
        if not config_value:
            raise ShadowIsolationError(
                "SHADOW_CONFIG_PATH is required; release config fallback is disabled"
            )
        config = _absolute(config_value, base=root)
        env_path = explicit(
            "SHADOW_ENV_PATH",
            "SHADOW_ENV_FILE",
            "ENV_PATH",
            default=standard.env_path,
        )
        db_value = next(
            (
                env.get(key)
                for key in ("SHADOW_DB_PATH", "DB_PATH")
                if env.get(key)
            ),
        )
        if db_value:
            db = _absolute(db_value, base=root)
        else:
            db_url = env.get("SHADOW_DB_URI") or env.get("DB_URL")
            db = _sqlite_path(db_url, base=root) if db_url else standard.db_path
        runtime = explicit(
            "SHADOW_RUNTIME_ROOT",
            "RUNTIME_ROOT",
            default=standard.runtime_root,
        )
        lock = explicit(
            "SHADOW_LOCK_PATH",
            "LOCK_PATH",
            default=standard.lock_path,
        )
        checkpoint = explicit(
            "SHADOW_CHECKPOINT_ROOT",
            "SHADOW_CHECKPOINT_PATH",
            "CHECKPOINT_ROOT",
            default=standard.checkpoint_root,
        )
        resolved = {
            "config_path": _resolved_under(config, root, label="SHADOW_CONFIG_PATH"),
            "env_path": _resolved_under(env_path, root, label="SHADOW_ENV_PATH"),
            "db_path": _resolved_under(db, root, label="SHADOW_DB_PATH"),
            "runtime_root": _resolved_under(runtime, root, label="SHADOW_RUNTIME_ROOT"),
            "lock_path": _resolved_under(lock, root, label="SHADOW_LOCK_PATH"),
            "checkpoint_root": _resolved_under(
                checkpoint, root, label="SHADOW_CHECKPOINT_ROOT"
            ),
        }
        # These are always derived below the validated root.  The service guard
        # may supply different names for config/db, but it must not redirect
        # event/control files to a release or production directory.
        for directory in (
            resolved["runtime_root"],
            resolved["checkpoint_root"],
            root / "comparisons",
            root / "raw",
            root / "logs",
            root / "db",
            root / "locks",
        ):
            _resolved_under(directory, root, label="shadow runtime directory")
            try:
                directory.mkdir(parents=True, exist_ok=True)
                directory.chmod(0o700)
            except OSError as exc:
                raise ShadowIsolationError(
                    f"shadow runtime directory cannot be created: {directory}"
                ) from exc
        for path in (
            resolved["config_path"],
            resolved["env_path"],
            resolved["db_path"],
            resolved["lock_path"],
        ):
            parent = path.parent
            _resolved_under(parent, root, label="shadow path parent")
            try:
                parent.mkdir(parents=True, exist_ok=True)
                parent.chmod(0o700)
            except OSError as exc:
                raise ShadowIsolationError(
                    f"shadow path parent cannot be created: {parent}"
                ) from exc
        return cls(
            root=root,
            config_path=resolved["config_path"],
            env_path=resolved["env_path"],
            db_path=resolved["db_path"],
            runtime_root=resolved["runtime_root"],
            lock_path=resolved["lock_path"],
            checkpoint_root=resolved["checkpoint_root"],
            comparison_root=root / "comparisons",
            raw_root=root / "raw",
            log_root=root / "logs",
            event_path=resolved["runtime_root"] / "shadow-events.jsonl",
            raw_control_path=resolved["runtime_root"] / "raw-capture.control.json",
            delivery_gate_path=resolved["runtime_root"] / "delivery-gate.json",
            shadow_identity=standard.shadow_identity,
        )

    @property
    def event_file(self) -> Path:
        """Compatibility alias matching ``shadow_guard.py``."""

        return self.event_path

    @property
    def events_path(self) -> Path:
        return self.event_path

    @property
    def control_path(self) -> Path:
        return self.raw_control_path

    @property
    def db(self) -> Path:
        return self.db_path

    @property
    def runtime(self) -> Path:
        return self.runtime_root

    @property
    def locks(self) -> Path:
        return self.lock_path.parent

    @property
    def checkpoints(self) -> Path:
        return self.checkpoint_root

    @property
    def comparisons(self) -> Path:
        return self.comparison_root

    @property
    def raw(self) -> Path:
        return self.raw_root

    @property
    def logs(self) -> Path:
        return self.log_root

    @property
    def raw_control_file(self) -> Path:
        return self.raw_control_path

    @property
    def db_uri(self) -> str:
        return _shadow_db_url(self.db_path)

    def contains(self, path: str | Path) -> bool:
        try:
            _resolved_under(_absolute(path, base=self.root), self.root, label="path")
        except ShadowIsolationError:
            return False
        return True


def _stable_identity(root: Path) -> str:
    import hashlib

    return hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:16]


def prepare_shadow_environment(paths: ShadowPaths) -> dict[str, str]:
    """Return a sanitized child environment for a shadow process."""

    inherited = dict(os.environ)
    sanitized: dict[str, str] = {}
    for key, value in inherited.items():
        upper = key.upper()
        if (
            upper in {"TELEGRAM_TOKEN", "BOT_TOKEN", "SIGNAL_CHAT_ID", "ALERTS_CHAT_ID", "TELEGRAM_CHAT_ID"}
            or "DATABASE_URL" in upper
            or "DATABASE_URI" in upper
            or upper == "DB_URL"
            or "CHAT_ID" in upper
            or "TELEGRAM" in upper
        ):
            continue
        sanitized[key] = value
    sanitized.update(
        {
            "SHADOW_MODE": "1",
            "SHADOW_ROOT": str(paths.root),
            "ROOT": str(paths.root),
            "SHADOW_CONFIG_PATH": str(paths.config_path),
            "SHADOW_ENV_PATH": str(paths.env_path),
            "SHADOW_ENV_FILE": str(paths.env_path),
            "ENV_PATH": str(paths.env_path),
            "SHADOW_DB_PATH": str(paths.db_path),
            "SHADOW_DB_URI": paths.db_uri,
            "DB_URL": paths.db_uri,
            "DB_PATH": str(paths.db_path),
            "SHADOW_RUNTIME_ROOT": str(paths.runtime_root),
            "SHADOW_LOCK_PATH": str(paths.lock_path),
            "LOCK_PATH": str(paths.lock_path),
            "SHADOW_CHECKPOINT_ROOT": str(paths.checkpoint_root),
            "CHECKPOINT_ROOT": str(paths.checkpoint_root),
            "SHADOW_COMPARISON_ROOT": str(paths.comparison_root),
            "SHADOW_RAW_ROOT": str(paths.raw_root),
            "SHADOW_LOG_ROOT": str(paths.log_root),
            "SHADOW_EVENT_FILE": str(paths.event_path),
            "SHADOW_RAW_CONTROL_FILE": str(paths.raw_control_path),
            "SHADOW_DELIVERY_GATE_FILE": str(paths.delivery_gate_path),
            "SHADOW_IDENTITY": paths.shadow_identity,
            "AUTOEXECUTION": "OFF",
            "SHADOW_DELIVERY_GATE": "CLOSED",
            "DELIVERY_GATE": "CLOSED",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "XDG_CACHE_HOME": str(paths.runtime_root / "cache"),
            "TMPDIR": str(paths.runtime_root / "tmp"),
        }
    )
    for directory in (paths.runtime_root / "cache", paths.runtime_root / "tmp"):
        try:
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o700)
        except OSError as exc:
            raise ShadowIsolationError(f"shadow temporary path cannot be created: {directory}") from exc
    return sanitized


def shadow_paths_from_environment(
    environment: Mapping[str, str] | None = None,
) -> ShadowPaths:
    """Functional alias for callers that do not need the class namespace."""

    return ShadowPaths.from_environment(environment)


def _model_values(config: Any) -> dict[str, Any]:
    if hasattr(config, "model_dump"):
        return dict(config.model_dump())
    if hasattr(config, "dict"):
        return dict(config.dict())
    return dict(vars(config))


def _copy_config(config: Any, updates: dict[str, Any]) -> Any:
    if hasattr(config, "model_copy"):
        return config.model_copy(update=updates)
    if hasattr(config, "copy"):
        return config.copy(update=updates)
    values = _model_values(config)
    values.update(updates)
    return type(config)(**values)


def _default_or_under(value: Any, paths: ShadowPaths, *, field: str) -> Path | None:
    if value is None or not isinstance(value, (str, Path)):
        return None
    text = str(value)
    if field == "market_frame_snapshot_path" and text in {
        _DEFAULT_MARKET_STORE,
        "data/lane-c-market-store.json.gz",
    }:
        return paths.runtime_root / "market-store.json.gz"
    if field == "research_spool_root" and text == _DEFAULT_RESEARCH_SPOOL:
        return paths.root / "research-spool"
    if field == "research_storage_health_path" and text == _DEFAULT_RESEARCH_HEALTH:
        return paths.runtime_root / "research-storage-health.json"
    if field == "db_url" and text in {_DEFAULT_DB_URL, "sqlite:///data/bot.sqlite"}:
        return paths.db_path
    if field == "db_url":
        return _sqlite_path(text, base=paths.root)
    return _absolute(text, base=paths.root)


def validate_shadow_config(config: Any, paths: ShadowPaths) -> Any:
    """Validate and normalize all known writable C config paths.

    The returned config has the shadow database and default market-store paths
    forced below ``SHADOW_ROOT``.  A user-specified path that escapes the root
    raises before the composition root can construct a database or provider.
    """

    values = _model_values(config)
    autoexecution = values.get("autoexecution")
    if autoexecution != "OFF":
        raise ShadowIsolationError("AUTOEXECUTION must be explicitly OFF in shadow mode")
    if any(values.get(field) not in (None, "") for field in _SENSITIVE_CONFIG_FIELDS):
        raise ShadowIsolationError("Telegram credentials are forbidden in shadow mode")

    updates: dict[str, Any] = {
        "db_url": paths.db_uri,
        "autoexecution": "OFF",
        "telegram_token": None,
        "signal_chat_id": None,
        "alerts_chat_id": None,
    }
    path_fields = {
        "db_url",
        "market_frame_snapshot_path",
        "research_spool_root",
        "research_storage_health_path",
    }
    for field, value in values.items():
        field_lower = str(field).casefold()
        if field not in path_fields and not any(
            marker in field_lower for marker in ("_path", "_root", "_dir", "_directory")
        ):
            continue
        candidate = _default_or_under(value, paths, field=field)
        if candidate is None:
            continue
        resolved = _resolved_under(candidate, paths.root, label=field)
        if field != "db_url":
            updates[field] = str(resolved)
    return _copy_config(config, updates)


def load_shadow_config(
    paths: ShadowPaths,
    *,
    loader: Any,
    manual_only: Any,
) -> Any:
    """Load only the validated shadow config/env pair and enforce its fences."""

    # ``loader`` and ``manual_only`` are injected to keep this stdlib module
    # importable before app.config.  No release ``.env`` fallback is possible.
    config = loader(config_path=paths.config_path, env_path=paths.env_path)
    config = manual_only(config)
    return validate_shadow_config(config, paths)


def _safe_reason(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ")[:96]
    return text if _SAFE_REASON_RE.fullmatch(text) else type(value).__name__


def _status_code(value: Any) -> int | None:
    for candidate in (
        getattr(value, "provider_code", None),
        getattr(value, "status_code", None),
        getattr(value, "status", None),
        getattr(value, "code", None),
        getattr(value, "ret_code", None),
        getattr(value, "retCode", None),
    ):
        try:
            integer = int(candidate)
        except (TypeError, ValueError):
            continue
        if integer in {429, 10006}:
            return integer
    for attribute in ("response", "resp", "http_response"):
        nested = getattr(value, attribute, None)
        if nested is not None:
            code = _status_code(nested)
            if code is not None:
                return code
    return None


def classify_rate_limit(value: Any) -> int | None:
    """Return 429 or Bybit 10006 for known REST/WS rate-limit failures."""

    code = _status_code(value)
    if code is not None:
        return code
    text = str(value).casefold()
    type_name = type(value).__name__.casefold()
    if (
        "10006" in text
        or "10006" in type_name
        or "too many visits" in text
        or "rate limit" in text
        or "rate-limit" in text
        or "rate_limited" in text
    ):
        return 10006
    if (
        "http 429" in text
        or "http429" in type_name
        or "status 429" in text
        or "429 too many" in text
        or re.search(r"\b429\b", text)
    ):
        return 429
    return None


def _transport(value: Any, explicit: str | None) -> str:
    name = str(explicit or "").casefold()
    if name in {"ws", "websocket", "web_socket"}:
        return "ws"
    if name in {"rest", "http", "httpx", "requests"}:
        return "rest"
    logger_name = str(getattr(value, "name", "")).casefold()
    if (
        "ws" in logger_name
        or "websocket" in logger_name
        or "market_data" in logger_name
    ):
        return "ws"
    return "rest"


@dataclass(frozen=True, slots=True)
class RawArtifactResult:
    path: Path | None
    written: bool
    reason: str | None = None


class ShadowControl:
    """Bounded supervisor control stream and safety state.

    ``emit_capability`` must be called before a storage guard can request raw
    capture shutdown.  The parent runtime should call ``poll_control`` at each
    bounded scan boundary and ``set_readiness(False)`` during shutdown.
    """

    def __init__(
        self,
        paths: ShadowPaths,
        *,
        max_events: int = 256,
        max_event_bytes: int = 256 * 1024,
        max_artifact_bytes: int = 16 * 1024 * 1024,
        max_raw_bytes: int = 1024 * 1024 * 1024,
    ) -> None:
        if max_events < 1 or max_event_bytes < 512:
            raise ValueError("shadow event bounds must be positive")
        if max_artifact_bytes < 1 or max_raw_bytes < 1:
            raise ValueError("shadow artifact bounds must be positive")
        self.paths = paths
        self.max_events = int(max_events)
        self.max_event_bytes = int(max_event_bytes)
        self.max_artifact_bytes = int(max_artifact_bytes)
        self.max_raw_bytes = int(max_raw_bytes)
        self._events = 0
        self._raw_capture_enabled = True
        self._raw_disable_ack = False
        self._raw_disable_control_fingerprint: str | None = None
        self._readiness = False
        self._rate_limit_seen = False
        self._closed = False
        self._safety_failure: ShadowSafetyError | None = None
        self._raw_bytes_written = 0
        self._comparison_bytes_written = 0
        self._ensure_paths()
        self._events = self._existing_event_count()
        if self._events >= self.max_events:
            failure = ShadowSafetyError("shadow control JSONL bound exhausted")
            self._safety_failure = failure
            raise failure

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None, **kwargs: Any) -> "ShadowControl":
        return cls(ShadowPaths.from_environment(environment), **kwargs)

    @property
    def raw_capture_enabled(self) -> bool:
        return self._raw_capture_enabled

    @property
    def raw_capture_disabled(self) -> bool:
        return not self._raw_capture_enabled

    @property
    def raw_disable_acknowledged(self) -> bool:
        return self._raw_disable_ack

    @property
    def readiness(self) -> bool:
        return self._readiness

    @property
    def rate_limit_seen(self) -> bool:
        return self._rate_limit_seen

    @property
    def safety_failed(self) -> bool:
        return self._safety_failure is not None

    @property
    def raw_bytes_written(self) -> int:
        return self._raw_bytes_written

    @property
    def comparison_bytes_written(self) -> int:
        return self._comparison_bytes_written

    def _ensure_paths(self) -> None:
        for directory in (
            self.paths.runtime_root,
            self.paths.checkpoint_root,
            self.paths.comparison_root,
            self.paths.raw_root,
            self.paths.log_root,
        ):
            try:
                directory.mkdir(parents=True, exist_ok=True)
                directory.chmod(0o700)
            except OSError as exc:
                raise ShadowIsolationError(f"shadow path cannot be prepared: {directory}") from exc
        for path in (
            self.paths.event_path,
            self.paths.raw_control_path,
            self.paths.delivery_gate_path,
        ):
            _resolved_under(path, self.paths.root, label="shadow control path")
            if path.exists() and path.is_dir():
                raise ShadowIsolationError(f"shadow control path is a directory: {path}")

    def _existing_event_count(self) -> int:
        if not self.paths.event_path.exists():
            return 0
        count = 0
        try:
            with self.paths.event_path.open("rb") as handle:
                for _line in handle:
                    count += 1
                    if count > self.max_events:
                        return count
        except OSError as exc:
            raise ShadowSafetyError("cannot inspect existing shadow safety events") from exc
        return count

    def _safe_event(self, event_type: str, **values: Any) -> dict[str, Any]:
        event: dict[str, Any] = {
            "type": str(event_type)[:80],
            "at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        for key in (
            "ready",
            "raw_capture_disable",
            "count",
            "code",
            "transport",
            "reason",
            "action",
            "schema_version",
        ):
            if key not in values:
                continue
            value = values[key]
            if key in {"ready", "raw_capture_disable"}:
                event[key] = bool(value)
            elif key in {"count", "code", "schema_version"}:
                try:
                    event[key] = max(0, int(value))
                except (TypeError, ValueError):
                    continue
            elif key == "transport":
                normalized = str(value).casefold()
                if normalized in {"rest", "ws"}:
                    event[key] = normalized
            elif key == "action":
                if str(value) == "disable_raw_capture":
                    event[key] = str(value)
            else:
                safe = _safe_reason(value)
                if safe is not None:
                    event[key] = safe
        return event

    def emit(self, event_type: str, **values: Any) -> dict[str, Any]:
        """Append one bounded metadata event, failing closed on any write error."""

        if self._closed and event_type != "readiness":
            raise ShadowSafetyError("shadow control is closed")
        event = self._safe_event(event_type, **values)
        encoded = (json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        try:
            current_size = self.paths.event_path.stat().st_size if self.paths.event_path.exists() else 0
            if self._events >= self.max_events or current_size + len(encoded) > self.max_event_bytes:
                raise ShadowSafetyError("shadow control JSONL bound exhausted")
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_CLOEXEC", 0)
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self.paths.event_path, flags | nofollow, 0o600)
            try:
                written = os.write(descriptor, encoded)
                if written != len(encoded):
                    raise OSError("short shadow control write")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._events += 1
            return event
        except ShadowSafetyError as exc:
            self._safety_failure = exc
            raise
        except (OSError, UnicodeError, TypeError, ValueError) as exc:
            failure = ShadowSafetyError("cannot write shadow safety event")
            self._safety_failure = failure
            raise failure from exc

    def emit_capability(self) -> dict[str, Any]:
        return self.emit(
            "capability", raw_capture_disable=True, schema_version=1
        )

    def set_readiness(self, ready: bool, *, reason: str | None = None) -> dict[str, Any]:
        self._readiness = bool(ready)
        return self.emit("readiness", ready=self._readiness, reason=reason)

    def poll_control(self) -> str | None:
        """Consume a supervisor control action and acknowledge it once."""

        path = self.paths.raw_control_path
        if not path.exists():
            return None
        try:
            payload_bytes = path.read_bytes()
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ShadowSafetyError("shadow raw-control file cannot be read") from exc
        if not isinstance(payload, dict):
            raise ShadowSafetyError("shadow raw-control payload must be an object")
        action = payload.get("action")
        if action != "disable_raw_capture":
            return None
        fingerprint = payload_bytes.hex()
        if fingerprint == self._raw_disable_control_fingerprint:
            return "disable_raw_capture"
        self._raw_disable_control_fingerprint = fingerprint
        self._raw_capture_enabled = False
        self._raw_disable_ack = True
        self.emit("raw_capture_disabled", action="disable_raw_capture")
        return "disable_raw_capture"

    def disable_raw_capture(self, *, reason: str = "supervisor_request") -> bool:
        if not self._raw_capture_enabled:
            return False
        self._raw_capture_enabled = False
        self._raw_disable_ack = True
        self.emit("raw_capture_disabled", reason=reason)
        return True

    handle_control = poll_control

    def record_rate_limit(self, error: Any, *, transport: str | None = None) -> bool:
        """Record a REST/WS 429 or Bybit 10006 and mark shadow unsafe."""

        code = classify_rate_limit(error)
        if code is None:
            return False
        channel = _transport(error, transport)
        self._rate_limit_seen = True
        event_type = "ws_429" if channel == "ws" else "rest_429"
        count = self._rate_limit_count(channel) + 1
        self.emit(event_type, count=count, code=code, transport=channel)
        return True

    def _rate_limit_count(self, channel: str) -> int:
        count = 0
        if not self.paths.event_path.exists():
            return count
        try:
            with self.paths.event_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if event.get("type") == f"{channel}_429":
                        count += 1
        except OSError as exc:
            failure = ShadowSafetyError("cannot inspect shadow rate-limit events")
            self._safety_failure = failure
            raise failure from exc
        return count

    def write_bounded_artifact(
        self,
        relative_path: str | Path,
        payload: bytes | bytearray | memoryview | str,
        *,
        comparison: bool = False,
    ) -> RawArtifactResult:
        """Write one bounded raw/comparison artifact below the shadow root.

        Raw capture stops immediately after the supervisor control action.  A
        comparison artifact is allowed after that point, but this API records
        no parity result; a caller must perform a real reference comparison.
        """

        if not comparison and not self._raw_capture_enabled:
            return RawArtifactResult(None, False, "raw_capture_disabled")
        if isinstance(payload, str):
            data = payload.encode("utf-8")
        else:
            data = bytes(payload)
        if len(data) > self.max_artifact_bytes:
            return RawArtifactResult(None, False, "artifact_too_large")
        base = self.paths.comparison_root if comparison else self.paths.raw_root
        target = _absolute(relative_path, base=base)
        try:
            target = _resolved_under(target, self.paths.root, label="artifact path")
            target.relative_to(base.resolve())
        except (ShadowIsolationError, ValueError) as exc:
            return RawArtifactResult(None, False, "artifact_path_escape")
        used = self._comparison_bytes_written if comparison else self._raw_bytes_written
        if used + len(data) > (self.max_raw_bytes if not comparison else self.max_raw_bytes):
            if not comparison:
                self.disable_raw_capture(reason="raw_budget_exhausted")
            return RawArtifactResult(None, False, "artifact_budget_exhausted")
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
            temporary.write_bytes(data)
            os.replace(temporary, target)
        except OSError as exc:
            failure = ShadowSafetyError("cannot write bounded shadow artifact")
            self._safety_failure = failure
            raise failure from exc
        if comparison:
            self._comparison_bytes_written += len(data)
        else:
            self._raw_bytes_written += len(data)
        return RawArtifactResult(target, True)

    # Concise aliases used by runtime adapters and tests.
    write_artifact = write_bounded_artifact
    on_rate_limit = record_rate_limit
    handle_rate_limit = record_rate_limit

    def record_rest_rate_limit(self, error: Any) -> bool:
        return self.record_rate_limit(error, transport="rest")

    def record_ws_rate_limit(self, error: Any) -> bool:
        return self.record_rate_limit(error, transport="ws")

    def shutdown(self, *, reason: str = "shutdown") -> None:
        if self._closed:
            return
        self._readiness = False
        self.emit("readiness", ready=False, reason=reason)
        self._closed = True

    close = shutdown

    def __enter__(self) -> "ShadowControl":
        return self

    def __exit__(self, *_: object) -> None:
        self.shutdown()


_ACTIVE_CONTROL: ShadowControl | None = None


def get_shadow_control(
    paths: ShadowPaths | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    **kwargs: Any,
) -> ShadowControl:
    """Return the process-wide control object for one shadow root.

    The launcher and composition root can both call this factory without
    creating competing JSONL writers.  A different root is a configuration
    error because two shadow identities in one process would make readiness
    and 429 stops ambiguous.
    """

    global _ACTIVE_CONTROL
    selected = paths or ShadowPaths.from_environment(environment)
    if _ACTIVE_CONTROL is None:
        _ACTIVE_CONTROL = ShadowControl(selected, **kwargs)
        return _ACTIVE_CONTROL
    if _ACTIVE_CONTROL.paths.root != selected.root:
        raise ShadowIsolationError("multiple shadow roots in one process")
    return _ACTIVE_CONTROL


class ShadowRateLimitLogHandler(logging.Handler):
    """Bridge provider log records to the machine-readable stop event."""

    def __init__(self, control: ShadowControl, *, transport: str | None = None) -> None:
        super().__init__(level=logging.WARNING)
        self.control = control
        self.transport = transport

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.control.record_rate_limit(record, transport=self.transport)
        except ShadowSafetyError:
            # Logging must not hide the fail-closed state.  The runtime can
            # inspect ``control.safety_failed`` and stop admission.
            return


RateLimitLogHandler = ShadowRateLimitLogHandler


def install_rate_limit_handler(
    control: ShadowControl,
    logger: logging.Logger | None = None,
    *,
    transport: str | None = None,
) -> ShadowRateLimitLogHandler:
    """Install one handler on an application logger and return it."""

    target = logger or logging.getLogger()
    for existing in target.handlers:
        if isinstance(existing, ShadowRateLimitLogHandler) and existing.control is control:
            return existing
    handler = ShadowRateLimitLogHandler(control, transport=transport)
    target.addHandler(handler)
    return handler


attach_rate_limit_handler = install_rate_limit_handler


__all__ = [
    "RawArtifactResult",
    "RateLimitLogHandler",
    "ShadowControl",
    "ShadowIsolationError",
    "ShadowPaths",
    "ShadowRateLimitLogHandler",
    "ShadowSafetyError",
    "attach_rate_limit_handler",
    "classify_rate_limit",
    "get_shadow_control",
    "install_rate_limit_handler",
    "is_shadow_mode",
    "load_shadow_config",
    "prepare_shadow_environment",
    "shadow_mode_enabled",
    "shadow_paths_from_environment",
    "validate_shadow_config",
]
