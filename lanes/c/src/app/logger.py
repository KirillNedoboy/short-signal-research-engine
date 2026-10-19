"""Logging helpers."""

from __future__ import annotations

import logging
import re
import traceback
from collections.abc import Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_TELEGRAM_BOT_TOKEN_RE = re.compile(r"bot[0-9]{6,}:[A-Za-z0-9_-]+", re.IGNORECASE)
_AUTH_RE = re.compile(
    r"(\bauthorization\s*:\s*(?:(?:bearer|basic)\s+)?|\b(?:bearer|basic)\s+)([^\s,;]+)",
    re.IGNORECASE,
)
_KEY_VALUE_RE = re.compile(
    r"(\b(?:access[_-]?token|api[_-]?key|auth[_-]?token|client[_-]?(?:id|secret)|id[_-]?token|password|passwd|private[_-]?key|refresh[_-]?token|secret|session[_-]?token|signature|signing[_-]?key|token)\s*[=:]\s*)([^\s,;&]+)",
    re.IGNORECASE,
)
_DB_URL_RE = re.compile(r"\b([a-z][a-z0-9+.-]*://)([^\s/@:]+)(:([^\s/@]+))?@", re.IGNORECASE)
_SECRET_QUERY_KEYS = {
    "access_token", "api_key", "apikey", "auth_token", "client_id", "client_secret",
    "id_token", "password", "passwd", "private_key", "refresh_token", "secret",
    "session_token", "signature", "signing_key", "token",
}
_MAX_SAFE_TEXT_LENGTH = 2000
_REDACTION_FACTORY_INSTALLED = False


class SecretRedactionFilter(logging.Filter):
    """Sanitize known secret patterns from log records before handlers emit them."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _sanitize_value(record.msg)
        if isinstance(record.args, Mapping):
            record.args = {key: _sanitize_value(value) for key, value in record.args.items()}
        elif isinstance(record.args, tuple):
            record.args = tuple(_sanitize_value(value) for value in record.args)

        if record.exc_info:
            record.exc_text = sanitize_text("".join(traceback.format_exception(*record.exc_info)))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = sanitize_text(record.exc_text)
        return True


def configure_logging(level: int = logging.INFO) -> None:
    """Configure process-wide logging once."""

    _install_log_record_factory()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    root = logging.getLogger()
    if not any(isinstance(filter_, SecretRedactionFilter) for filter_ in root.filters):
        root.addFilter(SecretRedactionFilter())
    for handler in root.handlers:
        if not any(isinstance(filter_, SecretRedactionFilter) for filter_ in handler.filters):
            handler.addFilter(SecretRedactionFilter())


def sanitize_text(value: object, *, max_length: int = _MAX_SAFE_TEXT_LENGTH) -> str:
    """Return bounded text with credentials and secret-like values removed."""

    text = str(value)
    text = _TELEGRAM_BOT_TOKEN_RE.sub("bot<redacted>", text)
    text = _DB_URL_RE.sub(lambda match: f"{match.group(1)}{match.group(2)}:***@", text)
    text = _AUTH_RE.sub(lambda match: f"{match.group(1)}<redacted>", text)
    text = _KEY_VALUE_RE.sub(lambda match: f"{match.group(1)}<redacted>", text)
    text = _sanitize_url_queries(text)
    if max_length <= 0:
        return ""
    if len(text) > max_length:
        if max_length <= ellipsis_length:
            return "." * max_length
        text = text[: max_length - ellipsis_length] + "..."
    return text


# Kept separate so the truncation expression remains easy to audit.
ellipsis_length = 3


def safe_exception_summary(exc: BaseException) -> str:
    """Return only the exception type and bounded, sanitized reason."""

    reason = sanitize_text(str(exc)).replace("\r", " ").replace("\n", " ").strip()
    return f"{type(exc).__name__}: {reason}" if reason else type(exc).__name__


def _sanitize_url_queries(text: str) -> str:
    def replace_url(match: re.Match[str]) -> str:
        raw = match.group(0)
        try:
            parts = urlsplit(raw)
            if not parts.query:
                return raw
            query = [
                (key, "<redacted>" if key.lower().replace("-", "_") in _SECRET_QUERY_KEYS else value)
                for key, value in parse_qsl(parts.query, keep_blank_values=True)
            ]
            return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, safe="<>"), parts.fragment))
        except ValueError:
            return raw

    return re.sub(r"\b[a-z][a-z0-9+.-]*://[^\s]+", replace_url, text, flags=re.IGNORECASE)


def _sanitize_value(value: object) -> object:
    if isinstance(value, str):
        return sanitize_text(value)
    rendered = str(value)
    return sanitize_text(rendered) if _contains_secret(rendered) else value


def _contains_secret(text: str) -> bool:
    return bool(
        _TELEGRAM_BOT_TOKEN_RE.search(text)
        or _AUTH_RE.search(text)
        or _KEY_VALUE_RE.search(text)
        or _DB_URL_RE.search(text)
        or _sanitize_url_queries(text) != text
    )


def _install_log_record_factory() -> None:
    global _REDACTION_FACTORY_INSTALLED
    if _REDACTION_FACTORY_INSTALLED:
        return

    previous_factory = logging.getLogRecordFactory()

    def redacting_factory(*args: object, **kwargs: object) -> logging.LogRecord:
        record = previous_factory(*args, **kwargs)
        SecretRedactionFilter().filter(record)
        return record

    logging.setLogRecordFactory(redacting_factory)
    _REDACTION_FACTORY_INSTALLED = True
