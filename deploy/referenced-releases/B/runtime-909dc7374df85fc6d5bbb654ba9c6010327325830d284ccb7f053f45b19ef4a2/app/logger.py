"""Logging helpers."""

from __future__ import annotations

import logging
import re


_TELEGRAM_BOT_TOKEN_RE = re.compile(r"bot[0-9]{6,}:[A-Za-z0-9_-]+")
_REDACTION_FACTORY_INSTALLED = False


class SecretRedactionFilter(logging.Filter):
    """Redact known secret patterns from log records before handlers emit them."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact(record.msg)
        if isinstance(record.args, dict):
            record.args = {key: _redact(value) for key, value in record.args.items()}
        elif isinstance(record.args, tuple):
            record.args = tuple(_redact(value) for value in record.args)
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


def _redact(value: object) -> object:
    if isinstance(value, str):
        return _TELEGRAM_BOT_TOKEN_RE.sub("bot<redacted>", value)
    text = str(value)
    if _TELEGRAM_BOT_TOKEN_RE.search(text):
        return _TELEGRAM_BOT_TOKEN_RE.sub("bot<redacted>", text)
    return value


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
