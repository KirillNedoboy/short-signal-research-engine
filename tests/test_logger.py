import logging
import sys

from app.logger import (
    SecretRedactionFilter,
    configure_logging,
    safe_exception_summary,
    sanitize_text,
)


def test_secret_redaction_filter_removes_telegram_bot_token() -> None:
    record = logging.LogRecord(
        name="httpx", level=logging.INFO, pathname=__file__, lineno=1,
        msg="HTTP Request: POST %s",
        args=("https://api.telegram.org/bot123456789:SECRET_token-1/getMe",),
        exc_info=None,
    )
    SecretRedactionFilter().filter(record)
    message = record.getMessage()
    assert "123456789:SECRET_token-1" not in message
    assert "bot<redacted>" in message


def test_configure_logging_redacts_tokens_from_any_logger(caplog) -> None:
    configure_logging()
    with caplog.at_level(logging.INFO, logger="httpx"):
        logging.getLogger("httpx").info(
            "HTTP Request: POST %s",
            "https://api.telegram.org/bot123456789:SECRET_token-1/getMe",
        )
    assert "123456789:SECRET_token-1" not in caplog.text
    assert "bot<redacted>" in caplog.text


def test_secret_redaction_filter_redacts_token_from_url_like_object() -> None:
    class UrlLike:
        def __str__(self) -> str:
            return "https://api.telegram.org/bot123456789:SECRET_token-1/getMe"

    record = logging.LogRecord(
        name="httpx", level=logging.INFO, pathname=__file__, lineno=1,
        msg="HTTP Request: POST %s", args=(UrlLike(),), exc_info=None,
    )
    SecretRedactionFilter().filter(record)
    message = record.getMessage()
    assert "123456789:SECRET_token-1" not in message
    assert "bot<redacted>" in message


def test_secret_redaction_filter_sanitizes_sensitive_url_like_query_objects() -> None:
    class UrlLike:
        def __str__(self) -> str:
            return "https://example.test/callback?access_token=url-secret&x=1"

    record = logging.LogRecord(
        name="httpx", level=logging.INFO, pathname=__file__, lineno=1,
        msg="HTTP Request: %s", args=(UrlLike(),), exc_info=None,
    )
    SecretRedactionFilter().filter(record)
    message = record.getMessage()
    assert "url-secret" not in message
    assert "x=1" in message


def test_secret_redaction_filter_sanitizes_non_string_object_even_if_equal_to_rendering() -> None:
    class EqualToRendering:
        def __str__(self) -> str:
            return "api_key=object-secret"

        def __eq__(self, other: object) -> bool:
            return other == "api_key=object-secret"

    record = logging.LogRecord(
        name="runtime", level=logging.INFO, pathname=__file__, lineno=1,
        msg="value=%s", args=(EqualToRendering(),), exc_info=None,
    )
    SecretRedactionFilter().filter(record)
    message = record.getMessage()
    assert "object-secret" not in message
    assert "<redacted>" in message


def test_sanitize_text_redacts_auth_credentials_database_urls_and_queries() -> None:
    value = (
        "Authorization: Bearer *** api_key=key-123 "
        "postgresql://alice:***@example.test/app?password=query-pass&x=1"
    )
    sanitized = sanitize_text(value)
    assert "bearer-secret" not in sanitized
    assert "key-123" not in sanitized
    assert "db-pass" not in sanitized
    assert "query-pass" not in sanitized
    assert "x=1" in sanitized


def test_sanitize_text_redacts_common_credential_aliases_in_text_and_urls() -> None:
    value = (
        "access_token=text-secret refresh_token=refresh-secret client_secret=client-secret "
        "https://example.test/callback?access_token=url-secret&refresh_token=url-refresh"
    )
    sanitized = sanitize_text(value)
    for secret in ("text-secret", "refresh-secret", "client-secret", "url-secret", "url-refresh"):
        assert secret not in sanitized
    assert "callback" in sanitized


def test_sanitize_text_bounds_short_limits_with_deterministic_marker() -> None:
    assert sanitize_text("secret-value", max_length=0) == ""
    assert sanitize_text("secret-value", max_length=1) == "."
    assert sanitize_text("secret-value", max_length=2) == ".."
    assert sanitize_text("secret-value", max_length=3) == "..."
    bounded = sanitize_text("secret-value", max_length=6)
    assert bounded == "sec..."
    assert len(bounded) <= 6


def test_safe_exception_summary_keeps_type_and_sanitized_reason_only() -> None:
    summary = safe_exception_summary(
        ValueError("api_key=secret-value; request failed at /private?token=abc")
    )
    assert summary.startswith("ValueError: ")
    assert "secret-value" not in summary
    assert "token=abc" not in summary
    assert "request failed" in summary


def test_secret_redaction_filter_removes_traceback_details() -> None:
    try:
        raise RuntimeError("password=trace-secret")
    except RuntimeError:
        record = logging.LogRecord(
            name="runtime", level=logging.ERROR, pathname=__file__, lineno=1,
            msg="operation failed", args=(), exc_info=sys.exc_info(),
        )
    SecretRedactionFilter().filter(record)
    assert record.exc_info is None
    assert "trace-secret" not in (record.exc_text or "")
    assert "RuntimeError" in (record.exc_text or "")
