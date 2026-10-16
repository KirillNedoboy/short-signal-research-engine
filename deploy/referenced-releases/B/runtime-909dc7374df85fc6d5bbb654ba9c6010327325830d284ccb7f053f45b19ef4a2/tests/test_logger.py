import logging

from app.logger import SecretRedactionFilter, configure_logging


def test_secret_redaction_filter_removes_telegram_bot_token() -> None:
    record = logging.LogRecord(
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
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
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="HTTP Request: POST %s",
        args=(UrlLike(),),
        exc_info=None,
    )

    SecretRedactionFilter().filter(record)

    message = record.getMessage()
    assert "123456789:SECRET_token-1" not in message
    assert "bot<redacted>" in message
