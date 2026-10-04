"""Async Telegram notifier."""

from __future__ import annotations

import logging

from telegram import Bot


class TelegramNotifier:
    """Long-lived Telegram Bot client with separate signal and alert chats."""

    def __init__(
        self,
        token: str | None,
        signal_chat_id: str | None,
        alerts_chat_id: str | None,
    ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._token = token
        self._signal_chat_id = signal_chat_id
        self._alerts_chat_id = alerts_chat_id
        self._bot = Bot(token=token) if token else None
        self._started = False

    @property
    def enabled(self) -> bool:
        """Return whether signal sending is configured."""

        return bool(self._bot and self._signal_chat_id)

    async def start(self) -> None:
        """Initialize the Telegram bot once."""

        if self._bot is None or self._started:
            return
        await self._bot.initialize()
        self._started = True

    async def close(self) -> None:
        """Shutdown the Telegram bot cleanly."""

        if self._bot is None or not self._started:
            return
        await self._bot.shutdown()
        self._started = False

    async def send_signal(self, message: str) -> bool:
        """Send a signal message to the configured signal chat."""

        if self._bot is None or not self._signal_chat_id:
            self._logger.warning("Telegram signal chat is not configured; skipping signal send.")
            return False
        await self.start()
        await self._bot.send_message(chat_id=self._signal_chat_id, text=message)
        return True

    async def send_alert(self, message: str) -> bool:
        """Send an operational alert message to the alert chat."""

        if self._bot is None or not self._alerts_chat_id:
            self._logger.warning("Telegram alerts chat is not configured; skipping alert send.")
            return False
        await self.start()
        await self._bot.send_message(chat_id=self._alerts_chat_id, text=message)
        return True
