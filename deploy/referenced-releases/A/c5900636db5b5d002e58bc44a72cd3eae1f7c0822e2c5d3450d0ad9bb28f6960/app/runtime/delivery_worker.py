"""Bounded in-process Telegram outbox delivery."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any


class DeliveryWorker:
    """Own durable outbox claiming and delivery without blocking full scans."""

    def __init__(
        self,
        repository: Any,
        notifier: Any,
        error_throttler: Any,
        *,
        send_watch_to_telegram: bool,
        send_early_drop_warning_to_telegram: bool = False,
        poll_interval_sec: float = 5.0,
        audit_interval_sec: float = 60.0,
        batch_size: int = 5,
        lease_seconds: int = 120,
        max_batches_per_wake: int = 10,
    ) -> None:
        self._repository = repository
        self._notifier = notifier
        self._error_throttler = error_throttler
        self._send_watch_to_telegram = send_watch_to_telegram
        self._send_early_drop_warning_to_telegram = send_early_drop_warning_to_telegram
        self._poll_interval_sec = poll_interval_sec
        self._audit_interval_sec = audit_interval_sec
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_batches_per_wake = max_batches_per_wake
        self._logger = logging.getLogger(self.__class__.__name__)
        self._wake_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._latencies_ms: deque[float] = deque(maxlen=256)
        self._delivered_total = 0
        self._retry_total = 0

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="delivery-worker")
        self._logger.info(
            "Delivery worker started | task=delivery-worker poll=%ss audit=%ss batch=%s lease=%ss watch=%s",
            self._poll_interval_sec,
            self._audit_interval_sec,
            self._batch_size,
            self._lease_seconds,
            self._send_watch_to_telegram,
        )
        self.wake()

    async def stop(self) -> None:
        self._stopping = True
        self.wake()
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def wake(self) -> None:
        self._wake_event.set()

    def stats(self) -> dict[str, float | int]:
        """Bounded delivery-funnel telemetry for the observable contract."""
        values = sorted(self._latencies_ms)

        def at(fraction: float) -> float:
            if not values:
                return 0.0
            index = min(len(values) - 1, max(0, round(fraction * (len(values) - 1))))
            return round(values[index], 1)

        return {
            "delivered_total": self._delivered_total,
            "retry_total": self._retry_total,
            "latency_ms_p50": at(0.50),
            "latency_ms_p95": at(0.95),
            "latency_ms_p99": at(0.99),
        }

    async def drain_once(self) -> int:
        delivered = 0
        now = datetime.now(timezone.utc)
        signal_claims = self._repository.claim_due_deliveries(
            now,
            limit=self._batch_size,
            lease_seconds=self._lease_seconds,
            entity_type="SIGNAL",
        )
        for delivery in signal_claims:
            delivered += await self._deliver_outbox_item(delivery)

        remaining = self._batch_size - len(signal_claims)
        if self._send_watch_to_telegram and remaining > 0:
            watch_claims = self._repository.claim_due_deliveries(
                datetime.now(timezone.utc),
                limit=remaining,
                lease_seconds=self._lease_seconds,
                entity_type="WATCH",
            )
            for delivery in watch_claims:
                delivered += await self._deliver_outbox_item(delivery)
            remaining -= len(watch_claims)
        if self._send_early_drop_warning_to_telegram and remaining > 0:
            warning_claims = self._repository.claim_due_deliveries(
                datetime.now(timezone.utc),
                limit=remaining,
                lease_seconds=self._lease_seconds,
                entity_type="EARLY_DROP_WARNING",
            )
            for delivery in warning_claims:
                delivered += await self._deliver_outbox_item(delivery)
        return delivered

    async def drain_until_idle(self, *, max_batches: int = 10) -> int:
        delivered = 0
        for _ in range(min(max_batches, self._max_batches_per_wake)):
            count = await self.drain_once()
            delivered += count
            if count == 0:
                break
            await asyncio.sleep(0)
        return delivered

    async def _deliver_outbox_item(self, delivery: dict[str, object]) -> int:
        delivery_id = int(delivery["id"])
        try:
            sent = await self._notifier.send_signal(str(delivery["payload"]))
        except Exception as exc:  # noqa: BLE001 - retry state isolates transport failures
            self._repository.mark_delivery_retry(
                delivery_id,
                error=f"{type(exc).__name__}: {exc}",
                next_attempt_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            )
            self._retry_total += 1
            self._logger.warning(
                "Telegram delivery retry scheduled | outbox_id=%s error=%s",
                delivery_id,
                type(exc).__name__,
            )
            return 0
        if sent:
            self._repository.mark_delivery_sent(delivery_id)
            created_at = delivery.get("created_at")
            if isinstance(created_at, datetime):
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                self._latencies_ms.append(
                    max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds() * 1000.0)
                )
            self._delivered_total += 1
            return 1
        self._retry_total += 1
        self._repository.mark_delivery_retry(
            delivery_id,
            error="telegram_send_returned_false",
            next_attempt_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        )
        return 0

    async def _audit_delivery_funnel(self) -> None:
        audit = getattr(self._repository, "audit_delivery_funnel", None)
        if audit is None:
            return
        try:
            findings = audit(now=datetime.now(timezone.utc))
        except Exception as exc:  # noqa: BLE001 - watchdog never stops delivery
            self._logger.warning("Delivery funnel watchdog failed | error=%s", type(exc).__name__)
            return
        if not findings:
            return
        kinds: dict[str, int] = {}
        for finding in findings:
            kind = str(finding.get("kind", "unknown"))
            kinds[kind] = kinds.get(kind, 0) + 1
        self._logger.error("Delivery funnel invariant violations | kinds=%s", kinds)
        alert_key = "delivery_funnel_dead" if "dead_delivery" in kinds else "delivery_funnel_violation"
        if not self._error_throttler.should_send(alert_key):
            return
        try:
            await self._notifier.send_alert(
                "Delivery funnel invariant violation: "
                + ", ".join(f"{key}={value}" for key, value in sorted(kinds.items()))
            )
        except Exception:  # noqa: BLE001 - alert channel remains non-fatal
            self._logger.exception("Delivery funnel operator alert failed")

    async def _run(self) -> None:
        next_audit_at = asyncio.get_running_loop().time()
        while not self._stopping:
            now = asyncio.get_running_loop().time()
            if now >= next_audit_at:
                await self._audit_delivery_funnel()
                next_audit_at = now + self._audit_interval_sec
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval_sec)
            except TimeoutError:
                pass
            self._wake_event.clear()
            if self._stopping:
                return
            delivered = await self.drain_until_idle(max_batches=self._max_batches_per_wake)
            if delivered >= self._batch_size * self._max_batches_per_wake:
                self.wake()
