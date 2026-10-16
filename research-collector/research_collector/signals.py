"""Read-only adapters for the three live lane SQLite databases."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import time
from typing import Any, Iterable, Mapping

from .config import LaneSource
from .models import DeliveredSignal


SENSITIVE_KEY_WORDS = ("token", "password", "secret", "credential", "chat_id", "api_key", "webhook")


def parse_time_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return int(number if number > 10_000_000_000 else number * 1000)
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text)
        return int(number if number > 10_000_000_000 else number * 1000)
    except ValueError:
        pass
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def sanitize(value: Any, *, depth: int = 0) -> Any:
    """Keep decision features while excluding accidental credential material."""

    if depth > 5:
        return "<depth-limit>"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            name = str(key)
            lower = name.lower()
            if any(word in lower for word in SENSITIVE_KEY_WORDS):
                continue
            result[name[:128]] = sanitize(child, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        return [sanitize(item, depth=depth + 1) for item in list(value)[:100]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        text = str(value)
        return text[:4_000] if isinstance(value, str) else value
    return str(value)[:4_000]


class ReadOnlyLaneReader:
    """Poll one lane without opening it in write mode or reading payloads."""

    def __init__(
        self,
        source: LaneSource,
        *,
        started_ms: int | None = None,
        freshness_sec: int = 15 * 60,
        batch_size: int = 100,
    ) -> None:
        self.source = source
        self.batch_size = batch_size
        self.started_ms = started_ms if started_ms is not None else int(time.time() * 1000)
        self.freshness_ms = int(freshness_sec * 1000)
        if self.freshness_ms <= 0:
            raise ValueError("freshness_sec must be positive")
        self.config_version = self._hash_active_config(source)
        self.last_time_ms = 0
        self.last_signal_id = ""
        self.last_time_raw = ""
        self.pending_ids: set[str] = set()

    @classmethod
    def _hash_active_config(cls, source: LaneSource) -> str:
        path = source.config_path
        if path is None or not path.exists() or path.name.lower() == ".env":
            path = cls._resolve_systemd_config(source.service_name)
        if path is None or path.name.lower() == ".env" or not path.exists():
            return "UNKNOWN"
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            return "UNKNOWN"
        return f"sha256:{digest.hexdigest()}"

    @staticmethod
    def _resolve_systemd_config(service_name: str | None) -> Path | None:
        if not service_name:
            return None
        try:
            result = subprocess.run(
                ["systemctl", "show", "--value", "--property=WorkingDirectory", service_name],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        workdir = result.stdout.strip()
        if not workdir or workdir == "/":
            try:
                pid_result = subprocess.run(
                    ["systemctl", "show", "--value", "--property=MainPID", service_name],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                pid = int(pid_result.stdout.strip())
                workdir = str(Path(f"/proc/{pid}/cwd").resolve()) if pid > 0 else ""
            except (OSError, ValueError, subprocess.SubprocessError):
                workdir = ""
        if not workdir or workdir == "/":
            return None
        root = Path(workdir)
        for name in ("config.yaml", "config.yml", "config.toml"):
            candidate = root / name
            if candidate.exists() and candidate.name.lower() != ".env":
                return candidate
        return None

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.source.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=1.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA busy_timeout=1000")
        return conn

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        except sqlite3.DatabaseError:
            return set()

    @staticmethod
    def _pick(row: sqlite3.Row, columns: set[str], names: Iterable[str], default: Any = None) -> Any:
        for name in names:
            if name in columns:
                return row[name]
        return default

    def _delivery(self, conn: sqlite3.Connection, signal_id: str, columns: set[str], row: sqlite3.Row, signal_ms: int) -> tuple[int | None, str]:
        # The outbox query deliberately selects only status/channel/sent_at;
        # payload and last_error are never read.
        try:
            outbox_columns = self._columns(conn, "telegram_delivery_outbox")
            required = {"entity_id", "status", "sent_at"}
            if required.issubset(outbox_columns):
                result = conn.execute(
                    "SELECT status,channel,sent_at FROM telegram_delivery_outbox "
                    "WHERE entity_type IN ('signal','signals') AND entity_id=? AND status='sent' "
                    "ORDER BY sent_at DESC LIMIT 1",
                    (str(signal_id),),
                ).fetchone()
                if result:
                    return parse_time_ms(result[2]), "OUTBOX_SENT"
        except sqlite3.DatabaseError:
            pass
        sent_at = self._pick(row, columns, ("telegram_sent_at", "sent_at", "delivery_time"))
        sent_flag = self._pick(row, columns, ("telegram_sent", "delivered", "is_delivered"), False)
        if bool(sent_flag):
            return parse_time_ms(sent_at) or signal_ms, "SIGNAL_DELIVERED_TIME_UNKNOWN"
        return None, "NOT_DELIVERED"

    def poll(self) -> list[DeliveredSignal]:
        if not self.source.db_path.exists():
            return []
        conn = self._connect()
        try:
            columns = self._columns(conn, "signals")
            if not {"id", "symbol", "signal_time"}.issubset(columns):
                return []
            selected = [name for name in (
                "id", "symbol", "signal_time", "event_id", "strategy_type", "strategy_subtype",
                "subtype", "type", "model_version", "market_price", "price", "context_json",
                "context", "telegram_sent", "telegram_sent_at", "sent_at",
            ) if name in columns]
            select_sql = ",".join(selected)
            # SQLite stores these timestamps as ISO text in the live lanes;
            # ordering by the source value remains stable for UTC ISO strings.
            if self.last_time_raw:
                rows = conn.execute(
                    f"SELECT {select_sql} FROM signals WHERE signal_time IS NOT NULL AND signal_time>=? "
                    "ORDER BY signal_time,id LIMIT ?",
                    (self.last_time_raw, self.batch_size * 4),
                ).fetchall()
            else:
                # On first start, the collector begins at the recent tail. It
                # does not replay the entire production history.
                rows = list(
                    reversed(
                        conn.execute(
                            f"SELECT {select_sql} FROM signals WHERE signal_time IS NOT NULL "
                            "ORDER BY signal_time DESC,id DESC LIMIT ?",
                            (self.batch_size * 4,),
                        ).fetchall()
                    )
                )
            if self.pending_ids:
                pending = list(self.pending_ids)[: self.batch_size]
                placeholders = ",".join("?" for _ in pending)
                rows.extend(
                    conn.execute(
                        f"SELECT {select_sql} FROM signals WHERE id IN ({placeholders})", pending
                    ).fetchall()
                )
            poll_now_ms = int(time.time() * 1000)
            seen_ids: set[str] = set()
            result: list[DeliveredSignal] = []
            for row in rows:
                signal_id = str(row["id"])
                if signal_id in seen_ids:
                    continue
                seen_ids.add(signal_id)
                signal_ms = parse_time_ms(row["signal_time"])
                if signal_ms is None:
                    continue
                raw_time = str(row["signal_time"])
                if (
                    self.last_time_raw
                    and (raw_time < self.last_time_raw or (raw_time == self.last_time_raw and signal_id <= self.last_signal_id))
                    and signal_id not in self.pending_ids
                ):
                    continue
                delivery_ms, delivery_status = self._delivery(conn, signal_id, columns, row, signal_ms)
                # Advance the source watermark for every observed signal,
                # including old/already-delivered rows. Otherwise the first
                # startup scan keeps replaying the historical tail forever.
                if not self.last_time_raw or raw_time > self.last_time_raw or (raw_time == self.last_time_raw and signal_id > self.last_signal_id):
                    self.last_time_raw, self.last_time_ms, self.last_signal_id = raw_time, signal_ms, signal_id
                if delivery_ms is None:
                    # A not-yet-sent signal may become delivered shortly after
                    # startup. Very old unsent rows are not admission work.
                    if signal_ms >= self.started_ms - self.freshness_ms:
                        self.pending_ids.add(signal_id)
                    else:
                        self.pending_ids.discard(signal_id)
                    continue
                self.pending_ids.discard(signal_id)
                if (
                    delivery_ms < self.started_ms
                    or delivery_ms > poll_now_ms + 60_000
                    or poll_now_ms - delivery_ms > self.freshness_ms
                ):
                    # Delivery happened before this collector run or outside
                    # the explicit freshness window; it must never create a
                    # research event or consume active-symbol capacity.
                    continue
                context_value = self._pick(row, columns, ("context_json", "context"), {})
                try:
                    context = json.loads(context_value) if isinstance(context_value, str) else context_value
                except (TypeError, ValueError):
                    context = {"context_status": "UNKNOWN_MALFORMED"}
                decision = sanitize(context if isinstance(context, Mapping) else {"context": context})
                strategy = self._pick(row, columns, ("strategy_subtype", "strategy_type", "subtype", "type"), "UNKNOWN")
                price_value = self._pick(row, columns, ("market_price", "price"))
                try:
                    signal_price = float(price_value) if price_value is not None else None
                except (TypeError, ValueError):
                    signal_price = None
                result.append(
                    DeliveredSignal(
                        lane=self.source.name,
                        signal_id=signal_id,
                        symbol=str(row["symbol"]).upper(),
                        signal_time_ms=signal_ms,
                        delivery_time_ms=delivery_ms,
                        delivery_status=delivery_status,
                        strategy=str(strategy or "UNKNOWN"),
                        model_version=str(self._pick(row, columns, ("model_version",)) or "UNKNOWN"),
                        config_version=self.config_version,
                        event_id=str(self._pick(row, columns, ("event_id",))) if self._pick(row, columns, ("event_id",)) is not None else None,
                        decision=decision,
                        signal_price=signal_price,
                    )
                )
                if len(result) >= self.batch_size:
                    break
            return result
        finally:
            conn.close()

