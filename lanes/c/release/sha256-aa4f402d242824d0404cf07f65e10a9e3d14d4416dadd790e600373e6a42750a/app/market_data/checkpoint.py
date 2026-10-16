"""Atomic, integrity-checked sidecar persistence for the market-data ring.

The sidecar intentionally contains only bounded closed-candle continuity data.
It is independent from the production SQLite signal/outbox database and is
safe to discard when no valid copy can be recovered.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .models import ConnectionEpoch, MarketCandle

if TYPE_CHECKING:
    from .continuity import GapTracker

CHECKPOINT_SCHEMA_VERSION = 1
DEFAULT_RETENTION_FILES = 2
CHECKPOINT_FILENAME = "market_data_checkpoint.json"


@dataclass(frozen=True, slots=True)
class CheckpointRestoreReport:
    """Outcome of a best-effort, per-symbol checkpoint restore."""

    source: str | None
    restored: int
    rejected: int
    considered: int
    diagnostics: tuple[str, ...] = ()

    @property
    def usable(self) -> bool:
        return self.restored > 0


class MarketDataCheckpoint:
    """Persist and recover a bounded GapTracker candle ring.

    path is the current copy.  .prev is the immediately previous
    generation, followed by a bounded number of .retention.N copies.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        retention_files: int = DEFAULT_RETENTION_FILES,
    ) -> None:
        if retention_files < 1:
            raise ValueError("retention_files must be positive")
        self.path = Path(path)
        self.retention_files = retention_files

    @classmethod
    def for_db_url(cls, db_url: str, **kwargs: Any) -> MarketDataCheckpoint | None:
        """Derive a sidecar path without adding another secret-bearing config."""

        if not db_url.startswith("sqlite:///"):
            return None
        database = Path(db_url.removeprefix("sqlite:///"))
        if database.name in {":memory:", ""}:
            return None
        return cls(database.parent / CHECKPOINT_FILENAME, **kwargs)

    @property
    def previous_path(self) -> Path:
        return Path(f"{self.path}.prev")

    def retention_path(self, index: int) -> Path:
        if index < 1 or index > self.retention_files:
            raise ValueError("retention index is outside the configured bound")
        return Path(f"{self.path}.retention.{index}")

    def save(
        self,
        candles_by_symbol: Mapping[str, Iterable[MarketCandle]],
        *,
        created_at: datetime | None = None,
    ) -> str:
        """Atomically write a new current generation and return its digest."""

        symbols: dict[str, dict[str, object]] = {}
        for raw_symbol, candles in sorted(candles_by_symbol.items()):
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            records = [
                _serialize_candle(candle)
                for candle in sorted(candles, key=lambda item: item.open_time)
                if candle.confirmed and candle.interval == "1"
            ]
            if not records:
                continue
            symbols[symbol] = {
                "checksum": _digest(records),
                "candles": records,
            }
        timestamp = _utc(created_at or datetime.now(UTC)).isoformat()
        envelope = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "created_at": timestamp,
            "symbols": symbols,
        }
        envelope["checksum"] = _digest(
            {
                "schema_version": envelope["schema_version"],
                "created_at": envelope["created_at"],
                "symbols": {
                    symbol: record["checksum"]
                    for symbol, record in sorted(symbols.items())
                },
            }
        )
        encoded = _canonical_json(envelope)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                dir=self.path.parent,
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            self._rotate()
            os.replace(temp_path, self.path)
            temp_path = None
            _fsync_directory(self.path.parent)
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
        return hashlib.sha256(encoded).hexdigest()

    def restore_into(
        self,
        tracker: GapTracker,
        *,
        symbols: Iterable[str] | None = None,
    ) -> CheckpointRestoreReport:
        """Restore valid candle records while isolating bad symbols/records."""

        requested = (
            {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
            if symbols is not None
            else None
        )
        diagnostics: list[str] = []
        for candidate in self._candidates():
            if not candidate.exists():
                continue
            try:
                envelope = self._read_envelope(candidate)
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                diagnostics.append(f"{candidate.name}: {type(exc).__name__}")
                continue
            restored: dict[str, list[MarketCandle]] = {}
            rejected = 0
            considered = 0
            raw_symbols = envelope["symbols"]
            for symbol, record in sorted(raw_symbols.items()):
                if requested is not None and symbol not in requested:
                    continue
                considered += 1
                if not isinstance(record, Mapping):
                    rejected += 1
                    diagnostics.append(f"{candidate.name}:{symbol}:invalid_record")
                    continue
                raw_candles = record.get("candles")
                expected = record.get("checksum")
                if not isinstance(raw_candles, list) or not isinstance(expected, str):
                    rejected += 1
                    diagnostics.append(f"{candidate.name}:{symbol}:invalid_payload")
                    continue
                if _digest(raw_candles) != expected:
                    rejected += 1
                    diagnostics.append(f"{candidate.name}:{symbol}:checksum_mismatch")
                    continue
                valid: list[MarketCandle] = []
                for raw_candle in raw_candles:
                    try:
                        valid.append(_deserialize_candle(raw_candle, symbol=symbol))
                    except (TypeError, ValueError, InvalidOperation, KeyError) as exc:
                        rejected += 1
                        diagnostics.append(
                            f"{candidate.name}:{symbol}:invalid_candle:{type(exc).__name__}"
                        )
                if valid:
                    restored[symbol] = valid
                elif raw_candles:
                    rejected += 1
            if restored:
                tracker.restore_candles(restored)
            return CheckpointRestoreReport(
                source=str(candidate),
                restored=sum(len(items) for items in restored.values()),
                rejected=rejected,
                considered=considered,
                diagnostics=tuple(diagnostics),
            )
        return CheckpointRestoreReport(
            source=None,
            restored=0,
            rejected=0,
            considered=0,
            diagnostics=tuple(diagnostics),
        )

    def verify(self) -> tuple[bool, str]:
        """Verify the current generation without mutating any file."""

        if not self.path.exists():
            return False, "missing"
        try:
            envelope = self._read_envelope(self.path)
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            return False, type(exc).__name__
        return True, str(envelope["checksum"])

    def _candidates(self) -> tuple[Path, ...]:
        return (
            self.path,
            self.previous_path,
            *(self.retention_path(index) for index in range(1, self.retention_files + 1)),
        )

    def _read_envelope(self, path: Path) -> dict[str, Any]:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError("checkpoint envelope must be an object")
        if value.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported checkpoint schema")
        created_at = value.get("created_at")
        symbols = value.get("symbols")
        checksum = value.get("checksum")
        if not isinstance(created_at, str) or not isinstance(symbols, dict) or not isinstance(checksum, str):
            raise TypeError("checkpoint envelope is incomplete")
        expected = _digest(
            {
                "schema_version": value["schema_version"],
                "created_at": created_at,
                "symbols": {
                    str(symbol): record.get("checksum")
                    for symbol, record in sorted(symbols.items())
                    if isinstance(record, Mapping)
                },
            }
        )
        if checksum != expected:
            raise ValueError("checkpoint envelope checksum mismatch")
        return value

    def _rotate(self) -> None:
        for index in range(self.retention_files, 1, -1):
            older = self.retention_path(index - 1)
            newer = self.retention_path(index)
            if older.exists():
                os.replace(older, newer)
        if self.previous_path.exists():
            os.replace(self.previous_path, self.retention_path(1))
        if self.path.exists():
            os.replace(self.path, self.previous_path)


def _serialize_candle(candle: MarketCandle) -> dict[str, object]:
    return {
        "symbol": candle.symbol.upper(),
        "open_time": _utc(candle.open_time).isoformat(),
        "close_time": _utc(candle.close_time).isoformat(),
        "interval": candle.interval,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
        "turnover": str(candle.turnover),
        "confirmed": candle.confirmed,
        "exchange_timestamp": (
            _utc(candle.exchange_timestamp).isoformat()
            if candle.exchange_timestamp is not None
            else None
        ),
        "received_at": _utc(candle.received_at).isoformat(),
        "connection_epoch": (
            {
                "shard_id": candle.connection_epoch.shard_id,
                "generation": candle.connection_epoch.generation,
            }
            if candle.connection_epoch is not None
            else None
        ),
        "source": candle.source,
    }


def _deserialize_candle(raw: object, *, symbol: str) -> MarketCandle:
    if not isinstance(raw, Mapping):
        raise TypeError("candle record must be an object")
    raw_symbol = str(raw.get("symbol", symbol)).strip().upper()
    if raw_symbol != symbol:
        raise ValueError("candle symbol mismatch")
    interval = str(raw["interval"])
    if interval != "1" or raw.get("confirmed") is not True:
        raise ValueError("only confirmed one-minute candles are recoverable")
    epoch_raw = raw.get("connection_epoch")
    epoch = None
    if epoch_raw is not None:
        if not isinstance(epoch_raw, Mapping):
            raise TypeError("invalid connection epoch")
        epoch = ConnectionEpoch(int(epoch_raw["shard_id"]), int(epoch_raw["generation"]))
    return MarketCandle(
        symbol=symbol,
        open_time=_parse_time(raw["open_time"]),
        close_time=_parse_time(raw["close_time"]),
        interval=interval,
        open=_decimal(raw["open"]),
        high=_decimal(raw["high"]),
        low=_decimal(raw["low"]),
        close=_decimal(raw["close"]),
        volume=_decimal(raw["volume"]),
        turnover=_decimal(raw["turnover"]),
        confirmed=True,
        exchange_timestamp=(
            _parse_time(raw["exchange_timestamp"])
            if raw.get("exchange_timestamp") is not None
            else None
        ),
        received_at=_parse_time(raw["received_at"]),
        connection_epoch=epoch,
        source=str(raw.get("source", "CHECKPOINT")),
    )


def _decimal(value: object) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError("timestamp must be an ISO string")
    return _utc(datetime.fromisoformat(value))


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
