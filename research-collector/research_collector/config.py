"""Configuration for the collection-only research process.

The collector has no exchange credentials and never receives Telegram
credentials.  TOML is used so the runtime only depends on Python's standard
library for configuration loading.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import tomllib
from typing import Any


@dataclass(frozen=True, slots=True)
class LaneSource:
    name: str
    db_path: Path
    config_path: Path | None = None
    service_name: str | None = None


@dataclass(frozen=True, slots=True)
class BybitConfig:
    linear_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    spot_ws_url: str = "wss://stream.bybit.com/v5/public/spot"
    rest_url: str = "https://api.bybit.com"
    orderbook_depth: int = 50
    stale_after_sec: float = 5.0
    rest_poll_sec: float = 60.0


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    ws_url: str = "wss://stream.binance.com:9443/stream"
    rest_url: str = "https://api.binance.com"
    depth_levels: int = 20
    stale_after_sec: float = 5.0


@dataclass(frozen=True, slots=True)
class StorageConfig:
    db_path: Path = Path("data/research-collector.sqlite3")
    max_bytes: int = 5 * 1024**3
    reserve_free_bytes: int = 10 * 1024**3
    raw_retention_sec: int = 15 * 60
    raw_max_rows: int = 2_000_000
    prune_interval_sec: float = 30.0


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    lanes: tuple[LaneSource, ...]
    max_active_symbols: int = 10
    signal_poll_sec: float = 5.0
    candidate_freshness_sec: int = 15 * 60
    feature_interval_sec: float = 1.0
    merge_window_sec: int = 10 * 60
    outcome_horizon_sec: tuple[int, ...] = (15 * 60, 60 * 60, 4 * 60 * 60)
    outcome_grace_sec: int = 90
    report_interval_sec: float = 60 * 60
    reconnect_initial_sec: float = 1.0
    reconnect_max_sec: float = 30.0
    storage: StorageConfig = field(default_factory=StorageConfig)
    bybit: BybitConfig = field(default_factory=BybitConfig)
    binance: BinanceConfig = field(default_factory=BinanceConfig)


# ``slots=True`` turns class-level field access into descriptors. Keep real
# default instances for the TOML loader instead of reading ``Config.field``
# when a key is absent.
_DEFAULT_BYBIT = BybitConfig()
_DEFAULT_BINANCE = BinanceConfig()
_DEFAULT_STORAGE = StorageConfig()
_DEFAULT_COLLECTOR = CollectorConfig(lanes=())


def _path(value: Any, *, base: Path) -> Path:
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else (base / path).resolve()


def _positive_number(value: Any, name: str, *, minimum: float = 0.0) -> float:
    result = float(value)
    if result <= minimum:
        raise ValueError(f"{name} must be > {minimum}")
    return result


def load_config(path: str | Path) -> CollectorConfig:
    """Load and validate TOML configuration.

    Lane source paths are deliberately explicit.  The service opens them only
    through SQLite's ``mode=ro`` URI and never writes to those files.
    """

    config_path = Path(path).expanduser().resolve()
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)
    root = raw.get("collector", raw)
    base = config_path.parent

    lane_rows = root.get("lanes", raw.get("lanes", []))
    if not isinstance(lane_rows, list):
        raise ValueError("lanes must be an array of tables")
    lanes: list[LaneSource] = []
    for row in lane_rows:
        if not isinstance(row, dict) or not row.get("name") or not row.get("db_path"):
            raise ValueError("each lane needs name and db_path")
        cfg = row.get("config_path")
        lanes.append(
            LaneSource(
                name=str(row["name"]),
                db_path=_path(row["db_path"], base=base),
                config_path=_path(cfg, base=base) if cfg else None,
                service_name=str(row["service_name"]) if row.get("service_name") else None,
            )
        )
    if not lanes:
        raise ValueError("at least one read-only lane source is required")

    storage_raw = root.get("storage", {})
    storage = StorageConfig(
        db_path=_path(storage_raw.get("db_path", _DEFAULT_STORAGE.db_path), base=base),
        max_bytes=int(storage_raw.get("max_bytes", _DEFAULT_STORAGE.max_bytes)),
        reserve_free_bytes=int(
            storage_raw.get("reserve_free_bytes", _DEFAULT_STORAGE.reserve_free_bytes)
        ),
        raw_retention_sec=int(
            storage_raw.get("raw_retention_sec", _DEFAULT_STORAGE.raw_retention_sec)
        ),
        raw_max_rows=int(storage_raw.get("raw_max_rows", _DEFAULT_STORAGE.raw_max_rows)),
        prune_interval_sec=_positive_number(
            storage_raw.get("prune_interval_sec", _DEFAULT_STORAGE.prune_interval_sec),
            "storage.prune_interval_sec",
        ),
    )
    if storage.max_bytes <= 0 or storage.reserve_free_bytes <= 0:
        raise ValueError("storage limits must be positive")
    if storage.raw_retention_sec < 60 or storage.raw_max_rows < 1_000:
        raise ValueError("raw retention and row limit are too small")

    bybit_raw = root.get("bybit", {})
    bybit = BybitConfig(
        linear_ws_url=str(bybit_raw.get("linear_ws_url", _DEFAULT_BYBIT.linear_ws_url)),
        spot_ws_url=str(bybit_raw.get("spot_ws_url", _DEFAULT_BYBIT.spot_ws_url)),
        rest_url=str(bybit_raw.get("rest_url", _DEFAULT_BYBIT.rest_url)).rstrip("/"),
        orderbook_depth=int(bybit_raw.get("orderbook_depth", _DEFAULT_BYBIT.orderbook_depth)),
        stale_after_sec=_positive_number(
            bybit_raw.get("stale_after_sec", _DEFAULT_BYBIT.stale_after_sec),
            "bybit.stale_after_sec",
        ),
        rest_poll_sec=_positive_number(
            bybit_raw.get("rest_poll_sec", _DEFAULT_BYBIT.rest_poll_sec),
            "bybit.rest_poll_sec",
        ),
    )
    if bybit.orderbook_depth <= 0:
        raise ValueError("bybit.orderbook_depth must be positive")

    binance_raw = root.get("binance", {})
    binance = BinanceConfig(
        ws_url=str(binance_raw.get("ws_url", _DEFAULT_BINANCE.ws_url)),
        rest_url=str(binance_raw.get("rest_url", _DEFAULT_BINANCE.rest_url)).rstrip("/"),
        depth_levels=int(binance_raw.get("depth_levels", _DEFAULT_BINANCE.depth_levels)),
        stale_after_sec=_positive_number(
            binance_raw.get("stale_after_sec", _DEFAULT_BINANCE.stale_after_sec),
            "binance.stale_after_sec",
        ),
    )
    if binance.depth_levels <= 0:
        raise ValueError("binance.depth_levels must be positive")

    max_active = int(root.get("max_active_symbols", 10))
    if not 1 <= max_active <= 10:
        raise ValueError("max_active_symbols must be between 1 and 10")
    freshness_sec = int(root.get("candidate_freshness_sec", _DEFAULT_COLLECTOR.candidate_freshness_sec))
    if freshness_sec <= 0:
        raise ValueError("candidate_freshness_sec must be positive")
    horizons = tuple(int(v) for v in root.get("outcome_horizon_sec", _DEFAULT_COLLECTOR.outcome_horizon_sec))
    if horizons != (15 * 60, 60 * 60, 4 * 60 * 60):
        raise ValueError("outcome_horizon_sec must be 15m, 1h, and 4h")
    return CollectorConfig(
        lanes=tuple(lanes),
        max_active_symbols=max_active,
        signal_poll_sec=_positive_number(
            root.get("signal_poll_sec", _DEFAULT_COLLECTOR.signal_poll_sec), "signal_poll_sec"
        ),
        candidate_freshness_sec=freshness_sec,
        feature_interval_sec=_positive_number(
            root.get("feature_interval_sec", _DEFAULT_COLLECTOR.feature_interval_sec),
            "feature_interval_sec",
        ),
        merge_window_sec=int(root.get("merge_window_sec", _DEFAULT_COLLECTOR.merge_window_sec)),
        outcome_horizon_sec=horizons,
        outcome_grace_sec=int(root.get("outcome_grace_sec", _DEFAULT_COLLECTOR.outcome_grace_sec)),
        report_interval_sec=_positive_number(
            root.get("report_interval_sec", _DEFAULT_COLLECTOR.report_interval_sec),
            "report_interval_sec",
        ),
        reconnect_initial_sec=_positive_number(
            root.get("reconnect_initial_sec", _DEFAULT_COLLECTOR.reconnect_initial_sec),
            "reconnect_initial_sec",
        ),
        reconnect_max_sec=_positive_number(
            root.get("reconnect_max_sec", _DEFAULT_COLLECTOR.reconnect_max_sec),
            "reconnect_max_sec",
        ),
        storage=storage,
        bybit=bybit,
        binance=binance,
    )
