"""Application configuration loading and validation."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from enum import StrEnum
from typing import Any

import yaml
from dotenv import dotenv_values
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

logger = logging.getLogger(__name__)


class ManualOnlyConfigurationError(RuntimeError):
    """Raised when a runtime config is not explicitly manual-only."""


class CanonicalMarketDataProviderMode(StrEnum):
    REST_ONLY = "REST_ONLY"
    HUB_SHADOW = "HUB_SHADOW"
    HUB_PREFERRED = "HUB_PREFERRED"


class ResearchTelemetryStorageMode(StrEnum):
    """Routing mode for bounded research telemetry.

    The default deliberately preserves the existing SQLite write path.  The
    spool modes are infrastructure controls and are excluded from strategy
    configuration fingerprints.
    """

    SQLITE_ONLY = "SQLITE_ONLY"
    SPOOL_SHADOW = "SPOOL_SHADOW"
    SPOOL_CANONICAL = "SPOOL_CANONICAL"


class AppConfig(BaseModel):
    """Validated runtime configuration."""

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, hide_input_in_errors=True
    )

    autoexecution: str | None = None

    scan_interval_sec: int = 60
    shortlist_size: int = 100
    min_24h_volume: float = 5_000_000
    exclude_symbols: list[str] = Field(default_factory=list)
    exclude_btc_eth: bool = True
    event_ret_15m_min: float = 6.0
    event_ret_1h_min: float = 8.0
    event_ret_4h_min: float = 20.0
    pullback_min_pct: float = 2.4
    pullback_max_pct: float = 8.0
    pullback_hold_vwap_min: float = 5.5
    pullback_hold_range_floor_pct: float = 0.55
    short_zone_mode: str = "event_range"
    short_zone_range_low_pct: float = 0.70
    short_zone_range_high_pct: float = 0.92
    short_zone_atr_low_mult: float = 0.3
    short_zone_atr_high_mult: float = 1.5
    dist_to_vwap_min: float = 7.5
    event_dist_to_vwap_min: float = 6.0
    upper_wick_min: float = 0.15
    rejection_min: float = 0.8
    vol_zscore_min: float = 0.8
    event_dist_to_ema20_atr_min: float = 2.0
    dist_to_ema20_atr_bonus: float = 2.5
    rsi_bonus_level: float = 70.0
    ret_1h_bonus_level: float = 8.0
    ret_4h_bonus_level: float = 20.0
    range_atr_bonus_level: float = 1.3
    signal_expiry_minutes: int = 90
    max_signal_age_minutes: int = 90
    derivatives_enabled: bool = False
    max_spread_pct: float = 0.30
    max_slippage_pct: float = 0.35
    min_orderbook_depth_usdt_1pct: float = 30_000
    min_orderbook_depth_usdt_2pct: float = 60_000
    cancel_on_new_event_high: bool = True
    cancel_on_volume_breakout: bool = True
    max_request_concurrency: int = 4
    request_min_delay_ms: int = 350
    request_jitter_min_ms: int = 100
    request_jitter_max_ms: int = 300
    request_timeout_sec: int = 20
    error_alert_ttl_sec: int = 300
    disk_min_safety_reserve_bytes: int = Field(default=4_764_729_344, ge=0)
    disk_log_wal_margin_bytes: int = Field(default=1_140_850_688, ge=0)
    disk_alert_cooldown_sec: int = Field(default=3600, gt=0)
    deep_scan_kline_limit: int = 300
    telegram_token: str | None = None
    signal_chat_id: str | None = None
    alerts_chat_id: str | None = None
    db_url: str = "sqlite:///./data/bot.sqlite"
    timezone: str = "Europe/Moscow"

    enable_watch_candidates: bool = False
    send_watch_to_telegram: bool = False
    watch_min_score: int = 45
    watch_max_per_cycle: int = 5
    min_public_signal_grade: str = "B"
    send_grade_c_to_telegram: bool = False
    grade_c_mode: str = "watch_only"
    baseline_live_delivery_enabled: bool = True
    volume_climax_live_delivery_enabled: bool = True
    low_volume_live_delivery_enabled: bool = True

    trapped_longs_reversal_enabled: bool = False
    trapped_longs_live_delivery_enabled: bool = False
    trapped_longs_min_oi_change_15m_pct: float = 1.0
    trapped_longs_min_closed_candles: int = 2
    trapped_longs_max_lifetime_minutes: int = 15
    trapped_longs_new_high_tolerance_pct: float = 0.30
    trapped_longs_min_rejection_pct: float = 1.0
    trapped_longs_min_signal_score: int = 70
    trapped_longs_min_public_grade: str = "B"
    trapped_longs_max_spread_pct: float = 0.80
    trapped_longs_max_slippage_pct: float = 1.00
    trapped_longs_min_depth_1pct_usdt: float = 5000
    trapped_longs_min_depth_2pct_usdt: float = 10000

    climax_short_enabled: bool = False
    climax_fast_monitor_enabled: bool = False
    climax_fast_poll_sec: int = 20
    climax_max_active_symbols: int = 10
    climax_candidate_ttl_minutes: int = 30
    climax_event_cooldown_minutes: int = 90
    climax_min_signal_score: int = 70
    shadow_outcome_runtime_batch_size: int = 25
    shadow_outcome_legacy_batch_size: int = 10
    shadow_outcome_runtime_min_batch: int = 25
    shadow_outcome_runtime_max_batch: int = 100
    shadow_outcome_legacy_min_batch: int = 10
    shadow_outcome_legacy_max_batch: int = 50
    shadow_outcome_capacity_margin: float = 2.0
    shadow_outcome_data_gap_retry_minutes: int = 30
    climax_min_public_grade: str = "B"
    climax_grade_a_score: int = 85
    climax_max_signal_age_minutes: int = 15
    volume_climax_unwind_enabled: bool = False
    volume_climax_min_ret_15m_pct: float = 12.0
    volume_climax_min_volume_ratio: float = 3.0
    volume_climax_min_volume_zscore: float = 2.5
    volume_climax_min_ret_5m_pct: float = 3.0
    volume_climax_max_oi_change_5m_pct: float = -1.0
    volume_climax_min_rejection_pct: float = 2.0
    volume_climax_max_entry_distance_below_high_pct: float = 20.0
    volume_climax_lifecycle_shadow_enabled: bool = True
    volume_climax_confirmation_window_minutes: int = 3
    climax_fresh_recheck_attempts: int = 2
    climax_fresh_recheck_retry_delay_sec: float = 3.0
    volume_climax_min_closed_candles_after_high: int = 2
    volume_climax_max_lifetime_minutes: int = 15
    low_volume_extension_enabled: bool = False
    low_volume_min_price_extension_pct: float = 5.0
    low_volume_max_current_previous_volume_ratio: float = 0.70
    low_volume_max_volume_efficiency_ratio: float = 0.70
    low_volume_min_rejection_pct: float = 2.0
    low_volume_min_failed_high_pct: float = 1.0
    low_volume_max_entry_distance_below_high_pct: float = 15.0
    low_volume_max_minutes_after_high: int = 15
    low_volume_min_closed_candles_after_high: int = 2
    low_volume_confirmation_window_minutes: int = 3
    low_volume_max_new_high_tolerance_pct: float = 0.30
    low_volume_require_close_below_breakout: bool = True
    low_volume_require_lower_high_or_failed_retest: bool = True
    low_volume_require_microstructure_break: bool = True
    low_volume_require_closed_candles_only: bool = True
    low_volume_require_equal_volume_windows: bool = True
    low_volume_block_price_acceleration_resumed: bool = True
    low_volume_block_new_high_before_delivery: bool = True
    low_volume_block_active_short_squeeze: bool = True
    low_volume_high_liquidity_risk_mode: str = "block"
    low_volume_frozen_initial_extension_enabled: bool = False
    low_volume_frozen_initial_extension_shadow_only: bool = True
    low_volume_current_ret5_gate_enabled: bool = True
    climax_root_event_tracking_enabled: bool = False
    root_detector_shadow_v2_enabled: bool = False
    climax_root_event_shadow_only: bool = True
    climax_root_event_ttl_minutes: int = 90
    climax_entry_attempt_ttl_minutes: int = 15
    climax_max_attempts_per_root_event: int = 3
    climax_block_confirmed_second_leg: bool = True
    climax_block_fresh_high_holding: bool = True
    climax_block_stale_candles: bool = True
    climax_max_spread_pct: float = 0.80
    climax_max_slippage_pct: float = 1.00
    climax_min_depth_1pct_usdt: float = 5000
    climax_min_depth_2pct_usdt: float = 10000

    enable_squeeze_guard: bool = True
    squeeze_guard_mode: str = "warn_only"

    retest_not_failed_penalty: int = 4
    shallow_pullback_penalty: int = 8
    weak_rejection_penalty: int = 8
    combined_early_short_risk_penalty: int = 6

    # Bybit public WebSocket shadow is disabled for legacy configurations.
    market_data_ws_shadow_enabled: bool = False
    canonical_market_data_provider_mode: CanonicalMarketDataProviderMode = (
        CanonicalMarketDataProviderMode.REST_ONLY
    )
    research_telemetry_storage_mode: ResearchTelemetryStorageMode = (
        ResearchTelemetryStorageMode.SQLITE_ONLY
    )
    research_spool_root: str = "<APP_ROOT>/shared/research_spool"
    research_spool_max_bytes: int = Field(default=1_073_741_824, ge=1)
    research_spool_record_max_bytes: int = Field(default=65_536, ge=1024)
    research_spool_shadow_headroom_bytes: int = Field(
        default=1_073_741_824, ge=1
    )
    research_storage_health_path: str = (
        "<APP_ROOT>/shared/runtime/research-storage-health.json"
    )

    @field_validator(
        "event_ret_15m_min",
        "event_ret_1h_min",
        "event_ret_4h_min",
        "pullback_min_pct",
        "pullback_max_pct",
        "pullback_hold_vwap_min",
        "pullback_hold_range_floor_pct",
        "short_zone_range_low_pct",
        "short_zone_range_high_pct",
        "short_zone_atr_low_mult",
        "short_zone_atr_high_mult",
        "dist_to_vwap_min",
        "event_dist_to_vwap_min",
        "upper_wick_min",
        "rejection_min",
        "vol_zscore_min",
        "event_dist_to_ema20_atr_min",
        "dist_to_ema20_atr_bonus",
        "rsi_bonus_level",
        "ret_1h_bonus_level",
        "ret_4h_bonus_level",
        "range_atr_bonus_level",
        "max_spread_pct",
        "max_slippage_pct",
        "min_orderbook_depth_usdt_1pct",
        "min_orderbook_depth_usdt_2pct",
    )
    @classmethod
    def _non_negative_float(cls, value: float) -> float:
        if value < 0:
            raise ValueError("value must be non-negative")
        return value

    @field_validator(
        "signal_expiry_minutes",
        "max_signal_age_minutes",
        "request_min_delay_ms",
        "watch_min_score",
        "watch_max_per_cycle",
        "retest_not_failed_penalty",
        "shallow_pullback_penalty",
        "weak_rejection_penalty",
        "combined_early_short_risk_penalty",
    )
    @classmethod
    def _non_negative_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("value must be non-negative")
        return value

    @field_validator("min_24h_volume")
    @classmethod
    def _positive_volume(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("min_24h_volume must be positive")
        return value

    @model_validator(mode="after")
    def _validate_ranges(self) -> AppConfig:
        if self.pullback_min_pct > self.pullback_max_pct:
            raise ValueError("pullback_min_pct must be <= pullback_max_pct")
        if self.short_zone_range_low_pct > self.short_zone_range_high_pct:
            raise ValueError(
                "short_zone_range_low_pct must be <= short_zone_range_high_pct"
            )
        if self.short_zone_atr_low_mult > self.short_zone_atr_high_mult:
            raise ValueError(
                "short_zone_atr_low_mult must be <= short_zone_atr_high_mult"
            )
        if self.request_jitter_min_ms > self.request_jitter_max_ms:
            raise ValueError("request_jitter_min_ms must be <= request_jitter_max_ms")
        if self.signal_chat_id and not self.telegram_token:
            raise ValueError(
                "telegram_token is required when signal_chat_id enables Telegram sending"
            )
        if self.send_watch_to_telegram and not self.enable_watch_candidates:
            raise ValueError(
                "enable_watch_candidates must be true when send_watch_to_telegram is enabled"
            )
        if self.squeeze_guard_mode not in {
            "warn_only",
            "score_penalty",
            "watch_only",
            "block_extreme",
        }:
            raise ValueError("invalid squeeze_guard_mode")
        if self.min_public_signal_grade not in {"A", "B", "C"}:
            raise ValueError("min_public_signal_grade must be one of A, B, C")
        if self.low_volume_high_liquidity_risk_mode not in {"block", "warn"}:
            raise ValueError(
                "low_volume_high_liquidity_risk_mode must be block or warn"
            )
        if self.climax_min_public_grade not in {"A", "B"}:
            raise ValueError("climax_min_public_grade must be A or B")
        if self.climax_grade_a_score < self.climax_min_signal_score:
            raise ValueError("climax_grade_a_score must be >= climax_min_signal_score")
        if self.trapped_longs_min_public_grade not in {"A", "B"}:
            raise ValueError("trapped_longs_min_public_grade must be A or B")
        if self.trapped_longs_min_signal_score < 0 or self.trapped_longs_min_closed_candles < 1:
            raise ValueError("trapped longs thresholds must be non-negative and candle count positive")
        if self.climax_fast_poll_sec < 1 or self.climax_max_active_symbols < 1:
            raise ValueError("climax fast monitor bounds must be positive")
        if (
            self.climax_root_event_ttl_minutes < 1
            or self.climax_entry_attempt_ttl_minutes < 1
            or self.climax_max_attempts_per_root_event < 1
        ):
            raise ValueError("shadow root-event lifecycle bounds must be positive")
        if self.grade_c_mode not in {"watch_only", "suppress"}:
            raise ValueError("grade_c_mode must be one of watch_only, suppress")
        return self

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_volume_climax_threshold(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        normalized = dict(values)
        if "canonical_market_data_provider_mode" not in normalized and normalized.get(
            "market_data_ws_shadow_enabled"
        ) is True:
            normalized["canonical_market_data_provider_mode"] = (
                CanonicalMarketDataProviderMode.HUB_SHADOW
            )
        legacy_value = normalized.pop("volume_climax_min_price_change_5m_pct", None)
        if legacy_value is None:
            return normalized
        canonical_value = normalized.get("volume_climax_min_ret_5m_pct")
        if canonical_value is None or _is_historical_volume_climax_conflict(
            canonical_value, legacy_value
        ):
            normalized["volume_climax_min_ret_5m_pct"] = legacy_value
        elif not _config_values_equal(canonical_value, legacy_value):
            raise ValueError(
                "conflicting volume climax 5m thresholds: use volume_climax_min_ret_5m_pct only"
            )
        logger.warning(
            "Deprecated config key volume_climax_min_price_change_5m_pct was migrated to "
            "volume_climax_min_ret_5m_pct"
        )
        return normalized


def _is_historical_volume_climax_conflict(
    canonical_value: Any, legacy_value: Any
) -> bool:
    try:
        return float(canonical_value) == 8.0 and float(legacy_value) == 3.0
    except (TypeError, ValueError):
        return False


def _config_values_equal(left: Any, right: Any) -> bool:
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _coerce_env_values(values: dict[str, Any]) -> dict[str, Any]:
    env_map = {
        "AUTOEXECUTION": "autoexecution",
        "TELEGRAM_TOKEN": "telegram_token",
        "SIGNAL_CHAT_ID": "signal_chat_id",
        "ALERTS_CHAT_ID": "alerts_chat_id",
        "DB_URL": "db_url",
        "TIMEZONE": "timezone",
        "REQUEST_MIN_DELAY_MS": "request_min_delay_ms",
        "REQUEST_TIMEOUT_SEC": "request_timeout_sec",
        "MIN_PUBLIC_SIGNAL_GRADE": "min_public_signal_grade",
        "SEND_GRADE_C_TO_TELEGRAM": "send_grade_c_to_telegram",
        "GRADE_C_MODE": "grade_c_mode",
        "CANONICAL_MARKET_DATA_PROVIDER_MODE": "canonical_market_data_provider_mode",
        "RESEARCH_TELEMETRY_STORAGE_MODE": "research_telemetry_storage_mode",
        "RESEARCH_SPOOL_ROOT": "research_spool_root",
        "RESEARCH_SPOOL_MAX_BYTES": "research_spool_max_bytes",
        "RESEARCH_SPOOL_RECORD_MAX_BYTES": "research_spool_record_max_bytes",
        "RESEARCH_SPOOL_SHADOW_HEADROOM_BYTES": "research_spool_shadow_headroom_bytes",
        "RESEARCH_STORAGE_HEALTH_PATH": "research_storage_health_path",
    }
    coerced: dict[str, Any] = {}
    for env_key, target_key in env_map.items():
        value = values.get(env_key)
        if value in {None, ""}:
            continue
        coerced[target_key] = value
    return coerced


def load_config(
    config_path: str | Path = "config.yaml",
    env_path: str | Path = ".env",
) -> AppConfig:
    """Load configuration from YAML with optional `.env` overrides."""

    config_file = Path(config_path)
    env_file = Path(env_path)

    raw_config = _read_yaml(config_file)
    if "AUTOEXECUTION" in raw_config:
        raw_value = raw_config["AUTOEXECUTION"]
        if raw_value is True:
            raw_value = "ON"
        elif raw_value is False:
            raw_value = "OFF"
        raw_config = {**raw_config, "autoexecution": raw_value}
        del raw_config["AUTOEXECUTION"]
    raw_env = dotenv_values(env_file) if env_file.exists() else {}
    if "AUTOEXECUTION" in os.environ:
        raw_env = {**raw_env, "AUTOEXECUTION": os.environ["AUTOEXECUTION"]}
    merged = {**raw_config, **_coerce_env_values(raw_env)}

    try:
        return AppConfig.model_validate(merged)
    except ValidationError as exc:
        raise RuntimeError(f"Invalid configuration: {exc}") from exc


def require_manual_only(config: AppConfig) -> AppConfig:
    """Fail closed unless the runtime explicitly declares AUTOEXECUTION=OFF."""

    if config.autoexecution != "OFF":
        raise ManualOnlyConfigurationError(
            "AUTOEXECUTION must be explicitly set to OFF for this manual-only runtime"
        )
    return config
