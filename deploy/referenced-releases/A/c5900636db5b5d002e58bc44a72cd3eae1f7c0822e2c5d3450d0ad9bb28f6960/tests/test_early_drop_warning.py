from datetime import datetime, timedelta, timezone
import math

from app.early_drop_warning import (
    EarlyDropSnapshot,
    EarlyDropWarningStatus,
    evaluate_early_drop_warning,
    format_early_drop_warning,
)


def snap(**overrides):
    now = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)
    values = dict(
        root_event_id="root-1",
        event_revision=2,
        symbol="ABCUSDT",
        root_started_at=now - timedelta(minutes=8),
        observed_at=now,
        market_asof=now,
        latest_candle_at=now - timedelta(minutes=1),
        impulse_pct=12.0,
        drawdown_pct=3.0,
        event_high=120.0,
        price=116.4,
        previous_close=118.0,
        current_close=116.4,
        spread_pct=0.10,
        slippage_pct=0.10,
        depth_1pct_usdt=20_000.0,
        depth_2pct_usdt=50_000.0,
    )
    values.update(overrides)
    return EarlyDropSnapshot(**values)


def test_strong_impulse_and_fresh_weakening_is_warning_not_actionable():
    result = evaluate_early_drop_warning(snap())

    assert result.status is EarlyDropWarningStatus.WARNING
    assert result.warning is True
    assert result.actionable is False
    assert result.root_event_id == "root-1"
    assert result.event_revision == 2


def test_stale_or_incomplete_or_nonfinite_data_fails_closed():
    for changes in (
        {"latest_candle_at": datetime(2026, 9, 16, 11, 57, tzinfo=timezone.utc)},
        {"depth_2pct_usdt": None},
        {"price": math.nan},
        {"market_asof": None},
    ):
        result = evaluate_early_drop_warning(snap(**changes))
        assert result.status is EarlyDropWarningStatus.DATA_DEGRADED
        assert result.warning is False
        assert result.actionable is False


def test_deduplication_key_is_root_and_revision_and_message_is_non_actionable():
    result = evaluate_early_drop_warning(snap())
    assert result.dedupe_key == "EARLY_DROP_WARNING:root-1:r2"

    message = format_early_drop_warning(result)
    assert "EARLY DROP WARNING" in message
    assert "НЕ ЯВЛЯЕТСЯ ГОТОВЫМ SHORT-СИГНАЛОМ" in message


def test_feature_flag_defaults_off():
    from app.config import AppConfig

    config = AppConfig()
    assert config.early_drop_warning_enabled is False


def test_warning_persistence_is_namespaced_and_deduplicated(tmp_path):
    from app.storage.db import Database
    from app.storage.repository import BotRepository

    db = Database(f"sqlite:///{tmp_path / 'warning.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    first = repo.save_early_drop_warning(evaluate_early_drop_warning(snap()), format_early_drop_warning(evaluate_early_drop_warning(snap())))
    second = repo.save_early_drop_warning(evaluate_early_drop_warning(snap()), format_early_drop_warning(evaluate_early_drop_warning(snap())))

    assert first.created is True
    assert second.created is False
    assert repo.count_early_drop_warnings() == 1
    assert repo.count_signals() == 0
    assert repo.count_delivery_outbox(entity_type="EARLY_DROP_WARNING") == 1
    assert repo.has_recent_early_drop_warning(
        "ABCUSDT", now=datetime(2026, 9, 16, 12, 1, tzinfo=timezone.utc), cooldown=timedelta(minutes=30)
    ) is True


def test_schema_contains_warning_and_dedicated_outbox_tables(tmp_path):
    from sqlalchemy import inspect
    from app.storage.db import Database

    db = Database(f"sqlite:///{tmp_path / 'schema.sqlite'}")
    db.create_all()
    tables = set(inspect(db.engine).get_table_names())
    assert "early_drop_warnings" in tables
    assert "telegram_delivery_outbox" in tables
