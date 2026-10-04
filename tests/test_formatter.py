"""Tests for Telegram signal message formatting."""

from __future__ import annotations

from datetime import datetime, timezone

from app.domain import SignalDecision, SignalType
from app.signals.formatter import format_signal_message


def _minimal_decision(**overrides: object) -> SignalDecision:
    base = dict(
        symbol="ONTUSDT",
        event_id="ONTUSDT:15m:1:111",
        signal_type=SignalType.CONFIRM,
        grade="B",
        score=72,
        market_price=112.0,
        short_zone_low=110.5,
        short_zone_high=113.8,
        signal_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
        reasons=["Dist to VWAP: +13.0%"],
        risk_flags=[],
        features_snapshot={},
        score_breakdown={},
    )
    base.update(overrides)
    return SignalDecision(**base)


def test_format_signal_message_russian_labels_and_footer() -> None:
    text = format_signal_message(_minimal_decision(), "UTC")

    assert "ШОРТ-СИГНАЛ | Bybit" in text
    assert "Символ: ONTUSDT" in text
    assert "Тип: Подтверждающий" in text
    assert "Время:" in text
    assert "Цена: 112.000000" in text
    assert "Шорт-зона: 110.500000 - 113.800000" in text
    assert "Класс: B" in text
    assert "Оценка: 72" in text
    assert "Сетап:" in text
    assert "Памп обнаружен -> Откат зафиксирован -> Шорт-зона активна" in text
    assert "Почему:" in text
    assert "- Отклонение от VWAP: +13.0%" in text
    assert "Риск:" in text
    assert "Существенных предупреждений нет." in text
    assert "Только ручной вход." in text
    assert "Автоисполнения нет." in text
    assert "SHORT SIGNAL" not in text
    assert "Symbol:" not in text


def test_format_signal_message_aggressive_and_risk_flags() -> None:
    text = format_signal_message(
        _minimal_decision(
            signal_type=SignalType.AGGRESSIVE,
            reasons=["Dist to EMA20 ATR: 1.25"],
            risk_flags=["Pullback is still shallow.", "Unknown future flag."],
        ),
        "Europe/Moscow",
    )

    assert "Тип: Агрессивный" in text
    assert "- Отклонение от EMA20 в ATR: 1.25" in text
    assert "- Откат ещё неглубокий." in text
    assert "- Unknown future flag." in text


def test_format_watch_message_is_not_actionable_short() -> None:
    text = format_signal_message(
        _minimal_decision(
            signal_type=SignalType.WATCH,
            grade="B",
            reasons=["Dist to VWAP: +10.0%"],
            risk_flags=["Orderbook spread is too wide."],
        ),
        "UTC",
    )

    assert "WATCH / НЕ ВХОД" in text
    assert "Orderbook spread is too wide." in text
    assert "SHORT SIGNAL" not in text


def test_format_strategy4_names_strategy_separately_from_signal_type() -> None:
    text = format_signal_message(
        _minimal_decision(
            strategy_subtype="TRAPPED_LONGS_REVERSAL",
            strategy_metadata={
                "breakout_reference": 100.0,
                "failed_retest_high": 101.0,
                "decision_price": 99.0,
                "event_high": 102.0,
                "entry_reference": 100.0,
                "distance_from_breakout_pct": 1.0,
                "distance_from_retest_high_pct": 1.98,
            },
        ),
        "UTC",
    )
    assert "Стратегия: TRAPPED_LONGS_REVERSAL" in text
    assert "Тип: Подтверждающий" in text
    assert "Breakout level: 100.000000" in text
    assert "Failed-retest high: 101.000000" in text
    assert "Шорт-зона:" not in text
