"""Telegram message formatting."""

from __future__ import annotations

from zoneinfo import ZoneInfo

from app.domain import SignalDecision, SignalType

_SIGNAL_TYPE_RU: dict[SignalType, str] = {
    SignalType.AGGRESSIVE: "Агрессивный",
    SignalType.CONFIRM: "Подтверждающий",
}

_REASON_PREFIX_RU: tuple[tuple[str, str], ...] = (
    ("Dist to EMA20 ATR:", "Отклонение от EMA20 в ATR:"),
    ("Dist to VWAP:", "Отклонение от VWAP:"),
    ("Pullback from event high:", "Откат от хая события:"),
    ("Volume z-score 30m:", "Аномалия объёма 30м:"),
    ("Rejection from high:", "Отбой от хая:"),
    ("Upper wick:", "Верхняя тень:"),
    ("RSI 15m:", "RSI 15м:"),
)
_RISK_FLAG_RU: dict[str, str] = {
    "Pullback is still shallow.": "Откат ещё неглубокий.",
    "Price is still too close to the event high.": "Цена всё ещё слишком близко к хаю события.",
    "Rejection candle is weak.": "Свеча отбоя слабая.",
    "VWAP stretch buffer is thin.": "Запас до VWAP небольшой.",
    "Recent high was just broken.": "Недавний хай только пробили.",
    "Continuation body is still too large.": "Тело продолжения всё ещё слишком большое.",
    "Retest failure is not fully confirmed.": "Провал ретеста не до конца подтверждён.",
}
_DEFAULT_RISK_LINE_RU = "Существенных предупреждений нет."


def _translate_reason_line(line: str) -> str:
    for en_prefix, ru_prefix in _REASON_PREFIX_RU:
        if line.startswith(en_prefix):
            return ru_prefix + line[len(en_prefix) :]
    return line


def _translate_risk_flag(flag: str) -> str:
    return _RISK_FLAG_RU.get(flag, flag)


def _watch_display_type(decision: SignalDecision) -> str:
    if decision.decision_type == "EARLY_PUMP_WATCH":
        return "EARLY_PUMP_WATCH"
    if decision.grade == "B":
        return "WATCH_B_BLOCKED"
    return "WATCH_C_STRONG_MOVE"


def _watch_non_signal_reasons(decision: SignalDecision) -> list[str]:
    reasons: list[str] = []
    reasons.extend(decision.blockers)
    reasons.extend(decision.squeeze_risk_reasons)
    for flag in decision.risk_flags:
        if flag == "Volume z-score is moderately below actionable threshold.":
            reasons.append("volume_weak")
        elif flag in {"Recent high was just broken.", "Continuation body is still too large."}:
            reasons.append("breakout_risk")
        elif flag == "Retest failure is not fully confirmed.":
            reasons.append("retest_not_failed")
        elif flag == "Orderbook depth within 1% is too thin.":
            reasons.append("thin_orderbook")
        elif flag == "Orderbook depth within 2% is too thin.":
            reasons.append("low_liquidity")
        else:
            reasons.append(flag)
    return list(dict.fromkeys(reason for reason in reasons if reason))


def format_signal_message(decision: SignalDecision, timezone_name: str) -> str:
    """Render a human-friendly Telegram message."""
    tz = ZoneInfo(timezone_name)
    local_time = decision.signal_time.astimezone(tz).strftime("%Y-%m-%d %H:%M %Z")
    if decision.signal_type == SignalType.WATCH:
        return _format_watch_message(decision, local_time)
    if decision.strategy_subtype == "TRAPPED_LONGS_REVERSAL":
        return _format_trapped_longs_message(decision, local_time, type_ru=_SIGNAL_TYPE_RU[decision.signal_type])
    if decision.strategy_subtype in {"VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"}:
        return _format_climax_message(decision)
    type_ru = _SIGNAL_TYPE_RU[decision.signal_type]
    why_lines = "\n".join(f"- {_translate_reason_line(line)}" for line in decision.reasons)
    risk_lines = "\n".join(f"- {_translate_risk_flag(flag)}" for flag in decision.risk_flags) if decision.risk_flags else f"- {_DEFAULT_RISK_LINE_RU}"
    optional_risk = f"\nSqueeze-risk: {decision.squeeze_risk_level}" if decision.squeeze_risk_level in {"MEDIUM", "HIGH", "EXTREME"} else ""
    return (
        "🔴 ШОРТ-СИГНАЛ | Bybit\n"
        f"Символ: {decision.symbol}\nТип: {type_ru}\nВремя: {local_time}\n\n"
        f"Цена: {decision.market_price:.6f}\nШорт-зона: {decision.short_zone_low:.6f} - {decision.short_zone_high:.6f}\n"
        f"Класс: {decision.grade}\nОценка: {decision.score}{optional_risk}\n\nСетап:\n"
        "Памп обнаружен -> Откат зафиксирован -> Шорт-зона активна\n\nПочему:\n"
        f"{why_lines}\n\nРиск:\n{risk_lines}\nТолько ручной вход.\nАвтоисполнения нет."
    )


def _format_trapped_longs_message(decision: SignalDecision, local_time: str, *, type_ru: str) -> str:
    m = decision.strategy_metadata
    def level(key: str) -> str:
        value = m.get(key)
        return "н/д" if value is None else f"{float(value):.6f}"
    def metric(key: str) -> str:
        value = m.get(key)
        return "н/д" if value is None else f"{float(value):.2f}%"
    why_lines = "\n".join(f"- {_translate_reason_line(line)}" for line in decision.reasons)
    return (
        "🔴 ШОРТ-СИГНАЛ | Bybit\n"
        f"Символ: {decision.symbol}\nСтратегия: TRAPPED_LONGS_REVERSAL\nТип: {type_ru}\nВремя: {local_time}\n\n"
        f"Цена решения: {level('decision_price')}\nBreakout level: {level('breakout_reference')}\n"
        f"Failed-retest high: {level('failed_retest_high')}\nEvent high: {level('event_high')}\n"
        f"Distance from breakout: {metric('distance_from_breakout_pct')}\n"
        f"Distance from retest high: {metric('distance_from_retest_high_pct')}\n"
        f"Класс: {decision.grade}\nОценка: {decision.score}\n\nПочему:\n{why_lines}\n\n"
        "Только ручной вход.\nАвтоисполнения нет."
    )


def _format_climax_message(decision: SignalDecision) -> str:
    m = decision.strategy_metadata
    liquidity = "Повышенный риск ликвидности" if m.get("liquidity_warning") else "normal"

    def val(key: str, suffix: str = "") -> str:
        x = m.get(key)
        return "н/д" if x is None else f"{x:.2f}{suffix}"

    event_high_text = "н/д" if m.get("event_high") is None else f"{float(m['event_high']):.8f}"
    if m.get("event_high") is not None and decision.market_price:
        computed_distance = (float(m["event_high"]) - decision.market_price) / float(m["event_high"]) * 100
        stored_distance = float(m.get("entry_distance_below_high_pct") or computed_distance)
        if abs(computed_distance - stored_distance) > 0.15:
            return ""
    if decision.strategy_subtype == "VOLUME_CLIMAX_UNWIND":
        body = (f"Объём: {val('volume_ratio', 'x')} / z={val('volume_zscore')}\n"
                f"Цена 5m: {val('price_change_5m', '%')}\nOI 5m: {val('oi_change_5m', '%')}\n")
        title = "VOLUME CLIMAX UNWIND"
        confirm = "short-covering exhaustion + failed continuation"
    else:
        body = (f"Рост текущего импульса: {val('price_change_5m', '%')}\n"
                f"Объём к предыдущему импульсу: {val('current_previous_volume_ratio', 'x')}\n"
                f"Эффективность объёма: {val('volume_efficiency_ratio', 'x')}\n"
                f"OI: {m.get('oi_confirmation_state', 'unavailable')}\n")
        title = "LOW-VOLUME EXTENSION"
        confirm = "weak extension + failed high"
    return ("🔴 SHORT-SIGNAL\n\n"
            f"Стратегия: {title}\nМонета: {decision.symbol}\nGrade: {decision.grade}\nScore: {decision.score}\n\n"
            f"Вход: {decision.market_price:.8f}\nEvent high: {event_high_text}\nРасстояние от хая: {val('entry_distance_below_high_pct', '%')}\n"
            f"{body}Отказ от хая: {val('rejection_pct', '%')}\n\nПодтверждение:\n{confirm}\n\nРиск ликвидности: {liquidity}\nТолько ручной вход. Autoexecution: OFF.")



def _format_watch_message(decision: SignalDecision, local_time: str) -> str:
    watch_type = _watch_display_type(decision)
    lifecycle_state = decision.lifecycle_state or "unknown"
    why_not_signal = _watch_non_signal_reasons(decision)
    why_not_signal_lines = "\n".join(f"- {reason}" for reason in why_not_signal) if why_not_signal else "- observation_only"
    context_lines = "\n".join(f"- {_translate_reason_line(line)}" for line in decision.reasons)
    return ("🟡 WATCH / НЕ ВХОД\n" f"Символ: {decision.symbol}\nТип WATCH: {watch_type}\nСостояние: {lifecycle_state}\n"
            f"Время: {local_time}\nОценка: {decision.score} | Класс: {decision.grade}\n\nПочему это НЕ signal:\n{why_not_signal_lines}\n\nКонтекст:\n{context_lines}")
