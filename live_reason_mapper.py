from __future__ import annotations

from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def human_guard_reason(reason: str) -> str:
    mapping = {
        "api_unstable_soft_guard": "Серия API-ошибок: бот временно на паузе.",
        "too_many_cycle_errors": "Слишком много ошибок подряд: временная пауза.",
        "max_trades_per_day": "Достигнут дневной лимит сделок.",
        "daily_drawdown_usdt_limit": "Достигнут дневной лимит просадки (USDT).",
        "daily_drawdown_pct_limit": "Достигнут дневной лимит просадки (%).",
        "market_volatility_anomaly": "Аномальная волатильность: вход временно приостановлен.",
        "price_jump_anomaly": "Резкий скачок цены: вход временно приостановлен.",
        "side_stoploss_streak_guard_long": "Серия стоп-лоссов по LONG: временная пауза.",
        "side_stoploss_streak_guard_short": "Серия стоп-лоссов по SHORT: временная пауза.",
    }
    raw = str(reason or "").strip()
    return mapping.get(raw, raw)


def human_sell_reason(reason: str) -> str:
    mapping = {
        "stop_loss": "сработал стоп-лосс",
        "take_profit": "сработал тейк-профит",
        "trailing_stop": "сработал трейлинг-стоп",
        "stall_exit": "рынок не дал развития, позиция закрыта",
        "smart_stagnation_exit": "рынок слабый, закрытие с контролем малого убытка",
        "time_stop": "слишком долго без результата",
        "ai_quality_fade": "качество сигнала ухудшилось",
        "slippage_guard": "вход выполнен с аномальным проскальзыванием",
        "signal": "сигнал AI/стратегии",
    }
    raw = str(reason or "").strip()
    return mapping.get(raw, raw if raw else "условие выхода")


def normalize_buy_blocks(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("buy_block_reasons", [])
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if str(x).strip()]


def prioritize_buy_blocks(blocks: list[str]) -> list[str]:
    priority = {
        "anomaly_risk_high": 10,
        "market_data_stale": 15,
        "post_time_stop_block": 20,
        "symbol_internal_cooldown": 30,
        "signal_debounce": 40,
        "mtf_not_confirmed": 50,
        "market_noise_high": 60,
        "expected_edge_too_low": 70,
        "ai_conf_low": 80,
        "ai_quality_low": 90,
        "short_model_weak": 100,
        "short_signal_spot_only": 110,
    }
    uniq: list[str] = []
    seen: set[str] = set()
    for item in blocks:
        key = str(item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(key)
    return sorted(uniq, key=lambda x: priority.get(x, 999))


def human_entry_block_reason(payload: dict[str, Any]) -> str:
    buy_blocks = prioritize_buy_blocks(normalize_buy_blocks(payload))
    ai_entry_quality = _safe_float(payload.get("ai_entry_quality"), 0.0)
    expected_edge_pct = _safe_float(payload.get("expected_edge_pct"), 0.0)
    post_time_stop_wait = int(_safe_float(payload.get("post_time_stop_wait_candles"), 0.0))

    if "ai_quality_low" in buy_blocks:
        return f"Сигнал слабый: качество {ai_entry_quality:.2f} ниже порога."
    if "ai_conf_low" in buy_blocks:
        return "Недостаточная уверенность AI для входа."
    if "mtf_not_confirmed" in buy_blocks:
        return "Старшие таймфреймы не подтверждают вход."
    if "short_signal_spot_only" in buy_blocks:
        return "AI видит short-сценарий; в спот-режиме новый вход пропущен."
    if "market_noise_high" in buy_blocks:
        return "Слишком шумный рынок: вход временно пропущен."
    if "market_data_stale" in buy_blocks:
        return "Рыночные данные устарели: вход временно пропущен."
    if "short_model_weak" in buy_blocks:
        return "AI ужесточил вход: short-сигналы сейчас менее надежны."
    if "expected_edge_too_low" in buy_blocks:
        return f"Ожидаемое преимущество слишком низкое: {expected_edge_pct * 100.0:.2f}%."
    if "anomaly_risk_high" in buy_blocks:
        return "Аномалия рынка: вход временно приостановлен."
    if "post_time_stop_block" in buy_blocks:
        return f"Пауза после выхода по time_stop: осталось ~{post_time_stop_wait} свеч."
    if "signal_debounce" in buy_blocks:
        current = int(_safe_float(payload.get("signal_debounce_count"), 0.0))
        required = max(1, int(_safe_float(payload.get("signal_debounce_required"), 1.0)))
        return f"AI-сигнал подтверждается: {current}/{required} циклов."
    if "symbol_internal_cooldown" in buy_blocks:
        rem = int(_safe_float(payload.get("symbol_internal_cooldown_remaining_sec"), 0.0))
        return f"Внутренняя пауза по паре: осталось ~{rem} сек."
    if buy_blocks:
        return "Условия входа не выполнены."
    return ""


def build_human_reason(payload: dict[str, Any]) -> str:
    event = str(payload.get("event", ""))
    error = str(payload.get("error", "") or "").strip()
    guard_reason = str(payload.get("guard_block_reason", "") or "").strip()
    ai_payload = payload.get("ai", {}) if isinstance(payload.get("ai"), dict) else {}
    ai_action = str(ai_payload.get("action", "") or "")
    usdt_free = _safe_float(payload.get("usdt_free"), 0.0)
    has_open_position = bool(payload.get("has_open_position", False))
    market_regime = str(payload.get("market_regime", "flat") or "flat")
    market_dangerous = bool(payload.get("market_dangerous", False))
    sell_reason = str(payload.get("sell_reason", "") or "").strip()
    post_time_stop_block = bool(payload.get("post_time_stop_block", False))
    post_time_stop_wait = int(_safe_float(payload.get("post_time_stop_wait_candles"), 0.0))

    if error:
        return f"Ошибка цикла: {error}"
    if guard_reason:
        return human_guard_reason(guard_reason)
    if event == "cooldown":
        return "Пауза после сделки (cooldown)."
    if event in {"entry_blocked", "risk_guard_blocked"}:
        entry_block = human_entry_block_reason(payload)
        if entry_block:
            return entry_block
        return "Условия входа не выполнены."
    if event == "market_data_unavailable":
        return "Нет корректных рыночных данных: бот временно пропускает вход."
    if event in {"buy_unfilled", "sell_unfilled"}:
        return "Ордер не исполнен биржей (минимум/ликвидность/фильтры)."
    if has_open_position:
        if sell_reason:
            return f"Позиция открыта: ожидается условие выхода ({human_sell_reason(sell_reason)})."
        return "Позиция открыта: бот сопровождает сделку."
    if market_dangerous:
        return f"Рынок: {market_regime} (опасный), риск снижен."
    if post_time_stop_block:
        return f"Пауза после выхода по time_stop: осталось ~{post_time_stop_wait} свеч."
    if ai_action != "LONG":
        return "AI не подтвердил вход в покупку."
    if usdt_free < 1.0:
        return "Недостаточно USDT для минимальной покупки."
    if market_regime == "flat":
        return "Рынок во флэте: входы строже."
    return "Подходящих условий для сделки пока нет."
