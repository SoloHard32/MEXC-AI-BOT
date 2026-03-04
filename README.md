# MEXC AI BOT v1.0

[![CI](https://github.com/SoloHard32/MEXC-AI-BOT/actions/workflows/ci.yml/badge.svg)](https://github.com/SoloHard32/MEXC-AI-BOT/actions/workflows/ci.yml)
![License](https://img.shields.io/github/license/SoloHard32/MEXC-AI-BOT?style=flat-square)
![Stars](https://img.shields.io/github/stars/SoloHard32/MEXC-AI-BOT?style=flat-square)
![Issues](https://img.shields.io/github/issues/SoloHard32/MEXC-AI-BOT?style=flat-square)
![Last Commit](https://img.shields.io/github/last-commit/SoloHard32/MEXC-AI-BOT?style=flat-square)


## RU

Автономный AI-бот для MEXC (spot) с desktop GUI (PySide6 + WebView), live-мониторингом, risk-guard, автоадаптацией параметров и онлайн-дообучением модели.

### Возможности

- 2 режима работы:
  - `training` — paper-trading/обучение (реальные ордера не отправляются)
  - `live` — реальные spot-ордера на MEXC
- AI-сигналы + фильтры качества входа/выхода
- Автоадаптация параметров под рыночный режим
- Онлайн-дообучение на реальных метках
- Ensemble-модель (несколько AI-моделей)
- Risk Guard:
  - лимиты просадки
  - лимиты количества сделок
  - API soft-guard при нестабильном API
- Сохранение и восстановление состояния позиции при рестарте
- Health report / Session report
- Экспорт/импорт AI-бандла
- Переключение языка интерфейса (`RU / EN`)

### Что внутри

Основные файлы:

- `mexc_bot.py` — торговое ядро
- `mexc_bot_gui.py` — entry-point GUI
- `mexc_bot_gui_web.py` — desktop GUI + bridge + runtime мониторинг
- `ai_signal.py` — AI-сигналы/обучение/статус
- `adaptive_agent.py` — автоадаптация параметров
- `market_context.py` — рыночный контекст и режимы
- `engine_adapter.py` — адаптер backend-движка исполнения
- `position_overlays.py` — overlay-логика сопровождения позиции
- `model_evolver.py` — эволюция/регистрация модели
- `ui_dashboard/` — веб-интерфейс дашборда

### Требования

- Python 3.11+ (рекомендуется)
- Windows 10/11 (основной целевой сценарий)
- Доступ к интернету
- API ключи MEXC для режима `live`

### Установка

```bash
pip install -r requirements.txt
```

### Быстрый старт

1. Скопируйте `.env.example` в `.env` и заполните:

```env
MEXC_API_KEY=
MEXC_API_SECRET=

BOT_MODE=training
DRY_RUN=true
AI_TRAINING_MODE=true
PAPER_START_USDT=100
```

2. Запуск GUI:

```bash
python mexc_bot_gui.py
```

3. Выберите режим:

- `training` для безопасного прогрева и обучения
- `live` для реальной торговли

### Логи и отчеты

- `logs/runtime/` — runtime-логи и live status
- `logs/training/` — модели и логи обучения
- `logs/reports/` — health/session отчеты

### Безопасность

- Никогда не коммитьте `.env` и приватные ключи
- Перед публикацией репозитория используйте clean-версию без `logs/*` и секретов
- Для первых запусков используйте `training`

### FAQ

**Почему в spot нет шорта?**  
MEXC spot-режим — long-only. SHORT-сигналы AI могут использоваться как информационные/для фильтров, но не как прямой spot-шорт-ордер.

**Почему бот может не торговать?**  
Частые причины:

- качество сигнала ниже порога
- expected edge ниже минимума
- активен cooldown
- активен Risk Guard
- недостаточный баланс под минимальный ордер

**Можно ли сменить язык GUI?**  
Да, в расширенных настройках (`Русский / English`).

---

## EN

Autonomous AI trading bot for MEXC Spot with desktop GUI (PySide6 + WebView), live monitoring, risk guard, adaptive parameters, and online model learning.

### Features

- 2 operating modes:
  - `training` — paper trading / learning (no real orders sent)
  - `live` — real spot orders on MEXC
- AI signals + entry/exit quality filters
- Automatic parameter adaptation to market regime
- Online learning from real labels
- Ensemble model support (multiple AI models)
- Risk Guard:
  - drawdown limits
  - daily trade limits
  - API soft-guard on unstable API periods
- Position state persistence and restore on restart
- Health report / Session report
- AI bundle export/import
- UI language switch (`RU / EN`)

### Project Layout

Key files:

- `mexc_bot.py` — trading core
- `mexc_bot_gui.py` — GUI entry point
- `mexc_bot_gui_web.py` — desktop GUI + bridge + runtime monitor
- `ai_signal.py` — AI signals / learning / status
- `adaptive_agent.py` — adaptive tuning layer
- `market_context.py` — market regime context
- `engine_adapter.py` — execution backend adapter
- `position_overlays.py` — position management overlays
- `model_evolver.py` — model evolution/registry
- `ui_dashboard/` — dashboard frontend

### Requirements

- Python 3.11+ (recommended)
- Windows 10/11 (primary target)
- Internet access
- MEXC API keys for `live` mode

### Installation

```bash
pip install -r requirements.txt
```

### Quick Start

1. Copy `.env.example` to `.env` and fill values:

```env
MEXC_API_KEY=
MEXC_API_SECRET=

BOT_MODE=training
DRY_RUN=true
AI_TRAINING_MODE=true
PAPER_START_USDT=100
```

2. Start GUI:

```bash
python mexc_bot_gui.py
```

3. Select mode:

- `training` for safe warm-up and learning
- `live` for real trading

### Logs and Reports

- `logs/runtime/` — runtime logs and live status
- `logs/training/` — model/training artifacts
- `logs/reports/` — health/session reports

### Security

- Never commit `.env` or private API keys
- Use a clean copy without `logs/*` and secrets before publishing
- Run `training` first before switching to `live`

### FAQ

**Why no short orders in spot?**  
MEXC spot is long-only. SHORT AI signals can still be used as context/filters, but not as direct short orders.

**Why can the bot skip trading?**  
Typical reasons:

- signal quality below threshold
- expected edge below minimum
- cooldown active
- Risk Guard active
- insufficient balance for exchange minimums

**Can I switch GUI language?**  
Yes, in advanced settings (`Русский / English`).

### Контакты

Если у вас есть предложения по улучшению бота или вопрос по сотрудничеству, напишите в личные сообщения.

### Contact

If you have suggestions to improve the bot or want to discuss collaboration, please send a direct message.



## Community

- Use **Issues** for bug reports and feature requests.
- Use **Discussions** for ideas and Q&A.
- For collaboration proposals, send a direct message.

