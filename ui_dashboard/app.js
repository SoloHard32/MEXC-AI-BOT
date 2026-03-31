let bridge = null;
let state = null;
let activeTab = 'all';
let _logsRenderSig = '';
let _chartRenderSig = '';
let _pendingState = null;
let _renderQueued = false;
let _lastLocaleSig = '';

function esc(s){ return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
const RIGHT_PANEL_KEYS = ['warnings', 'training', 'market', 'guard'];
let uiLang = 'ru';

const I18N = {
  ru: {
    conn_update: 'Обновление',
    conn_mode: 'Режим',
    banner_training: 'AI обучение активно. DRY RUN: реальные ордера не отправляются.',
    banner_live: 'Боевой режим LIVE: реальные ордера разрешены.',
    banner_stopped: 'Бот остановлен. Запусти его, чтобы телеметрия снова наполнилась.',
    labels: {
      symbol: 'Символ', cycle: 'Цикл', event: 'Событие',
      usdt_real: 'USDT (реальный)', usdt_train: 'USDT (обучение)', pnl: 'PnL %', pair: 'Пара', price: 'Цена',
      balance_source: 'Источник баланса',
      action: 'Действие', conf: 'Уверенность', quality: 'Качество', edge: 'Ожид. преимущество', min_edge: 'Мин. преимущество', health: 'Здоровье', drift: 'Дрейф',
      model: 'Модель', online: 'Онлайн', hit_rate: 'Точность', wf: 'WF', labels: 'Метки', samples: 'Сэмплы', real_labels: 'Реальные метки',
      regime: 'Режим рынка', flags: 'Флаги', volatility: 'Волатильность', anomaly: 'Аномалия', data_stale: 'Данные устарели',
      locked: 'Блокировка', reason: 'Причина', api_errors: 'Ошибки API', trades_day: 'Сделок/день', drawdown: 'Просадка',
      warnings: 'Предупреждения', errors: 'Ошибки', top_block: 'Главный блокер', events: 'Событий',
      no_critical: 'Нет критичных предупреждений', waiting: 'Ожидание телеметрии...',
      updating: 'Обновляется...',
      no_chart_data: 'Нет телеметрии для графика...',
      telemetry_wait_trade: 'Телеметрия пары появится только во время открытой сделки.',
      data_quality: 'Качество данных',
      signal_reason: 'Объяснение',
      telem_quality: 'Качество',
      telem_edge: 'Edge',
      telem_backend: 'Backend',
      important: 'Важные', search: 'Поиск...', all: 'Все', trades: 'Сделки', signals: 'Сигналы', errs: 'Ошибки',
      live_status: 'Статус', live_mode: 'Режим', live_pair: 'Пара', live_price: 'Цена', live_event: 'Событие', live_reason: 'Причина', live_guard: 'Защита',
      live_orders_empty: 'Нет данных', labels_observed: '(набл., live)', labels_real: '(реал.)',
    },
    ui: {
      hControl: 'Управление ботом',
      btnStart: 'Запуск', btnStop: 'Стоп', btnRestart: 'Рестарт', btnSave: 'Сохранить', btnLogs: 'Логи',
      openLiveBtn: 'Live окно', openAdvBtn: 'Настройки', btnReportsDir: 'Отчёты',
      modeLabel: 'Режим:', hBalance: 'Баланс', hAiModule: 'AI модуль', hTelemetry: 'Телеметрия', telemetryPill: 'Сигналы / health',
      hLogs: 'Логи', hWarnings: 'Предупреждения', hTraining: 'Обучение', hMarket: 'Рынок', hGuard: 'Защита риска',
      hAdv: 'Расширенные настройки', advCloseBtn: 'Закрыть', advSaveBtn: 'Сохранить настройки',
      lblApiKey: 'API key (MEXC)', lblApiSecret: 'API secret (MEXC)', lblAdvisoryEnable: 'Включить внешний advisory (overlay)',
      lblAdvisoryName: 'Название advisory-провайдера', lblAdvisoryUrl: 'URL advisory', lblTimeoutSec: 'Таймаут, сек', lblTtlSec: 'TTL, сек',
      lblWeightTraining: 'Вес training (0..0.35)', lblWeightLive: 'Вес live (0..0.35)',
      lblPaperStart: 'Paper стартовый баланс (USDT, training)',
      lblPaperApply: 'Применить сразу (сбросить paper-позиции, только при остановленном боте)',
      lblUiLanguage: 'Язык интерфейса',
      advStatusNotChecked: 'Статус: не проверено',
      advStatusChecking: 'Статус: проверка advisory...',
      advStatusPrefix: 'Статус:',
      advStatusCheckError: 'Статус: ошибка автопроверки advisory.',
      hLiveNow: 'Сейчас', hLiveAiMarket: 'AI / Рынок', hLiveRisk: 'Риск / Баланс',
      hLiveModal: 'Live мониторинг', liveCloseBtn: 'Закрыть',
    },
  },
  en: {
    conn_update: 'Updated',
    conn_mode: 'Mode',
    banner_training: 'AI training active. DRY RUN: real orders are not sent.',
    banner_live: 'LIVE mode: real orders are enabled.',
    banner_stopped: 'Bot is stopped. Start it to resume telemetry.',
    labels: {
      symbol: 'Symbol', cycle: 'Cycle', event: 'Event',
      usdt_real: 'USDT (real)', usdt_train: 'USDT (training)', pnl: 'PnL %', pair: 'Pair', price: 'Price',
      balance_source: 'Balance source',
      action: 'Action', conf: 'Conf', quality: 'Quality', edge: 'Expected edge', min_edge: 'Min edge', health: 'Health', drift: 'Drift',
      model: 'Model', online: 'Online', hit_rate: 'Hit-rate', wf: 'WF', labels: 'Labels', samples: 'Samples', real_labels: 'Real labels',
      regime: 'Regime', flags: 'Flags', volatility: 'Volatility', anomaly: 'Anomaly', data_stale: 'Data stale',
      locked: 'Locked', reason: 'Reason', api_errors: 'API errors', trades_day: 'Trades/day', drawdown: 'Drawdown',
      warnings: 'Warnings', errors: 'Errors', top_block: 'Top block', events: 'Events',
      no_critical: 'No critical warnings', waiting: 'Waiting for telemetry...',
      updating: 'Updating...',
      no_chart_data: 'No telemetry data for chart...',
      telemetry_wait_trade: 'Pair telemetry is shown only while a position is open.',
      data_quality: 'Data quality',
      signal_reason: 'Explanation',
      telem_quality: 'Quality',
      telem_edge: 'Edge',
      telem_backend: 'Backend',
      important: 'Important', search: 'Search...', all: 'All', trades: 'Trades', signals: 'Signals', errs: 'Errors',
      live_status: 'Status', live_mode: 'Mode', live_pair: 'Pair', live_price: 'Price', live_event: 'Event', live_reason: 'Reason', live_guard: 'Guard',
      live_orders_empty: 'No data', labels_observed: '(observed, live)', labels_real: '(real)',
    },
    ui: {
      hControl: 'Bot Control',
      btnStart: 'Start', btnStop: 'Stop', btnRestart: 'Restart', btnSave: 'Save', btnLogs: 'Logs',
      openLiveBtn: 'Live window', openAdvBtn: 'Settings', btnReportsDir: 'Reports',
      modeLabel: 'Mode:', hBalance: 'Balance', hAiModule: 'AI module', hTelemetry: 'Telemetry', telemetryPill: 'Signals / health',
      hLogs: 'Logs', hWarnings: 'Warnings', hTraining: 'Training', hMarket: 'Market', hGuard: 'Risk guard',
      hAdv: 'Advanced settings', advCloseBtn: 'Close', advSaveBtn: 'Save settings',
      lblApiKey: 'API key (MEXC)', lblApiSecret: 'API secret (MEXC)', lblAdvisoryEnable: 'Enable external advisory (overlay)',
      lblAdvisoryName: 'Advisory provider name', lblAdvisoryUrl: 'Advisory URL', lblTimeoutSec: 'Timeout sec', lblTtlSec: 'TTL sec',
      lblWeightTraining: 'Weight training (0..0.35)', lblWeightLive: 'Weight live (0..0.35)',
      lblPaperStart: 'Paper start balance (USDT, training)',
      lblPaperApply: 'Apply now (reset paper positions, only when bot is stopped)',
      lblUiLanguage: 'Interface language',
      advStatusNotChecked: 'Status: not checked',
      advStatusChecking: 'Status: checking advisory...',
      advStatusPrefix: 'Status:',
      advStatusCheckError: 'Status: advisory self-check failed.',
      hLiveNow: 'Now', hLiveAiMarket: 'AI / Market', hLiveRisk: 'Risk / Balance',
      hLiveModal: 'Live monitoring', liveCloseBtn: 'Close',
    },
  },
};

function tr(key){
  const pack = I18N[uiLang] || I18N.ru;
  const parts = String(key || '').split('.');
  let cur = pack;
  for (const p of parts) {
    if (cur && Object.prototype.hasOwnProperty.call(cur, p)) cur = cur[p];
    else return key;
  }
  return cur ?? key;
}

function setKV(elId, rows){
  const el = document.getElementById(elId);
  if (!el) return;
  el.innerHTML = rows.map(([k,v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(v)}</div>`).join('');
}

function displayLivePrice(rawPrice, status){
  const priceNum = Number(rawPrice || 0);
  const s = status || {};
  if (priceNum > 0) return rawPrice;
  const evt = String(s.event || '').toLowerCase();
  const statusText = String(s.status || '').toLowerCase();
  const running = statusText === 'работает' || statusText === 'running';
  if (running && evt === 'cycle_busy') return tr('labels.updating');
  return rawPrice || '-';
}

function translateBalanceSource(raw){
  const v = String(raw || '').trim().toLowerCase();
  if (!v) return '-';
  if (uiLang === 'en') {
    if (v === 'exchange') return 'Exchange';
    if (v === 'cache') return 'Cached exchange';
    if (v === 'paper') return 'Paper only';
    if (v === 'unknown') return 'Unknown';
    return raw;
  }
  if (v === 'exchange') return 'Биржа';
  if (v === 'cache') return 'Кэш биржи';
  if (v === 'paper') return 'Только paper';
  if (v === 'unknown') return 'Неизвестно';
  return raw;
}

function yesNo(value){
  const on = !!value;
  if (uiLang === 'en') return on ? 'ON' : 'OFF';
  return on ? 'ВКЛ' : 'ВЫКЛ';
}

function clamp(v, lo, hi){
  return Math.max(lo, Math.min(hi, Number(v || 0)));
}

function resizeCanvas(canvas){
  const dpr = window.devicePixelRatio || 1;
  const w = Math.max(200, Math.floor(canvas.clientWidth * dpr));
  const h = Math.max(160, Math.floor(canvas.clientHeight * dpr));
  if (canvas.width !== w || canvas.height !== h) {
    canvas.width = w;
    canvas.height = h;
  }
}

function tagFor(line){
  const s = line.toLowerCase();
  if ((s.includes('buy') && !s.includes('sell')) || s.includes('opened')) return ['OPEN','open'];
  if (s.includes('sell') || s.includes('close')) return ['CLOSE','close'];
  if (s.includes('guard')) return ['GUARD','guard'];
  if (s.includes('api') || s.includes('timeout')) return ['API','api'];
  if (s.includes('signal') || s.includes('quality') || s.includes('edge')) return ['AI','ai'];
  return ['CYCLE','cycle'];
}

function important(line){
  const s = line.toLowerCase();
  return ['[trade]','error','warning','guard','open','close','buy','sell','timeout','stale'].some(k => s.includes(k));
}

function translateWarningLine(line){
  if (uiLang !== 'en') return line;
  const s = String(line || '');
  const maps = [
    ['Guard lock:', 'Guard lock:'],
    ['Рыночные данные устарели', 'Market data is stale'],
    ['Ожидаемое преимущество ниже порога', 'Expected edge is below threshold'],
    ['Цикл обновляется нестабильно (высокая задержка телеметрии).', 'Cycle updates are unstable (high telemetry latency).'],
    ['Реальный MEXC баланс временно не подтвержден (показано последнее валидное значение).', 'Real MEXC balance is temporarily unconfirmed (showing last valid value).'],
    ['Рекомендуется soft режим live: backend нестабилен для strict.', 'Soft live mode is recommended: backend is unstable for strict.'],
    ['Backend готов: можно перейти на strict live.', 'Backend is ready: you can switch to strict live.'],
    ['Внешний движок недоступен: используется python fallback.', 'External engine unavailable: using python fallback.'],
  ];
  let out = s;
  for (const [ru, en] of maps) out = out.replaceAll(ru, en);
  return out;
}

function translateReasonLine(line){
  if (uiLang !== 'en') return line;
  const s = String(line || '');
  const maps = [
    ['Бот не запущен', 'Bot is not running'],
    ['Позиция открыта: бот сопровождает сделку.', 'Position is open: bot is managing the trade.'],
    ['Сигнал слабый', 'Signal is weak'],
    ['Ожидаемое преимущество ниже порога', 'Expected edge is below threshold'],
    ['Рыночные данные устарели', 'Market data is stale'],
    ['Серия API-ошибок', 'API error streak'],
  ];
  let out = s;
  for (const [ru, en] of maps) out = out.replaceAll(ru, en);
  return out;
}

function translateLogLine(line){
  if (uiLang !== 'en') return line;
  const s = String(line || '');
  const maps = [
    ['Бот запущен', 'Bot started'],
    ['Бот остановлен', 'Bot stopped'],
    ['Получен запрос на остановку.', 'Stop request received.'],
    ['Необработанная ошибка в цикле', 'Unhandled cycle error'],
    ['Пара ', 'Pair '],
    ['неактивна, цикл пропущен.', 'is inactive, cycle skipped.'],
    ['Сбой синхронизации позиций при старте', 'Startup position sync failed'],
    ['Синхронизация позиций при старте', 'Startup position sync'],
    ['восстановлено', 'restored'],
    ['очищено_устаревших', 'stale_cleared'],
    ['Выполняется BUY', 'Executing BUY'],
    ['Выполняется SELL', 'Executing SELL'],
    ['Режим=training', 'Mode=training'],
    ['Режим=live', 'Mode=live'],
    ['Автопилот=True', 'Autopilot=True'],
    ['Автопилот=False', 'Autopilot=False'],
    ['AI-сигнал=True', 'AI-signal=True'],
    ['AI-сигнал=False', 'AI-signal=False'],
    ['Состояние позиции недоступно/повреждено. Продолжаем без восстановления позиции.', 'Position state unavailable/corrupted. Continue without restored position state.'],
    ['Рыночные данные устарели', 'Market data is stale'],
    ['Ожидаемое преимущество ниже порога', 'Expected edge is below threshold'],
    ['Низкое качество сигнала', 'Low signal quality'],
    ['Низкая ликвидность пары', 'Low pair liquidity'],
    ['Низкий объем пары', 'Low pair volume'],
    ['Ошибка API', 'API error'],
    ['Превышен лимит API', 'API rate limit exceeded'],
    ['таймаут', 'timeout'],
    ['Трейлинг-стоп', 'Trailing stop'],
    ['тейк-профит', 'Take profit'],
    ['стоп-лосс', 'Stop loss'],
    ['Причина:', 'Reason:'],
    ['Позиция открыта', 'Position opened'],
    ['Позиция закрыта', 'Position closed'],
    ['Сделка пропущена', 'Trade skipped'],
    ['цикл пропущен', 'cycle skipped'],
    ['guard lock', 'guard lock'],
  ];
  let out = s;
  for (const [ru, en] of maps) out = out.replaceAll(ru, en);
  return out;
}

function matchTab(line){
  const s = line.toLowerCase();
  if (activeTab === 'trade') return s.includes('[trade]') || s.includes('buy') || s.includes('sell') || s.includes('close');
  if (activeTab === 'signal') return s.includes('signal') || s.includes('quality') || s.includes('edge') || s.includes('ai');
  if (activeTab === 'error') return s.includes('error') || s.includes('warning') || s.includes('exception') || s.includes('traceback');
  return true;
}

function renderLogs(lines){
  const q = document.getElementById('logSearch').value.trim().toLowerCase();
  const imp = document.getElementById('importantOnly').checked;
  const box = document.getElementById('logBox');
  const total = Array.isArray(lines) ? lines.length : 0;
  const last = total > 0 ? String(lines[total - 1] || '') : '';
  const sig = `${uiLang}|${activeTab}|${q}|${imp ? 1 : 0}|${total}|${last}`;
  if (sig === _logsRenderSig) return;
  _logsRenderSig = sig;
  const filtered = lines.filter(l => (!q || l.toLowerCase().includes(q)) && (!imp || important(l)) && matchTab(l));
  // Newest first: fresh logs must be shown from top to bottom.
  const compact = filtered.slice(-48).reverse();
  box.innerHTML = compact.map(line => {
    const [txt, cls] = tagFor(line);
    return `<div class="log-row ${cls}"><span class="tag ${cls}">${txt}</span>${esc(translateLogLine(line))}</div>`;
  }).join('');
}

function drawChart(payload, status){
  const c = document.getElementById('edgeChart');
  if (!c) return;
  const s = status || {};
  const candlesRaw = Array.isArray(payload?.candles) ? payload.candles : [];
  const nSig = candlesRaw.length;
  const lastSig = nSig > 0 ? candlesRaw[nSig - 1] : null;
  const positionOpen = !!s.has_open_position;
  const sig = `${uiLang}|${positionOpen ? 1 : 0}|${nSig}|${lastSig ? [lastSig.o,lastSig.h,lastSig.l,lastSig.c,lastSig.v].join('|') : '-'}`;
  if (sig === _chartRenderSig && c.width > 0 && c.height > 0) return;
  _chartRenderSig = sig;
  resizeCanvas(c);
  const ctx = c.getContext('2d');
  const w = c.width, h = c.height;
  ctx.clearRect(0,0,w,h);

  const left = 54;
  const right = w - 14;
  const top = 14;
  const volH = Math.max(54, Math.floor(h * 0.22));
  const gap = 10;
  const mainBottom = h - volH - gap - 22;
  const volTop = mainBottom + gap;
  const volBottom = h - 24;

  ctx.fillStyle = '#070d16';
  ctx.fillRect(0, 0, w, h);

  if (!positionOpen) {
    ctx.fillStyle = '#8da6c8';
    ctx.font = '13px Segoe UI';
    ctx.fillText(tr('labels.telemetry_wait_trade'), left, h / 2);
    return;
  }

  const candles = candlesRaw
    .map(x => ({
      o: Number(x?.o || 0),
      h: Number(x?.h || 0),
      l: Number(x?.l || 0),
      c: Number(x?.c || 0),
      v: Number(x?.v || 0),
    }))
    .filter(x => x.h > 0 && x.l > 0 && x.c > 0)
    .slice(-80);

  if (!candles.length) {
    const q = Array.isArray(payload?.ai_quality) ? payload.ai_quality.map(Number).filter(Number.isFinite) : [];
    const e = Array.isArray(payload?.expected_edge) ? payload.expected_edge.map(Number).filter(Number.isFinite).map(v => v * 100) : [];
    const bh = Array.isArray(payload?.backend_health) ? payload.backend_health.map(Number).filter(Number.isFinite) : [];
    const series = [
      { key: 'quality', label: tr('labels.telem_quality'), color: '#f3c44f', values: q.slice(-80), valueSuffix: '' },
      { key: 'edge', label: tr('labels.telem_edge'), color: '#9d63ff', values: e.slice(-80), valueSuffix: '%' },
      { key: 'backend', label: tr('labels.telem_backend'), color: '#00d8a8', values: bh.slice(-80), valueSuffix: '%' },
    ].filter(item => item.values.length > 1);

    if (!series.length) {
      ctx.fillStyle = '#8da6c8';
      ctx.font = '13px Segoe UI';
      ctx.fillText(tr('labels.no_chart_data'), left, h / 2);
      return;
    }

    const normalizeSeries = (values) => {
      let minV = Math.min(...values);
      let maxV = Math.max(...values);
      if (!Number.isFinite(minV) || !Number.isFinite(maxV) || maxV <= minV) {
        minV -= 1;
        maxV += 1;
      }
      const span = Math.max(1e-9, maxV - minV);
      return values.map(v => (v - minV) / span);
    };
    const mapY = (v) => mainBottom - v * (mainBottom - top);

    ctx.setLineDash([4, 6]);
    ctx.strokeStyle = 'rgba(120,146,180,0.20)';
    ctx.lineWidth = 1;
    for (let i = 0; i < 6; i++) {
      const y = top + i * (mainBottom - top) / 5;
      ctx.beginPath();
      ctx.moveTo(left, y);
      ctx.lineTo(right, y);
      ctx.stroke();
    }
    ctx.setLineDash([]);

    ctx.fillStyle = '#8da6c8';
    ctx.font = '12px Segoe UI';
    for (let i = 0; i < 6; i++) {
      const y = top + i * (mainBottom - top) / 5;
      const pct = 100 - (i * 20);
      ctx.fillText(`${pct}%`, 10, y + 4);
    }

    series.forEach((item, idx) => {
      const vals = normalizeSeries(item.values);
      const step = (right - left) / Math.max(1, vals.length - 1);
      ctx.strokeStyle = item.color;
      ctx.lineWidth = 1.8;
      ctx.beginPath();
      vals.forEach((value, i) => {
        const x = left + i * step;
        const y = mapY(value);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.fillStyle = item.color;
      const latestRaw = item.values[item.values.length - 1];
      const latestText = Number.isFinite(latestRaw) ? latestRaw.toFixed(item.key === 'backend' ? 0 : 2) : '-';
      ctx.fillText(`${item.label} ${latestText}${item.valueSuffix}`, left + idx * 122, top + 12);
    });
    ctx.fillStyle = '#8da6c8';
    ctx.fillText(uiLang === 'en' ? 'Normalized history' : 'Нормализованная история', left, volTop + 12);
    return;
  }

  const highs = candles.map(x => x.h);
  const lows = candles.map(x => x.l);
  const vols = candles.map(x => Math.max(0, x.v));
  let minP = Math.min(...lows);
  let maxP = Math.max(...highs);
  if (!Number.isFinite(minP) || !Number.isFinite(maxP) || maxP <= minP) {
    minP = Math.min(...candles.map(x => x.c));
    maxP = Math.max(...candles.map(x => x.c)) + 1e-6;
  }
  const pad = (maxP - minP) * 0.08;
  minP -= pad;
  maxP += pad;

  const mapY = (p) => mainBottom - ((p - minP) / Math.max(1e-9, maxP - minP)) * (mainBottom - top);
  const n = candles.length;
  const step = (right - left) / Math.max(1, n - 1);
  const bodyW = Math.max(3, Math.min(10, step * 0.6));

  // grid
  ctx.setLineDash([4, 6]);
  ctx.strokeStyle = 'rgba(120,146,180,0.20)';
  ctx.lineWidth = 1;
  for (let i = 0; i < 6; i++) {
    const y = top + i * (mainBottom - top) / 5;
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(right, y);
    ctx.stroke();
  }
  ctx.setLineDash([]);

  // y labels
  ctx.fillStyle = '#8da6c8';
  ctx.font = '12px Segoe UI';
  for (let i = 0; i < 6; i++) {
    const y = top + i * (mainBottom - top) / 5;
    const v = maxP - (i * (maxP - minP) / 5);
    ctx.fillText(v.toFixed(2), 6, y + 4);
  }

  // candles + volume
  const maxVol = Math.max(1e-9, ...vols);
  for (let i = 0; i < n; i++) {
    const x = left + i * step;
    const k = candles[i];
    const up = k.c >= k.o;
    const col = up ? '#00d8a8' : '#ff4d5f';

    // wick
    ctx.strokeStyle = col;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, mapY(k.h));
    ctx.lineTo(x, mapY(k.l));
    ctx.stroke();

    // body
    const yO = mapY(k.o);
    const yC = mapY(k.c);
    const yTop = Math.min(yO, yC);
    const hBody = Math.max(1, Math.abs(yC - yO));
    ctx.fillStyle = col;
    ctx.fillRect(x - bodyW / 2, yTop, bodyW, hBody);

    // volume
    const vh = (k.v / maxVol) * (volBottom - volTop);
    ctx.globalAlpha = 0.72;
    ctx.fillRect(x - bodyW / 2, volBottom - vh, bodyW, vh);
    ctx.globalAlpha = 1.0;
  }

  // MA overlays
  const drawMA = (period, color) => {
    if (n < period) return;
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < n; i++) {
      if (i < period - 1) continue;
      let s = 0;
      for (let j = i - period + 1; j <= i; j++) s += candles[j].c;
      const ma = s / period;
      const x = left + i * step;
      const y = mapY(ma);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    }
    ctx.stroke();
  };
  drawMA(5, '#f3c44f');
  drawMA(10, '#9d63ff');
  drawMA(20, '#9ccf74');

  // legends
  ctx.font = '12px Segoe UI';
  ctx.fillStyle = '#f3c44f';
  ctx.fillText('MA5', left, top + 12);
  ctx.fillStyle = '#9d63ff';
  ctx.fillText('MA10', left + 44, top + 12);
  ctx.fillStyle = '#9ccf74';
  ctx.fillText('MA20', left + 96, top + 12);

  ctx.fillStyle = '#8da6c8';
  ctx.fillText('VOL', left, volTop + 12);
}

function applyLocaleStatic(){
  const localeSig = `${uiLang}`;
  if (_lastLocaleSig === localeSig) return;
  _lastLocaleSig = localeSig;
  document.documentElement.lang = uiLang;
  const ui = (I18N[uiLang] || I18N.ru).ui || {};
  const setTxt = (id, val) => {
    const el = document.getElementById(id);
    if (el && typeof val === 'string') el.textContent = val;
  };
  [
    'hControl','btnStart','btnStop','btnRestart','btnSave','btnLogs','openLiveBtn','openAdvBtn',
    'btnReportsDir','modeLabel','hBalance','hAiModule','hTelemetry','telemetryPill',
    'hLogs','hWarnings','hTraining','hMarket','hGuard','hAdv','advCloseBtn','advSaveBtn',
    'lblApiKey','lblApiSecret','lblAdvisoryName','lblAdvisoryUrl','lblTimeoutSec','lblTtlSec','lblWeightTraining','lblWeightLive',
    'lblPaperStart','lblUiLanguage'
  ].forEach((id) => setTxt(id, ui[id]));

  const liveTitles = document.querySelectorAll('.live-grid .card h3');
  if (liveTitles.length >= 3) {
    liveTitles[0].textContent = ui.hLiveNow || liveTitles[0].textContent;
    liveTitles[1].textContent = ui.hLiveAiMarket || liveTitles[1].textContent;
    liveTitles[2].textContent = ui.hLiveRisk || liveTitles[2].textContent;
  }
  const liveHead = document.querySelector('#liveModal .modal-head h3');
  if (liveHead) liveHead.textContent = ui.hLiveModal || liveHead.textContent;
  setTxt('liveCloseBtn', ui.liveCloseBtn);

  const lblAdvisoryEnable = document.getElementById('lblAdvisoryEnable');
  if (lblAdvisoryEnable) {
    const cb = lblAdvisoryEnable.querySelector('input');
    lblAdvisoryEnable.innerHTML = '';
    if (cb) lblAdvisoryEnable.appendChild(cb);
    lblAdvisoryEnable.append(` ${ui.lblAdvisoryEnable || 'Enable external advisory (overlay)'}`);
  }
  const lblPaperApply = document.getElementById('lblPaperApply');
  if (lblPaperApply) {
    const cb = lblPaperApply.querySelector('input');
    lblPaperApply.innerHTML = '';
    if (cb) lblPaperApply.appendChild(cb);
    lblPaperApply.append(` ${ui.lblPaperApply || 'Apply now'}`);
  }

  const tsText = document.getElementById('tsText');
  if (tsText) tsText.textContent = `${tr('conn_update')}: -`;
  const modeText = document.getElementById('modeText');
  if (modeText) modeText.textContent = `${tr('conn_mode')}: -`;

  const search = document.getElementById('logSearch');
  if (search) search.placeholder = tr('labels.search');

  const importantLabel = document.getElementById('importantOnly')?.parentElement;
  if (importantLabel) {
    const cb = importantLabel.querySelector('input');
    importantLabel.innerHTML = '';
    if (cb) importantLabel.appendChild(cb);
    importantLabel.append(` ${tr('labels.important')}`);
  }

  const tabs = document.querySelectorAll('.tab');
  if (tabs.length >= 4) {
    tabs[0].textContent = tr('labels.all');
    tabs[1].textContent = tr('labels.trades');
    tabs[2].textContent = tr('labels.signals');
    tabs[3].textContent = tr('labels.errs');
  }

  const advTestStatus = document.getElementById('advTestStatus');
  if (advTestStatus) {
    const cur = String(advTestStatus.textContent || '').trim();
    if (
      !cur ||
      cur.startsWith('Статус:') ||
      cur.startsWith('Status:')
    ) {
      advTestStatus.textContent = ui.advStatusNotChecked || 'Status: not checked';
    }
  }
}

function render(st){
  state = st;
  const h = st.header || {};
  uiLang = String(h.lang || uiLang || 'ru').toLowerCase();
  if (!['ru', 'en'].includes(uiLang)) uiLang = 'ru';
  applyLocaleStatic();
  const conn = document.getElementById('connBadge');
  conn.textContent = h.connection_text || '-';
  conn.className = 'badge ' + (
    h.connection === 'connected' ? 'ok' :
    h.connection === 'stale' ? 'warn' :
    h.connection === 'sync' ? 'warn' :
    h.connection === 'idle' ? '' :
    'err'
  );
  document.getElementById('tsText').textContent = `${tr('conn_update')}: ${h.ts || '-'}`;
  document.getElementById('modeText').textContent = `${tr('conn_mode')}: ${h.mode || '-'}`;
  document.getElementById('marketText').textContent = `${h.market || 'SPOT'} / MEXC`;
  document.getElementById('pidText').textContent = `PID ${h.pid || '-'}`;
  const controls = st.controls || {};
  const proc = st.proc || {};
  const procBusy = !!proc.busy;
  const runningNow = !!controls.running;
  const bannerText = !runningNow && !procBusy
    ? tr('banner_stopped')
    : (String(controls.mode || '').toLowerCase() === 'live' ? tr('banner_live') : tr('banner_training'));
  document.getElementById('banner').textContent = bannerText;
  const mode = (controls.mode || 'training').toLowerCase();
  const bTrain = document.getElementById('modeTraining');
  const bLive = document.getElementById('modeLive');
  if (bTrain && bLive) {
    bTrain.classList.toggle('active', mode === 'training');
    bLive.classList.toggle('active', mode === 'live');
  }

  const s = st.status || {};
  const statusReason = s.no_entry_summary || s.reason || '-';
  const signalReason = s.signal_reason || statusReason;
  const livePriceText = displayLivePrice((st.balance || {}).price, s);
  setKV('statusMini', [
    [tr('labels.symbol'), s.symbol],
    [tr('labels.cycle'), s.cycle],
    [tr('labels.price'), livePriceText],
    [tr('labels.signal_reason'), translateReasonLine(signalReason)],
  ]);
  const b = st.balance || {};
  setKV('balanceCard', [
    [tr('labels.usdt_real'), b.exchange_usdt],
    [tr('labels.usdt_train'), b.paper_usdt],
    [tr('labels.pnl'), `${b.pnl_pct ?? 0}%`],
    [tr('labels.balance_source'), translateBalanceSource(b.exchange_balance_source || '-')],
  ]);
  const a = st.ai_signal || {};
  setKV('aiCard', [
    [tr('labels.action'), a.action],
    [tr('labels.conf'), a.conf],
    [tr('labels.quality'), a.quality],
    [tr('labels.edge'), a.edge_pct + '%'],
  ]);
  const t = st.training || {};
  const labelsReal = Number(t.labels || 0);
  const labelsBaseReal = Number(t.labels_real || labelsReal || 0);
  const samples = Number(t.samples || 0);
  const labelsMode = String(t.labels_mode || 'real').toLowerCase();
  const labelsView = labelsMode === 'observed'
    ? `${labelsReal} ${tr('labels.labels_observed')}`
    : `${labelsReal} ${tr('labels.labels_real')}`;
  const trainingRows = [
    [tr('labels.model'), t.model],
    [tr('labels.hit_rate'), t.hit_rate + '%'],
    [tr('labels.labels'), labelsView],
  ];
  if (labelsMode === 'observed') {
    trainingRows.push([tr('labels.real_labels'), labelsBaseReal]);
  }
  setKV('trainingCard', trainingRows);
  const m = st.market || {};
  setKV('marketCard', [
    [tr('labels.regime'), m.regime],
    [tr('labels.volatility'), m.volatility],
    [tr('labels.anomaly'), m.anomaly],
    [tr('labels.data_quality'), `${((m.data_quality_score ?? 0) * 100).toFixed(0)}%`],
  ]);
  const g = st.guard || {};
  setKV('guardCard', [
    [tr('labels.locked'), yesNo(g.locked)],
    [tr('labels.api_errors'), `${g.api_errors}/${g.api_max}`],
    [tr('labels.drawdown'), g.drawdown + '%'],
  ]);

  const warns = st.warnings || [];
  const signalExplain = Array.isArray(s.signal_explainer) ? s.signal_explainer.slice(0, 2).map(translateReasonLine) : [];
  const compactWarns = [...signalExplain, ...warns.map(translateWarningLine)].slice(0, 3);
  document.getElementById('warnBox').innerHTML = compactWarns.length ? compactWarns.map(w => `<div>• ${esc(w)}</div>`).join('') : tr('labels.no_critical');

  renderLogs(st.logs || []);
  drawChart(st.telemetry || {}, s);

  const aiBadge = document.getElementById('badgeAiSignal');
  const guardBadge = document.getElementById('badgeGuard');
  const ai = st.ai_signal || {};
  const gd = st.guard || {};
  if (aiBadge) {
    aiBadge.textContent = `AI signal: ${ai.action || '-'}`;
    aiBadge.className = 'small-badge ' + ((ai.quality || 0) >= 0.6 ? 'ok' : (ai.quality || 0) >= 0.45 ? 'warn' : 'err');
  }
  if (guardBadge) {
    guardBadge.textContent = `${tr('labels.live_guard')}: ${yesNo(gd.locked)}`;
    guardBadge.className = 'small-badge ' + (gd.locked ? 'warn' : 'ok');
  }

  renderLiveModal(st);

  const startBtn = document.querySelector('button[data-act="startBot"]');
  const stopBtn = document.querySelector('button[data-act="stopBot"]');
  const restartBtn = document.querySelector('button[data-act="restartBot"]');
  const saveBtn = document.querySelector('button[data-act="saveSettings"]');
  if (startBtn) startBtn.disabled = !!controls.running || procBusy;
  if (stopBtn) stopBtn.disabled = !controls.running || procBusy;
  if (restartBtn) restartBtn.disabled = procBusy;
  if (saveBtn) saveBtn.disabled = procBusy;

  if (procBusy) {
    const sec = Number(proc.elapsed_sec || 0).toFixed(1);
    const op = String(proc.op || 'operation');
    document.getElementById('banner').textContent = `Системная операция: ${op} (${sec}s)...`;
  }
}

function flushLatestState(){
  _renderQueued = false;
  if (document.hidden) return;
  const next = _pendingState;
  _pendingState = null;
  if (next) render(next);
}

function scheduleRender(st){
  _pendingState = st;
  if (document.hidden || _renderQueued) return;
  _renderQueued = true;
  window.requestAnimationFrame(flushLatestState);
}

function showToast(msg){
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 1800);
}

function collectRightPanelsState(){
  const out = {};
  document.querySelectorAll('.right-collapsible[data-right-key]').forEach(panel => {
    const key = panel.getAttribute('data-right-key');
    if (!key) return;
    out[key] = !!panel.open;
  });
  return out;
}

function applyRightPanelsState(saved){
  const src = (saved && typeof saved === 'object') ? saved : {};
  document.querySelectorAll('.right-collapsible[data-right-key]').forEach(panel => {
    const key = panel.getAttribute('data-right-key');
    if (!key) return;
    panel.open = !!src[key];
  });
}

function saveRightPanelsState(){
  if (!bridge || !bridge.saveUiState) return;
  const payload = { right_panels: collectRightPanelsState() };
  bridge.saveUiState(JSON.stringify(payload));
}

function initRightPanelsState(){
  // Дефолт при старте: справа все свернуто.
  const collapsedByDefault = {};
  RIGHT_PANEL_KEYS.forEach((k) => { collapsedByDefault[k] = false; });
  applyRightPanelsState(collapsedByDefault);

  if (bridge && bridge.getUiState) {
    bridge.getUiState((payload) => {
      try {
        const data = JSON.parse(payload || '{}');
        applyRightPanelsState(data.right_panels || collapsedByDefault);
      } catch (_) {
        applyRightPanelsState(collapsedByDefault);
      }
    });
  }

  document.querySelectorAll('.right-collapsible[data-right-key]').forEach(panel => {
    panel.addEventListener('toggle', () => saveRightPanelsState());
  });
}

function bindActions(){
  document.querySelectorAll('button[data-act]').forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.disabled) return;
      const m = btn.getAttribute('data-act');
      if (m === 'startBot' || m === 'stopBot' || m === 'restartBot' || m === 'saveSettings') {
        btn.disabled = true;
      }
      if (bridge && bridge[m]) bridge[m]();
    });
  });
  const bTrain = document.getElementById('modeTraining');
  const bLive = document.getElementById('modeLive');
  if (bTrain) bTrain.addEventListener('click', () => { if (bridge) bridge.setMode('training'); });
  if (bLive) bLive.addEventListener('click', () => { if (bridge) bridge.setMode('live'); });
  document.getElementById('logSearch').addEventListener('input', () => state && renderLogs(state.logs || []));
  document.getElementById('importantOnly').addEventListener('change', () => state && renderLogs(state.logs || []));
  document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
      t.classList.add('active');
      activeTab = t.getAttribute('data-tab');
      if (state) renderLogs(state.logs || []);
    });
  });
  const advModal = document.getElementById('advModal');
  const openAdvBtn = document.getElementById('openAdvBtn');
  const closeAdvBtn = document.getElementById('advCloseBtn');
  const saveAdvBtn = document.getElementById('advSaveBtn');
  const advTestStatus = document.getElementById('advTestStatus');
  const apiKeyInput = document.getElementById('apiKeyInput');
  const apiSecretInput = document.getElementById('apiSecretInput');
  const advisoryEnabledInput = document.getElementById('advisoryEnabledInput');
  const advisoryNameInput = document.getElementById('advisoryNameInput');
  const advisoryUrlInput = document.getElementById('advisoryUrlInput');
  const advisoryTimeoutInput = document.getElementById('advisoryTimeoutInput');
  const advisoryTtlInput = document.getElementById('advisoryTtlInput');
  const advisoryWeightTrainingInput = document.getElementById('advisoryWeightTrainingInput');
  const advisoryWeightLiveInput = document.getElementById('advisoryWeightLiveInput');
  const paperStartInput = document.getElementById('paperStartInput');
  const paperApplyNowInput = document.getElementById('paperApplyNowInput');
  const uiLanguageInput = document.getElementById('uiLanguageInput');
  const uiLangRuBtn = document.getElementById('uiLangRuBtn');
  const uiLangEnBtn = document.getElementById('uiLangEnBtn');
  function setUiLangButtons(langValue){
    const v = String(langValue || 'ru').toLowerCase() === 'en' ? 'en' : 'ru';
    if (uiLanguageInput) uiLanguageInput.value = v;
    if (uiLangRuBtn) uiLangRuBtn.classList.toggle('active', v === 'ru');
    if (uiLangEnBtn) uiLangEnBtn.classList.toggle('active', v === 'en');
  }
  if (uiLangRuBtn) uiLangRuBtn.addEventListener('click', () => setUiLangButtons('ru'));
  if (uiLangEnBtn) uiLangEnBtn.addEventListener('click', () => setUiLangButtons('en'));
  if (openAdvBtn) openAdvBtn.addEventListener('click', () => {
    if (bridge && (bridge.getAdvancedSettings || bridge.getApiSettings)) {
      const getter = bridge.getAdvancedSettings ? bridge.getAdvancedSettings.bind(bridge) : bridge.getApiSettings.bind(bridge);
      getter((payload) => {
        try {
          const data = JSON.parse(payload || '{}');
          apiKeyInput.value = data.api_key || '';
          apiSecretInput.value = data.api_secret || '';
          if (advisoryEnabledInput) advisoryEnabledInput.checked = !!data.advisory_enabled;
          if (advisoryNameInput) advisoryNameInput.value = data.advisory_name || 're7labs';
          if (advisoryUrlInput) advisoryUrlInput.value = data.advisory_url || '';
          if (advisoryTimeoutInput) advisoryTimeoutInput.value = String(data.advisory_timeout_sec || '0.35');
          if (advisoryTtlInput) advisoryTtlInput.value = String(data.advisory_ttl_sec || '10.0');
          if (advisoryWeightTrainingInput) advisoryWeightTrainingInput.value = String(data.advisory_weight_training || '0.15');
          if (advisoryWeightLiveInput) advisoryWeightLiveInput.value = String(data.advisory_weight_live || '0.22');
          if (paperStartInput) paperStartInput.value = String(data.paper_start_usdt || '100.00');
          if (paperApplyNowInput) paperApplyNowInput.checked = !!data.paper_apply_now;
          setUiLangButtons(data.ui_language || uiLang || 'ru');
          if (advTestStatus) advTestStatus.textContent = tr('ui.advStatusNotChecked');
        } catch (_) {}
      });
    }
    advModal.classList.remove('hidden');
  });
  if (closeAdvBtn) closeAdvBtn.addEventListener('click', () => advModal.classList.add('hidden'));
  if (saveAdvBtn) saveAdvBtn.addEventListener('click', () => {
    const payload = {
      api_key: apiKeyInput.value || '',
      api_secret: apiSecretInput.value || '',
      advisory_enabled: advisoryEnabledInput ? !!advisoryEnabledInput.checked : false,
      advisory_name: advisoryNameInput ? (advisoryNameInput.value || '') : 're7labs',
      advisory_url: advisoryUrlInput ? (advisoryUrlInput.value || '') : '',
      advisory_timeout_sec: advisoryTimeoutInput ? (advisoryTimeoutInput.value || '0.35') : '0.35',
      advisory_ttl_sec: advisoryTtlInput ? (advisoryTtlInput.value || '10.0') : '10.0',
      advisory_weight_training: advisoryWeightTrainingInput ? (advisoryWeightTrainingInput.value || '0.15') : '0.15',
      advisory_weight_live: advisoryWeightLiveInput ? (advisoryWeightLiveInput.value || '0.22') : '0.22',
      paper_start_usdt: paperStartInput ? (paperStartInput.value || '100.00') : '100.00',
      paper_apply_now: paperApplyNowInput ? !!paperApplyNowInput.checked : false,
      ui_language: uiLanguageInput ? (uiLanguageInput.value || 'ru') : 'ru',
    };
    if (bridge && bridge.saveAdvancedSettings) {
      bridge.saveAdvancedSettings(JSON.stringify(payload));
      uiLang = String(payload.ui_language || 'ru').toLowerCase();
      if (!['ru', 'en'].includes(uiLang)) uiLang = 'ru';
      applyLocaleStatic();
      if (advTestStatus) advTestStatus.textContent = tr('ui.advStatusChecking');
      if (bridge.testAdvisorySettings) {
        bridge.testAdvisorySettings(JSON.stringify(payload), (resp) => {
          try {
            const data = JSON.parse(resp || '{}');
            const ok = !!data.ok;
            const msg = data.message || (ok ? 'OK' : 'Ошибка');
            if (advTestStatus) advTestStatus.textContent = `${tr('ui.advStatusPrefix')} ${msg}`;
            showToast(ok ? 'Настройки сохранены, advisory OK' : `Настройки сохранены, advisory: ${msg}`);
          } catch (_) {
            if (advTestStatus) advTestStatus.textContent = tr('ui.advStatusCheckError');
            showToast('Настройки сохранены');
          }
          advModal.classList.add('hidden');
        });
      } else {
        showToast('Настройки сохранены');
        advModal.classList.add('hidden');
      }
      return;
    }
    if (bridge && bridge.saveApiSettings) {
      bridge.saveApiSettings(payload.api_key, payload.api_secret);
      showToast('API сохранен');
      advModal.classList.add('hidden');
    }
  });

  const liveModal = document.getElementById('liveModal');
  const openLiveBtn = document.getElementById('openLiveBtn');
  const liveCloseBtn = document.getElementById('liveCloseBtn');
  if (openLiveBtn) openLiveBtn.addEventListener('click', () => {
    liveModal.classList.remove('hidden');
    if (state) renderLiveModal(state);
  });
  if (liveCloseBtn) liveCloseBtn.addEventListener('click', () => liveModal.classList.add('hidden'));

  window.addEventListener('resize', () => { if (state) drawChart(state.telemetry || {}, (state.status || {})); });
  applyLocaleStatic();
  setUiLangButtons(uiLang || 'ru');
  initRightPanelsState();
}

function renderLiveModal(st){
  const s = st.status || {};
  const b = st.balance || {};
  const a = st.ai_signal || {};
  const m = st.market || {};
  const g = st.guard || {};
  const header = st.header || {};
  const livePriceText = displayLivePrice(b.price, s);
  setKV('liveNow', [
    [tr('labels.live_status'), s.status || '-'],
    [tr('labels.live_mode'), header.mode || '-'],
    [tr('labels.live_pair'), s.symbol || '-'],
    [tr('labels.live_price'), livePriceText],
    [tr('labels.live_reason'), translateReasonLine(s.signal_reason || s.no_entry_summary || s.reason || '-')],
    [tr('conn_update'), header.ts || '-'],
  ]);
  setKV('liveAiMarket', [
    [tr('labels.action'), a.action || '-'],
    [tr('labels.quality'), a.quality ?? '-'],
    [tr('labels.edge'), `${a.edge_pct ?? 0}%`],
    [tr('labels.regime'), m.regime || '-'],
    [tr('labels.volatility'), m.volatility ?? '-'],
    [tr('labels.data_quality'), `${((m.data_quality_score ?? 0) * 100).toFixed(0)}%`],
    [tr('labels.anomaly'), m.anomaly ?? '-'],
  ]);
  setKV('liveRisk', [
    [tr('labels.usdt_real'), b.exchange_usdt ?? '-'],
    [tr('labels.usdt_train'), b.paper_usdt ?? '-'],
    [tr('labels.pnl'), `${b.pnl_pct ?? 0}%`],
    [tr('labels.balance_source'), translateBalanceSource(b.exchange_balance_source || '-')],
    [tr('labels.live_guard'), yesNo(g.locked)],
    [tr('labels.api_errors'), `${g.api_errors ?? 0}/${g.api_max ?? 0}`],
    [tr('labels.trades_day'), `${g.trades_day ?? 0}/${g.trades_max ?? 0}`],
    [tr('labels.reason'), g.reason || '-'],
  ]);
}

new QWebChannel(qt.webChannelTransport, function(channel) {
  bridge = channel.objects.botBridge;
  bridge.stateChanged.connect(function(payload) {
    try { scheduleRender(JSON.parse(payload)); } catch (e) { console.error(e); }
  });
  bridge.toast.connect(function(msg, level) {
    showToast(msg);
  });
  bindActions();
});

document.addEventListener('visibilitychange', () => {
  if (!document.hidden && _pendingState && !_renderQueued) {
    _renderQueued = true;
    window.requestAnimationFrame(flushLatestState);
  }
});

window.addEventListener('focus', () => {
  if (_pendingState && !_renderQueued) {
    _renderQueued = true;
    window.requestAnimationFrame(flushLatestState);
  }
});

