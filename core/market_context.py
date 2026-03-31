from __future__ import annotations

import json
import math
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import numpy as np


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


@dataclass
class CachedSeries:
    ts: float
    ohlcv: list[list[float]]


class MarketContextEngine:
    """Build additional market features + anomaly context with lightweight caching."""

    def __init__(
        self,
        cache_ttl_sec: int = 35,
        telemetry_path: Path | None = None,
        use_okx_data: bool = True,
        use_global_market_data: bool = True,
    ) -> None:
        self.cache_ttl_sec = max(5, int(cache_ttl_sec))
        self._cache: dict[tuple[str, str, int], CachedSeries] = {}
        self._okx_enabled = bool(use_okx_data)
        self._global_market_enabled = bool(use_global_market_data)
        self._okx_cache_ttl_sec = max(60, int(cache_ttl_sec) * 3)
        self._okx_cache: dict[tuple[str, ...], tuple[float, object]] = {}
        self._external_refresh_sec = max(15.0, float(os.getenv("MCTX_EXTERNAL_REFRESH_SEC", "45")))
        self._external_budget_sec = max(0.20, float(os.getenv("MCTX_EXTERNAL_BUDGET_SEC", "0.9")))
        self._external_symbol_cache: dict[str, tuple[float, dict[str, float]]] = {}
        self._quality_hist: deque[float] = deque(maxlen=50)
        self._edge_hist: deque[float] = deque(maxlen=50)
        self._regime_hist: deque[int] = deque(maxlen=50)
        self._param_hist: dict[str, deque[float]] = {
            "qmin": deque(maxlen=50),
            "edge_min": deque(maxlen=50),
            "atr_tp": deque(maxlen=50),
        }
        self._telemetry_path = telemetry_path

    def _fetch_cached_ohlcv(self, trader: Any, symbol: str, timeframe: str, limit: int) -> list[list[float]]:
        key = (symbol.upper(), timeframe, int(limit))
        now = time.time()
        item = self._cache.get(key)
        if item and (now - item.ts) <= self.cache_ttl_sec:
            return item.ohlcv
        data = trader.fetch_ohlcv(symbol, timeframe, limit)
        self._cache[key] = CachedSeries(ts=now, ohlcv=data)
        return data

    @staticmethod
    def _okx_inst_id(symbol: str) -> str:
        return str(symbol or "").upper().replace("/", "-").strip()

    @staticmethod
    def _flat_inst_id(symbol: str) -> str:
        return str(symbol or "").upper().replace("/", "").replace("-", "").strip()

    @staticmethod
    def _okx_get_json(url: str) -> dict[str, object]:
        req = Request(url, headers={"User-Agent": "mexc-bot/1.0"})
        with urlopen(req, timeout=1.6) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, dict):
            return {}
        return data

    def _okx_cached(self, key: tuple[str, ...], loader: Any) -> object:
        now = time.time()
        item = self._okx_cache.get(key)
        if item and (now - item[0]) <= self._okx_cache_ttl_sec:
            return item[1]
        try:
            value = loader()
            self._okx_cache[key] = (now, value)
            return value
        except Exception:
            # Fallback to stale cached value if available.
            if item:
                return item[1]
            raise

    def _okx_last_price(self, inst_id: str) -> float:
        def _load() -> float:
            data = self._okx_get_json(f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}")
            rows = data.get("data", [])
            if isinstance(rows, list) and rows:
                last = _safe_float((rows[0] or {}).get("last"), 0.0) if isinstance(rows[0], dict) else 0.0
                return max(0.0, last)
            return 0.0

        try:
            val = self._okx_cached(("okx_ticker", inst_id), _load)
            return _safe_float(val, 0.0)
        except Exception:
            return 0.0

    def _okx_closes(self, inst_id: str, bar: str = "1m", limit: int = 120) -> list[float]:
        lim = max(30, min(300, int(limit)))

        def _load() -> list[float]:
            data = self._okx_get_json(f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={lim}")
            rows = data.get("data", [])
            if not isinstance(rows, list) or not rows:
                return []
            # OKX returns newest first; reverse to oldest->newest.
            out: list[float] = []
            for row in reversed(rows):
                if not isinstance(row, list) or len(row) < 5:
                    continue
                close = _safe_float(row[4], 0.0)
                if close > 0:
                    out.append(close)
            return out

        try:
            val = self._okx_cached(("okx_candles", inst_id, bar, str(lim)), _load)
            return list(val) if isinstance(val, list) else []
        except Exception:
            return []

    def _binance_last_price(self, inst_id: str) -> float:
        def _load() -> float:
            data = self._okx_get_json(f"https://api.binance.com/api/v3/ticker/price?symbol={inst_id}")
            return max(0.0, _safe_float(data.get("price"), 0.0))

        try:
            val = self._okx_cached(("binance_ticker", inst_id), _load)
            return _safe_float(val, 0.0)
        except Exception:
            return 0.0

    def _binance_closes(self, inst_id: str, interval: str = "1m", limit: int = 120) -> list[float]:
        lim = max(30, min(300, int(limit)))

        def _load() -> list[float]:
            data = self._okx_get_json(f"https://api.binance.com/api/v3/klines?symbol={inst_id}&interval={interval}&limit={lim}")
            if not isinstance(data, list):
                return []
            out: list[float] = []
            for row in data:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                close = _safe_float(row[4], 0.0)
                if close > 0:
                    out.append(close)
            return out

        try:
            val = self._okx_cached(("binance_candles", inst_id, interval, str(lim)), _load)
            return list(val) if isinstance(val, list) else []
        except Exception:
            return []

    def _bybit_last_price(self, inst_id: str) -> float:
        def _load() -> float:
            data = self._okx_get_json(f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={inst_id}")
            result = data.get("result", {})
            rows = result.get("list", []) if isinstance(result, dict) else []
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                return max(0.0, _safe_float(rows[0].get("lastPrice"), 0.0))
            return 0.0

        try:
            val = self._okx_cached(("bybit_ticker", inst_id), _load)
            return _safe_float(val, 0.0)
        except Exception:
            return 0.0

    def _bybit_closes(self, inst_id: str, interval: str = "1", limit: int = 120) -> list[float]:
        lim = max(30, min(300, int(limit)))

        def _load() -> list[float]:
            data = self._okx_get_json(
                f"https://api.bybit.com/v5/market/kline?category=spot&symbol={inst_id}&interval={interval}&limit={lim}"
            )
            result = data.get("result", {})
            rows = result.get("list", []) if isinstance(result, dict) else []
            if not isinstance(rows, list):
                return []
            out: list[float] = []
            # Bybit often returns newest first.
            for row in reversed(rows):
                if not isinstance(row, list) or len(row) < 5:
                    continue
                close = _safe_float(row[4], 0.0)
                if close > 0:
                    out.append(close)
            return out

        try:
            val = self._okx_cached(("bybit_candles", inst_id, interval, str(lim)), _load)
            return list(val) if isinstance(val, list) else []
        except Exception:
            return []

    @staticmethod
    def _ret_and_vol_from_closes(closes: list[float]) -> tuple[float, float]:
        if len(closes) < 3:
            return 0.0, 0.0
        ret1 = (closes[-1] / max(1e-12, closes[-2])) - 1.0
        log_ret = np.diff(np.log(np.clip(np.array(closes, dtype=float), 1e-12, None)))
        vol = float(np.std(log_ret[-60:])) if len(log_ret) >= 20 else float(np.std(log_ret))
        return float(ret1), float(vol)

    def _okx_extra_features(self, symbol: str, local_price: float, local_ret1: float) -> dict[str, float]:
        zero = {
            "okx_price_dev": 0.0,
            "okx_ret1": 0.0,
            "okx_volatility": 0.0,
            "okx_trend_agreement": 0.0,
            "okx_data_quality": 0.0,
        }
        if not self._okx_enabled:
            return zero
        inst_id = self._okx_inst_id(symbol)
        if not inst_id or "-" not in inst_id:
            return zero
        okx_price = self._okx_last_price(inst_id)
        okx_closes = self._okx_closes(inst_id, bar="1m", limit=120)
        if okx_price <= 0 or len(okx_closes) < 20 or local_price <= 0:
            return zero
        okx_ret1 = (okx_closes[-1] / max(1e-12, okx_closes[-2])) - 1.0
        okx_log_ret = np.diff(np.log(np.clip(np.array(okx_closes, dtype=float), 1e-12, None)))
        okx_vol = float(np.std(okx_log_ret[-60:])) if len(okx_log_ret) >= 20 else float(np.std(okx_log_ret))
        price_dev = (okx_price / max(1e-12, local_price)) - 1.0
        trend_agree = 1.0 if (okx_ret1 * local_ret1) >= 0 else -1.0
        quality = 1.0
        if abs(price_dev) > 0.025:
            quality *= 0.7
        if len(okx_closes) < 60:
            quality *= 0.85
        return {
            "okx_price_dev": _clamp(price_dev / 0.01, -1.0, 1.0),
            "okx_ret1": _clamp(okx_ret1 / 0.01, -1.0, 1.0),
            "okx_volatility": _clamp(okx_vol / 0.02, 0.0, 1.0),
            "okx_trend_agreement": _clamp(trend_agree, -1.0, 1.0),
            "okx_data_quality": _clamp(quality, 0.0, 1.0),
        }

    def _global_market_features(self, symbol: str, local_price: float, local_ret1: float, local_vol: float) -> dict[str, float]:
        out = {
            "binance_ret1": 0.0,
            "bybit_ret1": 0.0,
            "cross_ret_mean": 0.0,
            "cross_ret_std": 0.0,
            "cross_price_dispersion": 0.0,
            "cross_trend_agreement": 0.0,
            "cross_source_coverage": 0.0,
            "cross_source_quality": 0.0,
        }
        if not self._global_market_enabled:
            return out
        flat_id = self._flat_inst_id(symbol)
        if not flat_id or flat_id.endswith("USDT") is False:
            return out

        sources: dict[str, dict[str, float]] = {}
        try:
            b_px = self._binance_last_price(flat_id)
            b_closes = self._binance_closes(flat_id, interval="1m", limit=120)
            if b_px > 0 and len(b_closes) >= 20:
                b_ret, b_vol = self._ret_and_vol_from_closes(b_closes)
                sources["binance"] = {"px": b_px, "ret1": b_ret, "vol": b_vol}
                out["binance_ret1"] = _clamp(b_ret / 0.01, -1.0, 1.0)
        except Exception:
            pass
        try:
            y_px = self._bybit_last_price(flat_id)
            y_closes = self._bybit_closes(flat_id, interval="1", limit=120)
            if y_px > 0 and len(y_closes) >= 20:
                y_ret, y_vol = self._ret_and_vol_from_closes(y_closes)
                sources["bybit"] = {"px": y_px, "ret1": y_ret, "vol": y_vol}
                out["bybit_ret1"] = _clamp(y_ret / 0.01, -1.0, 1.0)
        except Exception:
            pass

        if not sources:
            return out

        prices = [local_price] + [v["px"] for v in sources.values() if v.get("px", 0.0) > 0]
        rets = [local_ret1] + [v["ret1"] for v in sources.values()]
        vols = [max(0.0, local_vol)] + [max(0.0, v["vol"]) for v in sources.values()]
        ret_mean = float(np.mean(np.array(rets, dtype=float))) if rets else 0.0
        ret_std = float(np.std(np.array(rets, dtype=float))) if rets else 0.0
        vol_ref = float(np.mean(np.array(vols, dtype=float))) if vols else 0.0
        px_mean = float(np.mean(np.array(prices, dtype=float))) if prices else 0.0
        px_std = float(np.std(np.array(prices, dtype=float))) if prices else 0.0
        px_disp = (px_std / max(1e-12, px_mean)) if px_mean > 0 else 0.0

        agree_vals: list[float] = []
        local_sign = 1.0 if local_ret1 > 0 else (-1.0 if local_ret1 < 0 else 0.0)
        for v in sources.values():
            ext_sign = 1.0 if v["ret1"] > 0 else (-1.0 if v["ret1"] < 0 else 0.0)
            agree_vals.append(1.0 if ext_sign == local_sign else (-1.0 if (ext_sign * local_sign) < 0 else 0.0))
        trend_agree = float(np.mean(np.array(agree_vals, dtype=float))) if agree_vals else 0.0

        coverage = len(sources) / 3.0  # okx/binance/bybit max
        quality = coverage * (1.0 - _clamp(px_disp / 0.03, 0.0, 0.7))
        if vol_ref > 0:
            quality *= (1.0 - _clamp(ret_std / max(1e-6, vol_ref * 2.5), 0.0, 0.6))

        out["cross_ret_mean"] = _clamp(ret_mean / 0.01, -1.0, 1.0)
        out["cross_ret_std"] = _clamp(ret_std / 0.01, 0.0, 1.0)
        out["cross_price_dispersion"] = _clamp(px_disp / 0.01, 0.0, 1.0)
        out["cross_trend_agreement"] = _clamp(trend_agree, -1.0, 1.0)
        out["cross_source_coverage"] = _clamp(coverage, 0.0, 1.0)
        out["cross_source_quality"] = _clamp(quality, 0.0, 1.0)
        return out

    @staticmethod
    def _ema(values: np.ndarray, period: int) -> float:
        if len(values) == 0:
            return 0.0
        alpha = 2.0 / (period + 1.0)
        ema_val = float(values[0])
        for v in values[1:]:
            ema_val = (alpha * float(v)) + ((1.0 - alpha) * ema_val)
        return ema_val

    @staticmethod
    def _atr(ohlcv: list[list[float]], period: int = 14) -> float:
        if len(ohlcv) < period + 1:
            return 0.0
        trs: list[float] = []
        for i in range(1, len(ohlcv)):
            if len(ohlcv[i]) < 5 or len(ohlcv[i - 1]) < 5:
                continue
            h = _safe_float(ohlcv[i][2], 0.0)
            low = _safe_float(ohlcv[i][3], 0.0)
            pc = _safe_float(ohlcv[i - 1][4], 0.0)
            if h <= 0 or low <= 0 or pc <= 0:
                continue
            trs.append(max(h - low, abs(h - pc), abs(low - pc)))
        if len(trs) < period:
            return 0.0
        return float(np.mean(np.array(trs[-period:], dtype=float)))

    @staticmethod
    def _adx_like(ohlcv: list[list[float]], period: int = 14) -> float:
        if len(ohlcv) < period + 2:
            return 0.0
        plus_dm: list[float] = []
        minus_dm: list[float] = []
        tr_list: list[float] = []
        for i in range(1, len(ohlcv)):
            row = ohlcv[i]
            prev = ohlcv[i - 1]
            if len(row) < 5 or len(prev) < 5:
                continue
            h = _safe_float(row[2], 0.0)
            low = _safe_float(row[3], 0.0)
            ph = _safe_float(prev[2], 0.0)
            pl = _safe_float(prev[3], 0.0)
            pc = _safe_float(prev[4], 0.0)
            up = h - ph
            dn = pl - low
            plus_dm.append(max(up, 0.0) if up > dn else 0.0)
            minus_dm.append(max(dn, 0.0) if dn > up else 0.0)
            tr_list.append(max(h - low, abs(h - pc), abs(low - pc)))
        if len(tr_list) < period:
            return 0.0
        tr = np.array(tr_list[-period:], dtype=float).sum()
        if tr <= 0:
            return 0.0
        pdi = 100.0 * (np.array(plus_dm[-period:], dtype=float).sum() / tr)
        mdi = 100.0 * (np.array(minus_dm[-period:], dtype=float).sum() / tr)
        denom = max(1e-9, pdi + mdi)
        dx = 100.0 * abs(pdi - mdi) / denom
        return float(_clamp(dx, 0.0, 100.0))

    @staticmethod
    def _cluster_volume_feature(ohlcv: list[list[float]], bins: int = 8, lookback: int = 80) -> float:
        subset = ohlcv[-lookback:] if len(ohlcv) >= lookback else ohlcv
        if len(subset) < 20:
            return 0.0
        highs = np.array([_safe_float(c[2], 0.0) for c in subset], dtype=float)
        lows = np.array([_safe_float(c[3], 0.0) for c in subset], dtype=float)
        closes = np.array([_safe_float(c[4], 0.0) for c in subset], dtype=float)
        vols = np.array([_safe_float(c[5], 0.0) for c in subset], dtype=float)
        lo = float(np.min(lows))
        hi = float(np.max(highs))
        if hi <= lo:
            return 0.0
        hist = np.zeros(max(3, bins), dtype=float)
        for px, vol in zip(closes, vols):
            idx = int(_clamp((px - lo) / (hi - lo), 0.0, 0.999) * len(hist))
            hist[idx] += max(0.0, vol)
        total = float(np.sum(hist))
        if total <= 0:
            return 0.0
        concentration = float(np.max(hist) / total)
        return _clamp(concentration, 0.0, 1.0)

    @staticmethod
    def _impulse_frequency(ohlcv: list[list[float]], window: int = 30, thr: float = 0.006) -> float:
        if len(ohlcv) < window + 2:
            return 0.0
        closes = np.array([_safe_float(c[4], 0.0) for c in ohlcv[-(window + 1) :]], dtype=float)
        rets = np.diff(np.log(np.clip(closes, 1e-12, None)))
        if len(rets) == 0:
            return 0.0
        freq = float(np.mean(np.abs(rets) >= thr))
        return _clamp(freq, 0.0, 1.0)

    @staticmethod
    def _anomaly_flags(ohlcv: list[list[float]]) -> dict[str, float | bool]:
        if len(ohlcv) < 25:
            return {
                "anomaly_volume_spike": False,
                "anomaly_big_candle": False,
                "anomaly_gap": False,
                "anomaly_stop_hunt": False,
                "anomaly_chop": False,
                "anomaly_score": 0.0,
            }
        vols = np.array([_safe_float(c[5], 0.0) for c in ohlcv[-25:]], dtype=float)
        closes = np.array([_safe_float(c[4], 0.0) for c in ohlcv[-25:]], dtype=float)
        highs = np.array([_safe_float(c[2], 0.0) for c in ohlcv[-25:]], dtype=float)
        lows = np.array([_safe_float(c[3], 0.0) for c in ohlcv[-25:]], dtype=float)
        opens = np.array([_safe_float(c[1], 0.0) for c in ohlcv[-25:]], dtype=float)
        v_spike = bool(vols[-1] > (np.mean(vols[:-1]) * 2.2)) if np.mean(vols[:-1]) > 0 else False
        last_rng = max(1e-12, highs[-1] - lows[-1])
        avg_rng = float(np.mean(highs[:-1] - lows[:-1]))
        big_candle = bool(last_rng > max(1e-12, avg_rng * 2.5))
        prev_close = max(1e-12, closes[-2])
        gap = bool(abs((opens[-1] / prev_close) - 1.0) > 0.012)
        upper_wick = max(0.0, highs[-1] - max(opens[-1], closes[-1])) / last_rng
        lower_wick = max(0.0, min(opens[-1], closes[-1]) - lows[-1]) / last_rng
        body_ratio = abs(closes[-1] - opens[-1]) / last_rng
        stop_hunt = bool((upper_wick > 0.55 or lower_wick > 0.55) and body_ratio < 0.30)
        rets = np.diff(np.log(np.clip(closes[-12:], 1e-12, None)))
        sign_changes = np.sum(np.sign(rets[1:]) != np.sign(rets[:-1])) if len(rets) >= 3 else 0
        chop = bool(sign_changes >= max(2, len(rets) * 0.65))
        score = (
            (0.28 if v_spike else 0.0)
            + (0.25 if big_candle else 0.0)
            + (0.20 if gap else 0.0)
            + (0.17 if stop_hunt else 0.0)
            + (0.18 if chop else 0.0)
        )
        return {
            "anomaly_volume_spike": v_spike,
            "anomaly_big_candle": big_candle,
            "anomaly_gap": gap,
            "anomaly_stop_hunt": stop_hunt,
            "anomaly_chop": chop,
            "anomaly_score": _clamp(score, 0.0, 1.0),
        }

    def build(
        self,
        trader: Any,
        symbol: str,
        ohlcv_1m: list[list[float]],
        runtime_params: dict[str, float],
        ai_quality: float,
        expected_edge_pct: float,
        regime_label: str,
    ) -> dict[str, object]:
        closes = np.array([_safe_float(c[4], 0.0) for c in ohlcv_1m], dtype=float)
        if len(closes) < 30:
            return {"extra_features": {}, "anomalies": {}, "telemetry": {}}
        px = float(closes[-1])
        ret1 = float((closes[-1] / max(1e-12, closes[-2])) - 1.0)
        mom_speed = ret1 - float((closes[-2] / max(1e-12, closes[-3])) - 1.0)
        log_ret = np.diff(np.log(np.clip(closes, 1e-12, None)))
        local_vol = float(np.std(log_ret[-60:])) if len(log_ret) >= 20 else float(np.std(log_ret))
        day_vol = float(np.std(log_ret[-1440:]) * math.sqrt(1440.0)) if len(log_ret) >= 120 else float(np.std(log_ret) * math.sqrt(max(1.0, len(log_ret))))
        local_hi = float(np.max(closes[-60:]))
        local_lo = float(np.min(closes[-60:]))
        dist_hi = (local_hi - px) / max(1e-12, px)
        dist_lo = (px - local_lo) / max(1e-12, px)
        adx_like = self._adx_like(ohlcv_1m, period=14)
        cluster_v = self._cluster_volume_feature(ohlcv_1m)
        impulse_freq = self._impulse_frequency(ohlcv_1m, window=30, thr=0.006)
        atr_pct = self._atr(ohlcv_1m, period=14) / max(1e-12, px)

        # Higher timeframe context.
        ohlcv_5m = self._fetch_cached_ohlcv(trader, symbol, "5m", 80)
        ohlcv_15m = self._fetch_cached_ohlcv(trader, symbol, "15m", 80)
        ohlcv_1h = self._fetch_cached_ohlcv(trader, symbol, "1h", 80)

        def _trend_tf(data: list[list[float]]) -> float:
            vals = np.array([_safe_float(c[4], 0.0) for c in data], dtype=float)
            vals = vals[vals > 0]
            if len(vals) < 25:
                return 0.0
            e_fast = self._ema(vals[-40:], 12)
            e_slow = self._ema(vals[-60:], 26)
            if e_slow <= 0:
                return 0.0
            return (e_fast / e_slow) - 1.0

        trend_5m = _trend_tf(ohlcv_5m)
        trend_15m = _trend_tf(ohlcv_15m)
        trend_1h = _trend_tf(ohlcv_1h)
        anomalies = self._anomaly_flags(ohlcv_1m)
        now_ts = time.time()
        cache_key = str(symbol or "").upper()
        cached_external = self._external_symbol_cache.get(cache_key)
        okx_features: dict[str, float]
        cross_features: dict[str, float]
        if cached_external and (now_ts - cached_external[0]) <= self._external_refresh_sec:
            merged = dict(cached_external[1])
            okx_features = {k: float(merged.get(k, 0.0)) for k in (
                "okx_price_dev", "okx_ret1", "okx_volatility", "okx_trend_agreement", "okx_data_quality"
            )}
            cross_features = {k: float(merged.get(k, 0.0)) for k in (
                "binance_ret1", "bybit_ret1", "cross_ret_mean", "cross_ret_std",
                "cross_price_dispersion", "cross_trend_agreement", "cross_source_coverage", "cross_source_quality"
            )}
        else:
            t_ext = time.perf_counter()
            okx_features = self._okx_extra_features(symbol, px, ret1)
            if (time.perf_counter() - t_ext) <= self._external_budget_sec:
                cross_features = self._global_market_features(symbol, px, ret1, local_vol)
            else:
                cross_features = {
                    "binance_ret1": 0.0,
                    "bybit_ret1": 0.0,
                    "cross_ret_mean": 0.0,
                    "cross_ret_std": 0.0,
                    "cross_price_dispersion": 0.0,
                    "cross_trend_agreement": 0.0,
                    "cross_source_coverage": 0.0,
                    "cross_source_quality": 0.0,
                }
            merged = dict(okx_features)
            merged.update(cross_features)
            self._external_symbol_cache[cache_key] = (now_ts, merged)

        extra_features = {
            "htf_trend_5m": _clamp(trend_5m / 0.02, -1.0, 1.0),
            "htf_trend_15m": _clamp(trend_15m / 0.03, -1.0, 1.0),
            "htf_trend_1h": _clamp(trend_1h / 0.05, -1.0, 1.0),
            "adx_like": _clamp(adx_like / 100.0, 0.0, 1.0),
            "momentum_speed": _clamp(mom_speed / 0.01, -1.0, 1.0),
            "day_volatility": _clamp(day_vol / 0.2, 0.0, 1.0),
            "dist_to_local_high": _clamp(dist_hi / 0.05, 0.0, 1.0),
            "dist_to_local_low": _clamp(dist_lo / 0.05, 0.0, 1.0),
            "cluster_volume_concentration": _clamp(cluster_v, 0.0, 1.0),
            "impulse_freq_n": _clamp(impulse_freq, 0.0, 1.0),
            "atr_pct_ctx": _clamp(atr_pct / 0.03, 0.0, 1.0),
            "anomaly_score": _clamp(_safe_float(anomalies.get("anomaly_score"), 0.0), 0.0, 1.0),
        }
        extra_features.update(okx_features)
        extra_features.update(cross_features)
        telemetry = self._telemetry_snapshot()
        return {"extra_features": extra_features, "anomalies": anomalies, "telemetry": telemetry}

    def record_telemetry(self, ai_quality: float, expected_edge_pct: float, regime_label: str, runtime_params: dict[str, float]) -> dict[str, object]:
        self._append_telemetry(
            ai_quality=ai_quality,
            expected_edge=expected_edge_pct,
            regime=regime_label,
            q_min=_safe_float(runtime_params.get("AI_ENTRY_MIN_QUALITY"), 0.0),
            edge_min=_safe_float(runtime_params.get("MIN_EXPECTED_EDGE_PCT"), 0.0),
            atr_tp=_safe_float(runtime_params.get("AI_ATR_TAKE_PROFIT_MULT"), 0.0),
        )
        return self._telemetry_snapshot()

    def _append_telemetry(self, ai_quality: float, expected_edge: float, regime: str, q_min: float, edge_min: float, atr_tp: float) -> None:
        regime_map = {"flat": 0, "trend": 1, "impulse": 2, "high_vol": 3, "dangerous": 4}
        self._quality_hist.append(_clamp(ai_quality, 0.0, 1.0))
        self._edge_hist.append(expected_edge)
        self._regime_hist.append(regime_map.get(regime, 0))
        self._param_hist["qmin"].append(q_min)
        self._param_hist["edge_min"].append(edge_min)
        self._param_hist["atr_tp"].append(atr_tp)
        if self._telemetry_path is None:
            return
        payload = self._telemetry_snapshot()
        payload["ts_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            self._telemetry_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._telemetry_path.with_suffix(self._telemetry_path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self._telemetry_path)
        except Exception:
            return

    def _telemetry_snapshot(self) -> dict[str, object]:
        return {
            "ai_quality": list(self._quality_hist),
            "expected_edge": list(self._edge_hist),
            "regime": list(self._regime_hist),
            "params": {
                "qmin": list(self._param_hist["qmin"]),
                "edge_min": list(self._param_hist["edge_min"]),
                "atr_tp": list(self._param_hist["atr_tp"]),
            },
        }
