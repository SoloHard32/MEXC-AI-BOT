from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Deque, Iterable, Sequence
from urllib.request import Request, urlopen
import csv


@dataclass
class AISignal:
    action: str  # LONG | SHORT | HOLD
    confidence: float
    score: float
    reason: str
    regime: str = "unknown"
    calibrated_confidence: float = 0.0
    uncertainty: float = 0.0
    trade_quality: float = 0.0
    trade_filter_pass: bool = False
    walkforward_hit_rate: float = 0.0
    model_health: str = "unknown"


@dataclass
class SourceQualityState:
    quality: float = 1.0
    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    muted_until_ts: float = 0.0
    last_ok_ts: float = 0.0


class InternetFeatureClient:
    """Fetch lightweight macro/sentiment features from public internet APIs."""

    def __init__(self, ttl_seconds: int = 180) -> None:
        self.ttl_seconds = ttl_seconds
        self.request_timeout_sec = max(0.35, float(os.getenv("AI_INTERNET_TIMEOUT_SEC", "1.2")))
        self.request_budget_sec = max(0.6, float(os.getenv("AI_INTERNET_BUDGET_SEC", "1.8")))
        self._last_ts = 0.0
        self._cache: dict[str, float] = {}
        self._last_quality = 1.0
        self._last_health_snapshot: dict[str, object] = {}
        self._source_defaults: dict[str, dict[str, float]] = {
            "fng": {"net_fng": 0.0},
            "mcap": {"net_mcap_chg": 0.0},
            "price": {"net_btc_24h": 0.0, "net_eth_24h": 0.0},
        }
        self._source_state: dict[str, SourceQualityState] = {
            key: SourceQualityState() for key in self._source_defaults
        }
        self._source_signal_history: dict[str, Deque[float]] = {
            key: deque(maxlen=60) for key in self._source_defaults
        }

    def fetch(self) -> dict[str, float]:
        now = time.time()
        if self._cache and (now - self._last_ts) < self.ttl_seconds:
            return dict(self._cache)

        features: dict[str, float] = {}
        deadline_ts = now + self.request_budget_sec
        features.update(self._fetch_source("fng", self._fetch_fng_feature, now, deadline_ts))
        features.update(self._fetch_source("mcap", self._fetch_marketcap_feature, now, deadline_ts))
        features.update(self._fetch_source("price", self._fetch_price_change_features, now, deadline_ts))

        for key in ("net_fng", "net_mcap_chg", "net_btc_24h", "net_eth_24h"):
            features.setdefault(key, 0.0)

        qualities = [state.quality for state in self._source_state.values()]
        self._last_quality = sum(qualities) / len(qualities) if qualities else 0.0
        self._last_health_snapshot = self._build_health_snapshot(now)

        self._cache = features
        self._last_ts = now
        return dict(features)

    def health_snapshot(self) -> dict[str, object]:
        return dict(self._last_health_snapshot)

    def _fetch_fng_feature(self) -> dict[str, float]:
        fng = self._get_json("https://api.alternative.me/fng/?limit=1")
        val = float((fng.get("data") or [{}])[0].get("value", 50))
        return {"net_fng": self._clamp((val - 50.0) / 50.0, -1.0, 1.0)}

    def _fetch_marketcap_feature(self) -> dict[str, float]:
        global_data = self._get_json("https://api.coingecko.com/api/v3/global")
        pct = float((global_data.get("data") or {}).get("market_cap_change_percentage_24h_usd", 0.0))
        return {"net_mcap_chg": self._clamp(pct / 10.0, -1.0, 1.0)}

    def _fetch_price_change_features(self) -> dict[str, float]:
        simple = self._get_json(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true"
        )
        btc_chg = float((simple.get("bitcoin") or {}).get("usd_24h_change", 0.0))
        eth_chg = float((simple.get("ethereum") or {}).get("usd_24h_change", 0.0))
        return {
            "net_btc_24h": self._clamp(btc_chg / 10.0, -1.0, 1.0),
            "net_eth_24h": self._clamp(eth_chg / 10.0, -1.0, 1.0),
        }

    def _fetch_source(
        self,
        source_name: str,
        loader: Callable[[], dict[str, float]],
        now_ts: float,
        deadline_ts: float,
    ) -> dict[str, float]:
        state = self._source_state[source_name]
        defaults = dict(self._source_defaults[source_name])
        if time.time() >= deadline_ts:
            # Keep cycle responsive: do not start another external call when budget exhausted.
            state.quality = max(0.10, state.quality * 0.985)
            return defaults
        if now_ts < state.muted_until_ts:
            state.quality = max(0.10, state.quality * 0.98)
            return defaults

        try:
            raw = loader()
            source_signal = self._source_signal(raw)
            stability = self._stability_factor(source_name, source_signal)
            state.successes += 1
            state.consecutive_failures = 0
            state.last_ok_ts = now_ts
            state.quality = max(0.10, min(1.0, (0.82 * state.quality) + (0.18 * stability)))
            q = state.quality
            return {k: self._clamp(float(v), -1.0, 1.0) * q for k, v in raw.items()}
        except Exception:
            state.failures += 1
            state.consecutive_failures += 1
            state.quality = max(0.10, state.quality * 0.62)
            if state.consecutive_failures >= 3:
                backoff_steps = min(state.consecutive_failures - 2, 6)
                state.muted_until_ts = now_ts + (self.ttl_seconds * (2 ** backoff_steps))
            return defaults

    def _stability_factor(self, source_name: str, source_signal: float) -> float:
        hist = self._source_signal_history[source_name]
        factor = 1.0
        if len(hist) >= 8:
            mean_val = statistics.fmean(hist)
            std_val = statistics.pstdev(hist)
            std_val = max(std_val, 0.08)
            z = abs((source_signal - mean_val) / std_val)
            if z > 7.0:
                factor = 0.20
            elif z > 5.0:
                factor = 0.40
            elif z > 3.5:
                factor = 0.65
        hist.append(source_signal)
        return factor

    @staticmethod
    def _source_signal(payload: dict[str, float]) -> float:
        vals = [float(v) for v in payload.values() if math.isfinite(float(v))]
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def _build_health_snapshot(self, now_ts: float) -> dict[str, object]:
        sources: dict[str, dict[str, object]] = {}
        for name, state in self._source_state.items():
            sources[name] = {
                "quality": round(state.quality, 4),
                "successes": state.successes,
                "failures": state.failures,
                "consecutive_failures": state.consecutive_failures,
                "muted": now_ts < state.muted_until_ts,
                "muted_for_sec": max(0, int(state.muted_until_ts - now_ts)),
            }
        return {
            "overall_quality": round(self._last_quality, 4),
            "sources": sources,
        }

    def _get_json(self, url: str) -> dict:
        req = Request(url, headers={"User-Agent": "mexc-bot/1.0"})
        with urlopen(req, timeout=self.request_timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
        return json.loads(payload)

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))


class OnlineLinearModel:
    """Tiny online model with persisted weights (no heavy dependencies)."""

    def __init__(self, model_path: str, learning_rate: float = 0.08, l2: float = 0.0005) -> None:
        self.path = Path(model_path)
        self.learning_rate = learning_rate
        self.l2 = l2
        self.bias = 0.0
        self.weights: dict[str, float] = {}
        self._updates = 0
        self._load()

    def predict(self, features: dict[str, float]) -> float:
        raw = self.bias
        for key, val in features.items():
            raw += self.weights.get(key, 0.0) * float(val)
        return math.tanh(raw)

    def update(self, features: dict[str, float], label: int, sample_weight: float = 1.0) -> None:
        if label not in (-1, 1):
            return
        sw = max(0.20, min(2.50, float(sample_weight)))
        lr = self.learning_rate * sw
        pred = self.predict(features)
        err = float(label) - pred

        self.bias += lr * err
        for key, val in features.items():
            w = self.weights.get(key, 0.0)
            w *= (1.0 - self.l2)
            w += lr * err * float(val)
            self.weights[key] = w

        self._updates += 1
        if self._updates % 20 == 0:
            self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "bias": self.bias,
            "weights": self.weights,
            "learning_rate": self.learning_rate,
            "l2": self.l2,
        }
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.bias = float(data.get("bias", 0.0))
            raw_weights = data.get("weights", {})
            self.weights = {str(k): float(v) for k, v in raw_weights.items()}
        except Exception:
            self.bias = 0.0
            self.weights = {}

    def reload(self) -> None:
        self._load()


class EnsembleOnlineModel:
    """Small ensemble of online linear models with weighted voting."""

    def __init__(self, model_path: str, learning_rate: float = 0.08) -> None:
        base = Path(model_path)
        suffix = base.suffix or ".json"
        stem = base.stem
        self.path = base
        self.model_paths = [
            base,
            base.with_name(f"{stem}_m2{suffix}"),
            base.with_name(f"{stem}_m3{suffix}"),
        ]
        lrs = [learning_rate * 0.90, learning_rate, learning_rate * 1.10]
        l2s = [0.00035, 0.00055, 0.00090]
        self.models = [
            OnlineLinearModel(model_path=str(p), learning_rate=lr, l2=l2)
            for p, lr, l2 in zip(self.model_paths, lrs, l2s, strict=False)
        ]
        self.base_weights = [0.42, 0.34, 0.24]
        self._bootstrap_members_from_primary()

    def _bootstrap_members_from_primary(self) -> None:
        """If side members are missing, initialize them from the primary model state."""
        if not self.model_paths[0].exists():
            return
        primary = self.models[0]
        if not primary.weights:
            return
        for idx in range(1, len(self.models)):
            side_path = self.model_paths[idx]
            if side_path.exists():
                continue
            m = self.models[idx]
            m.bias = float(primary.bias)
            m.weights = dict(primary.weights)
            # Tiny deterministic perturbation keeps diversity without losing base knowledge.
            bump = 1.0 + (0.003 * idx)
            for key in list(m.weights.keys()):
                m.weights[key] = float(m.weights[key]) * bump
            try:
                m.save()
            except Exception:
                continue

    def predict_members(self, features: dict[str, float]) -> list[float]:
        return [m.predict(features) for m in self.models]

    def predict(self, features: dict[str, float]) -> float:
        preds = self.predict_members(features)
        den = max(1e-9, sum(self.base_weights))
        voted = sum(w * p for w, p in zip(self.base_weights, preds, strict=False)) / den
        return max(-1.0, min(1.0, voted))

    def update(self, features: dict[str, float], label: int, sample_weight: float = 1.0) -> None:
        if label not in (-1, 1):
            return
        model_weight_factors = [0.95, 1.00, 1.07]
        for m, wf in zip(self.models, model_weight_factors, strict=False):
            m.update(features, label, sample_weight=sample_weight * wf)

    def save(self) -> None:
        for m in self.models:
            m.save()

    def reload(self) -> None:
        for m in self.models:
            m.reload()


class AISignalEngine:
    """Hybrid engine: technical baseline + optional internet features + online learning."""

    def __init__(
        self,
        fast_period: int = 7,
        slow_period: int = 20,
        bb_period: int = 20,
        rsi_period: int = 14,
        macd_fast: int = 12,
        macd_slow: int = 26,
        macd_signal: int = 9,
        use_internet_data: bool = True,
        online_learning_enabled: bool = True,
        model_path: str = "ai_model.json",
        training_log_path: str = "ai_training_log.csv",
        status_path: str = "ai_status.json",
        learning_rate: float = 0.08,
        horizon_candles: int = 3,
        label_threshold: float = 0.0015,
        fee_bps: float = 10.0,
        slippage_bps: float = 6.0,
        walkforward_window: int = 300,
        replay_size: int = 800,
    ) -> None:
        self.fast_period = max(3, fast_period)
        self.slow_period = max(self.fast_period + 1, slow_period)
        self.bb_period = max(10, bb_period)
        self.rsi_period = max(10, rsi_period)
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        
        self.use_internet_data = use_internet_data
        self.online_learning_enabled = online_learning_enabled
        self.runtime_dry_run = False
        self.horizon_candles = max(1, horizon_candles)
        self.label_threshold = max(0.0001, label_threshold)
        self.fee_bps = max(0.0, fee_bps)
        self.slippage_bps = max(0.0, slippage_bps)
        self.roundtrip_cost_fraction = ((self.fee_bps + self.slippage_bps) * 2.0) / 10000.0
        self.effective_label_threshold = max(self.label_threshold, self.roundtrip_cost_fraction)
        self.walkforward_window = max(50, walkforward_window)
        self.replay_size = max(200, replay_size)

        self.internet = InternetFeatureClient() if use_internet_data else None
        self.model = EnsembleOnlineModel(model_path=model_path, learning_rate=learning_rate)
        self.training_log_path = Path(training_log_path)
        self.status_path = Path(status_path)
        
        self.feature_names = [
            "trend", "ret1", "ret3", "ret5", "vol", "zscore", "momentum_quality",
            "rsi", "bb_width", "bb_pct", "macd_hist", "atr_pct",
            "range_pct", "body_ratio", "upper_wick_ratio", "lower_wick_ratio",
            "volume_z", "vwap_dist",
            "regime_trend", "regime_range", "regime_high_vol",
            "net_fng", "net_mcap_chg", "net_btc_24h", "net_eth_24h",
            "htf_trend_5m", "htf_trend_15m", "htf_trend_1h",
            "adx_like", "momentum_speed", "day_volatility",
            "dist_to_local_high", "dist_to_local_low",
            "cluster_volume_concentration", "impulse_freq_n",
            "atr_pct_ctx", "anomaly_score",
            "okx_price_dev", "okx_ret1", "okx_volatility", "okx_trend_agreement", "okx_data_quality",
            "binance_ret1", "bybit_ret1",
            "cross_ret_mean", "cross_ret_std", "cross_price_dispersion", "cross_trend_agreement",
            "cross_source_coverage", "cross_source_quality",
        ]

        self._pending: dict[str, list[tuple[float, dict[str, float]]]] = {}
        self._last_candle_ts: dict[str, int] = {}
        self._last_signal: AISignal | None = None
        self._last_internet_features: dict[str, float] = {}
        self._last_internet_health: dict[str, object] = {}
        self._last_training_update_ts: str = ""
        self._last_observation_update_ts: str = ""
        self._observed_samples_total = 0
        self._total_labels = 0
        self._correct_labels = 0
        self._long_labels = 0
        self._short_labels = 0
        self._long_correct = 0
        self._short_correct = 0
        self._walkforward_hits: Deque[int] = deque(maxlen=self.walkforward_window)
        self._replay: Deque[tuple[dict[str, float], int, float, str, float]] = deque(maxlen=self.replay_size)
        self._hard_negatives: Deque[tuple[dict[str, float], int, float, str, float]] = deque(maxlen=max(120, self.replay_size // 3))
        self._regime_label_total: dict[str, int] = defaultdict(int)
        self._regime_label_correct: dict[str, int] = defaultdict(int)
        self._bin_total: list[int] = [0] * 10
        self._bin_hit: list[int] = [0] * 10
        self._regime_bin_total: dict[str, list[int]] = defaultdict(lambda: [0] * 10)
        self._regime_bin_hit: dict[str, list[int]] = defaultdict(lambda: [0] * 10)
        self._net_edge_ema = 0.0
        self._net_edge_alpha = 0.10
        self._last_regime = "unknown"
        self._last_trade_quality = 0.0
        self._last_uncertainty = 1.0
        self._last_calibrated_confidence = 0.0
        self._drift_short_window: Deque[dict[str, float]] = deque(maxlen=36)
        self._drift_long_window: Deque[dict[str, float]] = deque(maxlen=240)
        self._drift_score = 0.0
        self._drift_active_until_ts = 0.0
        self._drift_alert_count = 0
        self._model_health = "cold_start"
        self._ensure_training_log_header()
        self._load_historical_metrics()
        self._write_status(model_status="loaded")

    def predict(self, ohlcv: Sequence[Sequence[float]], symbol: str = "", extra_features: dict[str, float] | None = None) -> AISignal:
        closes = self._closes(ohlcv)
        min_needed = max(self.slow_period + 5, self.bb_period + 5, self.rsi_period + 5, self.macd_slow + self.macd_signal, 30)
        if len(closes) < min_needed:
            signal = AISignal(action="HOLD", confidence=0.0, score=0.0, reason=f"not_enough_data:{len(closes)}<{min_needed}")
            self._last_signal = signal
            self._write_status(model_status="running")
            return signal

        base_features = self._technical_features(ohlcv, closes)
        regime = self._market_regime(base_features)
        regime_flags = self._regime_one_hot(regime)

        features = dict(base_features)
        features.update(regime_flags)
        if isinstance(extra_features, dict):
            for k, v in extra_features.items():
                features[str(k)] = float(v)

        net_features = self.internet.fetch() if self.internet is not None else {}
        net_quality = 1.0
        if self.internet is not None:
            self._last_internet_health = self.internet.health_snapshot()
            net_quality = float(self._last_internet_health.get("overall_quality", 1.0) or 1.0)
        self._last_internet_features = dict(net_features)
        features.update(net_features)

        # Ensure all feature keys exist for logging
        for key in self.feature_names:
            features.setdefault(key, 0.0)

        base_score = self._baseline_score(base_features, regime)
        member_preds = self.model.predict_members(features)
        ml_adj = self.model.predict(features)
        score = math.tanh((0.55 * base_score) + (0.90 * ml_adj))
        drift_score, drift_active = self._update_drift_state(base_features)

        uncertainty = self._estimate_uncertainty(base_score, ml_adj, base_features)
        trade_quality = self._trade_quality(base_features, score, uncertainty, regime)
        trade_quality *= (0.70 + (0.30 * max(0.0, min(1.0, net_quality))))
        trade_quality = max(0.0, min(1.0, trade_quality))
        raw_confidence = min(0.99, abs(score))
        calibrated_confidence = self._calibrate_confidence(raw_confidence, trade_quality, regime)
        calibrated_confidence *= (0.75 + (0.25 * max(0.0, min(1.0, net_quality))))
        calibrated_confidence = max(0.0, min(0.99, calibrated_confidence))
        direction_threshold = self._regime_direction_threshold(regime)
        filter_threshold = self._regime_filter_threshold(regime)
        if drift_active:
            drift_boost = max(0.0, min(0.16, 0.035 * drift_score))
            direction_threshold = min(0.75, direction_threshold + drift_boost)
            filter_threshold = min(0.92, filter_threshold + (0.55 * drift_boost))
            calibrated_confidence = max(0.0, min(0.99, calibrated_confidence * (1.0 - min(0.28, 0.07 * drift_score))))
            trade_quality = max(0.0, min(1.0, trade_quality * (1.0 - min(0.22, 0.05 * drift_score))))

        trade_filter_pass = trade_quality >= filter_threshold
        action = "HOLD"
        if trade_filter_pass and score >= direction_threshold:
            action = "LONG"
        elif trade_filter_pass and score <= -direction_threshold:
            action = "SHORT"

        self._last_regime = regime
        self._last_trade_quality = trade_quality
        self._last_uncertainty = uncertainty
        self._last_calibrated_confidence = calibrated_confidence

        candle_ts = int(ohlcv[-1][0]) if ohlcv and len(ohlcv[-1]) > 0 else 0
        if symbol:
            self._online_learn(symbol, candle_ts, closes[-1], features)

        wf_hit = self._walkforward_hit_rate()
        self._model_health = self._compute_model_health(wf_hit)

        reason = (
            f"score={score:.3f};base={base_score:.3f};ml={ml_adj:.3f};"
            f"ens=[{member_preds[0]:.3f},{member_preds[1]:.3f},{member_preds[2]:.3f}];"
            f"regime={regime};quality={trade_quality:.3f};unc={uncertainty:.3f};"
            f"conf_raw={raw_confidence:.3f};conf_cal={calibrated_confidence:.3f};"
            f"drift={drift_score:.2f}/{int(drift_active)};"
            f"net_q={net_quality:.3f};"
            f"filt={int(trade_filter_pass)};thr={direction_threshold:.3f};"
            f"rsi={features['rsi']:.1f};bb_pct={features['bb_pct']:.3f};"
            f"macd_hist={features['macd_hist']:.4f};vol={features['vol']:.5f}"
        )
        signal = AISignal(
            action=action,
            confidence=calibrated_confidence,
            score=score,
            reason=reason,
            regime=regime,
            calibrated_confidence=calibrated_confidence,
            uncertainty=uncertainty,
            trade_quality=trade_quality,
            trade_filter_pass=trade_filter_pass,
            walkforward_hit_rate=wf_hit,
            model_health=self._model_health,
        )
        self._last_signal = signal
        self._write_status(model_status="running")
        return signal

    def set_runtime_mode(self, dry_run: bool) -> None:
        self.runtime_dry_run = bool(dry_run)

    def diagnostics(self) -> dict[str, object]:
        wf = self._walkforward_hit_rate()
        hit = (self._correct_labels / max(self._total_labels, 1)) if self._total_labels > 0 else 0.0
        return {
            "wf_hit_rate": wf,
            "hit_rate": hit,
            "model_health": self._model_health,
            "total_labels": self._total_labels,
            "model_path": str(self.model.path),
            "drift_score": self._drift_score,
            "drift_active": self._is_drift_active(),
        }

    def side_performance_snapshot(self) -> dict[str, float]:
        long_labels = max(0, int(self._long_labels))
        short_labels = max(0, int(self._short_labels))
        long_hit_rate = (self._long_correct / long_labels) if long_labels > 0 else 0.0
        short_hit_rate = (self._short_correct / short_labels) if short_labels > 0 else 0.0
        gap = max(0.0, float(long_hit_rate) - float(short_hit_rate))
        return {
            "long_labels": float(long_labels),
            "short_labels": float(short_labels),
            "long_hit_rate": float(long_hit_rate),
            "short_hit_rate": float(short_hit_rate),
            "short_deficit": float(gap),
        }

    def _online_learn(self, symbol: str, candle_ts: int, close: float, features: dict[str, float]) -> None:
        prev_ts = self._last_candle_ts.get(symbol)
        if prev_ts is not None and candle_ts <= prev_ts:
            return
        self._last_candle_ts[symbol] = candle_ts

        queue = self._pending.setdefault(symbol, [])
        queue.append((close, dict(features)))

        if len(queue) <= self.horizon_candles:
            return

        old_close, old_feat = queue.pop(0)
        ret = self._ret(close, old_close)
        label = 0
        threshold = self.effective_label_threshold
        if ret > threshold:
            label = 1
        elif ret < -threshold:
            label = -1
        
        # Always log data for offline training, even if label is 0
        pred_before = self.model.predict(old_feat)
        self._append_training_log(symbol, old_close, close, ret, label, pred_before, old_feat)

        if self.online_learning_enabled and (not self.runtime_dry_run) and label != 0:
            regime = self._infer_regime_from_features(old_feat)
            sample_weight = self._learning_sample_weight(old_feat, ret, label, regime)
            if self._is_drift_active():
                sample_weight = max(0.25, min(2.90, sample_weight * 1.24))
            self.model.update(old_feat, label, sample_weight=sample_weight)
            ts_now = time.time()
            self._replay.append((dict(old_feat), label, sample_weight, regime, ts_now))
            self._total_labels += 1
            hit = 1 if ((pred_before >= 0 and label == 1) or (pred_before < 0 and label == -1)) else 0
            self._walkforward_hits.append(hit)
            self._update_calibration_stats(abs(pred_before), hit, regime)
            self._regime_label_total[regime] += 1
            if hit:
                self._correct_labels += 1
                self._regime_label_correct[regime] += 1
            if label > 0:
                self._long_labels += 1
                if hit:
                    self._long_correct += 1
            elif label < 0:
                self._short_labels += 1
                if hit:
                    self._short_correct += 1
            self._last_training_update_ts = datetime.now(timezone.utc).isoformat()

            ret_net = abs(float(ret)) - self.roundtrip_cost_fraction
            self._net_edge_ema = (1.0 - self._net_edge_alpha) * self._net_edge_ema + (self._net_edge_alpha * ret_net)
            if (not hit) and abs(float(pred_before)) >= 0.62:
                hn_weight = max(0.35, min(3.2, sample_weight * 1.25))
                self._hard_negatives.append((dict(old_feat), label, hn_weight, regime, ts_now))

            retrain_every = 10 if self._is_drift_active() else 22
            if self._total_labels % retrain_every == 0:
                if self._is_drift_active():
                    self._mini_batch_retrain(steps=24, batch_size=72)
                else:
                    self._mini_batch_retrain(steps=14, batch_size=56)
            self._write_status(model_status="training")

    def _is_drift_active(self) -> bool:
        return time.time() < self._drift_active_until_ts

    def _update_drift_state(self, features: dict[str, float]) -> tuple[float, bool]:
        core = {
            "trend": float(features.get("trend", 0.0)),
            "ret1": float(features.get("ret1", 0.0)),
            "ret3": float(features.get("ret3", 0.0)),
            "ret5": float(features.get("ret5", 0.0)),
            "vol": float(features.get("vol", 0.0)),
            "atr_pct": float(features.get("atr_pct", 0.0)),
            "bb_width": float(features.get("bb_width", 0.0)),
            "volume_z": float(features.get("volume_z", 0.0)),
        }
        self._drift_short_window.append(core)
        self._drift_long_window.append(core)
        if len(self._drift_short_window) < 20 or len(self._drift_long_window) < 120:
            self._drift_score = 0.0
            return self._drift_score, self._is_drift_active()

        score_parts: list[float] = []
        keys = tuple(core.keys())
        for key in keys:
            short_vals = [x.get(key, 0.0) for x in self._drift_short_window]
            long_vals = [x.get(key, 0.0) for x in self._drift_long_window]
            short_mean = statistics.fmean(short_vals)
            long_mean = statistics.fmean(long_vals)
            long_std = statistics.pstdev(long_vals) if len(long_vals) > 1 else 0.0
            denom = max(1e-6, long_std, abs(long_mean) * 0.05)
            z = abs(short_mean - long_mean) / denom
            score_parts.append(max(0.0, min(6.0, z)))
        drift_score = sum(score_parts) / max(1, len(score_parts))
        self._drift_score = drift_score

        now = time.time()
        # Earlier activation for regime shifts in crypto micro-accounts.
        if drift_score >= 0.78:
            self._drift_active_until_ts = max(self._drift_active_until_ts, now + 12 * 60)
            self._drift_alert_count += 1
        elif drift_score <= 0.45 and now >= self._drift_active_until_ts:
            self._drift_active_until_ts = 0.0

        return self._drift_score, self._is_drift_active()

    def _learning_sample_weight(self, features: dict[str, float], ret: float, label: int, regime: str) -> float:
        """
        Weight learning samples by information quality.
        Strong/clean directional moves -> higher weight, noisy/anomalous -> lower.
        """
        move = abs(float(ret))
        thr = max(self.effective_label_threshold, 1e-6)
        move_factor = max(0.60, min(2.10, move / (thr * 1.8)))

        anomaly = max(0.0, min(1.0, float(features.get("anomaly_score", 0.0))))
        vol_penalty = max(0.0, min(1.0, float(features.get("vol", 0.0)) / 0.02))
        wick_noise = max(
            0.0,
            min(
                1.0,
                float(features.get("upper_wick_ratio", 0.0)) + float(features.get("lower_wick_ratio", 0.0)),
            ),
        )
        noise_factor = 1.0 - (0.35 * anomaly + 0.20 * vol_penalty + 0.15 * wick_noise)
        noise_factor = max(0.40, min(1.15, noise_factor))

        momentum_q = float(features.get("momentum_quality", 0.0))
        directional_agreement = 1.0 if ((label > 0 and momentum_q >= 0.0) or (label < 0 and momentum_q <= 0.0)) else 0.0
        agreement_factor = 1.0 + (0.20 * directional_agreement) - (0.10 * (1.0 - directional_agreement))

        quality = max(0.0, min(1.0, float(self._last_trade_quality)))
        quality_factor = 0.75 + (0.50 * quality)

        ret_net = max(0.0, move - self.roundtrip_cost_fraction)
        net_edge_factor = 0.80 + min(0.80, ret_net / max(thr, 1e-6))
        source_quality = max(
            0.25,
            min(
                1.20,
                (0.65 * float(features.get("cross_source_quality", 1.0)))
                + (0.35 * float(features.get("okx_data_quality", 1.0))),
            ),
        )
        regime_total = max(0, int(self._regime_label_total.get(regime, 0)))
        all_total = max(1, self._total_labels)
        target_share = 1.0 / 3.0
        current_share = regime_total / all_total
        regime_balance_factor = max(0.82, min(1.28, 1.0 + ((target_share - current_share) * 0.55)))

        sample_weight = move_factor * noise_factor * agreement_factor * quality_factor * net_edge_factor * source_quality * regime_balance_factor
        return max(0.25, min(2.40, sample_weight))

    def flush(self) -> None:
        self.model.save()
        self._write_status(model_status="saved")

    def reload_models(self) -> None:
        self.model.reload()
        self._write_status(model_status="reloaded")

    def _technical_features(self, ohlcv: Sequence[Sequence[float]], closes: Sequence[float]) -> dict[str, float]:
        last = closes[-1]
        ret1 = self._ret(last, closes[-2])
        ret3 = self._ret(last, closes[-4])
        ret5 = self._ret(last, closes[-6])

        sma_fast = self._sma(closes[-self.fast_period:])
        sma_slow = self._sma(closes[-self.slow_period:])
        trend = self._ret(sma_fast, sma_slow)

        returns = [self._ret(closes[i], closes[i - 1]) for i in range(1, len(closes))]
        vol = statistics.pstdev(returns[-20:]) if len(returns) >= 20 else statistics.pstdev(returns)
        vol = max(vol, 1e-9)

        zscore = self._zscore(closes[-20:])
        momentum_quality = (ret1 + ret3 + ret5) / (3.0 * vol)
        
        rsi = self._rsi(closes, period=self.rsi_period)
        bb = self._bollinger_bands(closes, period=self.bb_period)
        macd = self._macd(closes, self.macd_fast, self.macd_slow, self.macd_signal)
        
        atr = self._atr(ohlcv, period=14)
        atr_pct = (atr / last) if last > 0 else 0.0
        candle = ohlcv[-1] if ohlcv else [0, 0, 0, 0, last, 0]
        o = float(candle[1]) if len(candle) > 1 else last
        h = float(candle[2]) if len(candle) > 2 else last
        low = float(candle[3]) if len(candle) > 3 else last
        c = float(candle[4]) if len(candle) > 4 else last
        rng = max(h - low, 1e-12)
        body = abs(c - o)
        body_ratio = body / rng
        upper_wick_ratio = max(h - max(o, c), 0.0) / rng
        lower_wick_ratio = max(min(o, c) - low, 0.0) / rng
        range_pct = (rng / max(c, 1e-12))

        volumes = [float(x[5]) for x in ohlcv if len(x) > 5]
        if len(volumes) >= 20:
            vol_z = self._zscore(volumes[-20:])
        elif len(volumes) > 1:
            vol_z = self._zscore(volumes)
        else:
            vol_z = 0.0

        vwap_dist = 0.0
        if len(ohlcv) >= 5:
            subset = ohlcv[-20:]
            pv = 0.0
            vv = 0.0
            for row in subset:
                if len(row) < 6:
                    continue
                high = float(row[2])
                low = float(row[3])
                close = float(row[4])
                vol_i = max(float(row[5]), 0.0)
                typical = (high + low + close) / 3.0
                pv += typical * vol_i
                vv += vol_i
            if vv > 0:
                vwap = pv / vv
                vwap_dist = self._ret(c, vwap)

        return {
            "trend": trend,
            "ret1": ret1,
            "ret3": ret3,
            "ret5": ret5,
            "vol": vol,
            "zscore": zscore,
            "momentum_quality": momentum_quality,
            "rsi": rsi,
            "bb_width": bb["width"],
            "bb_pct": bb["pct"],
            "macd_hist": macd["hist"],
            "atr_pct": atr_pct,
            "range_pct": range_pct,
            "body_ratio": body_ratio,
            "upper_wick_ratio": upper_wick_ratio,
            "lower_wick_ratio": lower_wick_ratio,
            "volume_z": vol_z,
            "vwap_dist": vwap_dist,
        }

    @staticmethod
    def _baseline_score(features: dict[str, float], regime: str) -> float:
        """Deterministic technical score in [-1, 1] used as a stable prior."""
        trend = max(-1.0, min(1.0, features.get("trend", 0.0) / 0.03))
        ret1 = max(-1.0, min(1.0, features.get("ret1", 0.0) / 0.015))
        ret3 = max(-1.0, min(1.0, features.get("ret3", 0.0) / 0.025))
        ret5 = max(-1.0, min(1.0, features.get("ret5", 0.0) / 0.035))
        momentum_quality = math.tanh(features.get("momentum_quality", 0.0) * 0.15)

        rsi = max(0.0, min(100.0, features.get("rsi", 50.0)))
        rsi_signal = ((rsi - 50.0) / 50.0)
        bb_pct = max(0.0, min(1.0, features.get("bb_pct", 0.5)))
        bb_signal = (bb_pct - 0.5) * 2.0
        macd_hist = max(-1.0, min(1.0, features.get("macd_hist", 0.0) / 0.01))
        zscore = max(-1.0, min(1.0, features.get("zscore", 0.0) / 3.0))
        vwap_dist = max(-1.0, min(1.0, features.get("vwap_dist", 0.0) / 0.01))

        vol_norm = max(0.0, min(1.0, features.get("vol", 0.0) / 0.02))
        atr_norm = max(0.0, min(1.0, features.get("atr_pct", 0.0) / 0.03))
        volume_z = max(-1.0, min(1.0, features.get("volume_z", 0.0) / 3.0))

        regime_mult = 1.0
        if regime == "high_vol":
            regime_mult = 0.80
        elif regime == "range":
            regime_mult = 0.90

        raw = (
            1.00 * trend
            + 0.35 * ret1
            + 0.65 * ret3
            + 0.90 * ret5
            + 0.65 * momentum_quality
            + 0.35 * rsi_signal
            + 0.30 * bb_signal
            + 0.85 * macd_hist
            + 0.25 * vwap_dist
            + 0.10 * volume_z
            - 0.20 * zscore
        )
        damping = 1.0 - (0.38 * max(vol_norm, atr_norm))
        return math.tanh(raw * damping * regime_mult)

    @staticmethod
    def _regime_one_hot(regime: str) -> dict[str, float]:
        return {
            "regime_trend": 1.0 if regime == "trend" else 0.0,
            "regime_range": 1.0 if regime == "range" else 0.0,
            "regime_high_vol": 1.0 if regime == "high_vol" else 0.0,
        }

    @staticmethod
    def _market_regime(features: dict[str, float]) -> str:
        trend_strength = abs(features.get("trend", 0.0)) / max(features.get("vol", 1e-9), 1e-9)
        bb_width = max(features.get("bb_width", 0.0), 0.0)
        atr_pct = max(features.get("atr_pct", 0.0), 0.0)
        vol = max(features.get("vol", 0.0), 0.0)

        if atr_pct > 0.012 or vol > 0.0045:
            return "high_vol"
        if trend_strength < 1.8 and bb_width < 0.03:
            return "range"
        return "trend"

    @staticmethod
    def _regime_direction_threshold(regime: str) -> float:
        if regime == "high_vol":
            return 0.34
        if regime == "range":
            return 0.30
        return 0.24

    @staticmethod
    def _regime_filter_threshold(regime: str) -> float:
        if regime == "high_vol":
            return 0.58
        if regime == "range":
            return 0.52
        return 0.46

    @staticmethod
    def _estimate_uncertainty(base_score: float, ml_adj: float, features: dict[str, float]) -> float:
        disagreement = abs(base_score - ml_adj)
        vol_norm = max(0.0, min(1.0, features.get("vol", 0.0) / 0.01))
        wick_noise = min(1.0, features.get("upper_wick_ratio", 0.0) + features.get("lower_wick_ratio", 0.0))
        uncertainty = (0.50 * disagreement) + (0.35 * vol_norm) + (0.15 * wick_noise)
        return max(0.0, min(1.0, uncertainty))

    @staticmethod
    def _trade_quality(features: dict[str, float], direction_score: float, uncertainty: float, regime: str) -> float:
        edge = abs(direction_score)
        momentum = max(0.0, min(1.0, abs(features.get("momentum_quality", 0.0)) / 3.0))
        liquidity = max(0.0, min(1.0, (features.get("volume_z", 0.0) + 3.0) / 6.0))
        vol_penalty = max(0.0, min(1.0, features.get("atr_pct", 0.0) / 0.02))

        regime_bias = 1.0
        if regime == "high_vol":
            regime_bias = 0.85
        elif regime == "range":
            regime_bias = 0.92

        quality = (0.55 * edge) + (0.20 * momentum) + (0.15 * liquidity) - (0.25 * uncertainty) - (0.10 * vol_penalty)
        return max(0.0, min(1.0, quality * regime_bias))

    def _calibrate_confidence(self, raw_confidence: float, trade_quality: float, regime: str) -> float:
        wf = self._walkforward_hit_rate()
        total_quality = 0.5 if self._total_labels < 100 else max(0.0, min(1.0, (self._correct_labels / max(self._total_labels, 1))))
        health_factor = 0.40 + (0.35 * wf) + (0.25 * total_quality)
        empirical = self._empirical_confidence(raw_confidence, regime)
        regime_total = max(1, int(self._regime_label_total.get(regime, 0)))
        regime_hit = (
            float(self._regime_label_correct.get(regime, 0)) / regime_total
            if regime_total >= 10
            else wf
        )
        data_strength = max(0.0, min(1.0, regime_total / 120.0))
        empirical_blend = (0.65 * empirical) + (0.35 * regime_hit)
        calibrated = raw_confidence * (0.62 + (0.38 * health_factor)) * (0.74 + (0.26 * trade_quality))
        calibrated = ((1.0 - (0.40 * data_strength)) * calibrated) + ((0.40 * data_strength) * empirical_blend)
        return max(0.0, min(0.99, calibrated))

    def _walkforward_hit_rate(self) -> float:
        if not self._walkforward_hits:
            return 0.5
        return sum(self._walkforward_hits) / len(self._walkforward_hits)

    def _compute_model_health(self, wf_hit_rate: float) -> str:
        if self._total_labels < 120:
            return "warming_up"
        if wf_hit_rate >= 0.60:
            return "healthy"
        if wf_hit_rate >= 0.53:
            return "watch"
        return "degraded"

    def _mini_batch_retrain(self, steps: int = 12, batch_size: int = 48) -> None:
        if len(self._replay) < 80:
            return
        data = list(self._replay)
        hn = list(self._hard_negatives)
        for _ in range(max(1, steps)):
            sample_size = min(batch_size, len(data))
            base_batch = self._sample_replay_balanced(data, sample_size)
            hard_take = min(max(0, sample_size // 4), len(hn))
            hard_batch = random.sample(hn, hard_take) if hard_take > 0 else []
            for feat, label, weight, _reg, ts in (base_batch + hard_batch):
                recency = max(0.82, min(1.30, 1.0 + ((ts - (time.time() - 3600.0)) / 14400.0)))
                self.model.update(feat, label, sample_weight=max(0.2, min(3.2, weight * recency)))

    @staticmethod
    def _infer_regime_from_features(features: dict[str, float]) -> str:
        if float(features.get("regime_high_vol", 0.0)) > 0.5:
            return "high_vol"
        if float(features.get("regime_range", 0.0)) > 0.5:
            return "range"
        if float(features.get("regime_trend", 0.0)) > 0.5:
            return "trend"
        return "unknown"

    def _update_calibration_stats(self, raw_confidence: float, hit: int, regime: str) -> None:
        conf = max(0.0, min(0.999, float(raw_confidence)))
        idx = min(9, int(conf * 10.0))
        self._bin_total[idx] += 1
        self._bin_hit[idx] += 1 if int(hit) > 0 else 0
        reg_total = self._regime_bin_total[regime]
        reg_hit = self._regime_bin_hit[regime]
        reg_total[idx] += 1
        reg_hit[idx] += 1 if int(hit) > 0 else 0

    def _empirical_confidence(self, raw_confidence: float, regime: str) -> float:
        conf = max(0.0, min(0.999, float(raw_confidence)))
        idx = min(9, int(conf * 10.0))
        total = self._bin_total[idx]
        hit = self._bin_hit[idx]
        global_emp = (hit / total) if total >= 8 else self._walkforward_hit_rate()
        reg_total = self._regime_bin_total[regime][idx]
        reg_hit = self._regime_bin_hit[regime][idx]
        reg_emp = (reg_hit / reg_total) if reg_total >= 6 else global_emp
        blend = 0.55 * reg_emp + 0.45 * global_emp
        return max(0.0, min(0.99, blend))

    def _sample_replay_balanced(
        self,
        data: list[tuple[dict[str, float], int, float, str, float]],
        sample_size: int,
    ) -> list[tuple[dict[str, float], int, float, str, float]]:
        if sample_size <= 0 or not data:
            return []
        by_regime: dict[str, list[tuple[dict[str, float], int, float, str, float]]] = defaultdict(list)
        for item in data:
            by_regime[str(item[3] or "unknown")].append(item)
        regimes = [r for r in by_regime.keys() if by_regime[r]]
        if len(regimes) <= 1:
            return random.sample(data, min(sample_size, len(data)))

        out: list[tuple[dict[str, float], int, float, str, float]] = []
        per_reg = max(1, sample_size // len(regimes))
        for reg in regimes:
            bucket = by_regime[reg]
            take = min(per_reg, len(bucket))
            out.extend(random.sample(bucket, take))
        if len(out) < sample_size:
            remaining = [x for x in data if x not in out]
            if remaining:
                extra = random.sample(remaining, min(sample_size - len(out), len(remaining)))
                out.extend(extra)
        return out[:sample_size]

    @staticmethod
    def _ema(values: Sequence[float], period: int) -> list[float]:
        if not values or period <= 0 or len(values) < period:
            return []
        alpha = 2 / (period + 1.0)
        # Seed EMA with SMA of first 'period' values
        ema_values = [sum(values[:period]) / period]
        for val in values[period:]:
            ema_values.append(val * alpha + ema_values[-1] * (1 - alpha))
        return ema_values
    
    @staticmethod
    def _atr(ohlcv_data: Sequence[Sequence[float]], period: int = 14) -> float:
        if len(ohlcv_data) < period + 1:
            return 0.0
        
        true_ranges = []
        for i in range(1, len(ohlcv_data)):
            high = ohlcv_data[i][2]
            low = ohlcv_data[i][3]
            prev_close = ohlcv_data[i-1][4]
            
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)
            
        if not true_ranges or len(true_ranges) < period:
            return 0.0

        # Simple Moving Average for the first ATR value
        first_atr = sum(true_ranges[:period]) / period
        
        # Exponential Moving Average for the rest
        atr = first_atr
        for tr in true_ranges[period:]:
            atr = (atr * (period - 1) + tr) / period
            
        return atr

    @staticmethod
    def _macd(closes: Sequence[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> dict[str, float]:
        """
        Calculates the MACD indicator, including the MACD line, signal line, and histogram.
        """
        required_len = slow_period + signal_period
        if len(closes) < required_len:
            return {"line": 0, "signal": 0, "hist": 0}

        # Calculate the fast and slow exponential moving averages (EMAs)
        ema_fast = AISignalEngine._ema(closes, fast_period)
        ema_slow = AISignalEngine._ema(closes, slow_period)

        if not ema_slow:
            return {"line": 0, "signal": 0, "hist": 0}

        # The MACD line is the difference between the fast and slow EMAs.
        # The lengths of the EMA lists are different, so we need to align them.
        # We align the longer (fast) EMA to the end of the shorter (slow) EMA.
        ema_fast_aligned = ema_fast[len(ema_fast) - len(ema_slow) :]
        # EMAs are aligned by construction; strict=False keeps compatibility if upstream sizes drift.
        macd_line = [f - s for f, s in zip(ema_fast_aligned, ema_slow, strict=False)]

        if len(macd_line) < signal_period:
            return {"line": macd_line[-1] if macd_line else 0, "signal": 0, "hist": 0}

        # The signal line is an EMA of the MACD line.
        signal_line = AISignalEngine._ema(macd_line, signal_period)
        if not signal_line:
            return {"line": macd_line[-1] if macd_line else 0, "signal": 0, "hist": 0}

        # The MACD histogram is the difference between the MACD line and the signal line.
        # Again, we need to align the MACD line to the signal line.
        macd_line_aligned = macd_line[len(macd_line) - len(signal_line) :]
        hist = macd_line_aligned[-1] - signal_line[-1] if macd_line_aligned and signal_line else 0

        return {
            "line": macd_line_aligned[-1] if macd_line_aligned else 0,
            "signal": signal_line[-1] if signal_line else 0,
            "hist": hist,
        }

    # ... (static methods _closes, _ret, _sma, _zscore, _rsi, _bollinger_bands are unchanged) ...
    @staticmethod
    def _closes(ohlcv: Sequence[Sequence[float]]) -> list[float]:
        return [float(c[4]) for c in ohlcv if len(c) > 4]

    @staticmethod
    def _ret(a: float, b: float) -> float:
        return (a / b) - 1.0 if b != 0 else 0.0

    @staticmethod
    def _sma(values: Iterable[float]) -> float:
        vals = list(values)
        return sum(vals) / len(vals) if vals else 0.0

    @staticmethod
    def _zscore(values: Sequence[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = statistics.mean(values)
        std = statistics.pstdev(values)
        return (values[-1] - mean) / std if std != 0 else 0.0

    @staticmethod
    def _rsi(closes: Sequence[float], period: int = 14) -> float:
        if len(closes) <= period:
            return 50.0
        deltas = [(closes[i] - closes[i-1]) for i in range(1, len(closes))]
        deltas = deltas[-period:]
        gains = sum(d for d in deltas if d > 0) / period
        losses = sum(abs(d) for d in deltas if d < 0) / period
        if losses == 0:
            return 100.0
        rs = gains / losses
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _bollinger_bands(values: Sequence[float], period: int = 20, num_std_dev: int = 2) -> dict[str, float]:
        if len(values) < period:
            return {"middle": 0, "upper": 0, "lower": 0, "width": 0, "pct": 0.5}
        subset = values[-period:]
        middle = statistics.mean(subset)
        std_dev = statistics.pstdev(subset)
        upper = middle + (num_std_dev * std_dev)
        lower = middle - (num_std_dev * std_dev)
        width = (upper - lower) / middle if middle > 0 else 0.0
        price = values[-1]
        pct = (price - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
        return {"middle": middle, "upper": upper, "lower": lower, "width": width, "pct": pct}

    def _ensure_training_log_header(self) -> None:
        self.training_log_path.parent.mkdir(parents=True, exist_ok=True)
        header_cols = ["ts_utc", "symbol", "old_close", "new_close", "return", "label", "pred_before", "hit"] + self.feature_names
        header = ",".join(header_cols)

        if not self.training_log_path.exists() or self.training_log_path.stat().st_size == 0:
            self.training_log_path.write_text(header + "\n", encoding="utf-8")
            return

        try:
            with self.training_log_path.open("r", encoding="utf-8", errors="replace") as f:
                current_header = f.readline().strip()
        except Exception:
            current_header = ""

        if current_header == header:
            return

        self._migrate_training_log_schema(header_cols)

    def _migrate_training_log_schema(self, header_cols: list[str]) -> None:
        header = ",".join(header_cols)
        ts_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        legacy_path = self.training_log_path.with_name(f"{self.training_log_path.stem}.legacy_{ts_tag}{self.training_log_path.suffix}")

        try:
            self.training_log_path.replace(legacy_path)
        except Exception:
            self.training_log_path.write_text(header + "\n", encoding="utf-8")
            return

        def _as_float(value: object, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        def _as_int(value: object, default: int = 0) -> int:
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return default

        with self.training_log_path.open("w", encoding="utf-8", newline="") as dst:
            writer = csv.DictWriter(dst, fieldnames=header_cols)
            writer.writeheader()
            try:
                with legacy_path.open("r", encoding="utf-8", errors="replace", newline="") as src:
                    reader = csv.DictReader(src)
                    for row in reader:
                        if not isinstance(row, dict):
                            continue
                        ts_val = str(row.get("ts_utc", "") or "").strip()
                        symbol_val = str(row.get("symbol", "") or "").strip()
                        if not ts_val or not symbol_val:
                            continue
                        label_val = _as_int(row.get("label", 0), 0)
                        hit_val = _as_int(row.get("hit", 0), 0)
                        out: dict[str, object] = {
                            "ts_utc": ts_val,
                            "symbol": symbol_val,
                            "old_close": f"{_as_float(row.get('old_close', 0.0), 0.0):.10f}",
                            "new_close": f"{_as_float(row.get('new_close', 0.0), 0.0):.10f}",
                            "return": f"{_as_float(row.get('return', 0.0), 0.0):.10f}",
                            "label": label_val,
                            "pred_before": f"{_as_float(row.get('pred_before', 0.0), 0.0):.10f}",
                            "hit": hit_val,
                        }
                        for key in self.feature_names:
                            out[key] = f"{_as_float(row.get(key, 0.0), 0.0):.10f}"
                        writer.writerow(out)
            except Exception:
                return

    def _load_historical_metrics(self) -> None:
        if not self.training_log_path.exists():
            return
        total, correct, last_ts = 0, 0, ""
        observed_total, observed_last_ts = 0, ""
        long_total, short_total = 0, 0
        long_correct, short_correct = 0, 0
        wf_hits: Deque[int] = deque(maxlen=self.walkforward_window)
        regime_total: dict[str, int] = defaultdict(int)
        regime_correct: dict[str, int] = defaultdict(int)
        try:
            with self.training_log_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        label = int(str(row.get("label", "0")).strip())
                    except (ValueError, TypeError):
                        # Skip malformed rows (for example, accidental runtime log lines).
                        continue
                    observed_total += 1
                    ts_any = str(row.get("ts_utc", "") or "").strip()
                    if ts_any:
                        observed_last_ts = ts_any
                    if label == 0:
                        continue
                    total += 1
                    try:
                        hit_val = int(row.get("hit", "0") or 0)
                        correct += hit_val
                        wf_hits.append(1 if hit_val > 0 else 0)
                        try:
                            pred_before = abs(float(row.get("pred_before", "0") or 0.0))
                        except (TypeError, ValueError):
                            pred_before = 0.0
                        reg = "unknown"
                        try:
                            if float(row.get("regime_high_vol", 0.0) or 0.0) > 0.5:
                                reg = "high_vol"
                            elif float(row.get("regime_range", 0.0) or 0.0) > 0.5:
                                reg = "range"
                            elif float(row.get("regime_trend", 0.0) or 0.0) > 0.5:
                                reg = "trend"
                        except (TypeError, ValueError):
                            reg = "unknown"
                        regime_total[reg] += 1
                        if hit_val > 0:
                            regime_correct[reg] += 1
                        self._update_calibration_stats(pred_before, 1 if hit_val > 0 else 0, reg)
                        if label > 0:
                            long_total += 1
                            long_correct += 1 if hit_val > 0 else 0
                        elif label < 0:
                            short_total += 1
                            short_correct += 1 if hit_val > 0 else 0
                    except (ValueError, TypeError):
                        pass
                    ts = row.get("ts_utc", "") or ""
                    if ts:
                        last_ts = ts
            self._total_labels = total
            self._observed_samples_total = observed_total
            self._correct_labels = correct
            self._long_labels = long_total
            self._short_labels = short_total
            self._long_correct = long_correct
            self._short_correct = short_correct
            self._walkforward_hits = wf_hits
            self._regime_label_total = regime_total
            self._regime_label_correct = regime_correct
            if last_ts:
                self._last_training_update_ts = last_ts
            if observed_last_ts:
                self._last_observation_update_ts = observed_last_ts
            self._model_health = self._compute_model_health(self._walkforward_hit_rate())
        except Exception:
            self._observed_samples_total = 0
            self._total_labels, self._correct_labels = 0, 0
            self._long_labels, self._short_labels = 0, 0
            self._long_correct, self._short_correct = 0, 0
            self._model_health = self._compute_model_health(self._walkforward_hit_rate())

    def heartbeat_status(self, min_interval_sec: float = 2.0) -> None:
        """Keep ai_status.json fresh even when no new labels are produced this cycle."""
        now = time.time()
        last = _safe_float(getattr(self, "_last_status_heartbeat_ts", 0.0), 0.0)
        if (now - last) < max(0.5, float(min_interval_sec)):
            return
        self._last_status_heartbeat_ts = now
        self._write_status(model_status="running")

    def _append_training_log(self, symbol: str, old_close: float, new_close: float, ret: float, label: int, pred_before: float, features: dict[str, float]) -> None:
        hit = 1 if ((pred_before >= 0 and label == 1) or (pred_before < 0 and label == -1)) else 0
        now_iso = datetime.now(timezone.utc).isoformat()
        
        row_data = [
            now_iso,
            symbol, f"{old_close:.10f}", f"{new_close:.10f}", f"{ret:.10f}",
            label, f"{pred_before:.10f}", hit
        ]
        
        for key in self.feature_names:
            row_data.append(f"{features.get(key, 0.0):.10f}")
            
        row = ",".join(map(str, row_data)) + "\n"
        
        with self.training_log_path.open("a", encoding="utf-8", newline="") as f:
            f.write(row)
        self._observed_samples_total += 1
        self._last_observation_update_ts = now_iso

    def _write_status(self, model_status: str) -> None:
        try:
            self.status_path.parent.mkdir(parents=True, exist_ok=True)
            signal_payload: dict[str, object] = {}
            if self._last_signal is not None:
                signal_payload = {
                    "action": self._last_signal.action,
                    "confidence": self._last_signal.confidence,
                    "score": self._last_signal.score,
                    "reason": self._last_signal.reason,
                    "regime": self._last_signal.regime,
                    "calibrated_confidence": self._last_signal.calibrated_confidence,
                    "uncertainty": self._last_signal.uncertainty,
                    "trade_quality": self._last_signal.trade_quality,
                    "trade_filter_pass": self._last_signal.trade_filter_pass,
                    "walkforward_hit_rate": self._last_signal.walkforward_hit_rate,
                    "model_health": self._last_signal.model_health,
                }

            hit_rate = (self._correct_labels / self._total_labels) if self._total_labels > 0 else 0.0
            long_hit_rate = (self._long_correct / self._long_labels) if self._long_labels > 0 else 0.0
            short_hit_rate = (self._short_correct / self._short_labels) if self._short_labels > 0 else 0.0
            wf_hit_rate = self._walkforward_hit_rate()
            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "model_status": model_status,
                "model_path": str(self.model.path),
                "ensemble_enabled": True,
                "ensemble_model_paths": [str(p) for p in self.model.model_paths],
                "training_log_path": str(self.training_log_path),
                "use_internet_data": self.use_internet_data,
                "online_learning_enabled": self.online_learning_enabled,
                "runtime_dry_run": self.runtime_dry_run,
                "online_learning_effective": bool(self.online_learning_enabled and (not self.runtime_dry_run)),
                "last_training_update": self._last_training_update_ts,
                "last_observation_update": self._last_observation_update_ts,
                "observed_samples_total": self._observed_samples_total,
                "total_labels": self._total_labels,
                "correct_labels": self._correct_labels,
                "long_labels": self._long_labels,
                "long_correct_labels": self._long_correct,
                "short_labels": self._short_labels,
                "short_correct_labels": self._short_correct,
                "hit_rate": hit_rate,
                "long_hit_rate": long_hit_rate,
                "short_hit_rate": short_hit_rate,
                "walkforward_window": self.walkforward_window,
                "walkforward_hit_rate": wf_hit_rate,
                "drift_score": self._drift_score,
                "drift_active": self._is_drift_active(),
                "drift_active_for_sec": max(0, int(self._drift_active_until_ts - time.time())),
                "drift_alert_count": self._drift_alert_count,
                "effective_label_threshold": self.effective_label_threshold,
                "roundtrip_cost_fraction": self.roundtrip_cost_fraction,
                "net_edge_ema": self._net_edge_ema,
                "model_health": self._model_health,
                "regime_stats": {
                    reg: {
                        "labels": int(self._regime_label_total.get(reg, 0)),
                        "correct": int(self._regime_label_correct.get(reg, 0)),
                        "hit_rate": (
                            float(self._regime_label_correct.get(reg, 0)) / max(1, int(self._regime_label_total.get(reg, 0)))
                        ),
                    }
                    for reg in ("trend", "range", "high_vol", "unknown")
                },
                "confidence_calibration": {
                    "global_bins_total": list(self._bin_total),
                    "global_bins_hit": list(self._bin_hit),
                },
                "last_signal": signal_payload,
                "internet_features": dict(self._last_internet_features),
                "internet_health": dict(self._last_internet_health),
            }

            tmp_path = self.status_path.with_suffix(self.status_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(self.status_path)
        except Exception:
            # Status updates must never break trading loop.
            return
