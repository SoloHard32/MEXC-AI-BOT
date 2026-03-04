import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


class AdaptiveAgent:
    """Adaptive strategy layer that tunes runtime parameters above core trading logic."""

    PARAM_KEYS = (
        "AI_ENTRY_MIN_QUALITY",
        "AI_ENTRY_SOFT_QUALITY",
        "AI_LOW_QUALITY_RISK_FACTOR",
        "MIN_EXPECTED_EDGE_PCT",
        "AI_ATR_STOP_MULT",
        "AI_ATR_TAKE_PROFIT_MULT",
        "AI_ATR_TRAILING_MULT",
        "TIME_STOP_MIN_CANDLES",
        "TIME_STOP_MAX_CANDLES",
        "TIME_STOP_VOL_REF",
        "TRAILING_ACTIVATION_PCT",
        "AI_MIN_RR",
    )

    def __init__(
        self,
        state_path: Path,
        trade_log_path: Path,
        base_params: dict[str, float],
        history_window: int = 180,
    ) -> None:
        self.state_path = state_path
        self.trade_log_path = trade_log_path
        self.history_window = max(50, int(history_window))
        self.base_params = {k: _safe_float(base_params.get(k), 0.0) for k in self.PARAM_KEYS}
        self.last_adaptation: dict[str, object] = {
            "ts_utc": "",
            "changed": False,
            "reasons": [],
            "changed_keys": [],
        }
        self.state: dict[str, object] = {
            "version": 1,
            "params": dict(self.base_params),
            "recent_trades": [],
            "regime_profiles": self._default_regime_profiles(),
            "last_adaptation_utc": "",
        }
        self._load_state()

    @staticmethod
    def _default_regime_profiles() -> dict[str, dict[str, float]]:
        return {
            "trend": {
                "quality_shift": -0.01,
                "edge_shift": -0.0001,
                "risk_mult": 1.05,
                "atr_stop_mult": 1.00,
                "atr_tp_mult": 1.10,
                "atr_ts_mult": 1.00,
                "time_stop_mult": 1.10,
                "trailing_activation_mult": 1.00,
            },
            "flat": {
                "quality_shift": 0.03,
                "edge_shift": 0.0004,
                "risk_mult": 0.80,
                "atr_stop_mult": 0.95,
                "atr_tp_mult": 0.92,
                "atr_ts_mult": 0.95,
                "time_stop_mult": 0.80,
                "trailing_activation_mult": 1.10,
            },
            "impulse": {
                "quality_shift": 0.02,
                "edge_shift": 0.0002,
                "risk_mult": 0.85,
                "atr_stop_mult": 1.10,
                "atr_tp_mult": 1.15,
                "atr_ts_mult": 1.05,
                "time_stop_mult": 0.95,
                "trailing_activation_mult": 0.85,
            },
            "high_vol": {
                "quality_shift": 0.04,
                "edge_shift": 0.0005,
                "risk_mult": 0.75,
                "atr_stop_mult": 1.20,
                "atr_tp_mult": 1.20,
                "atr_ts_mult": 1.15,
                "time_stop_mult": 0.85,
                "trailing_activation_mult": 0.90,
            },
            "dangerous": {
                "quality_shift": 0.08,
                "edge_shift": 0.0010,
                "risk_mult": 0.50,
                "atr_stop_mult": 1.30,
                "atr_tp_mult": 1.30,
                "atr_ts_mult": 1.20,
                "time_stop_mult": 0.70,
                "trailing_activation_mult": 0.80,
            },
        }

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                return
            raw_params = payload.get("params", {})
            if isinstance(raw_params, dict):
                for key in self.PARAM_KEYS:
                    if key in raw_params:
                        self.state["params"][key] = _safe_float(raw_params.get(key), self.base_params[key])  # type: ignore[index]
            raw_trades = payload.get("recent_trades", [])
            if isinstance(raw_trades, list):
                self.state["recent_trades"] = raw_trades[-self.history_window :]
            raw_profiles = payload.get("regime_profiles", {})
            if isinstance(raw_profiles, dict):
                merged = self._default_regime_profiles()
                for regime, profile in raw_profiles.items():
                    if regime in merged and isinstance(profile, dict):
                        for k, v in profile.items():
                            if k in merged[regime]:
                                merged[regime][k] = _safe_float(v, merged[regime][k])
                self.state["regime_profiles"] = merged
            self.state["last_adaptation_utc"] = str(payload.get("last_adaptation_utc", "") or "")
        except Exception:
            return

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "version": 1,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "params": self.state.get("params", {}),
                "recent_trades": self.state.get("recent_trades", []),
                "regime_profiles": self.state.get("regime_profiles", {}),
                "last_adaptation_utc": str(self.state.get("last_adaptation_utc", "") or ""),
            }
            tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self.state_path)
        except Exception:
            return

    @staticmethod
    def _ema(values: np.ndarray, period: int) -> float:
        if len(values) == 0:
            return 0.0
        alpha = 2.0 / (period + 1.0)
        ema_val = float(values[0])
        for x in values[1:]:
            ema_val = (alpha * float(x)) + ((1.0 - alpha) * ema_val)
        return ema_val

    def detect_market_regime(self, ohlcv: list[list[float]]) -> dict[str, object]:
        closes = np.array([_safe_float(c[4], 0.0) for c in ohlcv if len(c) > 4], dtype=float)
        volumes = np.array([_safe_float(c[5], 0.0) for c in ohlcv if len(c) > 5], dtype=float)
        if len(closes) < 30:
            return {
                "regime": "flat",
                "flags": ["insufficient_data"],
                "volatility": 0.0,
                "trend_strength": 0.0,
                "impulse_score": 0.0,
                "dangerous": False,
            }
        valid = closes > 0
        closes = closes[valid]
        if len(closes) < 30:
            return {
                "regime": "flat",
                "flags": ["insufficient_data"],
                "volatility": 0.0,
                "trend_strength": 0.0,
                "impulse_score": 0.0,
                "dangerous": False,
            }
        returns = np.log(closes[1:] / closes[:-1])
        volatility = float(np.std(returns))
        last_jump = _safe_float((closes[-1] / closes[-2]) - 1.0, 0.0) if len(closes) >= 2 else 0.0
        ema20 = self._ema(closes[-50:], 20)
        ema50 = self._ema(closes[-80:] if len(closes) >= 80 else closes, 50)
        trend_strength = abs(ema20 - ema50) / max(1e-9, closes[-1])
        impulse_score = abs(last_jump) / max(1e-6, volatility)

        vol_high = volatility >= 0.020
        vol_low = volatility <= 0.006
        trend = trend_strength >= 0.0035
        impulse = abs(last_jump) >= 0.012 and impulse_score >= 2.0
        dangerous = abs(last_jump) >= 0.035 or volatility >= 0.045
        regime = "flat"
        if dangerous:
            regime = "dangerous"
        elif vol_high:
            regime = "high_vol"
        elif impulse:
            regime = "impulse"
        elif trend:
            regime = "trend"

        flags: list[str] = []
        if trend:
            flags.append("trend")
        if impulse:
            flags.append("impulse")
        if vol_high:
            flags.append("high_vol")
        if vol_low:
            flags.append("low_vol")
        if dangerous:
            flags.append("dangerous")
        if len(volumes) >= 20:
            vol_now = float(volumes[-1])
            vol_mean = float(np.mean(volumes[-20:]))
            if vol_mean > 0 and vol_now >= vol_mean * 1.8:
                flags.append("volume_spike")

        return {
            "regime": regime,
            "flags": flags,
            "volatility": volatility,
            "trend_strength": trend_strength,
            "impulse_score": impulse_score,
            "dangerous": dangerous,
        }

    def build_runtime_overrides(self, market_ctx: dict[str, object]) -> dict[str, float]:
        params = dict(self.state.get("params", self.base_params))
        regime = str(market_ctx.get("regime", "flat"))
        profiles = self.state.get("regime_profiles", {})
        profile = profiles.get(regime, {}) if isinstance(profiles, dict) else {}
        if isinstance(profile, dict):
            q_shift = _safe_float(profile.get("quality_shift"), 0.0)
            e_shift = _safe_float(profile.get("edge_shift"), 0.0)
            risk_mult = _safe_float(profile.get("risk_mult"), 1.0)
            atr_stop_mult = _safe_float(profile.get("atr_stop_mult"), 1.0)
            atr_tp_mult = _safe_float(profile.get("atr_tp_mult"), 1.0)
            atr_ts_mult = _safe_float(profile.get("atr_ts_mult"), 1.0)
            time_stop_mult = _safe_float(profile.get("time_stop_mult"), 1.0)
            trail_act_mult = _safe_float(profile.get("trailing_activation_mult"), 1.0)

            params["AI_ENTRY_MIN_QUALITY"] = _clamp(_safe_float(params.get("AI_ENTRY_MIN_QUALITY"), 0.55) + q_shift, 0.45, 0.90)
            params["AI_ENTRY_SOFT_QUALITY"] = _clamp(_safe_float(params.get("AI_ENTRY_SOFT_QUALITY"), 0.70) + q_shift, 0.50, 0.95)
            params["MIN_EXPECTED_EDGE_PCT"] = _clamp(_safe_float(params.get("MIN_EXPECTED_EDGE_PCT"), 0.003) + e_shift, 0.0005, 0.0300)
            params["AI_LOW_QUALITY_RISK_FACTOR"] = _clamp(_safe_float(params.get("AI_LOW_QUALITY_RISK_FACTOR"), 0.65) * risk_mult, 0.20, 1.00)
            params["AI_ATR_STOP_MULT"] = _clamp(_safe_float(params.get("AI_ATR_STOP_MULT"), 1.0) * atr_stop_mult, 0.4, 3.0)
            params["AI_ATR_TAKE_PROFIT_MULT"] = _clamp(_safe_float(params.get("AI_ATR_TAKE_PROFIT_MULT"), 1.8) * atr_tp_mult, 0.8, 4.0)
            params["AI_ATR_TRAILING_MULT"] = _clamp(_safe_float(params.get("AI_ATR_TRAILING_MULT"), 1.1) * atr_ts_mult, 0.4, 3.0)
            params["TIME_STOP_MIN_CANDLES"] = float(int(_clamp(_safe_float(params.get("TIME_STOP_MIN_CANDLES"), 15) * time_stop_mult, 5, 240)))
            params["TIME_STOP_MAX_CANDLES"] = float(int(_clamp(_safe_float(params.get("TIME_STOP_MAX_CANDLES"), 180) * time_stop_mult, 20, 480)))
            params["TRAILING_ACTIVATION_PCT"] = _clamp(_safe_float(params.get("TRAILING_ACTIVATION_PCT"), 0.003) * trail_act_mult, 0.0003, 0.0300)

        if _safe_float(params.get("TIME_STOP_MAX_CANDLES"), 180) < _safe_float(params.get("TIME_STOP_MIN_CANDLES"), 15):
            params["TIME_STOP_MAX_CANDLES"] = params["TIME_STOP_MIN_CANDLES"]
        return {k: _safe_float(params.get(k), self.base_params[k]) for k in self.PARAM_KEYS}

    def _append_trade_log(self, trade_record: dict[str, object]) -> None:
        try:
            self.trade_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.trade_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(trade_record, ensure_ascii=False) + "\n")
        except Exception:
            return

    def on_real_trade_closed(self, trade_record: dict[str, object]) -> dict[str, object]:
        self._append_trade_log(trade_record)
        recent = self.state.get("recent_trades", [])
        if not isinstance(recent, list):
            recent = []
        compact = {
            "ts_utc": trade_record.get("exit_ts_utc", datetime.now(timezone.utc).isoformat()),
            "regime": trade_record.get("regime", "flat"),
            "pnl_pct": _safe_float(trade_record.get("pnl_pct"), 0.0),
            "exit_reason": str(trade_record.get("exit_reason", "") or ""),
            "loss_class": str(trade_record.get("loss_class", "") or ""),
            "entry_quality": _safe_float(trade_record.get("entry_quality"), 0.0),
            "entry_edge": _safe_float(trade_record.get("entry_edge"), 0.0),
            "duration_sec": _safe_float(trade_record.get("duration_sec"), 0.0),
        }
        recent.append(compact)
        recent = recent[-self.history_window :]
        self.state["recent_trades"] = recent
        changed, reasons, changed_keys = self._adapt_from_recent(recent)
        now_utc = datetime.now(timezone.utc).isoformat()
        self.state["last_adaptation_utc"] = now_utc
        self.last_adaptation = {
            "ts_utc": now_utc,
            "changed": changed,
            "reasons": reasons,
            "changed_keys": changed_keys,
        }
        self._save_state()
        return dict(self.last_adaptation)

    def _adapt_from_recent(self, recent: list[dict[str, object]]) -> tuple[bool, list[str], list[str]]:
        params = self.state.get("params", {})
        if not isinstance(params, dict):
            params = dict(self.base_params)
            self.state["params"] = params
        if len(recent) < 12:
            return False, ["warmup"], []

        pnl = np.array([_safe_float(x.get("pnl_pct"), 0.0) for x in recent], dtype=float)
        wins = pnl > 0
        win_rate = float(np.mean(wins)) if len(pnl) else 0.0
        avg_pnl = float(np.mean(pnl)) if len(pnl) else 0.0
        time_stop_rate = float(np.mean([1.0 if str(x.get("exit_reason", "")) == "time_stop" else 0.0 for x in recent]))
        stop_rate = float(np.mean([1.0 if str(x.get("exit_reason", "")) == "stop_loss" else 0.0 for x in recent]))
        losses = [str(x.get("loss_class", "")) for x in recent if _safe_float(x.get("pnl_pct"), 0.0) < 0]
        low_liq_rate = float(np.mean([1.0 if cls == "low_liquidity_entry" else 0.0 for cls in losses])) if losses else 0.0
        sideway_rate = float(np.mean([1.0 if cls == "entry_in_sideways" else 0.0 for cls in losses])) if losses else 0.0
        against_impulse_rate = float(np.mean([1.0 if cls == "entry_against_impulse" else 0.0 for cls in losses])) if losses else 0.0

        old = {k: _safe_float(params.get(k), self.base_params[k]) for k in self.PARAM_KEYS}
        new = dict(old)
        reasons: list[str] = []
        step_q = 0.008
        step_edge = 0.00015
        step_risk = 0.020

        if win_rate < 0.46 or avg_pnl < 0:
            new["AI_ENTRY_MIN_QUALITY"] += step_q
            new["AI_ENTRY_SOFT_QUALITY"] += step_q
            new["MIN_EXPECTED_EDGE_PCT"] += step_edge
            new["AI_LOW_QUALITY_RISK_FACTOR"] -= step_risk
            new["AI_MIN_RR"] += 0.04
            reasons.append("defensive_shift")
        elif win_rate > 0.58 and avg_pnl > 0:
            new["AI_ENTRY_MIN_QUALITY"] -= step_q * 0.7
            new["AI_ENTRY_SOFT_QUALITY"] -= step_q * 0.7
            new["MIN_EXPECTED_EDGE_PCT"] -= step_edge * 0.7
            new["AI_LOW_QUALITY_RISK_FACTOR"] += step_risk * 0.7
            new["AI_MIN_RR"] -= 0.03
            reasons.append("offensive_shift")

        if time_stop_rate > 0.35:
            new["TIME_STOP_MIN_CANDLES"] += 2.0
            new["TIME_STOP_MAX_CANDLES"] += 6.0
            new["TIME_STOP_VOL_REF"] += 0.0004
            new["AI_ATR_TAKE_PROFIT_MULT"] -= 0.04
            reasons.append("high_time_stop")

        if stop_rate > 0.30:
            new["AI_ENTRY_MIN_QUALITY"] += 0.01
            new["MIN_EXPECTED_EDGE_PCT"] += 0.0002
            new["AI_ATR_STOP_MULT"] += 0.06
            new["AI_ATR_TRAILING_MULT"] += 0.04
            new["AI_LOW_QUALITY_RISK_FACTOR"] -= 0.02
            reasons.append("high_stop_rate")

        if low_liq_rate > 0.25:
            new["MIN_EXPECTED_EDGE_PCT"] += 0.0002
            new["AI_ENTRY_MIN_QUALITY"] += 0.008
            reasons.append("low_liquidity_losses")
        if sideway_rate > 0.30:
            new["AI_ENTRY_MIN_QUALITY"] += 0.006
            new["AI_MIN_RR"] += 0.03
            reasons.append("sideways_losses")
        if against_impulse_rate > 0.22:
            new["AI_ATR_STOP_MULT"] += 0.04
            new["TRAILING_ACTIVATION_PCT"] -= 0.0001
            reasons.append("against_impulse_losses")

        # Evolve regime profiles from regime-specific outcomes.
        profiles = self.state.get("regime_profiles", {})
        if isinstance(profiles, dict):
            by_regime: dict[str, list[float]] = {}
            for item in recent:
                reg = str(item.get("regime", "flat") or "flat")
                by_regime.setdefault(reg, []).append(_safe_float(item.get("pnl_pct"), 0.0))
            for reg, vals in by_regime.items():
                if reg not in profiles or not isinstance(profiles.get(reg), dict) or len(vals) < 6:
                    continue
                avg_reg = float(np.mean(np.array(vals, dtype=float)))
                profile = profiles[reg]
                if avg_reg < 0:
                    profile["risk_mult"] = _clamp(_safe_float(profile.get("risk_mult"), 1.0) * 0.97, 0.40, 1.20)
                    profile["quality_shift"] = _clamp(_safe_float(profile.get("quality_shift"), 0.0) + 0.004, -0.05, 0.18)
                    profile["edge_shift"] = _clamp(_safe_float(profile.get("edge_shift"), 0.0) + 0.00005, -0.001, 0.003)
                elif avg_reg > 0:
                    profile["risk_mult"] = _clamp(_safe_float(profile.get("risk_mult"), 1.0) * 1.01, 0.40, 1.20)
                    profile["quality_shift"] = _clamp(_safe_float(profile.get("quality_shift"), 0.0) - 0.002, -0.05, 0.18)
                    profile["edge_shift"] = _clamp(_safe_float(profile.get("edge_shift"), 0.0) - 0.00003, -0.001, 0.003)
            self.state["regime_profiles"] = profiles

        # Clamp all tuned params.
        new["AI_ENTRY_MIN_QUALITY"] = _clamp(new["AI_ENTRY_MIN_QUALITY"], 0.45, 0.90)
        new["AI_ENTRY_SOFT_QUALITY"] = _clamp(new["AI_ENTRY_SOFT_QUALITY"], 0.50, 0.95)
        new["AI_LOW_QUALITY_RISK_FACTOR"] = _clamp(new["AI_LOW_QUALITY_RISK_FACTOR"], 0.20, 1.00)
        new["MIN_EXPECTED_EDGE_PCT"] = _clamp(new["MIN_EXPECTED_EDGE_PCT"], 0.0005, 0.0300)
        new["AI_ATR_STOP_MULT"] = _clamp(new["AI_ATR_STOP_MULT"], 0.4, 3.0)
        new["AI_ATR_TAKE_PROFIT_MULT"] = _clamp(new["AI_ATR_TAKE_PROFIT_MULT"], 0.8, 4.0)
        new["AI_ATR_TRAILING_MULT"] = _clamp(new["AI_ATR_TRAILING_MULT"], 0.4, 3.0)
        new["TIME_STOP_MIN_CANDLES"] = float(int(_clamp(new["TIME_STOP_MIN_CANDLES"], 5, 240)))
        new["TIME_STOP_MAX_CANDLES"] = float(int(_clamp(new["TIME_STOP_MAX_CANDLES"], new["TIME_STOP_MIN_CANDLES"], 480)))
        new["TIME_STOP_VOL_REF"] = _clamp(new["TIME_STOP_VOL_REF"], 0.001, 0.030)
        new["TRAILING_ACTIVATION_PCT"] = _clamp(new["TRAILING_ACTIVATION_PCT"], 0.0003, 0.0300)
        new["AI_MIN_RR"] = _clamp(new["AI_MIN_RR"], 1.10, 3.00)

        changed_keys = [k for k in self.PARAM_KEYS if abs(new[k] - old[k]) > 1e-12]
        changed = bool(changed_keys)
        if changed:
            for k in self.PARAM_KEYS:
                params[k] = new[k]
        return changed, reasons, changed_keys

    def current_base_params(self) -> dict[str, float]:
        params = self.state.get("params", {})
        if not isinstance(params, dict):
            return dict(self.base_params)
        return {k: _safe_float(params.get(k), self.base_params[k]) for k in self.PARAM_KEYS}
