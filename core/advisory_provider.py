from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional


def _safe_float(v: object, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _safe_bool(v: object, d: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "on", "ok"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
    return d


def _norm_action(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"BUY", "LONG"}:
        return "LONG"
    if s in {"SELL", "SHORT"}:
        return "SHORT"
    if s in {"HOLD", "WAIT", "NONE"}:
        return "HOLD"
    return ""


@dataclass
class AdvisorySignal:
    ok: bool
    source: str
    ts_utc: str
    action: str
    confidence: float
    quality: float
    edge_bias_pct: float
    reason: str = ""


class AdvisoryProvider:
    """
    Optional external advisory layer (e.g. re7 labs API) to enrich AI decisions.
    It never replaces core signal logic and can be disabled with one flag.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        name: str,
        url: str,
        timeout_sec: float = 0.35,
        ttl_sec: float = 10.0,
    ) -> None:
        self.enabled = bool(enabled) and bool(str(url or "").strip())
        self.name = str(name or "external").strip() or "external"
        self.url = str(url or "").strip()
        self.timeout_sec = max(0.10, float(timeout_sec))
        self.ttl_sec = max(1.0, float(ttl_sec))
        self._cache_key = ""
        self._cache_ts = 0.0
        self._cache_payload: Optional[AdvisorySignal] = None
        self._last_error_ts = 0.0
        self._last_latency_ms = 0.0
        self._ok_count = 0
        self._err_count = 0
        self._mute_until_ts = 0.0

    def _parse_payload(self, payload: dict[str, Any]) -> Optional[AdvisorySignal]:
        if not isinstance(payload, dict):
            return None
        ok = _safe_bool(payload.get("ok"), True)
        data = payload.get("signal") if isinstance(payload.get("signal"), dict) else payload
        action = _norm_action(data.get("action"))
        conf = _safe_float(data.get("confidence"), _safe_float(data.get("conf"), 0.0))
        quality = _safe_float(data.get("quality"), _safe_float(data.get("trade_quality"), 0.0))
        edge_bias = _safe_float(data.get("edge_bias_pct"), _safe_float(data.get("edge_bias"), 0.0))
        reason = str(data.get("reason", "") or "")
        ts_utc = str(data.get("ts_utc") or payload.get("ts_utc") or "")
        if action == "":
            action = "HOLD"
        conf = max(0.0, min(1.0, conf))
        quality = max(0.0, min(1.0, quality))
        edge_bias = max(-0.02, min(0.02, edge_bias))
        return AdvisorySignal(
            ok=ok,
            source=self.name,
            ts_utc=ts_utc,
            action=action,
            confidence=conf,
            quality=quality,
            edge_bias_pct=edge_bias,
            reason=reason,
        )

    def fetch(self, *, symbol: str, timeframe: str, mode: str, regime: str = "") -> Optional[AdvisorySignal]:
        if not self.enabled:
            return None
        sym = str(symbol or "").strip().upper()
        tf = str(timeframe or "").strip().lower()
        md = str(mode or "").strip().lower()
        rg = str(regime or "").strip().lower()
        key = f"{sym}|{tf}|{md}|{rg}"
        now = time.time()
        if now < self._mute_until_ts:
            if self._cache_payload is not None and self._cache_key == key:
                return self._cache_payload
            return None
        if self._cache_payload is not None and self._cache_key == key and (now - self._cache_ts) <= self.ttl_sec:
            return self._cache_payload

        try:
            params = urllib.parse.urlencode(
                {
                    "symbol": sym,
                    "timeframe": tf,
                    "mode": md,
                    "regime": rg,
                    "source": self.name,
                }
            )
            sep = "&" if "?" in self.url else "?"
            url = f"{self.url}{sep}{params}"
            t0 = time.perf_counter()
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            self._last_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
            payload = json.loads(raw) if raw.strip() else {}
            parsed = self._parse_payload(payload)
            if parsed is None:
                self._err_count += 1
                return None
            self._ok_count += 1
            self._cache_key = key
            self._cache_ts = now
            self._cache_payload = parsed
            self._mute_until_ts = 0.0
            return parsed
        except Exception as exc:
            self._err_count += 1
            self._mute_until_ts = now + min(30.0, max(5.0, self.ttl_sec * 2.0))
            if (now - self._last_error_ts) >= 60.0:
                logging.info("Advisory provider unavailable (%s): %s", self.name, exc)
                self._last_error_ts = now
            return None

    def status(self) -> dict[str, object]:
        return {
            "enabled": bool(self.enabled),
            "name": self.name,
            "url": self.url,
            "timeout_sec": round(self.timeout_sec, 3),
            "ttl_sec": round(self.ttl_sec, 2),
            "ok_count": int(self._ok_count),
            "err_count": int(self._err_count),
            "last_latency_ms": round(self._last_latency_ms, 1),
            "cached": bool(self._cache_payload is not None),
            "mute_for_sec": round(max(0.0, self._mute_until_ts - time.time()), 1),
        }
