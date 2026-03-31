from __future__ import annotations

from collections import deque
from collections import defaultdict
import logging
import time
import urllib.parse
import urllib.request
import urllib.error
import json
from datetime import datetime, timezone
from typing import Any


class BaseEngineAdapter:
    """
    Adapter facade for market/execution backend.
    By default, delegates to the existing Python Trader implementation.
    """

    backend_name = "python"

    def __init__(self, trader: Any) -> None:
        self._trader = trader

    def __getattr__(self, item: str) -> Any:
        return getattr(self._trader, item)

    @property
    def exchange(self) -> Any:
        return self._trader.exchange

    def fetch_cycle_bundle(self, symbol: str, timeframe: str, limit: int) -> dict[str, object]:
        return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)

    def get_position(self, symbol: str) -> dict[str, object]:
        return self._trader.get_position(symbol)

    def last_price(self, symbol: str, ttl_sec: float | None = None, force_refresh: bool = False) -> float:
        return float(self._trader.last_price(symbol, ttl_sec=ttl_sec, force_refresh=force_refresh))

    def buy_market(self, symbol: str, usdt_cost: float) -> Any:
        return self._trader.buy_market(symbol, usdt_cost)

    def sell_market(self, symbol: str, amount_base: float) -> Any:
        return self._trader.sell_market(symbol, amount_base)

    def futures_open_market(self, symbol: str, side: str, usdt_margin: float) -> Any:
        return self._trader.futures_open_market(symbol, side, usdt_margin)

    def futures_close_market(self, symbol: str, side: str, contracts: float) -> Any:
        return self._trader.futures_close_market(symbol, side, contracts)

    def backend_status(self) -> dict[str, object]:
        return {"name": self.backend_name, "healthy": True, "mode": "native"}

    # --- Execution hooks (phase 3) ---
    def place_spot_buy(self, symbol: str, usdt_cost: float, *, client_order_id: str | None = None) -> Any:
        return self._trader.buy_market(symbol, usdt_cost)

    def place_spot_sell(self, symbol: str, base_amount: float, *, client_order_id: str | None = None) -> Any:
        return self._trader.sell_market(symbol, base_amount)

    def place_futures_open(
        self,
        symbol: str,
        side: str,
        usdt_margin: float,
        *,
        client_order_id: str | None = None,
    ) -> Any:
        return self._trader.futures_open_market(symbol, side, usdt_margin)

    def place_futures_close(
        self,
        symbol: str,
        side: str,
        contracts: float,
        *,
        client_order_id: str | None = None,
    ) -> Any:
        return self._trader.futures_close_market(symbol, side, contracts)

    def confirm_buy_position(self, symbol: str, pre_base_free: float) -> tuple[bool, float] | None:
        return None

    def confirm_sell_position(self, symbol: str, pre_base_free: float) -> tuple[bool, float] | None:
        return None


class PythonEngineAdapter(BaseEngineAdapter):
    backend_name = "python"


class RustEngineAdapter(BaseEngineAdapter):
    """
    Stage-1 placeholder:
    - keeps bot fully working through Python trader
    - prepares explicit backend switch path for future Rust service
    """

    backend_name = "rust-shadow"

    def __init__(
        self,
        trader: Any,
        rust_url: str = "",
        http_timeout_sec: float = 0.45,
        execution_gateway: bool = False,
        order_timeout_sec: float = 0.70,
        strict_mode: bool = False,
        snapshot_max_age_sec: float = 35.0,
        bot_mode: str = "",
        auto_strict_fallback_in_live: bool = True,
        live_strict_rearm_sec: float = 90.0,
    ) -> None:
        super().__init__(trader)
        self.rust_url = str(rust_url or "").strip()
        self.http_timeout_sec = max(0.15, float(http_timeout_sec))
        self.execution_gateway = bool(execution_gateway)
        self.order_timeout_sec = max(0.15, float(order_timeout_sec))
        self.strict_mode = bool(strict_mode)
        self.snapshot_max_age_sec = max(5.0, float(snapshot_max_age_sec))
        self.bot_mode = str(bot_mode or "").strip().lower()
        self.auto_strict_fallback_in_live = bool(auto_strict_fallback_in_live)
        self.live_strict_rearm_sec = max(15.0, float(live_strict_rearm_sec))
        self._live_strict_fallback_triggered = False
        self._strict_downgraded_ts = 0.0
        self._last_health_ok_ts = 0.0
        self._last_health_err_log_ts = 0.0
        self._last_snapshot_err_log_ts = 0.0
        self._last_order_err_log_ts = 0.0
        self._health_cache_ts = 0.0
        self._health_cache_ok = False
        self._health_latency_ms = 0.0
        self._snapshot_latency_ms = 0.0
        self._order_latency_ms = 0.0
        self._fallback_snapshot_count = 0
        self._snapshot_bypass_count = 0
        self._snapshot_fallback_reasons: dict[str, int] = defaultdict(int)
        self._snapshot_bypass_reasons: dict[str, int] = defaultdict(int)
        self._snapshot_fallback_ts: deque[float] = deque(maxlen=400)
        self._snapshot_bypass_ts: deque[float] = deque(maxlen=400)
        self._health_fail_ts: deque[float] = deque(maxlen=400)
        self._order_fail_ts: deque[float] = deque(maxlen=400)
        self._fallback_order_count = 0
        self._health_fail_count = 0
        self._health_series: deque[float] = deque(maxlen=40)
        self._snapshot_latency_series: deque[float] = deque(maxlen=40)
        self._order_latency_series: deque[float] = deque(maxlen=40)
        self._exec_consecutive_failures = 0
        self._exec_degraded_until_ts = 0.0
        self._exec_degrade_seconds = 25.0
        self._order_retries = 3
        self._last_service_symbol = ""
        self._last_service_symbol_ts = 0.0
        logging.info(
            "Rust engine adapter enabled in shadow mode. URL=%s | execution_gateway=%s | strict_mode=%s",
            self.rust_url or "-",
            "on" if self.execution_gateway else "off",
            "on" if self.strict_mode else "off",
        )

    @staticmethod
    def _prune_recent(q: deque[float], window_sec: float = 300.0) -> int:
        now = time.time()
        win = max(30.0, float(window_sec))
        while q and (now - float(q[0])) > win:
            q.popleft()
        return len(q)

    def _note_snapshot_fallback(self, reason: str) -> None:
        self._fallback_snapshot_count += 1
        key = str(reason or "unknown").strip().lower() or "unknown"
        self._snapshot_fallback_reasons[key] += 1
        self._snapshot_fallback_ts.append(time.time())

    def _note_snapshot_bypass(self, reason: str) -> None:
        self._snapshot_bypass_count += 1
        key = str(reason or "unknown").strip().lower() or "unknown"
        self._snapshot_bypass_reasons[key] += 1
        self._snapshot_bypass_ts.append(time.time())

    def _snapshot_timeout_sec(self) -> float:
        base = max(0.12, float(self.http_timeout_sec))
        samples = list(self._snapshot_latency_series)[-24:]
        if not samples:
            return base
        ordered = sorted(max(0.0, float(v)) for v in samples)
        idx = min(len(ordered) - 1, int(len(ordered) * 0.90))
        p90_ms = ordered[idx]
        dynamic = (p90_ms / 1000.0) * 2.2
        if self._health_fail_count > 0:
            dynamic += 0.15
        return min(max(base, dynamic), 2.2)

    def _note_health(self, ok: bool) -> None:
        self._health_series.append(100.0 if ok else 0.0)

    def _note_snapshot_latency(self, latency_ms: float) -> None:
        self._snapshot_latency_series.append(max(0.0, float(latency_ms)))

    def _note_order_latency(self, latency_ms: float) -> None:
        self._order_latency_series.append(max(0.0, float(latency_ms)))

    def _mark_exec_fail(self) -> None:
        self._exec_consecutive_failures += 1
        if self._exec_consecutive_failures >= 2:
            self._exec_degraded_until_ts = max(self._exec_degraded_until_ts, time.time() + self._exec_degrade_seconds)

    def _mark_exec_success(self) -> None:
        self._exec_consecutive_failures = 0
        self._exec_degraded_until_ts = 0.0

    def _exec_degraded_active(self) -> bool:
        return time.time() < self._exec_degraded_until_ts

    def _strict_fail(self, op_name: str, reason: str) -> None:
        if self.execution_gateway and self.strict_mode:
            if self.bot_mode == "live" and self.auto_strict_fallback_in_live:
                # Safety valve for live mode: auto-downgrade strict->soft instead of hard-failing cycle.
                self.strict_mode = False
                self._live_strict_fallback_triggered = True
                self._strict_downgraded_ts = time.time()
                logging.warning(
                    "Live strict auto-fallback activated: op=%s reason=%s. Continue in soft gateway mode.",
                    op_name,
                    reason,
                )
                return
            raise RuntimeError(f"Strict gateway mode: {op_name} blocked ({reason})")

    def _maybe_rearm_live_strict(self) -> None:
        if self.bot_mode != "live":
            return
        if self.strict_mode:
            return
        if not self.auto_strict_fallback_in_live:
            return
        if not self._live_strict_fallback_triggered:
            return
        if (time.time() - self._strict_downgraded_ts) < self.live_strict_rearm_sec:
            return
        if self._exec_degraded_active():
            return
        if not self._health_ok():
            return
        self.strict_mode = True
        self._live_strict_fallback_triggered = False
        self._strict_downgraded_ts = 0.0
        logging.info("Live strict mode auto-rearmed after stable health window.")

    def _gateway_place_order(self, req: dict[str, object], *, op_name: str) -> Any | None:
        self._maybe_rearm_live_strict()
        if not self.execution_gateway:
            return None
        if self._exec_degraded_active():
            self._fallback_order_count += 1
            self._mark_exec_fail()
            self._strict_fail(op_name, "gateway_degraded")
            return None
        if not self._health_ok():
            self._fallback_order_count += 1
            self._mark_exec_fail()
            self._strict_fail(op_name, "backend_unhealthy")
            return None

        last_res: dict[str, object] | None = None
        for _ in range(max(1, int(self._order_retries))):
            t0 = time.perf_counter()
            res = self._http_json_post("/order/place", req, timeout_sec=self.order_timeout_sec)
            self._order_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
            self._note_order_latency(self._order_latency_ms)
            last_res = res if isinstance(res, dict) else None
            if isinstance(last_res, dict) and bool(last_res.get("ok", False)) and isinstance(last_res.get("order"), dict):
                raw_order = last_res.get("order")
                safe_order = self._sanitize_gateway_order(raw_order, req=req, op_name=op_name)
                if safe_order is not None:
                    self._mark_exec_success()
                    return safe_order
                self._order_fail_ts.append(time.time())
        self._fallback_order_count += 1
        self._order_fail_ts.append(time.time())
        self._mark_exec_fail()
        self._strict_fail(op_name, "gateway_place_failed")
        now = time.time()
        if (now - self._last_order_err_log_ts) >= 10.0:
            logging.info("Rust execution %s unavailable -> fallback to python", op_name)
            self._last_order_err_log_ts = now
        return None

    def _sanitize_gateway_order(self, order: object, *, req: dict[str, object], op_name: str) -> dict[str, object] | None:
        if not isinstance(order, dict):
            return None
        out = dict(order)
        action = str(req.get("action", "") or "").strip().lower()
        symbol = str(req.get("symbol", "") or "").strip()
        filled = float(out.get("filled", 0.0) or 0.0)
        cost = float(out.get("cost", 0.0) or 0.0)
        avg = float(out.get("average", 0.0) or 0.0)
        px = float(out.get("price", 0.0) or 0.0)
        used_px = avg if avg > 0 else px
        if used_px <= 0 and filled > 0 and cost > 0:
            used_px = cost / filled
            out["average"] = used_px
        if action in {"buy", "sell"}:
            if filled <= 0:
                return None
            if cost <= 0 and used_px > 0:
                cost = filled * used_px
                out["cost"] = cost
            if cost <= 0:
                return None
            # Market sanity check: reject absurd fill prices from stale/broken gateway payload.
            try:
                ref_px = float(self._trader.last_price(symbol, ttl_sec=0.2, force_refresh=False) or 0.0)
            except Exception:
                ref_px = 0.0
            if ref_px > 0 and used_px > 0:
                drift = abs((used_px / ref_px) - 1.0)
                if drift > 0.60:
                    logging.warning(
                        "Gateway order rejected by sanity guard (%s): symbol=%s used_px=%.6f ref_px=%.6f drift=%.2f%%",
                        op_name,
                        symbol,
                        used_px,
                        ref_px,
                        drift * 100.0,
                    )
                    return None
            # Notional consistency for spot path.
            if used_px > 0:
                implied = filled * used_px
                if cost > 0:
                    err = abs((implied / cost) - 1.0)
                    if err > 0.35:
                        out["filled"] = max(0.0, cost / used_px)
        return out

    def _gateway_confirm_order(self, req: dict[str, object], *, op_name: str) -> tuple[bool, float] | None:
        self._maybe_rearm_live_strict()
        if not self.execution_gateway:
            return None
        if self._exec_degraded_active():
            self._fallback_order_count += 1
            self._mark_exec_fail()
            self._strict_fail(op_name, "gateway_degraded")
            return None
        if not self._health_ok():
            self._fallback_order_count += 1
            self._mark_exec_fail()
            self._strict_fail(op_name, "backend_unhealthy")
            return None

        for _ in range(max(1, int(self._order_retries))):
            t0 = time.perf_counter()
            res = self._http_json_post("/order/confirm", req, timeout_sec=self.order_timeout_sec)
            self._order_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
            self._note_order_latency(self._order_latency_ms)
            if isinstance(res, dict) and bool(res.get("ok", False)):
                self._mark_exec_success()
                return bool(res.get("confirmed", False)), float(res.get("base_free", req.get("pre_base_free", 0.0)) or 0.0)
        self._fallback_order_count += 1
        self._order_fail_ts.append(time.time())
        self._mark_exec_fail()
        self._strict_fail(op_name, "gateway_confirm_failed")
        return None

    def _http_json(
        self,
        path: str,
        params: dict[str, object] | None = None,
        *,
        timeout_sec: float | None = None,
    ) -> dict[str, object] | None:
        if not self.rust_url:
            return None
        base = self.rust_url.rstrip("/")
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{base}{path}{query}"
        try:
            req = urllib.request.Request(url, method="GET")
            t = self.http_timeout_sec if timeout_sec is None else max(0.1, float(timeout_sec))
            with urllib.request.urlopen(req, timeout=t) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw.strip() else {}
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def _http_json_post(self, path: str, payload: dict[str, object], timeout_sec: float | None = None) -> dict[str, object] | None:
        if not self.rust_url:
            return None
        base = self.rust_url.rstrip("/")
        url = f"{base}{path}"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        t = self.http_timeout_sec if timeout_sec is None else max(0.1, float(timeout_sec))
        try:
            req = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            with urllib.request.urlopen(req, timeout=t) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw.strip() else {}
            return data if isinstance(data, dict) else None
        except urllib.error.URLError:
            return None
        except Exception:
            return None

    def _health_ok(self) -> bool:
        now = time.time()
        # cache health result briefly to avoid repeated overhead each call
        if (now - self._health_cache_ts) <= 1.2:
            return self._health_cache_ok
        t0 = time.perf_counter()
        data = self._http_json("/health")
        self._health_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
        ok = bool(data and bool(data.get("ok", True)))
        self._note_health(ok)
        self._health_cache_ok = ok
        self._health_cache_ts = now
        if ok:
            self._last_health_ok_ts = now
        else:
            self._health_fail_count += 1
            self._health_fail_ts.append(now)
            if (now - self._last_health_err_log_ts) >= 60.0:
                logging.info("Rust shadow health unavailable -> fallback to python backend")
                self._last_health_err_log_ts = now
        return ok

    def _snapshot(self, symbol: str, timeframe: str, limit: int) -> dict[str, object] | None:
        req_sym = self._norm_symbol(symbol)
        # Shadow service serves one active symbol at a time.
        # Avoid pointless HTTP call + noisy fallback metric when we know symbol is different.
        if (
            req_sym
            and self._last_service_symbol
            and req_sym != self._last_service_symbol
            and (time.time() - self._last_service_symbol_ts) <= 3.0
        ):
            self._note_snapshot_bypass("symbol_prefilter")
            return None
        if not self._health_ok():
            self._note_snapshot_fallback("backend_unhealthy")
            return None
        t0 = time.perf_counter()
        data = self._http_json(
            "/snapshot",
            params={"symbol": symbol, "timeframe": timeframe, "limit": int(limit)},
            timeout_sec=self._snapshot_timeout_sec(),
        )
        self._snapshot_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
        self._note_snapshot_latency(self._snapshot_latency_ms)
        if not data:
            self._note_snapshot_fallback("snapshot_unavailable")
            now = time.time()
            if (now - self._last_snapshot_err_log_ts) >= 60.0:
                logging.info("Rust shadow snapshot unavailable -> fallback to python backend")
                self._last_snapshot_err_log_ts = now
            return None
        source_sym = self._norm_symbol(data.get("source_symbol", "") or data.get("symbol", ""))
        if source_sym:
            self._last_service_symbol = source_sym
            self._last_service_symbol_ts = time.time()
        return data

    @staticmethod
    def _norm_symbol(s: object) -> str:
        return str(s or "").strip().upper()

    @staticmethod
    def _iso_to_ts(v: object) -> float:
        raw = str(v or "").strip()
        if not raw:
            return 0.0
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    @staticmethod
    def _timeframe_to_sec(tf: object) -> float:
        raw = str(tf or "").strip().lower()
        if not raw:
            return 60.0
        try:
            if raw.endswith("ms"):
                return max(0.1, float(raw[:-2]) / 1000.0)
            if raw.endswith("s"):
                return max(1.0, float(raw[:-1]))
            if raw.endswith("m"):
                return max(1.0, float(raw[:-1]) * 60.0)
            if raw.endswith("h"):
                return max(1.0, float(raw[:-1]) * 3600.0)
            if raw.endswith("d"):
                return max(1.0, float(raw[:-1]) * 86400.0)
            if raw.endswith("w"):
                return max(1.0, float(raw[:-1]) * 7.0 * 86400.0)
            if raw.endswith("mo"):
                return max(1.0, float(raw[:-2]) * 30.0 * 86400.0)
        except Exception:
            return 60.0
        return 60.0

    def fetch_cycle_bundle(self, symbol: str, timeframe: str, limit: int) -> dict[str, object]:
        snap = self._snapshot(symbol, timeframe, limit)
        if not snap:
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)

        # Minimal contract:
        # {
        #   "symbol":"SOL/USDT", "timeframe":"1m", "price":84.3,
        #   "ohlcv":[[ts,o,h,l,c,v],...], "position":{...}, "profile":{"market_fetch_ms":...}
        # }
        ohlcv = snap.get("ohlcv", [])
        price = snap.get("price", 0.0)
        position = snap.get("position", {})
        profile = snap.get("profile", {})
        if not isinstance(ohlcv, list) or not ohlcv:
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        if bool(snap.get("market_data_stale", False)):
            self._note_snapshot_fallback("stale_market_data")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        req_sym = self._norm_symbol(symbol)
        snap_sym = self._norm_symbol(snap.get("symbol", ""))
        src_sym = self._norm_symbol(snap.get("source_symbol", ""))
        if (not snap_sym) and (not src_sym):
            self._note_snapshot_fallback("missing_symbol_tag")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        if snap_sym and req_sym and snap_sym != req_sym:
            self._note_snapshot_bypass("snapshot_symbol_mismatch")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        if src_sym and req_sym and src_sym != req_sym:
            self._note_snapshot_bypass("source_symbol_mismatch")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        try:
            last_ts_ms = float(ohlcv[-1][0]) if isinstance(ohlcv[-1], (list, tuple)) and len(ohlcv[-1]) >= 5 else 0.0
        except Exception:
            last_ts_ms = 0.0
        if last_ts_ms <= 0:
            self._note_snapshot_fallback("invalid_last_candle_ts")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        now_ms = time.time() * 1000.0
        tf_sec = self._timeframe_to_sec(timeframe)
        max_candle_age_sec = max(self.snapshot_max_age_sec, tf_sec * 2.2)
        if (now_ms - last_ts_ms) > (max_candle_age_sec * 1000.0):
            self._note_snapshot_fallback("candle_too_old")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        status_ts = self._iso_to_ts(snap.get("status_write_ts_utc", ""))
        if status_ts > 0 and (time.time() - status_ts) > max(self.snapshot_max_age_sec, 30.0):
            self._note_snapshot_fallback("status_too_old")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        if not isinstance(position, dict):
            position = self._trader.get_position(symbol)
        # Prevent cross-symbol position contamination from stale snapshot payload.
        try:
            req_base = str(symbol).split("/")[0].upper()
            pos_base = str(position.get("base_currency", "") or "").upper() if isinstance(position, dict) else ""
            if pos_base and req_base and pos_base != req_base:
                self._note_snapshot_bypass("position_symbol_mismatch")
                position = self._trader.get_position(symbol)
        except Exception:
            pass
        if not isinstance(profile, dict):
            profile = {}
        px = float(price or 0.0)
        if px <= 0:
            try:
                px = float(ohlcv[-1][4])
            except Exception:
                px = 0.0
        if px <= 0:
            self._note_snapshot_fallback("invalid_price")
            return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        # Price sanity against direct trader feed (non-shadow) to avoid stale/cross-symbol spikes.
        try:
            direct_px = float(self._trader.last_price(symbol, ttl_sec=0.2, force_refresh=False) or 0.0)
        except Exception:
            direct_px = 0.0
        if direct_px > 0 and px > 0:
            drift_bps = abs((px / direct_px) - 1.0) * 10_000.0
            if drift_bps > 3500.0:
                self._note_snapshot_bypass("price_symbol_mismatch")
                return self._trader.fetch_cycle_bundle(symbol, timeframe, limit)
        out = {
            "ohlcv": ohlcv,
            "position": position,
            "price": px,
            "profile": profile,
        }
        return out

    def last_price(self, symbol: str, ttl_sec: float | None = None, force_refresh: bool = False) -> float:
        if force_refresh or ttl_sec == 0.0:
            snap = self._snapshot(symbol, timeframe="1m", limit=2)
            if snap:
                px = float(snap.get("price", 0.0) or 0.0)
                if px > 0:
                    return px
        return float(self._trader.last_price(symbol, ttl_sec=ttl_sec, force_refresh=force_refresh))

    def backend_status(self) -> dict[str, object]:
        self._maybe_rearm_live_strict()
        now = time.time()
        healthy = (now - self._last_health_ok_ts) <= 8.0 if self._last_health_ok_ts > 0 else False
        if not self.execution_gateway:
            phase = "phase1_shadow_readonly"
        elif self.execution_gateway and not self.strict_mode:
            phase = "phase2_gateway_soft"
        else:
            phase = "phase3_gateway_strict"
        ready_for_live_strict = bool(
            healthy
            and (not self._exec_degraded_active())
            and int(self._health_fail_count) == 0
            and int(self._fallback_order_count) == 0
        )
        snapshot_fallback_5m = self._prune_recent(self._snapshot_fallback_ts, 300.0)
        snapshot_bypass_5m = self._prune_recent(self._snapshot_bypass_ts, 300.0)
        health_fail_5m = self._prune_recent(self._health_fail_ts, 300.0)
        order_fail_5m = self._prune_recent(self._order_fail_ts, 300.0)
        return {
            "name": self.backend_name,
            "healthy": healthy,
            "mode": "shadow",
            "rollout_phase": phase,
            "ready_for_live_strict": ready_for_live_strict,
            "recommended_live_mode": "strict" if ready_for_live_strict else "soft",
            "url": self.rust_url or "",
            "exec_gateway": bool(self.execution_gateway),
            "health_latency_ms": round(self._health_latency_ms, 1),
            "snapshot_latency_ms": round(self._snapshot_latency_ms, 1),
            "snapshot_timeout_sec": round(self._snapshot_timeout_sec(), 3),
            "order_latency_ms": round(self._order_latency_ms, 1),
            "fallback_snapshot_count": int(self._fallback_snapshot_count),
            "snapshot_bypass_count": int(self._snapshot_bypass_count),
            "snapshot_fallback_5m": int(snapshot_fallback_5m),
            "snapshot_bypass_5m": int(snapshot_bypass_5m),
            "snapshot_fallback_reasons": dict(self._snapshot_fallback_reasons),
            "snapshot_bypass_reasons": dict(self._snapshot_bypass_reasons),
            "fallback_order_count": int(self._fallback_order_count),
            "order_fail_5m": int(order_fail_5m),
            "health_fail_count": int(self._health_fail_count),
            "health_fail_5m": int(health_fail_5m),
            "strict_mode": bool(self.strict_mode),
            "auto_strict_fallback_in_live": bool(self.auto_strict_fallback_in_live),
            "live_strict_rearm_sec": float(self.live_strict_rearm_sec),
            "live_strict_fallback_triggered": bool(self._live_strict_fallback_triggered),
            "exec_degraded": bool(self._exec_degraded_active()),
            "exec_degraded_left_sec": round(max(0.0, self._exec_degraded_until_ts - now), 1),
            "health_series": [round(v, 2) for v in self._health_series],
            "snapshot_latency_series": [round(v, 2) for v in self._snapshot_latency_series],
            "order_latency_series": [round(v, 2) for v in self._order_latency_series],
        }

    def place_spot_buy(self, symbol: str, usdt_cost: float, *, client_order_id: str | None = None) -> Any:
        req = {
            "market": "spot",
            "action": "buy",
            "symbol": symbol,
            "quote_usdt": float(usdt_cost),
            "client_order_id": client_order_id or "",
        }
        order = self._gateway_place_order(req, op_name="place_spot_buy")
        if order is not None:
            return order
        return self._trader.buy_market(symbol, usdt_cost)

    def place_spot_sell(self, symbol: str, base_amount: float, *, client_order_id: str | None = None) -> Any:
        req = {
            "market": "spot",
            "action": "sell",
            "symbol": symbol,
            "base_amount": float(base_amount),
            "client_order_id": client_order_id or "",
        }
        order = self._gateway_place_order(req, op_name="place_spot_sell")
        if order is not None:
            return order
        return self._trader.sell_market(symbol, base_amount)

    def place_futures_open(
        self,
        symbol: str,
        side: str,
        usdt_margin: float,
        *,
        client_order_id: str | None = None,
    ) -> Any:
        req = {
            "market": "futures",
            "action": "open",
            "symbol": symbol,
            "side": side,
            "quote_usdt": float(usdt_margin),
            "client_order_id": client_order_id or "",
        }
        order = self._gateway_place_order(req, op_name="place_futures_open")
        if order is not None:
            return order
        return self._trader.futures_open_market(symbol, side, usdt_margin)

    def place_futures_close(
        self,
        symbol: str,
        side: str,
        contracts: float,
        *,
        client_order_id: str | None = None,
    ) -> Any:
        req = {
            "market": "futures",
            "action": "close",
            "symbol": symbol,
            "side": side,
            "base_amount": float(contracts),
            "client_order_id": client_order_id or "",
        }
        order = self._gateway_place_order(req, op_name="place_futures_close")
        if order is not None:
            return order
        return self._trader.futures_close_market(symbol, side, contracts)

    def confirm_buy_position(self, symbol: str, pre_base_free: float) -> tuple[bool, float] | None:
        req = {
            "market": "spot",
            "action": "buy",
            "symbol": symbol,
            "pre_base_free": float(pre_base_free),
        }
        confirmed = self._gateway_confirm_order(req, op_name="confirm_buy_position")
        return confirmed

    def confirm_sell_position(self, symbol: str, pre_base_free: float) -> tuple[bool, float] | None:
        req = {
            "market": "spot",
            "action": "sell",
            "symbol": symbol,
            "pre_base_free": float(pre_base_free),
        }
        confirmed = self._gateway_confirm_order(req, op_name="confirm_sell_position")
        return confirmed


def build_engine_adapter(config: Any, trader: Any) -> BaseEngineAdapter:
    mode = str(getattr(config, "ENGINE_BACKEND", "python") or "python").strip().lower()
    rust_url = str(getattr(config, "RUST_ENGINE_URL", "") or "").strip()
    strict = bool(getattr(config, "ENGINE_STRICT", False))

    if mode == "python":
        logging.info("Engine backend: python")
        return PythonEngineAdapter(trader)

    if mode == "rust":
        if strict and not rust_url:
            raise RuntimeError("ENGINE_BACKEND=rust requires RUST_ENGINE_URL in strict mode.")
        logging.info("Engine backend: rust-shadow (phase1)")
        return RustEngineAdapter(
            trader,
            rust_url=rust_url,
            http_timeout_sec=float(getattr(config, "ENGINE_HTTP_TIMEOUT_SEC", 0.45)),
            execution_gateway=bool(getattr(config, "ENGINE_EXECUTION_GATEWAY", False)),
            order_timeout_sec=float(getattr(config, "ENGINE_ORDER_TIMEOUT_SEC", 0.70)),
            strict_mode=bool(getattr(config, "ENGINE_STRICT", False)),
            snapshot_max_age_sec=float(getattr(config, "ENGINE_SNAPSHOT_MAX_AGE_SEC", 20.0)),
            bot_mode=str(getattr(config, "BOT_MODE", "") or ""),
            auto_strict_fallback_in_live=bool(getattr(config, "AUTO_STRICT_FALLBACK_IN_LIVE", True)),
            live_strict_rearm_sec=float(getattr(config, "LIVE_STRICT_REARM_SEC", 90.0)),
        )

    if mode == "auto":
        if rust_url:
            logging.info("Engine backend: auto -> rust-shadow (phase1)")
            return RustEngineAdapter(
                trader,
                rust_url=rust_url,
                http_timeout_sec=float(getattr(config, "ENGINE_HTTP_TIMEOUT_SEC", 0.45)),
                execution_gateway=bool(getattr(config, "ENGINE_EXECUTION_GATEWAY", False)),
                order_timeout_sec=float(getattr(config, "ENGINE_ORDER_TIMEOUT_SEC", 0.70)),
                strict_mode=bool(getattr(config, "ENGINE_STRICT", False)),
                snapshot_max_age_sec=float(getattr(config, "ENGINE_SNAPSHOT_MAX_AGE_SEC", 20.0)),
                bot_mode=str(getattr(config, "BOT_MODE", "") or ""),
                auto_strict_fallback_in_live=bool(getattr(config, "AUTO_STRICT_FALLBACK_IN_LIVE", True)),
                live_strict_rearm_sec=float(getattr(config, "LIVE_STRICT_REARM_SEC", 90.0)),
            )
        logging.info("Engine backend: auto -> python")
        return PythonEngineAdapter(trader)

    logging.warning("Unknown ENGINE_BACKEND=%s, fallback to python.", mode)
    return PythonEngineAdapter(trader)
