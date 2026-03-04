from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiohttp import web


BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "logs" / "runtime"
LIVE_PATH = RUNTIME_DIR / "bot_live_status.json"
LOCK_PATH = RUNTIME_DIR / "rust_engine_service.lock"
LOG_PATH = RUNTIME_DIR / "rust_engine_service.log"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: object, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _safe_int(v: object, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


class SingleInstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.acquired = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                old = json.loads(self.path.read_text(encoding="utf-8-sig"))
                old_pid = _safe_int(old.get("pid"), 0) if isinstance(old, dict) else 0
                if old_pid > 0 and _pid_alive(old_pid):
                    raise RuntimeError(f"rust_engine_service already running (pid={old_pid})")
            except RuntimeError:
                raise
            except Exception:
                pass
        payload = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "ts_utc": _iso_now(),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.acquired = True

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            if self.path.exists():
                self.path.unlink()
        except Exception:
            pass
        self.acquired = False


@dataclass
class EngineStats:
    started_ts: float = field(default_factory=time.time)
    health_count: int = 0
    snapshot_count: int = 0
    order_place_count: int = 0
    order_confirm_count: int = 0
    errors_count: int = 0
    last_error: str = ""
    snapshot_latency_ms: float = 0.0
    order_latency_ms: float = 0.0

    def uptime_sec(self) -> float:
        return max(0.0, time.time() - self.started_ts)


class RustEngineService:
    def __init__(self) -> None:
        self.host = os.getenv("RUST_ENGINE_HOST", "127.0.0.1").strip() or "127.0.0.1"
        self.port = _safe_int(os.getenv("RUST_ENGINE_PORT", "17890"), 17890)
        self.enable_exchange = str(os.getenv("RUST_ENGINE_ENABLE_EXCHANGE", "false")).strip().lower() in {"1", "true", "yes", "on"}
        self.snapshot_limit_max = _safe_int(os.getenv("RUST_ENGINE_SNAPSHOT_MAX", "200"), 200)
        self.app = web.Application()
        self.stats = EngineStats()
        self.lock = SingleInstanceLock(LOCK_PATH)
        self._wire_routes()

    def _wire_routes(self) -> None:
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/snapshot", self.handle_snapshot)
        self.app.router.add_post("/order/place", self.handle_order_place)
        self.app.router.add_post("/order/confirm", self.handle_order_confirm)

    def _read_live(self) -> dict[str, Any]:
        try:
            if not LIVE_PATH.exists():
                return {}
            raw = LIVE_PATH.read_text(encoding="utf-8-sig", errors="replace")
            data = json.loads(raw) if raw.strip() else {}
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            self.stats.errors_count += 1
            self.stats.last_error = f"live_read: {exc}"
            return {}

    @staticmethod
    def _ok(payload: dict[str, Any]) -> web.Response:
        return web.json_response(payload, status=200, dumps=lambda d: json.dumps(d, ensure_ascii=False))

    @staticmethod
    def _bad(msg: str, status: int = 400) -> web.Response:
        return web.json_response({"ok": False, "error": msg}, status=status, dumps=lambda d: json.dumps(d, ensure_ascii=False))

    async def handle_health(self, request: web.Request) -> web.Response:
        self.stats.health_count += 1
        payload = {
            "ok": True,
            "engine": "rust-market-core-python-service",
            "mode": "exchange" if self.enable_exchange else "shadow",
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "ts_utc": _iso_now(),
            "uptime_sec": round(self.stats.uptime_sec(), 2),
            "stats": {
                "health_count": self.stats.health_count,
                "snapshot_count": self.stats.snapshot_count,
                "order_place_count": self.stats.order_place_count,
                "order_confirm_count": self.stats.order_confirm_count,
                "errors_count": self.stats.errors_count,
                "last_error": self.stats.last_error,
                "snapshot_latency_ms": round(self.stats.snapshot_latency_ms, 2),
                "order_latency_ms": round(self.stats.order_latency_ms, 2),
            },
        }
        return self._ok(payload)

    async def handle_snapshot(self, request: web.Request) -> web.Response:
        t0 = time.perf_counter()
        self.stats.snapshot_count += 1
        q = request.rel_url.query
        symbol = str(q.get("symbol", "") or "")
        timeframe = str(q.get("timeframe", "1m") or "1m")
        limit = min(self.snapshot_limit_max, max(5, _safe_int(q.get("limit", "80"), 80)))

        live = self._read_live()
        chart = live.get("chart", {}) if isinstance(live.get("chart"), dict) else {}
        candles = chart.get("candles", []) if isinstance(chart.get("candles"), list) else []
        candles = candles[-limit:]
        ohlcv: list[list[float]] = []
        for c in candles:
            if not isinstance(c, dict):
                continue
            ts = _safe_int(c.get("ts"), 0)
            o = _safe_float(c.get("o"), 0.0)
            h = _safe_float(c.get("h"), 0.0)
            l = _safe_float(c.get("l"), 0.0)
            cl = _safe_float(c.get("c"), 0.0)
            v = _safe_float(c.get("v"), 0.0)
            if cl <= 0:
                continue
            ohlcv.append([ts, o, h, l, cl, v])

        pos = {
            "usdt_free": _safe_float(live.get("usdt_free"), 0.0),
            "base_free": _safe_float(live.get("base_free"), 0.0),
            "base_currency": str(live.get("base_currency", "")),
            "position_side": str(live.get("position_side", "")),
            "position_qty": _safe_float(live.get("position_qty"), 0.0),
            "raw": {},
        }
        profile = {
            "market_fetch_ms": _safe_float(live.get("market_fetch_ms"), 0.0),
            "price_ms": _safe_float(live.get("price_ms"), 0.0),
            "position_ms": _safe_float(live.get("position_ms"), 0.0),
        }
        payload = {
            "ok": True,
            "symbol": symbol or str(live.get("symbol", "SOL/USDT")),
            "source_symbol": str(live.get("symbol", "")),
            "timeframe": timeframe,
            "price": _safe_float(live.get("price"), 0.0),
            "ohlcv": ohlcv,
            "position": pos,
            "profile": profile,
            "market_data_stale": bool(live.get("market_data_stale", False)),
            "status_write_ts_utc": str(live.get("status_write_ts_utc", "") or live.get("ts_utc", "")),
            "ts_utc": _iso_now(),
        }
        self.stats.snapshot_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
        return self._ok(payload)

    async def handle_order_place(self, request: web.Request) -> web.Response:
        t0 = time.perf_counter()
        self.stats.order_place_count += 1
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            return self._bad("invalid_payload")

        # Stage: safe shadow order response (no real exchange execution here).
        live = self._read_live()
        px = _safe_float(live.get("price"), 0.0)
        if px <= 0:
            px = 1.0
        market = str(payload.get("market", "spot"))
        action = str(payload.get("action", "buy"))
        symbol = str(payload.get("symbol", "SOL/USDT"))
        quote = _safe_float(payload.get("quote_usdt"), 0.0)
        base = _safe_float(payload.get("base_amount"), 0.0)
        if base <= 0 and quote > 0:
            base = quote / px
        cost = quote if quote > 0 else base * px
        out = {
            "ok": True,
            "engine": "rust-market-core-python-service",
            "mode": "exchange" if self.enable_exchange else "shadow",
            "order": {
                "id": f"svc_{market}_{action}_{int(time.time() * 1000)}",
                "status": "closed",
                "average": px,
                "price": px,
                "filled": base,
                "cost": cost,
                "symbol": symbol,
                "side": action,
            },
            "ts_utc": _iso_now(),
        }
        self.stats.order_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
        return self._ok(out)

    async def handle_order_confirm(self, request: web.Request) -> web.Response:
        t0 = time.perf_counter()
        self.stats.order_confirm_count += 1
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            return self._bad("invalid_payload")

        action = str(payload.get("action", "buy")).lower().strip()
        symbol = str(payload.get("symbol", "SOL/USDT"))
        pre_base = _safe_float(payload.get("pre_base_free"), 0.0)
        live = self._read_live()
        base_free = _safe_float(live.get("base_free"), pre_base)
        confirmed = False
        if action == "buy":
            confirmed = base_free > pre_base
        elif action == "sell":
            confirmed = base_free < (pre_base * 0.95) or base_free <= 1e-10

        out = {
            "ok": True,
            "engine": "rust-market-core-python-service",
            "symbol": symbol,
            "confirmed": bool(confirmed),
            "base_free": base_free,
            "ts_utc": _iso_now(),
        }
        self.stats.order_latency_ms = max(0.0, (time.perf_counter() - t0) * 1000.0)
        return self._ok(out)

    async def run(self) -> int:
        self.lock.acquire()
        runner = web.AppRunner(self.app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logging.info("rust_engine_service started on http://%s:%s", self.host, self.port)
        stop_event = asyncio.Event()

        def _stop() -> None:
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                # Windows fallback (CTRL+C still handled).
                pass

        try:
            await stop_event.wait()
        finally:
            await runner.cleanup()
            self.lock.release()
        return 0


def _configure_logging() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def main() -> int:
    _configure_logging()
    svc = RustEngineService()
    try:
        return asyncio.run(svc.run())
    except RuntimeError as exc:
        logging.error("Service startup failed: %s", exc)
        return 1
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
