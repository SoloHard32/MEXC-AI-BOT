from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def _safe_float(v: object, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _norm_action(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"BUY", "LONG"}:
        return "LONG"
    if s in {"SELL", "SHORT"}:
        return "SHORT"
    return "HOLD"


class AdvisoryMockHandler(BaseHTTPRequestHandler):
    server_version = "AdvisoryMock/1.0"

    def _json(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/health":
            self._json(
                200,
                {
                    "ok": True,
                    "service": "advisory_mock",
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                },
            )
            return
        if path != "/advisory":
            self._json(404, {"ok": False, "error": "not_found"})
            return

        q = parse_qs(parsed.query or "")
        symbol = str((q.get("symbol") or ["SOL/USDT"])[0] or "SOL/USDT").upper()
        timeframe = str((q.get("timeframe") or ["1m"])[0] or "1m").lower()
        mode = str((q.get("mode") or ["training"])[0] or "training").lower()
        regime = str((q.get("regime") or ["flat"])[0] or "flat").lower()

        # Deterministic lightweight synthetic advisory signal:
        # depends on time bucket + symbol/regime, without random module.
        minute_bucket = int(time.time() // 60)
        phase_seed = (sum(ord(c) for c in symbol) + len(timeframe) * 7 + len(regime) * 5 + minute_bucket) % 360
        phase = math.radians(float(phase_seed))
        wave = math.sin(phase) * 0.5 + math.cos(phase * 0.35) * 0.5

        base_conf = 0.48 + (wave * 0.24)
        base_quality = 0.50 + (wave * 0.20)
        conf = max(0.05, min(0.95, _safe_float(base_conf, 0.5)))
        quality = max(0.05, min(0.95, _safe_float(base_quality, 0.5)))

        if regime in {"trend", "impulse"}:
            edge_bias_pct = 0.0035 + max(-0.0015, min(0.0030, wave * 0.0035))
        elif regime in {"flat", "range"}:
            edge_bias_pct = max(-0.0020, min(0.0015, wave * 0.0025))
        else:
            edge_bias_pct = max(-0.0020, min(0.0020, wave * 0.0020))
        if mode == "training":
            edge_bias_pct *= 0.8

        action = "HOLD"
        if conf >= 0.62 and quality >= 0.58:
            action = "LONG" if wave >= 0 else "SHORT"
        elif conf < 0.52:
            action = "HOLD"
        action = _norm_action(action)

        payload = {
            "ok": True,
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "signal": {
                "action": action,
                "confidence": round(conf, 4),
                "quality": round(quality, 4),
                "edge_bias_pct": round(edge_bias_pct, 5),
                "reason": f"mock|mode={mode}|regime={regime}|wave={wave:.3f}",
            },
        }
        self._json(200, payload)

    def log_message(self, fmt: str, *args) -> None:  # silence default noisy log
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Local advisory mock API for bot integration tests.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), AdvisoryMockHandler)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

