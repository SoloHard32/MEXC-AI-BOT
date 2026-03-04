Rust Engine Contract (Phase 2, Read-only)

Purpose
- Let Python bot consume low-latency market snapshot from external engine.
- Keep full fallback to Python trader if Rust endpoint is unavailable.

Environment
- ENGINE_BACKEND=python|auto|rust
- RUST_ENGINE_URL=http://127.0.0.1:17890
- ENGINE_HTTP_TIMEOUT_SEC=0.45
- ENGINE_STRICT=true
- ENGINE_EXECUTION_GATEWAY=false
- ENGINE_ORDER_TIMEOUT_SEC=0.70

Endpoints
1) GET /health
Response example:
{
  "ok": true,
  "engine": "rust-market-core",
  "ts_utc": "2026-03-01T01:00:00Z"
}

2) GET /snapshot?symbol=SOL/USDT&timeframe=1m&limit=80
Response example:
{
  "symbol": "SOL/USDT",
  "timeframe": "1m",
  "price": 84.72,
  "ohlcv": [
    [1709251200000, 84.5, 84.8, 84.4, 84.7, 12000.0]
  ],
  "position": {
    "usdt_free": 2.79,
    "base_free": 0.0,
    "base_currency": "SOL",
    "position_side": "",
    "position_qty": 0.0,
    "raw": {}
  },
  "profile": {
    "market_fetch_ms": 4.2,
    "price_ms": 1.1,
    "position_ms": 0.3
  }
}

Notes
- Python adapter uses this only in shadow/read mode for now.
- On any error/timeout, bot immediately falls back to native Python data path.
- Trading execution is still Python in this phase.

Phase 4 (optional write-path)
3) POST /order/place
Request example:
{
  "market": "spot",
  "action": "buy",
  "symbol": "SOL/USDT",
  "quote_usdt": 2.7,
  "client_order_id": "..."
}
or
{
  "market": "spot",
  "action": "sell",
  "symbol": "SOL/USDT",
  "base_amount": 0.032
}

Response example:
{
  "ok": true,
  "order": {
    "id": "...",
    "status": "closed",
    "average": 84.7,
    "price": 84.7,
    "filled": 0.032,
    "cost": 2.71
  }
}

4) POST /order/confirm
Request example:
{
  "market": "spot",
  "action": "buy",
  "symbol": "SOL/USDT",
  "pre_base_free": 0.0
}
Response example:
{
  "ok": true,
  "confirmed": true,
  "base_free": 0.032
}

Safety
- Recommended staged rollout:
- `ENGINE_EXECUTION_GATEWAY=true`
- `ENGINE_STRICT=true`
- `ENABLE_STRICT_IN_TRAINING=true`
- `ENABLE_STRICT_IN_LIVE=false`
- `AUTO_ENABLE_LIVE_STRICT=true` (GUI promotes strict in live only when backend reports readiness)
- `AUTO_STRICT_FALLBACK_IN_LIVE=true` (if strict live degrades, adapter auto-downgrades to soft without killing cycle)
- With this setup, strict gateway is validated in training only, while live keeps soft fallback behavior.
- If gateway is OFF, adapter always executes orders via Python trader (existing stable path).

Run
- Primary service launcher: `python rust_engine_service.py`
- Legacy alias (still works): `python rust_engine_mock.py`

Current implementation note
- In this repository, the service is a production skeleton implemented in Python (`aiohttp`) to validate contract, failover and telemetry paths before migrating the same contract to native Rust.
