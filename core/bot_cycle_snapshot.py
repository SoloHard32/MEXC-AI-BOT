from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CycleSnapshot:
    symbol: str
    price: float
    market_volatility: float
    price_jump_pct: float
    equity_usdt: float
    usdt_free: float
    base_free: float
    has_open_position: bool
    regime: str
    dangerous: bool
    micro_noise: float
    anomaly_score: float
    ai_action: str
    ai_conf: float
    ai_quality: float
    expected_edge_pct: float
    min_expected_edge_pct: float

