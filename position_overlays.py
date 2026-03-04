from __future__ import annotations

from dataclasses import dataclass


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


@dataclass
class OverlayDecision:
    risk: dict[str, float]
    partial_exit_fraction: float = 0.0
    partial_reason: str = ""
    breakeven_armed: bool = False


class PositionOverlayEngine:
    """Non-invasive overlays that tune active risk around existing core exits."""

    def apply(
        self,
        active_risk: dict[str, float],
        ai_quality: float,
        momentum_speed: float,
        anomaly_score: float,
        pnl_pct: float,
        has_open_position: bool,
    ) -> OverlayDecision:
        risk = dict(active_risk)
        if not has_open_position:
            return OverlayDecision(risk=risk)

        q = _clamp(ai_quality, 0.0, 1.0)
        mom = _safe_float(momentum_speed, 0.0)
        anom = _clamp(anomaly_score, 0.0, 1.0)
        partial_exit_fraction = 0.0
        partial_reason = ""
        breakeven_armed = False

        # Dynamic stop tightening with better quality, relaxed by anomaly risk.
        tighten_factor = _clamp(1.0 - (0.22 * q) + (0.15 * anom), 0.70, 1.20)
        risk["stop_loss"] = max(0.0002, _safe_float(risk.get("stop_loss"), 0.0) * tighten_factor)

        # Expand TP in strong impulse contexts.
        if mom > 0.0025 and q > 0.60 and anom < 0.65:
            risk["take_profit"] = _safe_float(risk.get("take_profit"), 0.0) * 1.10

        # Partial close when quality degrades after reaching some profit.
        if pnl_pct > 0.004 and q < 0.42:
            partial_exit_fraction = 0.35
            partial_reason = "partial_quality_fade"

        # Breakeven transfer after mini target.
        mini_target = max(0.003, _safe_float(risk.get("take_profit"), 0.0) * 0.33)
        if pnl_pct >= mini_target:
            risk["stop_loss"] = min(_safe_float(risk.get("stop_loss"), 0.0), 0.0003)
            breakeven_armed = True

        # Trailing can be a bit tighter when quality high.
        if q > 0.75:
            risk["trailing_stop"] = _safe_float(risk.get("trailing_stop"), 0.0) * 0.90

        risk["stop_loss"] = _clamp(_safe_float(risk.get("stop_loss"), 0.0), 0.0002, 0.30)
        risk["take_profit"] = _clamp(_safe_float(risk.get("take_profit"), 0.0), 0.0004, 0.50)
        risk["trailing_stop"] = _clamp(_safe_float(risk.get("trailing_stop"), 0.0), 0.0002, 0.30)
        return OverlayDecision(
            risk=risk,
            partial_exit_fraction=partial_exit_fraction,
            partial_reason=partial_reason,
            breakeven_armed=breakeven_armed,
        )

