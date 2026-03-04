from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from live_reason_mapper import prioritize_buy_blocks


@dataclass
class EntryPolicyResult:
    can_enter: bool
    reasons: list[str]
    primary_reason: str


class EntryPolicy:
    """Non-invasive decision layer over existing core signals."""

    def evaluate(
        self,
        *,
        ai_enabled: bool,
        ai_quality_ok: bool,
        edge_override_buy: bool,
        ai_conf_ok: bool,
        mtf_ok: bool,
        mtf_soft_bypass: bool,
        is_short_spot_block: bool,
        market_noise_high: bool,
        market_data_stale: bool,
        expected_edge_ok: bool,
        short_model_weak_block: bool,
        anomaly_risk_high: bool,
        post_time_stop_block: bool,
        signal_debounce_pass: bool,
        symbol_internal_cooldown_block: bool,
        buy_blocked_by_guard: bool,
        has_open_position: bool,
        buy_intent: bool,
    ) -> EntryPolicyResult:
        reasons: list[str] = []
        if ai_enabled and (not ai_quality_ok) and (not edge_override_buy):
            reasons.append("ai_quality_low")
        if ai_enabled and (not ai_conf_ok) and (not edge_override_buy):
            reasons.append("ai_conf_low")
        if ai_enabled and (not mtf_ok) and (not mtf_soft_bypass):
            reasons.append("mtf_not_confirmed")
        if is_short_spot_block:
            reasons.append("short_signal_spot_only")
        if market_noise_high:
            reasons.append("market_noise_high")
        if market_data_stale:
            reasons.append("market_data_stale")
        if not expected_edge_ok:
            reasons.append("expected_edge_too_low")
        if short_model_weak_block:
            reasons.append("short_model_weak")
        if anomaly_risk_high:
            reasons.append("anomaly_risk_high")
        if post_time_stop_block:
            reasons.append("post_time_stop_block")
        if not signal_debounce_pass:
            reasons.append("signal_debounce")
        if symbol_internal_cooldown_block:
            reasons.append("symbol_internal_cooldown")
        if buy_blocked_by_guard:
            reasons.append("risk_guard_blocked")

        reasons = prioritize_buy_blocks(reasons)
        if len(reasons) > 3:
            reasons = reasons[:3]
        primary = reasons[0] if reasons else ""

        can_enter = bool(
            buy_intent
            and (not has_open_position)
            and (not buy_blocked_by_guard)
            and expected_edge_ok
            and (not anomaly_risk_high)
            and (not post_time_stop_block)
            and (not symbol_internal_cooldown_block)
            and signal_debounce_pass
        )
        return EntryPolicyResult(can_enter=can_enter, reasons=reasons, primary_reason=primary)
