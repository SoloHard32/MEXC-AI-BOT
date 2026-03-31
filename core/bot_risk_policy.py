from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass
class RiskPolicyResult:
    blocked: bool
    reason: str


class RiskPolicy:
    """Thin guard-policy layer around existing guard evaluation."""

    @staticmethod
    def evaluate(reason_provider: Callable[[], str]) -> RiskPolicyResult:
        try:
            reason = str(reason_provider() or "").strip()
        except Exception:
            reason = "guard_eval_error"
        return RiskPolicyResult(blocked=bool(reason), reason=reason)

