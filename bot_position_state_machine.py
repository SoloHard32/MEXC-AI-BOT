from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PositionState:
    state: str = "idle"
    last_event: str = "booting"


class PositionStateMachine:
    """Lightweight position lifecycle tracker for observability."""

    def __init__(self) -> None:
        self._st = PositionState()

    def transition(self, *, has_open_position: bool, event: str) -> PositionState:
        ev = str(event or "").strip()
        if ev in {"buy", "training_buy_signal"}:
            self._st.state = "open"
        elif ev in {"sell", "training_sell_signal", "partial_sell"} and not has_open_position:
            self._st.state = "idle"
        elif has_open_position:
            self._st.state = "open"
        elif ev in {"cooldown"}:
            self._st.state = "cooldown"
        elif ev in {"entry_blocked", "risk_guard_blocked"}:
            self._st.state = "waiting_entry"
        else:
            self._st.state = "idle"
        self._st.last_event = ev or self._st.last_event
        return PositionState(state=self._st.state, last_event=self._st.last_event)

