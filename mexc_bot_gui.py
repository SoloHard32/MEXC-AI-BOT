from __future__ import annotations

from typing import Any


class BotGuiApp:
    @staticmethod
    def _apply_mode_profile(gui: Any, mode: str) -> None:
        m = str(mode or "").strip().lower()
        if m not in {"training", "live"}:
            return

        common_true = {
            "AUTO_PILOT_MODE": "true",
            "USE_AI_SIGNAL": "true",
            "AI_AUTO_RISK": "true",
            "AI_USE_INTERNET_DATA": "true",
            "POSITION_RECONCILE_ON_START": "true",
            "RISK_GUARD_ENABLED": "true",
            "SPOT_USE_FULL_BALANCE": "true",
        }
        mode_flags = {
            "DRY_RUN": "true" if m == "training" else "false",
            "AI_TRAINING_MODE": "true" if m == "training" else "false",
        }
        values = {**common_true, **mode_flags}

        vars_map = getattr(gui, "vars", {})
        for key, val in values.items():
            v = vars_map.get(key)
            if v is None:
                continue
            setter = getattr(v, "set", None)
            if callable(setter):
                setter(val)


if __name__ == "__main__":
    from mexc_bot_gui_web import main as web_main

    raise SystemExit(web_main())
