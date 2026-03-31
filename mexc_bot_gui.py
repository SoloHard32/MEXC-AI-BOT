from __future__ import annotations

import os
import sys
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


def _run_embedded_entrypoint(argv: list[str]) -> int | None:
    if len(argv) < 2:
        return None

    cmd = str(argv[1] or "").strip().lower()
    rest = argv[2:]

    if cmd == "--run-bot":
        from mexc_bot import main as bot_main

        result = bot_main()
        return int(result) if isinstance(result, int) else 0

    if cmd == "--run-engine-service":
        from rust_engine_service import main as engine_main

        sys.argv = [argv[0], *rest]
        result = engine_main()
        return int(result) if isinstance(result, int) else 0

    if cmd == "--run-advisory-mock":
        from advisory_mock_server import main as advisory_main

        sys.argv = [argv[0], *rest]
        result = advisory_main()
        return int(result) if isinstance(result, int) else 0

    return None


def _configure_gui_runtime() -> None:
    chromium_flags = [
        "--disable-gpu",
        "--disable-gpu-compositing",
        "--disable-gpu-vsync",
        "--disable-features=VizDisplayCompositor",
        "--disable-logging",
        "--log-level=3",
    ]
    existing_flags = str(os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "") or "").strip()
    merged_flags = " ".join([existing_flags, *chromium_flags]).strip()
    if merged_flags:
        os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = merged_flags
    os.environ.setdefault("QT_OPENGL", "software")
    os.environ.setdefault("QT_ANGLE_PLATFORM", "software")
    os.environ.setdefault("QT_QUICK_BACKEND", "software")
    os.environ.setdefault("QT_LOGGING_RULES", "*.debug=false;qt.webenginecontext.debug=false")


if __name__ == "__main__":
    embedded_exit = _run_embedded_entrypoint(sys.argv)
    if embedded_exit is not None:
        raise SystemExit(embedded_exit)

    _configure_gui_runtime()
    from mexc_bot_gui_web import main as web_main

    raise SystemExit(web_main())
