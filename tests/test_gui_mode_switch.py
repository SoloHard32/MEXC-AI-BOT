import unittest

from mexc_bot_gui import BotGuiApp


class _Var:
    def __init__(self, value: str = "") -> None:
        self._v = str(value)

    def get(self) -> str:
        return self._v

    def set(self, value: str) -> None:
        self._v = str(value)


class _DummyGui:
    def __init__(self) -> None:
        keys = [
            "AUTO_PILOT_MODE",
            "USE_AI_SIGNAL",
            "AI_AUTO_RISK",
            "AI_USE_INTERNET_DATA",
            "POSITION_RECONCILE_ON_START",
            "RISK_GUARD_ENABLED",
            "SPOT_USE_FULL_BALANCE",
            "DRY_RUN",
            "AI_TRAINING_MODE",
        ]
        self.vars = {k: _Var("false") for k in keys}


class GuiModeSwitchTests(unittest.TestCase):
    def test_training_mode_profile(self) -> None:
        g = _DummyGui()
        BotGuiApp._apply_mode_profile(g, "training")
        self.assertEqual(g.vars["DRY_RUN"].get(), "true")
        self.assertEqual(g.vars["AI_TRAINING_MODE"].get(), "true")
        self.assertEqual(g.vars["AUTO_PILOT_MODE"].get(), "true")
        self.assertEqual(g.vars["USE_AI_SIGNAL"].get(), "true")

    def test_live_mode_profile(self) -> None:
        g = _DummyGui()
        BotGuiApp._apply_mode_profile(g, "live")
        self.assertEqual(g.vars["DRY_RUN"].get(), "false")
        self.assertEqual(g.vars["AI_TRAINING_MODE"].get(), "false")
        self.assertEqual(g.vars["AUTO_PILOT_MODE"].get(), "true")
        self.assertEqual(g.vars["USE_AI_SIGNAL"].get(), "true")


if __name__ == "__main__":
    unittest.main()
