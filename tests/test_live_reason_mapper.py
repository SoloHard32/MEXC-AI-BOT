import unittest

from live_reason_mapper import build_human_reason


class LiveReasonMapperTests(unittest.TestCase):
    def test_guard_reason_has_priority(self) -> None:
        payload = {
            "event": "no_trade_signal",
            "guard_block_reason": "api_unstable_soft_guard",
            "ai": {"action": "LONG"},
        }
        text = build_human_reason(payload)
        self.assertIn("API-ошибок", text)

    def test_entry_block_quality_reason(self) -> None:
        payload = {
            "event": "entry_blocked",
            "buy_block_reasons": ["ai_quality_low"],
            "ai_entry_quality": 0.22,
        }
        text = build_human_reason(payload)
        self.assertIn("Сигнал слабый", text)

    def test_open_position_reason(self) -> None:
        payload = {
            "event": "no_trade_signal",
            "has_open_position": True,
            "sell_reason": "take_profit",
            "ai": {"action": "HOLD"},
        }
        text = build_human_reason(payload)
        self.assertIn("ожидается условие выхода", text)


if __name__ == "__main__":
    unittest.main()
