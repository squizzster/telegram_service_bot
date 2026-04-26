from __future__ import annotations

import unittest
from types import SimpleNamespace

from bot_libs.action_dispatch import DispatchingActionProcessor
from bot_libs.action_models import (
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_NONE,
    ACTION_SET_REMINDER,
)
from bot_libs.queue_processor_errors import PermanentJobError


class ActionDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_temporary_real_actions_succeed_without_random_retry_gate(self) -> None:
        processor = DispatchingActionProcessor()
        bot = SimpleNamespace()

        for action_code in (
            ACTION_LOG_EXPENSES,
            ACTION_LOG_INCOME,
            ACTION_SET_REMINDER,
        ):
            with self.subTest(action_code=action_code):
                result = await processor.process(
                    bot,
                    {"id": 1, "action_code": action_code},
                )
                self.assertEqual(result["outcome"], "processed")
                self.assertEqual(result["action_code"], action_code)
                self.assertNotIn("temporary_roll", result)
                if action_code in {ACTION_LOG_EXPENSES, ACTION_LOG_INCOME}:
                    self.assertEqual(result["ledger_state"], "pending_calculation")
                    self.assertIn("Pending next /calculate", result["ledger_status_text"])

    async def test_none_action_is_noop_success_if_it_ever_exists(self) -> None:
        processor = DispatchingActionProcessor()
        bot = SimpleNamespace()

        result = await processor.process(
            bot,
            {"id": 1, "action_code": ACTION_NONE},
        )

        self.assertEqual(
            result,
            {
                "outcome": "processed",
                "processor": "none_noop",
                "action_code": ACTION_NONE,
                "none": True,
            },
        )

    async def test_unknown_action_is_permanent_failure(self) -> None:
        processor = DispatchingActionProcessor()
        bot = SimpleNamespace()

        with self.assertRaises(PermanentJobError):
            await processor.process(bot, {"id": 1, "action_code": "NOPE"})
