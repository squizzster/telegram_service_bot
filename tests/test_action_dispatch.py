from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot_libs.action_dispatch import DispatchingActionProcessor
from bot_libs.action_models import (
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_NONE,
    ACTION_SET_REMINDER,
)
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError


class ActionDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_temporary_real_actions_succeed_when_roll_is_three(self) -> None:
        processor = DispatchingActionProcessor()
        bot = SimpleNamespace()

        with patch("bot_libs.action_processors.temporary.random.randint", return_value=3):
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
                    self.assertEqual(result["temporary_roll"], 3)

    async def test_temporary_real_actions_retry_when_roll_is_not_three(self) -> None:
        processor = DispatchingActionProcessor()
        bot = SimpleNamespace()

        with patch("bot_libs.action_processors.temporary.random.randint", return_value=4):
            with self.assertRaises(RetryableJobError) as exc_info:
                await processor.process(
                    bot,
                    {"id": 1, "action_code": ACTION_LOG_EXPENSES},
                )

        self.assertIn("roll=4", str(exc_info.exception))

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
