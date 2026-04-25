from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot_libs.queue_processor_errors import RetryableJobError
from bot_libs.action_detection_openai import (
    DEFAULT_OPENAI_ACTION_PROMPT_ID,
    DEFAULT_OPENAI_ACTION_PROMPT_VERSION,
    _detect_actions_sync,
    _extract_actions_text,
    _summarize_response,
    detect_actions_with_openai,
    resolve_openai_action_prompt_id,
    resolve_openai_action_prompt_version,
)


class OpenAIActionDetectionTests(unittest.IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {}, clear=True)
    def test_prompt_defaults(self) -> None:
        self.assertEqual(resolve_openai_action_prompt_id(), DEFAULT_OPENAI_ACTION_PROMPT_ID)
        self.assertEqual(
            resolve_openai_action_prompt_version(),
            DEFAULT_OPENAI_ACTION_PROMPT_VERSION,
        )

    @patch.dict(
        "os.environ",
        {
            "OPENAI_ACTION_DETECTION_PROMPT_ID": "prompt-custom",
            "OPENAI_ACTION_DETECTION_PROMPT_VERSION": "9",
        },
        clear=True,
    )
    def test_prompt_env_overrides(self) -> None:
        self.assertEqual(resolve_openai_action_prompt_id(), "prompt-custom")
        self.assertEqual(resolve_openai_action_prompt_version(), "9")

    @patch("openai.OpenAI")
    def test_detect_actions_sync_uses_responses_prompt_contract(
        self,
        openai_cls: object,
    ) -> None:
        client = openai_cls.return_value
        client.responses.create.return_value = SimpleNamespace(
            output_text='["reporting_expenses"]'
        )

        response = _detect_actions_sync(
            "I spent 30 on petrol",
            "prompt-id",
            "3",
        )

        self.assertEqual(response.output_text, '["reporting_expenses"]')
        openai_cls.assert_called_once_with()
        client.responses.create.assert_called_once_with(
            prompt={
                "id": "prompt-id",
                "version": "3",
                "variables": {
                    "incoming_text": "I spent 30 on petrol",
                },
            }
        )

    @patch("bot_libs.action_detection_openai.asyncio.to_thread")
    async def test_openai_provider_exception_is_retryable(self, to_thread: object) -> None:
        to_thread.side_effect = RuntimeError("network down")

        with self.assertRaisesRegex(RetryableJobError, "network down"):
            await detect_actions_with_openai(incoming_text="I spent 30 on petrol")

    def test_extract_actions_text_accepts_output_text(self) -> None:
        self.assertEqual(_extract_actions_text('["none"]'), '["none"]')

    def test_extract_actions_text_strips_single_json_code_fence(self) -> None:
        for fence_language in ("json", "json_block"):
            with self.subTest(fence_language=fence_language):
                self.assertEqual(
                    _extract_actions_text(
                        f"```{fence_language}\n"
                        '["reporting_expenses"]\n'
                        "```"
                    ),
                    '["reporting_expenses"]',
                )

    def test_response_summary_keeps_serializable_provider_metadata(self) -> None:
        response = SimpleNamespace(
            id="resp_123",
            model="gpt-test",
            status="completed",
            output_text='["none"]',
            usage={"input_tokens": 12, "output_tokens": 3},
        )

        self.assertEqual(
            _summarize_response(
                response,
                response_text='```json\n["none"]\n```',
                raw_actions_text='["none"]',
            ),
            {
                "output_text": '```json\n["none"]\n```',
                "raw_actions_text": '["none"]',
                "id": "resp_123",
                "model": "gpt-test",
                "status": "completed",
                "usage": {"input_tokens": 12, "output_tokens": 3},
            },
        )
