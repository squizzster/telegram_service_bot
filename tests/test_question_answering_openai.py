from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot_libs.queue_processor_errors import RetryableJobError
from bot_libs.question_answering_openai import (
    DEFAULT_OPENAI_QUESTION_ANSWERING_PROMPT_ID,
    _answer_question_sync,
    _extract_answer_block,
    _summarize_response,
    answer_question_with_openai,
    resolve_openai_question_answering_prompt_id,
)


class OpenAIQuestionAnsweringTests(unittest.IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {}, clear=True)
    def test_prompt_defaults(self) -> None:
        self.assertEqual(
            resolve_openai_question_answering_prompt_id(),
            DEFAULT_OPENAI_QUESTION_ANSWERING_PROMPT_ID,
        )

    @patch.dict(
        "os.environ",
        {"OPENAI_QUESTION_ANSWERING_PROMPT_ID": "prompt-custom"},
        clear=True,
    )
    def test_prompt_env_override(self) -> None:
        self.assertEqual(resolve_openai_question_answering_prompt_id(), "prompt-custom")

    @patch("openai.OpenAI")
    def test_answer_question_sync_uses_responses_prompt_contract(
        self,
        openai_cls: object,
    ) -> None:
        client = openai_cls.return_value
        client.responses.create.return_value = SimpleNamespace(
            output_text="```answer\n2\n```"
        )

        response = _answer_question_sync("What is 1+1?", "prompt-id")

        self.assertEqual(response.output_text, "```answer\n2\n```")
        openai_cls.assert_called_once_with()
        client.responses.create.assert_called_once_with(
            prompt={
                "id": "prompt-id",
                "variables": {
                    "question_to_answer": "What is 1+1?",
                },
            }
        )

    @patch("bot_libs.question_answering_openai.asyncio.to_thread")
    async def test_openai_provider_exception_is_retryable(self, to_thread: object) -> None:
        to_thread.side_effect = RuntimeError("network down")

        with self.assertRaisesRegex(RetryableJobError, "network down"):
            await answer_question_with_openai(question_to_answer="What is 1+1?")

    def test_extract_answer_block(self) -> None:
        self.assertEqual(
            _extract_answer_block("prefix\n```answer\nThe answer is 2.\n```\nignored"),
            "The answer is 2.",
        )

    def test_extract_answer_block_rejects_missing_or_empty_answer(self) -> None:
        invalid_outputs = (
            "The answer is 2.",
            "```json\n{\"answer\": \"2\"}\n```",
            "```answer\n```",
            "```answer\n2",
        )

        for output in invalid_outputs:
            with self.subTest(output=output):
                with self.assertRaises(RetryableJobError):
                    _extract_answer_block(output)

    def test_response_summary_keeps_serializable_provider_metadata(self) -> None:
        response = SimpleNamespace(
            id="resp_123",
            model="gpt-test",
            status="completed",
            output_text="```answer\n2\n```",
            usage={"input_tokens": 12, "output_tokens": 3},
        )

        self.assertEqual(
            _summarize_response(
                response,
                response_text="```answer\n2\n```",
                answer_text="2",
            ),
            {
                "output_text": "```answer\n2\n```",
                "answer_text": "2",
                "id": "resp_123",
                "model": "gpt-test",
                "status": "completed",
                "usage": {"input_tokens": 12, "output_tokens": 3},
            },
        )
