from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot_libs.action_processors.questions import process_answer_question
from bot_libs.question_answering_openai import QuestionAnsweringResult
from bot_libs.stage_names import STAGE_ANSWERING_QUESTION, STAGE_SENDING_ACTION_RESPONSE


def telegram_sent_message(
    message_id: int,
    *,
    reply_to_message_id: int | None = None,
) -> SimpleNamespace:
    reply_to_message = (
        SimpleNamespace(message_id=reply_to_message_id)
        if reply_to_message_id is not None
        else None
    )
    return SimpleNamespace(message_id=message_id, reply_to_message=reply_to_message)


class FakeQuestionProvider:
    def __init__(self, answer: str = "2") -> None:
        self.answer = answer
        self.calls: list[str] = []

    async def __call__(self, *, question_to_answer: str) -> QuestionAnsweringResult:
        self.calls.append(question_to_answer)
        return QuestionAnsweringResult(
            provider="fake",
            prompt_id="prompt-question",
            answer_text=self.answer,
            raw_response={"answer_text": self.answer},
        )


class FakeActionContext:
    def __init__(self) -> None:
        self.stages: list[str] = []
        self.outbound_calls: list[dict[str, object]] = []

    async def set_stage(self, stage: str) -> None:
        self.stages.append(stage)

    async def set_outbound_json(
        self,
        outbound_json: dict[str, object],
        stage: str | None,
    ) -> None:
        self.outbound_calls.append(
            {
                "outbound_json": json.loads(json.dumps(outbound_json)),
                "stage": stage,
            }
        )


class QuestionActionProcessorTests(unittest.IsolatedAsyncioTestCase):
    def make_row(self, **overrides: object) -> dict[str, object]:
        row: dict[str, object] = {
            "id": 7,
            "queue_id": 42,
            "action_code": "ANSWER_QUESTION",
            "processing_text": "What is 1+1?",
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
            "message_thread_id": 789,
        }
        row.update(overrides)
        return row

    async def test_answer_question_calls_provider_sends_reply_and_persists_outbound(
        self,
    ) -> None:
        provider = FakeQuestionProvider("2")
        context = FakeActionContext()
        bot = SimpleNamespace(
            send_message=AsyncMock(
                return_value=telegram_sent_message(9001, reply_to_message_id=41)
            ),
            delete_message=AsyncMock(),
        )

        result = await process_answer_question(
            bot,
            self.make_row(),
            context=context,
            provider=provider,
        )

        self.assertEqual(provider.calls, ["What is 1+1?"])
        bot.send_message.assert_awaited_once_with(
            chat_id=-1003986727769,
            text="2",
            message_thread_id=789,
            reply_to_message_id=41,
            allow_sending_without_reply=False,
        )
        bot.delete_message.assert_not_awaited()
        self.assertEqual(
            context.stages,
            [STAGE_ANSWERING_QUESTION, STAGE_SENDING_ACTION_RESPONSE],
        )
        self.assertEqual(context.outbound_calls[0]["stage"], STAGE_ANSWERING_QUESTION)
        self.assertEqual(
            context.outbound_calls[-1]["outbound_json"]["question_answer"]["messages"],
            [{"index": 0, "message_id": 9001}],
        )
        self.assertEqual(
            result,
            {
                "outcome": "processed",
                "processor": "question_answer",
                "action_code": "ANSWER_QUESTION",
                "provider": "fake",
                "prompt_id": "prompt-question",
                "answer_text": "2",
                "message_ids": [9001],
            },
        )

    async def test_answer_question_resumes_from_persisted_answer_and_message(self) -> None:
        provider = FakeQuestionProvider("should not be used")
        context = FakeActionContext()
        bot = SimpleNamespace(send_message=AsyncMock(), delete_message=AsyncMock())
        outbound_json = json.dumps(
            {
                "question_answer": {
                    "answer_text": "2",
                    "provider": "fake",
                    "prompt_id": "prompt-question",
                    "messages": [{"index": 0, "message_id": 9001}],
                }
            }
        )

        result = await process_answer_question(
            bot,
            self.make_row(outbound_json=outbound_json),
            context=context,
            provider=provider,
        )

        self.assertEqual(provider.calls, [])
        bot.send_message.assert_not_awaited()
        self.assertEqual(context.stages, [STAGE_SENDING_ACTION_RESPONSE])
        self.assertEqual(result["message_ids"], [9001])

    async def test_answer_question_deletes_sent_message_if_outbound_persist_fails(
        self,
    ) -> None:
        provider = FakeQuestionProvider("2")
        context = FakeActionContext()
        bot = SimpleNamespace(
            send_message=AsyncMock(
                return_value=telegram_sent_message(9001, reply_to_message_id=41)
            ),
            delete_message=AsyncMock(),
        )

        async def fail_on_second_persist(
            outbound_json: dict[str, object],
            stage: str | None,
        ) -> None:
            del outbound_json, stage
            if len(context.outbound_calls) >= 1:
                raise RuntimeError("db write failed")
            context.outbound_calls.append({"outbound_json": {}, "stage": None})

        context.set_outbound_json = fail_on_second_persist

        with self.assertRaisesRegex(RuntimeError, "db write failed"):
            await process_answer_question(
                bot,
                self.make_row(),
                context=context,
                provider=provider,
            )

        bot.delete_message.assert_awaited_once_with(
            chat_id=-1003986727769,
            message_id=9001,
        )
