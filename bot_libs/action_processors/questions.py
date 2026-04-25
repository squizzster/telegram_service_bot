from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from telegram import Bot
from telegram.error import TelegramError

from bot_libs.action_models import ACTION_ANSWER_QUESTION
from bot_libs.action_processing_context import ActionProcessingContext
from bot_libs.queue_processor_errors import (
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
)
from bot_libs.question_answering_openai import (
    QuestionAnsweringResult,
    answer_question_with_openai,
)
from bot_libs.stage_names import STAGE_ANSWERING_QUESTION, STAGE_SENDING_ACTION_RESPONSE
from bot_libs.telegram_error_policy import job_error_from_telegram_error
from bot_libs.telegram_message_utils import split_telegram_text

log = logging.getLogger(__name__)

QuestionAnswerProvider = Callable[..., Awaitable[QuestionAnsweringResult]]


async def process_answer_question(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
    provider: QuestionAnswerProvider = answer_question_with_openai,
) -> dict[str, object]:
    question = _question_text(row)
    outbound_state = _row_outbound_state(row)
    answer_state = _outbound_kind_state(outbound_state, "question_answer")
    answer_text = _stored_answer_text(answer_state)
    provider_name: str | None = _stored_text(answer_state, "provider")
    prompt_id: str | None = _stored_text(answer_state, "prompt_id")

    if answer_text is None:
        await _set_stage(context, STAGE_ANSWERING_QUESTION)
        answer_result = await provider(question_to_answer=question)
        answer_text = answer_result.answer_text
        provider_name = answer_result.provider
        prompt_id = answer_result.prompt_id
        answer_state = {
            **answer_state,
            "answer_text": answer_text,
            "provider": answer_result.provider,
            "prompt_id": answer_result.prompt_id,
            "raw_response": answer_result.raw_response,
            "messages": answer_state.get("messages", []),
        }
        outbound_state["question_answer"] = answer_state
        await _set_outbound_json(context, outbound_state, STAGE_ANSWERING_QUESTION)

    await _set_stage(context, STAGE_SENDING_ACTION_RESPONSE)
    message_ids = await _send_answer_messages(
        bot,
        row,
        answer_text=answer_text,
        outbound_state=outbound_state,
        answer_state=answer_state,
        context=context,
    )

    return {
        "outcome": "processed",
        "processor": "question_answer",
        "action_code": ACTION_ANSWER_QUESTION,
        "provider": provider_name,
        "prompt_id": prompt_id,
        "answer_text": answer_text,
        "message_ids": message_ids,
    }


async def _send_answer_messages(
    bot: Bot,
    row: Mapping[str, object],
    *,
    answer_text: str,
    outbound_state: dict[str, object],
    answer_state: dict[str, object],
    context: ActionProcessingContext | None,
) -> list[int]:
    chat_id = row.get("chat_id")
    if chat_id is None:
        raise RetryableJobError("chat_id missing for question answer send")

    persisted_message_ids = _persisted_message_ids(answer_state)
    message_thread_id = row.get("message_thread_id")
    source_message_id = row.get("message_id")
    answer_chunks = split_telegram_text(answer_text)
    sent_message_ids: list[int] = []

    for index, answer_chunk in enumerate(answer_chunks):
        persisted_message_id = persisted_message_ids.get(index)
        if persisted_message_id is not None:
            sent_message_ids.append(persisted_message_id)
            continue

        send_kwargs: dict[str, object] = {
            "chat_id": chat_id,
            "text": answer_chunk,
        }
        if message_thread_id is not None:
            send_kwargs["message_thread_id"] = int(message_thread_id)
        if index == 0 and source_message_id is not None:
            send_kwargs["reply_to_message_id"] = int(source_message_id)
            send_kwargs["allow_sending_without_reply"] = False

        try:
            sent_message = await bot.send_message(**send_kwargs)
        except TelegramError as exc:
            raise job_error_from_telegram_error(
                exc,
                action="send_question_answer",
            ) from exc

        await _verify_reply_target(
            bot,
            sent_message,
            expected_message_id=source_message_id if index == 0 else None,
            chat_id=chat_id,
        )
        sent_message_id = int(sent_message.message_id)
        updated_answer_state = _record_outbound_message(
            answer_state,
            index=index,
            message_id=sent_message_id,
        )
        outbound_state["question_answer"] = updated_answer_state
        try:
            await _set_outbound_json(
                context,
                outbound_state,
                STAGE_SENDING_ACTION_RESPONSE,
            )
        except BaseException:
            await _delete_message_best_effort(
                bot,
                chat_id=chat_id,
                message_id=sent_message_id,
            )
            raise

        answer_state = updated_answer_state
        sent_message_ids.append(sent_message_id)

    return sent_message_ids


def _question_text(row: Mapping[str, object]) -> str:
    value = row.get("processing_text")
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise PermanentJobError("question action requires processing_text")


def _row_outbound_state(row: Mapping[str, object]) -> dict[str, object]:
    value = row.get("outbound_json")
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        log.warning("action_job=%s outbound_json_invalid ignored=true", row.get("id"))
        return {}
    if not isinstance(decoded, dict):
        log.warning("action_job=%s outbound_json_not_object ignored=true", row.get("id"))
        return {}
    return decoded


def _outbound_kind_state(
    outbound_state: Mapping[str, object],
    key: str,
) -> dict[str, object]:
    value = outbound_state.get(key)
    if isinstance(value, dict):
        return dict(value)
    return {}


def _stored_answer_text(answer_state: Mapping[str, object]) -> str | None:
    return _stored_text(answer_state, "answer_text")


def _stored_text(answer_state: Mapping[str, object], key: str) -> str | None:
    value = answer_state.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _persisted_message_ids(kind_state: Mapping[str, object]) -> dict[int, int]:
    messages = kind_state.get("messages")
    if not isinstance(messages, list):
        return {}

    persisted: dict[int, int] = {}
    for message in messages:
        if not isinstance(message, dict):
            continue
        index = _coerce_int(message.get("index"))
        message_id = _coerce_int(message.get("message_id"))
        if index is not None and message_id is not None:
            persisted[index] = message_id
    return persisted


def _record_outbound_message(
    kind_state: Mapping[str, object],
    *,
    index: int,
    message_id: int,
) -> dict[str, object]:
    updated = dict(kind_state)
    existing_messages = updated.get("messages")
    messages = list(existing_messages) if isinstance(existing_messages, list) else []
    messages = [
        item
        for item in messages
        if not (isinstance(item, dict) and _coerce_int(item.get("index")) == index)
    ]
    messages.append({"index": index, "message_id": message_id})
    messages.sort(key=lambda item: int(item["index"]))
    updated["messages"] = messages
    return updated


async def _verify_reply_target(
    bot: Bot,
    sent_message: object,
    *,
    expected_message_id: object | None,
    chat_id: object,
) -> None:
    if expected_message_id is None:
        return

    reply_to_message = getattr(sent_message, "reply_to_message", None)
    actual_message_id = getattr(reply_to_message, "message_id", None)
    if actual_message_id == int(expected_message_id):
        return

    sent_message_id = getattr(sent_message, "message_id", None)
    if sent_message_id is not None:
        await _delete_message_best_effort(
            bot,
            chat_id=chat_id,
            message_id=int(sent_message_id),
        )

    raise OriginalMessageUnavailableError(
        "original message unavailable during send_question_answer: "
        "reply target no longer attached"
    )


async def _delete_message_best_effort(
    bot: Bot,
    *,
    chat_id: object,
    message_id: int,
) -> None:
    delete_message = getattr(bot, "delete_message", None)
    if delete_message is None:
        return
    try:
        await delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramError as exc:
        log.debug(
            "failed to delete untracked question answer chat_id=%s message_id=%s error=%s",
            chat_id,
            message_id,
            exc,
        )


async def _set_stage(context: ActionProcessingContext | None, stage: str) -> None:
    if context is not None:
        await context.set_stage(stage)


async def _set_outbound_json(
    context: ActionProcessingContext | None,
    outbound_state: dict[str, object],
    stage: str,
) -> None:
    if context is not None:
        await context.set_outbound_json(outbound_state, stage)


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return None
