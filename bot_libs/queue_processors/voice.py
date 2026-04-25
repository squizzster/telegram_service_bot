from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any

from telegram import Bot
from telegram.error import TelegramError

from bot_libs.queue_processing_context import QueueProcessingContext
from bot_libs.queue_processor_errors import (
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
)
from bot_libs.queue_processors.audio_validation import (
    MAX_AUDIO_DURATION_SECONDS,
    MIN_AUDIO_DURATION_SECONDS,
    probe_audio_duration_seconds,
    validate_audio_duration_limit,
)
from bot_libs.queue_processors.file_common import get_telegram_file_and_info
from bot_libs.queue_processors.speech_to_text import transcribe_audio_bytes
from bot_libs.action_models import (
    ACTION_DETECTION_NOT_APPLICABLE,
)
from bot_libs.reaction_policy import (
    REACTION_CALLING_STT_API,
    REACTION_DOWNLOADING_FILE,
    REACTION_FAILURE,
    set_row_reaction_result,
)
from bot_libs.stage_names import (
    STAGE_CALLING_STT_API,
    STAGE_DOWNLOADING_FILE,
    STAGE_SENDING_RESPONSE,
)
from bot_libs.telegram_error_policy import job_error_from_telegram_error

log = logging.getLogger(__name__)

TRANSCRIPT_PREFIX = "Transcript:\n\n"
CONTINUED_TRANSCRIPT_PREFIX = "Transcript (continued):\n\n"
TELEGRAM_MESSAGE_LIMIT = 4096
UNUNDERSTOOD_AUDIO_MESSAGE = (
    "We apologise, however, we couldn't understand that, please re-send."
)
UNINTELLIGIBLE_SPEECH_MESSAGE = "Sorry, I couldn't understand that speech."
TOO_LONG_AUDIO_MESSAGE = (
    "Sorry, we support only AUDIO which is 60 seconds or less, please re-send that..."
)
TOO_SHORT_AUDIO_MESSAGE = (
    "Sorry, we support only AUDIO which is at least 3 seconds, please re-send that..."
)


async def process(
    bot: Bot,
    row: Mapping[str, object],
    payload: Mapping[str, Any],
    context: QueueProcessingContext | None = None,
) -> Mapping[str, object]:
    return await process_audio_like(
        bot,
        row,
        payload,
        context=context,
        processor_name="voice",
    )


async def process_audio_like(
    bot: Bot,
    row: Mapping[str, object],
    payload: Mapping[str, Any],
    context: QueueProcessingContext | None = None,
    *,
    processor_name: str,
) -> Mapping[str, object]:
    stored_transcript = _row_processing_text(row)
    sending_stage_already_set = False
    if stored_transcript:
        file_info = _row_file_info(row)
        transcript = stored_transcript
        transcript_result = {
            "provider": "stored",
            "model_id": "stored",
            "transcript": stored_transcript,
        }
        probed_duration_seconds = None
    else:
        transcript_result: Mapping[str, str]
        await _set_stage(context, STAGE_DOWNLOADING_FILE)
        reaction_result = await set_row_reaction_result(
            bot,
            row,
            REACTION_DOWNLOADING_FILE,
            reason="downloading_voice_file",
        )
        _raise_if_original_message_unavailable(
            reaction_result,
            stage=STAGE_DOWNLOADING_FILE,
        )

        tg_file, file_info = await get_telegram_file_and_info(bot, row)

        try:
            audio_bytes = bytes(await tg_file.download_as_bytearray())
        except TelegramError as exc:
            raise job_error_from_telegram_error(exc, action="download_file") from exc

        try:
            probed_duration_seconds = probe_audio_duration_seconds(
                audio_bytes,
                suffix=_suffix_for_mime_type(_row_mime_type(row)),
            )
            validate_audio_duration_limit(probed_duration_seconds)
        except PermanentJobError as exc:
            message = _audio_validation_failure_message(str(exc))
            await _reject_audio_permanently(
                bot,
                row,
                message,
                context=context,
                reason=str(exc),
            )

        try:
            async with _calling_stt_activity(bot, row, context):
                transcript_result = await transcribe_audio_bytes(
                    audio_bytes=audio_bytes,
                    mime_type=_row_mime_type(row),
                    duration_seconds=probed_duration_seconds,
                )
        except OriginalMessageUnavailableError:
            raise
        except PermanentJobError as exc:
            await _reject_audio_permanently(
                bot,
                row,
                UNINTELLIGIBLE_SPEECH_MESSAGE,
                context=context,
                reason=str(exc),
            )
        transcript = transcript_result["transcript"]

        await _set_processing_text(context, transcript, STAGE_SENDING_RESPONSE)
        sending_stage_already_set = True

    extra = payload.get("extra", {})
    duration_seconds = extra.get("duration_seconds") if isinstance(extra, dict) else None
    if not sending_stage_already_set:
        await _set_stage(context, STAGE_SENDING_RESPONSE)
    if _should_defer_transcript_reply(row):
        return {
            "processor": processor_name,
            **file_info,
            "duration_seconds": duration_seconds,
            "audio_duration_seconds": probed_duration_seconds,
            "transcript_length": len(transcript),
            "transcript_preview": transcript[:200],
            "processing_text": transcript,
            "transcript_provider": transcript_result.get("provider"),
            "transcript_model": transcript_result["model_id"],
            "transcript_message_ids": [],
            "transcript_reply_deferred": True,
        }

    transcript_message_ids = await _send_transcript_messages(
        bot,
        row,
        transcript,
        context=context,
    )

    return {
        "processor": processor_name,
        **file_info,
        "duration_seconds": duration_seconds,
        "audio_duration_seconds": probed_duration_seconds,
        "transcript_length": len(transcript),
        "transcript_preview": transcript[:200],
        "processing_text": transcript,
        "transcript_provider": transcript_result.get("provider"),
        "transcript_model": transcript_result["model_id"],
        "transcript_message_ids": transcript_message_ids,
    }


async def send_deferred_transcript_reply(
    bot: Bot,
    row: Mapping[str, object],
    *,
    action_detection_result: Mapping[str, object],
    context: QueueProcessingContext | None = None,
) -> list[int]:
    transcript = _row_processing_text(row)
    if transcript is None:
        raise RetryableJobError("processing_text missing for deferred transcript send")

    await _set_stage(context, STAGE_SENDING_RESPONSE)
    return await _send_transcript_messages(
        bot,
        row,
        transcript,
        context=context,
        action_labels=_action_labels_for_reply(action_detection_result),
    )


def _audio_validation_failure_message(reason: str) -> str:
    if "exceeds" in reason and str(MAX_AUDIO_DURATION_SECONDS) in reason:
        return TOO_LONG_AUDIO_MESSAGE
    if "below" in reason and str(MIN_AUDIO_DURATION_SECONDS) in reason:
        return TOO_SHORT_AUDIO_MESSAGE
    return UNUNDERSTOOD_AUDIO_MESSAGE


async def _set_stage(
    context: QueueProcessingContext | None,
    stage: str,
) -> None:
    if context is not None:
        await context.set_stage(stage, None)


async def _set_processing_text(
    context: QueueProcessingContext | None,
    processing_text: str,
    stage: str,
) -> None:
    if context is not None:
        await context.set_processing_text(processing_text, stage, None)


async def _set_outbound_state(
    context: QueueProcessingContext | None,
    outbound_state: dict[str, object],
    stage: str | None,
) -> None:
    if context is not None:
        await context.set_outbound_json(outbound_state, stage, None)


@asynccontextmanager
async def _calling_stt_activity(
    bot: Bot,
    row: Mapping[str, object],
    context: QueueProcessingContext | None,
):
    if context is None:
        reaction_result = await set_row_reaction_result(
            bot,
            row,
            REACTION_CALLING_STT_API,
            reason="voice_to_text_api_call",
        )
        _raise_if_original_message_unavailable(
            reaction_result,
            stage=STAGE_CALLING_STT_API,
        )
        activity = _ManualActivity()
        yield activity
        return

    async with context.activity(STAGE_CALLING_STT_API, None) as activity:
        raise_if_unavailable = getattr(
            activity,
            "raise_if_original_message_unavailable",
            None,
        )
        if raise_if_unavailable is not None:
            raise_if_unavailable()
        yield activity


async def _send_transcript_messages(
    bot: Bot,
    row: Mapping[str, object],
    transcript: str,
    context: QueueProcessingContext | None,
    *,
    action_labels: list[str] | None = None,
) -> list[int]:
    chat_id = row.get("chat_id")
    if chat_id is None:
        raise RetryableJobError("chat_id missing for transcript send")

    outbound_state = _row_outbound_state(row)
    transcript_state = _outbound_kind_state(outbound_state, "transcript")
    persisted_message_ids = _persisted_message_ids(transcript_state)
    message_thread_id = row.get("message_thread_id")
    source_message_id = row.get("message_id")
    transcript_chunks = _build_transcript_chunks(
        transcript,
        action_labels=action_labels,
    )
    sent_message_ids: list[int] = []

    for index, transcript_chunk in enumerate(transcript_chunks):
        persisted_message_id = persisted_message_ids.get(index)
        if persisted_message_id is not None:
            log.debug(
                "job=%s transcript_chunk_already_sent index=%s message_id=%s",
                row.get("id"),
                index,
                persisted_message_id,
            )
            sent_message_ids.append(persisted_message_id)
            continue

        send_kwargs: dict[str, object] = {
            "chat_id": chat_id,
            "text": transcript_chunk,
        }
        if message_thread_id is not None:
            send_kwargs["message_thread_id"] = int(message_thread_id)
        if index == 0 and source_message_id is not None:
            send_kwargs["reply_to_message_id"] = int(source_message_id)
            send_kwargs["allow_sending_without_reply"] = False

        try:
            sent_message = await bot.send_message(**send_kwargs)
        except TelegramError as exc:
            raise job_error_from_telegram_error(exc, action="send_transcript") from exc

        await _verify_reply_target(
            bot,
            sent_message,
            expected_message_id=source_message_id if index == 0 else None,
            chat_id=chat_id,
            stage="send_transcript",
        )
        sent_message_id = int(sent_message.message_id)
        try:
            transcript_state = _record_outbound_message(
                transcript_state,
                index=index,
                message_id=sent_message_id,
                chars=len(transcript_chunk),
            )
            outbound_state["transcript"] = transcript_state
            await _set_outbound_state(
                context,
                outbound_state,
                STAGE_SENDING_RESPONSE,
            )
        except Exception:
            await _delete_message_best_effort(
                bot,
                chat_id=chat_id,
                message_id=sent_message_id,
            )
            raise

        sent_message_ids.append(sent_message_id)
        if index == 0:
            log.debug(
                "job=%s transcript_sent chat_id=%s reply_to=%s response_message_id=%s chars=%s",
                row.get("id"),
                chat_id,
                source_message_id,
                sent_message_id,
                len(transcript_chunk),
            )

    return sent_message_ids


async def _reject_audio_permanently(
    bot: Bot,
    row: Mapping[str, object],
    message: str,
    *,
    context: QueueProcessingContext | None,
    reason: str,
) -> None:
    reaction_result = await set_row_reaction_result(
        bot,
        row,
        REACTION_FAILURE,
        reason="audio_rejected",
    )
    _raise_if_original_message_unavailable(
        reaction_result,
        stage="audio_rejected",
    )
    await _send_single_reply(bot, row, message, context=context, outbound_key="rejection")
    raise PermanentJobError(reason)


async def _send_single_reply(
    bot: Bot,
    row: Mapping[str, object],
    text: str,
    *,
    context: QueueProcessingContext | None,
    outbound_key: str,
) -> int:
    chat_id = row.get("chat_id")
    if chat_id is None:
        raise RetryableJobError("chat_id missing for failure send")

    outbound_state = _row_outbound_state(row)
    existing_state = _outbound_kind_state(outbound_state, outbound_key)
    existing_message_id = existing_state.get("message_id")
    if isinstance(existing_message_id, int):
        log.debug(
            "job=%s outbound_reply_already_sent kind=%s message_id=%s",
            row.get("id"),
            outbound_key,
            existing_message_id,
        )
        return existing_message_id

    send_kwargs: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
    }
    message_thread_id = row.get("message_thread_id")
    source_message_id = row.get("message_id")
    if message_thread_id is not None:
        send_kwargs["message_thread_id"] = int(message_thread_id)
    if source_message_id is not None:
        send_kwargs["reply_to_message_id"] = int(source_message_id)
        send_kwargs["allow_sending_without_reply"] = False

    try:
        sent_message = await bot.send_message(**send_kwargs)
    except TelegramError as exc:
        raise job_error_from_telegram_error(exc, action="send_rejection_reply") from exc

    await _verify_reply_target(
        bot,
        sent_message,
        expected_message_id=source_message_id,
        chat_id=chat_id,
        stage="send_rejection_reply",
    )
    sent_message_id = int(sent_message.message_id)
    try:
        outbound_state[outbound_key] = {
            "message_id": sent_message_id,
            "chars": len(text),
        }
        await _set_outbound_state(context, outbound_state, None)
    except Exception:
        await _delete_message_best_effort(
            bot,
            chat_id=chat_id,
            message_id=sent_message_id,
        )
        raise
    return sent_message_id


class _ManualActivity:
    def __init__(self) -> None:
        self._original_message_unavailable_detail: str | None = None

    def mark_original_message_unavailable(self, detail: str | None) -> None:
        self._original_message_unavailable_detail = detail or "unknown Telegram error"

    def raise_if_original_message_unavailable(self) -> None:
        if self._original_message_unavailable_detail is None:
            return
        raise OriginalMessageUnavailableError(
            "original message unavailable during stage "
            f"{STAGE_CALLING_STT_API}: {self._original_message_unavailable_detail}"
        )


def _raise_if_original_message_unavailable(
    reaction_result: object,
    *,
    stage: str,
) -> None:
    if not getattr(reaction_result, "original_message_unavailable", False):
        return
    detail = getattr(reaction_result, "error_text", None) or "unknown Telegram error"
    raise OriginalMessageUnavailableError(
        f"original message unavailable during stage {stage}: {detail}"
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
            "failed to delete unthreaded transcript chat_id=%s message_id=%s error=%s",
            chat_id,
            message_id,
            exc,
        )


async def _verify_reply_target(
    bot: Bot,
    sent_message: object,
    *,
    expected_message_id: object | None,
    chat_id: object,
    stage: str,
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
        "original message unavailable during "
        f"{stage}: reply target no longer attached"
    )


def _row_processing_text(row: Mapping[str, object]) -> str | None:
    value = row.get("processing_text")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _row_outbound_state(row: Mapping[str, object]) -> dict[str, object]:
    value = row.get("outbound_json")
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        log.warning("job=%s outbound_json_invalid ignored=true", row.get("id"))
        return {}
    if not isinstance(decoded, dict):
        log.warning("job=%s outbound_json_not_object ignored=true", row.get("id"))
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


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _normalized_outbound_message(message: object) -> dict[str, object] | None:
    if not isinstance(message, dict):
        return None

    normalized_index = _coerce_int(message.get("index"))
    normalized_message_id = _coerce_int(message.get("message_id"))
    if normalized_index is None or normalized_message_id is None:
        return None

    normalized = dict(message)
    normalized["index"] = normalized_index
    normalized["message_id"] = normalized_message_id
    for key in ("chars",):
        normalized_value = _coerce_int(normalized.get(key))
        if normalized_value is not None:
            normalized[key] = normalized_value
    return normalized


def _record_outbound_message(
    kind_state: Mapping[str, object],
    *,
    index: int,
    message_id: int,
    chars: int,
) -> dict[str, object]:
    existing_messages = kind_state.get("messages")
    messages: list[dict[str, object]] = []
    if isinstance(existing_messages, list):
        for message in existing_messages:
            normalized = _normalized_outbound_message(message)
            if normalized is None or normalized["index"] == index:
                continue
            messages.append(normalized)
    messages.append(
        {
            "index": index,
            "message_id": message_id,
            "chars": chars,
        }
    )
    messages.sort(key=lambda message: int(message["index"]))
    return {
        **dict(kind_state),
        "messages": messages,
    }


def _row_file_info(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "file_id": row.get("file_id"),
        "file_unique_id": row.get("file_unique_id"),
        "file_name": row.get("file_name"),
        "mime_type": row.get("mime_type"),
        "file_size": row.get("file_size"),
        "telegram_file_path": None,
    }


def _build_transcript_chunks(
    transcript: str,
    *,
    action_labels: list[str] | None = None,
) -> list[str]:
    transcript_text = transcript.strip()
    if not transcript_text:
        transcript_text = "(empty transcript)"

    labels_line = _format_action_labels_line(action_labels)
    if labels_line is not None:
        transcript_text = f"{transcript_text}\n{labels_line}"
        first_prefix = ""
    else:
        first_prefix = TRANSCRIPT_PREFIX

    chunks: list[str] = []
    current_prefix = first_prefix
    remaining = transcript_text

    while remaining:
        limit = TELEGRAM_MESSAGE_LIMIT - len(current_prefix)
        if len(remaining) <= limit:
            chunks.append(current_prefix + remaining)
            break

        split_at = _find_split_index(remaining, limit)
        chunks.append(current_prefix + remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
        current_prefix = CONTINUED_TRANSCRIPT_PREFIX

    return chunks


def _should_defer_transcript_reply(row: Mapping[str, object]) -> bool:
    status = row.get("action_detection_status")
    if not isinstance(status, str):
        return False
    return status.strip() not in {"", ACTION_DETECTION_NOT_APPLICABLE}


def _action_labels_for_reply(
    action_detection_result: Mapping[str, object],
) -> list[str]:
    labels = action_detection_result.get("provider_labels")
    if isinstance(labels, list):
        return [str(label).strip() for label in labels if str(label).strip()]
    if isinstance(labels, tuple):
        return [str(label).strip() for label in labels if str(label).strip()]
    if action_detection_result.get("none") is True:
        return ["none"]
    action_codes = action_detection_result.get("action_codes")
    if isinstance(action_codes, (list, tuple)):
        return [
            str(action_code).strip()
            for action_code in action_codes
            if str(action_code).strip()
        ]
    return ["none"]


def _format_action_labels_line(action_labels: list[str] | None) -> str | None:
    if action_labels is None:
        return None
    labels = [label.strip() for label in action_labels if label.strip()]
    if not labels:
        labels = ["none"]
    return "[" + ",".join(labels) + "]"


def _find_split_index(text: str, limit: int) -> int:
    newline_index = text.rfind("\n", 0, limit)
    if newline_index >= limit // 2:
        return newline_index

    space_index = text.rfind(" ", 0, limit)
    if space_index >= limit // 2:
        return space_index

    return limit


def _row_mime_type(row: Mapping[str, object]) -> str:
    mime_type = row.get("mime_type")
    if isinstance(mime_type, str) and mime_type.strip():
        return mime_type.strip()
    return "audio/ogg"


def _suffix_for_mime_type(mime_type: str) -> str:
    if "/" not in mime_type:
        return ".audio"
    suffix = mime_type.rsplit("/", 1)[-1].strip().lower()
    if not suffix:
        return ".audio"
    return f".{suffix}"
