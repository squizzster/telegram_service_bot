from __future__ import annotations

from datetime import timedelta

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_VOICE, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return message.voice is not None


def extract(message: Message) -> MessageContent:
    voice = message.voice
    if voice is None:
        raise ValueError("voice payload is missing")

    return MessageContent(
        content_type=CONTENT_TYPE_VOICE,
        is_supported=True,
        caption=message.caption,
        file_details=FileDetails(
            file_id=voice.file_id,
            file_unique_id=voice.file_unique_id,
            mime_type=voice.mime_type,
            file_size=voice.file_size,
        ),
        payload_extra={
            "duration_seconds": _duration_seconds(voice.duration),
        },
    )


def _duration_seconds(value: timedelta | int | None) -> int | None:
    if value is None:
        return None
    if hasattr(value, "total_seconds"):
        return int(value.total_seconds())
    return int(value)
