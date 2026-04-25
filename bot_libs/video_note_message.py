from __future__ import annotations

from datetime import timedelta

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_VIDEO_NOTE, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return message.video_note is not None


def extract(message: Message) -> MessageContent:
    video_note = message.video_note
    if video_note is None:
        raise ValueError("video_note payload is missing")

    return MessageContent(
        content_type=CONTENT_TYPE_VIDEO_NOTE,
        is_supported=False,
        caption=message.caption,
        file_details=FileDetails(
            file_id=video_note.file_id,
            file_unique_id=video_note.file_unique_id,
            file_size=video_note.file_size,
        ),
        payload_extra={
            "duration_seconds": _duration_seconds(video_note.duration),
            "length": video_note.length,
            "thumbnail": _thumbnail_payload(video_note.thumbnail),
        },
        unsupported_reason="video_note_not_supported_yet",
    )


def _thumbnail_payload(thumbnail: object | None) -> dict[str, object] | None:
    if thumbnail is None:
        return None
    return {
        "file_id": getattr(thumbnail, "file_id", None),
        "file_unique_id": getattr(thumbnail, "file_unique_id", None),
        "file_size": getattr(thumbnail, "file_size", None),
        "width": getattr(thumbnail, "width", None),
        "height": getattr(thumbnail, "height", None),
    }


def _duration_seconds(value: timedelta | int | None) -> int | None:
    if value is None:
        return None
    if hasattr(value, "total_seconds"):
        return int(value.total_seconds())
    return int(value)
