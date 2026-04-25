from __future__ import annotations

from datetime import timedelta

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_VIDEO, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return message.video is not None


def extract(message: Message) -> MessageContent:
    video = message.video
    if video is None:
        raise ValueError("video payload is missing")

    return MessageContent(
        content_type=CONTENT_TYPE_VIDEO,
        is_supported=False,
        caption=message.caption,
        file_details=FileDetails(
            file_id=video.file_id,
            file_unique_id=video.file_unique_id,
            file_name=video.file_name,
            mime_type=video.mime_type,
            file_size=video.file_size,
        ),
        payload_extra={
            "duration_seconds": _duration_seconds(video.duration),
            "width": video.width,
            "height": video.height,
            "thumbnail": _thumbnail_payload(video.thumbnail),
        },
        unsupported_reason="video_not_supported_yet",
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
