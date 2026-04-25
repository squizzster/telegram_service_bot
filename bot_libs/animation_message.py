from __future__ import annotations

from datetime import timedelta

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_ANIMATION, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return message.animation is not None


def extract(message: Message) -> MessageContent:
    animation = message.animation
    if animation is None:
        raise ValueError("animation payload is missing")

    return MessageContent(
        content_type=CONTENT_TYPE_ANIMATION,
        is_supported=True,
        caption=message.caption,
        file_details=FileDetails(
            file_id=animation.file_id,
            file_unique_id=animation.file_unique_id,
            file_name=animation.file_name,
            mime_type=animation.mime_type,
            file_size=animation.file_size,
        ),
        payload_extra={
            "duration_seconds": _duration_seconds(animation.duration),
            "width": animation.width,
            "height": animation.height,
            "thumbnail": _thumbnail_payload(animation.thumbnail),
        },
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
