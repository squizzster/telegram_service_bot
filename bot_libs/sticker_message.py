from __future__ import annotations

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_STICKER, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return message.sticker is not None


def extract(message: Message) -> MessageContent:
    sticker = message.sticker
    if sticker is None:
        raise ValueError("sticker payload is missing")

    return MessageContent(
        content_type=CONTENT_TYPE_STICKER,
        is_supported=False,
        caption=message.caption,
        file_details=FileDetails(
            file_id=sticker.file_id,
            file_unique_id=sticker.file_unique_id,
            file_size=sticker.file_size,
        ),
        payload_extra={
            "emoji": sticker.emoji,
            "set_name": sticker.set_name,
            "is_animated": sticker.is_animated,
            "is_video": sticker.is_video,
            "thumbnail": _thumbnail_payload(sticker.thumbnail),
        },
        unsupported_reason="sticker_not_supported_yet",
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
