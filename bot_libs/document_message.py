from __future__ import annotations

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_DOCUMENT, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return message.document is not None


def extract(message: Message) -> MessageContent:
    document = message.document
    if document is None:
        raise ValueError("document payload is missing")

    return MessageContent(
        content_type=CONTENT_TYPE_DOCUMENT,
        is_supported=True,
        caption=message.caption,
        file_details=FileDetails(
            file_id=document.file_id,
            file_unique_id=document.file_unique_id,
            file_name=document.file_name,
            mime_type=document.mime_type,
            file_size=document.file_size,
        ),
        payload_extra={
            "thumbnail": _thumbnail_payload(document.thumbnail),
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
