from __future__ import annotations

from telegram import Message, PhotoSize

from bot_libs.queue_models import CONTENT_TYPE_PHOTO, FileDetails, MessageContent


def matches(message: Message) -> bool:
    return bool(message.photo)


def extract(message: Message) -> MessageContent:
    photo_sizes = list(message.photo or [])
    largest = max(photo_sizes, key=_photo_rank)

    return MessageContent(
        content_type=CONTENT_TYPE_PHOTO,
        is_supported=True,
        caption=message.caption,
        file_details=FileDetails(
            file_id=largest.file_id,
            file_unique_id=largest.file_unique_id,
            file_size=largest.file_size,
        ),
        payload_extra={
            "photo_sizes": [
                {
                    "file_id": item.file_id,
                    "file_unique_id": item.file_unique_id,
                    "file_size": item.file_size,
                    "width": item.width,
                    "height": item.height,
                }
                for item in photo_sizes
            ]
        },
    )


def _photo_rank(photo: PhotoSize) -> tuple[int, int, int]:
    width = photo.width or 0
    height = photo.height or 0
    file_size = photo.file_size or 0
    return (width * height, file_size, width + height)
