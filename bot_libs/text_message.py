from __future__ import annotations

from telegram import Message

from bot_libs.queue_models import CONTENT_TYPE_TEXT, MessageContent


def matches(message: Message) -> bool:
    return message.text is not None


def extract(message: Message) -> MessageContent:
    return MessageContent(
        content_type=CONTENT_TYPE_TEXT,
        is_supported=True,
        text=message.text,
        caption=message.caption,
    )
