from __future__ import annotations

from typing import Callable

from telegram import Message, Update

from bot_libs import (
    animation_message,
    audio_message,
    document_message,
    photo_message,
    sticker_message,
    text_message,
    video_message,
    video_note_message,
    voice_message,
)
from bot_libs.action_models import initial_action_detection_status
from bot_libs.queue_models import (
    CONTENT_TYPE_UNKNOWN,
    CONTENT_TYPE_TEXT,
    FileDetails,
    MessageContent,
    QueueJobData,
    enum_value,
    stable_json_dumps,
)
from bot_libs.stage_names import initial_stage_for_job

Extractor = tuple[Callable[[Message], bool], Callable[[Message], MessageContent]]

EXTRACTORS: tuple[Extractor, ...] = (
    (text_message.matches, text_message.extract),
    (animation_message.matches, animation_message.extract),
    (photo_message.matches, photo_message.extract),
    (document_message.matches, document_message.extract),
    (voice_message.matches, voice_message.extract),
    (audio_message.matches, audio_message.extract),
    (video_message.matches, video_message.extract),
    (video_note_message.matches, video_note_message.extract),
    (sticker_message.matches, sticker_message.extract),
)

UNKNOWN_FIELD_NAMES = (
    "contact",
    "location",
    "venue",
    "poll",
    "dice",
    "game",
    "story",
    "paid_media",
    "checklist",
)


def extract_queue_job(update: Update, *, max_attempts: int) -> QueueJobData | None:
    message = update.effective_message
    if message is None:
        return None

    if update.update_id is None:
        raise ValueError("update_id is missing")

    chat = update.effective_chat or message.chat
    if chat is None:
        raise ValueError("chat is missing from message update")

    content = _extract_message_content(message)
    processing_text = content.text if content.content_type == CONTENT_TYPE_TEXT else None
    action_detection_status = initial_action_detection_status(
        content_type=content.content_type,
        is_supported=content.is_supported,
        processing_text=processing_text,
    )
    stage = initial_stage_for_job(
        is_supported=content.is_supported,
        processing_text=processing_text,
    )
    sender_chat = message.sender_chat
    from_user = message.from_user

    payload = {
        "message": {
            "update_id": update.update_id,
            "message_id": message.message_id,
            "chat_id": chat.id,
            "chat_type": enum_value(chat.type),
            "message_thread_id": message.message_thread_id,
            "media_group_id": message.media_group_id,
            "telegram_date": message.date.isoformat(),
            "reply_to_message_id": getattr(message.reply_to_message, "message_id", None),
        },
        "sender": {
            "from_id": getattr(from_user, "id", None),
            "from_first_name": getattr(from_user, "first_name", None),
            "from_username": getattr(from_user, "username", None),
            "from_language_code": getattr(from_user, "language_code", None),
            "is_bot_sender": bool(getattr(from_user, "is_bot", False)),
            "sender_chat_id": getattr(sender_chat, "id", None),
            "sender_chat_type": enum_value(getattr(sender_chat, "type", None)),
            "sender_chat_title": getattr(sender_chat, "title", None),
        },
        "content": {
            "job_kind": "incoming_message",
            "type": content.content_type,
            "is_supported": content.is_supported,
            "has_file": content.file_details.has_file,
            "text": content.text,
            "caption": content.caption,
            "processing_text": processing_text,
            "action_detection_status": action_detection_status,
        },
        "file": {
            "file_id": content.file_details.file_id,
            "file_unique_id": content.file_details.file_unique_id,
            "file_name": content.file_details.file_name,
            "mime_type": content.file_details.mime_type,
            "file_size": content.file_details.file_size,
        },
        "diagnostics": {
            "recognized_type": content.content_type,
            "unsupported_reason": content.unsupported_reason,
        },
        "extra": dict(content.payload_extra),
    }

    return QueueJobData(
        update_id=update.update_id,
        chat_id=chat.id,
        chat_type=enum_value(chat.type),
        chat_title=getattr(chat, "title", None),
        message_id=message.message_id,
        message_thread_id=message.message_thread_id,
        media_group_id=message.media_group_id,
        telegram_date=message.date.isoformat(),
        from_id=getattr(from_user, "id", None),
        from_first_name=getattr(from_user, "first_name", None),
        from_username=getattr(from_user, "username", None),
        from_language_code=getattr(from_user, "language_code", None),
        is_bot_sender=bool(getattr(from_user, "is_bot", False)),
        sender_chat_id=getattr(sender_chat, "id", None),
        sender_chat_type=enum_value(getattr(sender_chat, "type", None)),
        sender_chat_title=getattr(sender_chat, "title", None),
        reply_to_message_id=getattr(message.reply_to_message, "message_id", None),
        content_type=content.content_type,
        is_supported=content.is_supported,
        has_file=content.file_details.has_file,
        file_id=content.file_details.file_id,
        file_unique_id=content.file_details.file_unique_id,
        file_name=content.file_details.file_name,
        mime_type=content.file_details.mime_type,
        file_size=content.file_details.file_size,
        stage=stage,
        text=content.text,
        processing_text=processing_text,
        caption=content.caption,
        action_detection_status=action_detection_status,
        payload_json=stable_json_dumps(payload),
        raw_update_json=stable_json_dumps(update.to_dict()),
        max_attempts=max_attempts,
    )


def _extract_message_content(message: Message) -> MessageContent:
    for matches, extract in EXTRACTORS:
        if matches(message):
            return extract(message)
    return _extract_unknown_message(message)


def _extract_unknown_message(message: Message) -> MessageContent:
    present_fields = [
        field_name
        for field_name in UNKNOWN_FIELD_NAMES
        if getattr(message, field_name, None) is not None
    ]

    return MessageContent(
        content_type=CONTENT_TYPE_UNKNOWN,
        is_supported=False,
        caption=message.caption,
        file_details=FileDetails(),
        payload_extra={"present_message_fields": present_fields},
        unsupported_reason="message_type_not_supported_yet",
    )
