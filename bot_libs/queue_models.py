from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping

from bot_libs.action_models import (
    ACTION_DETECTION_NOT_APPLICABLE,
    initial_action_detection_status,
)
from bot_libs.retry_policy import DEFAULT_QUEUE_MAX_ATTEMPTS
from bot_libs.stage_names import STAGE_QUEUED

STATUS_QUEUED = "queued"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_DEAD = "dead"

JOB_KIND_INCOMING_MESSAGE = "incoming_message"

CONTENT_TYPE_TEXT = "text"
CONTENT_TYPE_PHOTO = "photo"
CONTENT_TYPE_DOCUMENT = "document"
CONTENT_TYPE_VOICE = "voice"
CONTENT_TYPE_AUDIO = "audio"
CONTENT_TYPE_ANIMATION = "animation"
CONTENT_TYPE_VIDEO = "video"
CONTENT_TYPE_VIDEO_NOTE = "video_note"
CONTENT_TYPE_STICKER = "sticker"
CONTENT_TYPE_UNKNOWN = "unknown"

SUPPORTED_CONTENT_TYPES = frozenset(
    {
        CONTENT_TYPE_TEXT,
        CONTENT_TYPE_PHOTO,
        CONTENT_TYPE_DOCUMENT,
        CONTENT_TYPE_VOICE,
        CONTENT_TYPE_AUDIO,
        CONTENT_TYPE_ANIMATION,
    }
)

KNOWN_CONTENT_TYPES = frozenset(
    {
        CONTENT_TYPE_TEXT,
        CONTENT_TYPE_PHOTO,
        CONTENT_TYPE_DOCUMENT,
        CONTENT_TYPE_VOICE,
        CONTENT_TYPE_AUDIO,
        CONTENT_TYPE_ANIMATION,
        CONTENT_TYPE_VIDEO,
        CONTENT_TYPE_VIDEO_NOTE,
        CONTENT_TYPE_STICKER,
        CONTENT_TYPE_UNKNOWN,
    }
)


def stable_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def to_db_bool(value: bool) -> int:
    return 1 if value else 0


def enum_value(value: object | None) -> str | None:
    if value is None:
        return None

    enum_member = getattr(value, "value", None)
    if isinstance(enum_member, str):
        return enum_member

    text = str(value)
    return text or None


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return getattr(value, "value", str(value))
    return str(value)


@dataclass(frozen=True, slots=True)
class FileDetails:
    file_id: str | None = None
    file_unique_id: str | None = None
    file_name: str | None = None
    mime_type: str | None = None
    file_size: int | None = None

    @property
    def has_file(self) -> bool:
        return any(
            value is not None
            for value in (
                self.file_id,
                self.file_unique_id,
                self.file_name,
                self.mime_type,
                self.file_size,
            )
        )


@dataclass(frozen=True, slots=True)
class MessageContent:
    content_type: str
    is_supported: bool
    text: str | None = None
    caption: str | None = None
    file_details: FileDetails = field(default_factory=FileDetails)
    payload_extra: Mapping[str, Any] = field(default_factory=dict)
    unsupported_reason: str | None = None


@dataclass(frozen=True, slots=True)
class QueueJobData:
    update_id: int
    chat_id: int
    telegram_date: str
    content_type: str
    is_supported: bool
    payload_json: str
    raw_update_json: str
    max_attempts: int
    job_kind: str = JOB_KIND_INCOMING_MESSAGE
    chat_type: str | None = None
    chat_title: str | None = None
    message_id: int | None = None
    message_thread_id: int | None = None
    media_group_id: str | None = None
    from_id: int | None = None
    from_first_name: str | None = None
    from_username: str | None = None
    from_language_code: str | None = None
    is_bot_sender: bool = False
    sender_chat_id: int | None = None
    sender_chat_type: str | None = None
    sender_chat_title: str | None = None
    reply_to_message_id: int | None = None
    has_file: bool = False
    file_id: str | None = None
    file_unique_id: str | None = None
    file_name: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    stage: str = STAGE_QUEUED
    stage_detail: str | None = None
    text: str | None = None
    processing_text: str | None = None
    caption: str | None = None
    action_detection_status: str | None = None

    def as_db_params(self) -> dict[str, Any]:
        action_detection_status = self.action_detection_status
        if action_detection_status is None:
            action_detection_status = initial_action_detection_status(
                content_type=self.content_type,
                is_supported=self.is_supported,
                processing_text=self.processing_text,
            )
        if not action_detection_status:
            action_detection_status = ACTION_DETECTION_NOT_APPLICABLE

        return {
            "status": STATUS_QUEUED,
            "stage": self.stage,
            "stage_detail": self.stage_detail,
            "job_kind": self.job_kind,
            "content_type": self.content_type,
            "is_supported": to_db_bool(self.is_supported),
            "update_id": self.update_id,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type,
            "chat_title": self.chat_title,
            "message_id": self.message_id,
            "message_thread_id": self.message_thread_id,
            "media_group_id": self.media_group_id,
            "telegram_date": self.telegram_date,
            "from_id": self.from_id,
            "from_first_name": self.from_first_name,
            "from_username": self.from_username,
            "from_language_code": self.from_language_code,
            "is_bot_sender": to_db_bool(self.is_bot_sender),
            "sender_chat_id": self.sender_chat_id,
            "sender_chat_type": self.sender_chat_type,
            "sender_chat_title": self.sender_chat_title,
            "reply_to_message_id": self.reply_to_message_id,
            "has_file": to_db_bool(self.has_file),
            "file_id": self.file_id,
            "file_unique_id": self.file_unique_id,
            "file_name": self.file_name,
            "mime_type": self.mime_type,
            "file_size": self.file_size,
            "text": self.text,
            "processing_text": self.processing_text,
            "caption": self.caption,
            "action_detection_status": action_detection_status,
            "payload_json": self.payload_json,
            "raw_update_json": self.raw_update_json,
            "max_attempts": self.max_attempts,
        }


@dataclass(frozen=True, slots=True)
class QueueInsertResult:
    queue_id: int | None
    duplicate: bool = False
