from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from telegram import Bot

from bot_libs.queue_processing_context import QueueProcessingContext
from bot_libs.queue_models import (
    CONTENT_TYPE_ANIMATION,
    CONTENT_TYPE_AUDIO,
    CONTENT_TYPE_DOCUMENT,
    CONTENT_TYPE_PHOTO,
    CONTENT_TYPE_TEXT,
    CONTENT_TYPE_VOICE,
)
from bot_libs.queue_processor_errors import PermanentJobError
from bot_libs.queue_processors import animation, audio, document, photo, text, voice

Processor = Callable[
    [Bot, Mapping[str, object], Mapping[str, Any], QueueProcessingContext | None],
    Awaitable[Mapping[str, object]],
]


PROCESSORS: dict[str, Processor] = {
    CONTENT_TYPE_TEXT: text.process,
    CONTENT_TYPE_PHOTO: photo.process,
    CONTENT_TYPE_DOCUMENT: document.process,
    CONTENT_TYPE_VOICE: voice.process,
    CONTENT_TYPE_AUDIO: audio.process,
    CONTENT_TYPE_ANIMATION: animation.process,
}


class DispatchingQueueJobProcessor:
    async def process(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        context: QueueProcessingContext | None = None,
    ) -> Mapping[str, object]:
        content_type = str(row.get("content_type") or "")

        processor = PROCESSORS.get(content_type)
        if processor is None:
            raise PermanentJobError(
                f"no processor registered for content_type={content_type!r}"
            )

        payload = _load_payload(row)
        result = dict(await processor(bot, row, payload, context))
        result.setdefault("outcome", "processed")
        result.setdefault("content_type", content_type)
        result.setdefault("processor", content_type)
        return result


def _load_payload(row: Mapping[str, object]) -> Mapping[str, Any]:
    raw_payload = row.get("payload_json")
    if not isinstance(raw_payload, str) or not raw_payload.strip():
        raise PermanentJobError("payload_json missing or empty")

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise PermanentJobError(f"payload_json is invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise PermanentJobError("payload_json must decode to an object")

    return payload
