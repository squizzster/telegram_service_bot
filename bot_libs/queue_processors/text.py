from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from telegram import Bot

from bot_libs.queue_processing_context import QueueProcessingContext


async def process(
    bot: Bot,
    row: Mapping[str, object],
    payload: Mapping[str, Any],
    context: QueueProcessingContext | None = None,
) -> Mapping[str, object]:
    del bot, payload, context

    text = row.get("processing_text") or row.get("text")
    text_value = str(text) if text is not None else ""

    return {
        "processor": "text",
        "text_length": len(text_value),
        "has_text": bool(text_value),
    }
