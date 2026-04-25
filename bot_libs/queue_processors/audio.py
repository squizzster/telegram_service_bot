from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from telegram import Bot

from bot_libs.queue_processing_context import QueueProcessingContext
from bot_libs.queue_processors.voice import process_audio_like


async def process(
    bot: Bot,
    row: Mapping[str, object],
    payload: Mapping[str, Any],
    context: QueueProcessingContext | None = None,
) -> Mapping[str, object]:
    return await process_audio_like(
        bot,
        row,
        payload,
        context=context,
        processor_name="audio",
    )
