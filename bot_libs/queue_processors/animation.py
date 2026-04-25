from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from telegram import Bot

from bot_libs.queue_processing_context import QueueProcessingContext
from bot_libs.queue_processors.file_common import get_telegram_file_info


async def process(
    bot: Bot,
    row: Mapping[str, object],
    payload: Mapping[str, Any],
    context: QueueProcessingContext | None = None,
) -> Mapping[str, object]:
    del context

    file_info = await get_telegram_file_info(bot, row)

    extra = payload.get("extra", {})
    if not isinstance(extra, dict):
        extra = {}

    return {
        "processor": "animation",
        **file_info,
        "duration_seconds": extra.get("duration_seconds"),
        "width": extra.get("width"),
        "height": extra.get("height"),
    }
