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
    photo_sizes = extra.get("photo_sizes") if isinstance(extra, dict) else None

    return {
        "processor": "photo",
        **file_info,
        "photo_size_count": len(photo_sizes) if isinstance(photo_sizes, list) else None,
    }
