from __future__ import annotations

from collections.abc import Mapping

from telegram import Bot, File
from telegram.error import TelegramError

from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError
from bot_libs.telegram_error_policy import job_error_from_telegram_error


async def get_telegram_file_and_info(
    bot: Bot,
    row: Mapping[str, object],
) -> tuple[File, dict[str, object]]:
    file_id = row.get("file_id")
    if not file_id:
        raise PermanentJobError("file_id missing")

    file_id_text = str(file_id)

    try:
        tg_file = await bot.get_file(file_id_text)
    except TelegramError as exc:
        raise job_error_from_telegram_error(exc, action="get_file") from exc

    return tg_file, {
        "file_id": file_id_text,
        "file_unique_id": row.get("file_unique_id"),
        "file_name": row.get("file_name"),
        "mime_type": row.get("mime_type"),
        "file_size": row.get("file_size"),
        "telegram_file_path": _normalize_telegram_file_path(tg_file.file_path),
    }


async def get_telegram_file_info(
    bot: Bot,
    row: Mapping[str, object],
) -> dict[str, object]:
    _, file_info = await get_telegram_file_and_info(bot, row)
    return file_info


def _normalize_telegram_file_path(file_path: object) -> str | None:
    if not isinstance(file_path, str):
        return None

    normalized = file_path.strip()
    if not normalized:
        return None

    marker = "/file/bot"
    marker_index = normalized.find(marker)
    if marker_index < 0:
        return normalized

    path_start = normalized.find("/", marker_index + len(marker))
    if path_start < 0:
        return normalized

    return normalized[path_start + 1 :]
