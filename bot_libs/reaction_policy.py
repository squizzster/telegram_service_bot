from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

from telegram import Update
from telegram.constants import ChatType
from telegram.error import RetryAfter, TelegramError

from bot_libs.logging_utils import TRACE_LEVEL
from bot_libs.queue_models import STATUS_DEAD, STATUS_DONE
from bot_libs.retry_policy import RETRY_MEDIUM_MAX_SECONDS, RETRY_SHORT_MAX_SECONDS
from bot_libs.stage_names import (
    STAGE_CALLING_STT_API,
    STAGE_DONE,
    STAGE_DOWNLOADING_FILE,
    STAGE_FAILED,
    STAGE_MESSAGE_REMOVED,
    STAGE_QUEUED,
    STAGE_READY_TO_PROCESS,
    STAGE_RETRY_WAITING,
    STAGE_SENDING_RESPONSE,
    STAGE_UNSUPPORTED,
)
from bot_libs.telegram_error_policy import (
    is_original_message_unavailable_error,
    telegram_error_text,
)

log = logging.getLogger(__name__)
_REACTION_RATE_LIMIT_SUPPRESSED_UNTIL: dict[object, float] = {}

REACTION_ACCEPTED = "👀"
REACTION_DOWNLOADING_FILE = "⚡"
REACTION_CALLING_STT_API = "✍"
REACTION_SUCCESS = "👌"
REACTION_UNSUPPORTED = "🤷"
REACTION_RETRY_SHORT = "🤔"
REACTION_RETRY_MEDIUM = "🥱"
REACTION_RETRY_LONG = "😴"
REACTION_FAILURE = "💔"

VALID_REACTION_EMOJIS = frozenset(
    {
        "👍",
        "👎",
        "❤",
        "🔥",
        "🥰",
        "👏",
        "😁",
        "🤔",
        "🤯",
        "😱",
        "🤬",
        "😢",
        "🎉",
        "🤩",
        "🤮",
        "💩",
        "🙏",
        "👌",
        "🕊",
        "🤡",
        "🥱",
        "🥴",
        "😍",
        "🐳",
        "❤️‍🔥",
        "🌚",
        "🌭",
        "💯",
        "🤣",
        "⚡",
        "🍌",
        "🏆",
        "💔",
        "🤨",
        "😐",
        "🍓",
        "🍾",
        "💋",
        "🖕",
        "😈",
        "😴",
        "😭",
        "🤓",
        "👻",
        "👨‍💻",
        "👀",
        "🎃",
        "🙈",
        "😇",
        "😨",
        "🤝",
        "✍",
        "🤗",
        "🫡",
        "🎅",
        "🎄",
        "☃",
        "💅",
        "🤪",
        "🗿",
        "🆒",
        "💘",
        "🙉",
        "🦄",
        "😘",
        "💊",
        "🙊",
        "😎",
        "👾",
        "🤷‍♂️",
        "🤷",
        "🤷‍♀️",
        "😡",
    }
)

REACTION_ELIGIBLE_CHAT_TYPES = frozenset(
    {
        ChatType.GROUP.value,
        ChatType.SUPERGROUP.value,
    }
)


@dataclass(frozen=True, slots=True)
class RowReactionResult:
    ok: bool
    skipped: bool = False
    original_message_unavailable: bool = False
    error_text: str | None = None

    def __bool__(self) -> bool:
        return self.ok


def retry_reaction_for_delay(delay_seconds: int) -> str:
    if delay_seconds < RETRY_SHORT_MAX_SECONDS:
        return REACTION_RETRY_SHORT
    if delay_seconds < RETRY_MEDIUM_MAX_SECONDS:
        return REACTION_RETRY_MEDIUM
    return REACTION_RETRY_LONG


def reaction_for_row_state(row: Mapping[str, object]) -> str | None:
    status = _row_text(row.get("status"))
    stage = _row_text(row.get("stage"))

    if stage == STAGE_MESSAGE_REMOVED:
        return None
    if stage == STAGE_UNSUPPORTED or _row_bool(row.get("is_supported")) is False:
        return REACTION_UNSUPPORTED
    if status == STATUS_DONE or stage == STAGE_DONE:
        return REACTION_SUCCESS
    if status == STATUS_DEAD or stage == STAGE_FAILED:
        return REACTION_FAILURE
    if stage == STAGE_RETRY_WAITING:
        return retry_reaction_for_delay(_retry_delay_seconds_from_row(row))
    if stage == STAGE_DOWNLOADING_FILE:
        return REACTION_DOWNLOADING_FILE
    if stage in {STAGE_CALLING_STT_API, STAGE_SENDING_RESPONSE}:
        return REACTION_CALLING_STT_API
    if stage in {STAGE_QUEUED, STAGE_READY_TO_PROCESS, None}:
        return REACTION_ACCEPTED
    return REACTION_ACCEPTED


def reaction_is_terminal_for_row(row: Mapping[str, object]) -> bool:
    status = _row_text(row.get("status"))
    stage = _row_text(row.get("stage"))
    return (
        status in {STATUS_DONE, STATUS_DEAD}
        or stage in {STAGE_DONE, STAGE_FAILED, STAGE_MESSAGE_REMOVED, STAGE_UNSUPPORTED}
    )


def _row_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        if value.strip() in {"0", "false", "False"}:
            return False
        if value.strip() in {"1", "true", "True"}:
            return True
    return None


def _retry_delay_seconds_from_row(row: Mapping[str, object]) -> int:
    available_at = _parse_sqlite_datetime(row.get("available_at"))
    if available_at is None:
        return 0
    return max(0, math.ceil((available_at - datetime.now(timezone.utc)).total_seconds()))


def _parse_sqlite_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def can_use_status_reactions_for_chat_type(chat_type: object | None) -> bool:
    value = getattr(chat_type, "value", chat_type)
    if value is None:
        return False
    return str(value) in REACTION_ELIGIBLE_CHAT_TYPES


def can_use_status_reactions(update: Update) -> bool:
    message = update.effective_message
    chat = update.effective_chat
    return (
        message is not None
        and chat is not None
        and can_use_status_reactions_for_chat_type(getattr(chat, "type", None))
    )


async def set_update_reaction(
    update: Update,
    emoji: str,
    *,
    reason: str,
    retry_delay_seconds: float = 0.0,
) -> bool:
    if not can_use_status_reactions(update):
        return False

    _validate_emoji(emoji)

    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None:
        return False
    if _reaction_suppressed(chat.id):
        log.debug(
            "reaction_skipped reason=rate_limit_suppressed chat_id=%s "
            "message_id=%s emoji=%s nonfatal=true",
            chat.id,
            message.message_id,
            emoji,
        )
        return False

    attempts = 2 if retry_delay_seconds > 0 else 1
    for attempt in range(1, attempts + 1):
        try:
            result = await message.set_reaction(emoji)
            return bool(result)
        except TelegramError as exc:
            if isinstance(exc, RetryAfter):
                _log_reaction_failure(
                    emoji=emoji,
                    reason=reason,
                    chat_id=chat.id,
                    message_id=message.message_id,
                    exc=exc,
                )
                return False
            if attempt < attempts:
                log.debug(
                    "reaction_failed emoji=%s reason=%s chat_id=%s message_id=%s "
                    "attempt=%s/%s retrying_in=%.2fs error=%s nonfatal=true",
                    emoji,
                    reason,
                    chat.id,
                    message.message_id,
                    attempt,
                    attempts,
                    retry_delay_seconds,
                    exc,
                )
                await asyncio.sleep(retry_delay_seconds)
                continue

            _log_reaction_failure(
                emoji=emoji,
                reason=reason,
                chat_id=chat.id,
                message_id=message.message_id,
                exc=exc,
            )
    return False


async def set_row_reaction(
    bot: object,
    row: Mapping[str, object],
    emoji: str,
    *,
    reason: str,
) -> bool:
    return bool(
        await set_row_reaction_result(
            bot,
            row,
            emoji,
            reason=reason,
        )
    )


async def set_row_reaction_result(
    bot: object,
    row: Mapping[str, object],
    emoji: str,
    *,
    reason: str,
) -> RowReactionResult:
    _validate_emoji(emoji)

    set_reaction = getattr(bot, "set_message_reaction", None)
    if set_reaction is None:
        return RowReactionResult(
            ok=False,
            skipped=True,
            error_text="bot_has_no_set_message_reaction",
        )

    chat_id = row.get("chat_id")
    message_id = row.get("message_id")
    if chat_id is None or message_id is None:
        return RowReactionResult(
            ok=False,
            skipped=True,
            error_text="missing_chat_or_message_id",
        )
    if _reaction_suppressed(chat_id):
        log.debug(
            "reaction_skipped reason=rate_limit_suppressed chat_id=%s "
            "message_id=%s emoji=%s nonfatal=true",
            chat_id,
            message_id,
            emoji,
        )
        return RowReactionResult(
            ok=False,
            skipped=True,
            error_text="reaction_rate_limited",
        )
    if not can_use_status_reactions_for_chat_type(row.get("chat_type")):
        log.debug(
            "Skipping %s reaction reason=%s chat_id=%s message_id=%s chat_type=%s",
            emoji,
            reason,
            chat_id,
            message_id,
            row.get("chat_type"),
        )
        return RowReactionResult(
            ok=False,
            skipped=True,
            error_text="chat_type_not_reaction_eligible",
        )

    try:
        result = await set_reaction(
            chat_id=chat_id,
            message_id=int(message_id),
            reaction=emoji,
        )
    except TelegramError as exc:
        original_message_unavailable = is_original_message_unavailable_error(exc)
        _log_reaction_failure(
            emoji=emoji,
            reason=reason,
            chat_id=chat_id,
            message_id=message_id,
            exc=exc,
        )
        return RowReactionResult(
            ok=False,
            original_message_unavailable=original_message_unavailable,
            error_text=telegram_error_text(exc),
        )

    if not result:
        log.debug(
            "Telegram returned false while setting %s reaction reason=%s chat_id=%s "
            "message_id=%s",
            emoji,
            reason,
            chat_id,
            message_id,
        )
        return RowReactionResult(ok=False, error_text="telegram_returned_false")

    log.log(
        TRACE_LEVEL,
        "Set %s reaction reason=%s chat_id=%s message_id=%s",
        emoji,
        reason,
        chat_id,
        message_id,
    )
    return RowReactionResult(ok=True)


def _reaction_suppressed(chat_id: object) -> bool:
    return time.monotonic() < _REACTION_RATE_LIMIT_SUPPRESSED_UNTIL.get(chat_id, 0.0)


def _log_reaction_failure(
    *,
    emoji: str,
    reason: str,
    chat_id: object,
    message_id: object,
    exc: TelegramError,
) -> None:
    if isinstance(exc, RetryAfter):
        retry_after = _retry_after_seconds(exc.retry_after)
        now = time.monotonic()
        suppressed_until = _REACTION_RATE_LIMIT_SUPPRESSED_UNTIL.get(chat_id, 0.0)
        if now >= suppressed_until:
            _REACTION_RATE_LIMIT_SUPPRESSED_UNTIL[chat_id] = now + retry_after
            log.warning(
                "reaction updates rate-limited chat_id=%s retry_after=%s; "
                "suppressing reaction updates temporarily",
                chat_id,
                retry_after,
            )
            return

        log.debug(
            "reaction_failed reason=flood_control chat_id=%s message_id=%s "
            "retry_after=%s nonfatal=true",
            chat_id,
            message_id,
            retry_after,
        )
        return

    if is_original_message_unavailable_error(exc):
        log.debug(
            "reaction_failed emoji=%s reason=%s chat_id=%s message_id=%s "
            "error=%s original_message_unavailable=true",
            emoji,
            reason,
            chat_id,
            message_id,
            exc,
        )
        return

    log.debug(
        "reaction_failed emoji=%s reason=%s chat_id=%s message_id=%s "
        "error=%s nonfatal=true",
        emoji,
        reason,
        chat_id,
        message_id,
        exc,
    )


def _retry_after_seconds(value: object) -> int:
    total_seconds = getattr(value, "total_seconds", None)
    if callable(total_seconds):
        return max(1, int(math.ceil(float(total_seconds()))))
    return max(1, int(math.ceil(float(value))))


def _validate_emoji(emoji: str) -> None:
    if emoji not in VALID_REACTION_EMOJIS:
        raise ValueError(
            f"reaction emoji is not in the allowed Telegram set: {emoji!r}"
        )
