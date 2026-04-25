from __future__ import annotations

import math

from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError

from bot_libs.queue_processor_errors import (
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
)

ORIGINAL_MESSAGE_UNAVAILABLE_MARKERS = (
    "message to react not found",
    "message to be replied not found",
    "message to reply not found",
    "reply message not found",
    "message_id_invalid",
)


def telegram_error_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def is_original_message_unavailable_error(exc: BaseException) -> bool:
    if not isinstance(exc, TelegramError):
        return False

    normalized = telegram_error_text(exc).lower()
    return any(
        marker in normalized for marker in ORIGINAL_MESSAGE_UNAVAILABLE_MARKERS
    )


def job_error_from_telegram_error(
    exc: TelegramError,
    *,
    action: str,
) -> PermanentJobError | RetryableJobError:
    error_text = telegram_error_text(exc)
    if is_original_message_unavailable_error(exc):
        return OriginalMessageUnavailableError(
            f"original message unavailable during {action}: {error_text}"
        )

    if isinstance(exc, (BadRequest, Forbidden)):
        return PermanentJobError(
            f"Telegram {action} failed permanently: {error_text}"
        )

    if isinstance(exc, RetryAfter):
        retry_after = _retry_after_seconds(exc.retry_after)
        return RetryableJobError(
            f"Telegram {action} rate-limited: {error_text}",
            retry_after_seconds=retry_after,
        )

    if isinstance(exc, NetworkError):
        return RetryableJobError(f"Telegram {action} failed temporarily: {error_text}")

    return RetryableJobError(f"Telegram {action} failed: {error_text}")


def _retry_after_seconds(value: object) -> int:
    total_seconds = getattr(value, "total_seconds", None)
    if callable(total_seconds):
        return max(1, int(math.ceil(float(total_seconds()))))
    return max(1, int(math.ceil(float(value))))
