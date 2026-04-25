from __future__ import annotations

RETRY_DELAYS_SECONDS = (
    1,
    3,
    9,
    27,
    81,
    243,
    729,
    2187,
    6561,
    19683,
    59049,
)

DEFAULT_QUEUE_MAX_ATTEMPTS = len(RETRY_DELAYS_SECONDS) + 1

RETRY_SHORT_MAX_SECONDS = 15 * 60
RETRY_MEDIUM_MAX_SECONDS = 2 * 60 * 60
RETRY_LONG_MAX_SECONDS = 17 * 60 * 60


def next_retry_delay_seconds(*, attempts: int, max_attempts: int) -> int | None:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if max_attempts > DEFAULT_QUEUE_MAX_ATTEMPTS:
        raise ValueError(
            "retry schedule only supports max_attempts up to "
            f"{DEFAULT_QUEUE_MAX_ATTEMPTS}"
        )
    if attempts >= max_attempts:
        return None

    delay_index = attempts - 1
    if delay_index >= len(RETRY_DELAYS_SECONDS):
        return None

    return RETRY_DELAYS_SECONDS[delay_index]
