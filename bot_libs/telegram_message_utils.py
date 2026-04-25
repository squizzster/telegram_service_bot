from __future__ import annotations


TELEGRAM_MESSAGE_LIMIT = 4096


def split_telegram_text(text: str, *, prefix: str = "") -> list[str]:
    body = text.strip() or "(empty)"
    chunks: list[str] = []
    current_prefix = prefix
    remaining = body

    while remaining:
        limit = TELEGRAM_MESSAGE_LIMIT - len(current_prefix)
        if limit < 1:
            raise ValueError("prefix is too long for a Telegram message")
        if len(remaining) <= limit:
            chunks.append(current_prefix + remaining)
            break

        split_at = _find_split_index(remaining, limit)
        chunks.append(current_prefix + remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
        current_prefix = ""

    return chunks


def _find_split_index(text: str, limit: int) -> int:
    newline_index = text.rfind("\n", 0, limit)
    if newline_index >= limit // 2:
        return newline_index

    space_index = text.rfind(" ", 0, limit)
    if space_index >= limit // 2:
        return space_index

    return limit
