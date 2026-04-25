from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass

from bot_libs.reaction_policy import RowReactionResult, set_row_reaction_result

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ReactionFeedbackClient:
    bot: object
    row: Mapping[str, object]

    async def send_reaction(self, emoji: str, *, reason: str) -> RowReactionResult:
        try:
            return await set_row_reaction_result(
                self.bot,
                self.row,
                emoji,
                reason=reason,
            )
        except ValueError:
            raise
        except Exception as exc:
            log.warning(
                "Reaction feedback failed emoji=%s reason=%s chat_id=%s message_id=%s: %s",
                emoji,
                reason,
                self.row.get("chat_id"),
                self.row.get("message_id"),
                exc,
            )
            return RowReactionResult(ok=False, error_text=str(exc))
