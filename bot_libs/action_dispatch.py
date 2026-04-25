from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from telegram import Bot

from bot_libs.action_models import ACTION_ANSWER_QUESTION
from bot_libs.action_processing_context import ActionProcessingContext
from bot_libs.action_processors.questions import process_answer_question
from bot_libs.action_processors.temporary import TEMPORARY_ACTION_PROCESSORS
from bot_libs.queue_processor_errors import PermanentJobError

ActionProcessorFunc = Callable[..., Awaitable[Mapping[str, object]]]

DEFAULT_ACTION_PROCESSORS = {
    **TEMPORARY_ACTION_PROCESSORS,
    ACTION_ANSWER_QUESTION: process_answer_question,
}


class DispatchingActionProcessor:
    def __init__(
        self,
        processors: Mapping[
            str,
            ActionProcessorFunc,
        ]
        | None = None,
    ) -> None:
        self.processors = dict(processors or DEFAULT_ACTION_PROCESSORS)

    async def process(
        self,
        bot: Bot,
        row: Mapping[str, object],
        *,
        context: ActionProcessingContext | None = None,
    ) -> Mapping[str, object]:
        action_code = str(row.get("action_code") or "").strip()
        if not action_code:
            raise PermanentJobError("action row is missing action_code")

        processor = self.processors.get(action_code)
        if processor is None:
            raise PermanentJobError(f"unknown action_code={action_code!r}")

        return await processor(bot, row, context=context)
