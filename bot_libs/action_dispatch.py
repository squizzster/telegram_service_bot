from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from telegram import Bot

from bot_libs.action_models import (
    ACTION_ANSWER_QUESTION,
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_SHOW_ALL,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
    ACTION_SHOW_INCOME,
)
from bot_libs.action_processing_context import ActionProcessingContext
from bot_libs.action_processors.income_expenses import (
    process_calculate_income_expenses,
    process_show_income_expense_report,
)
from bot_libs.action_processors.questions import process_answer_question
from bot_libs.action_processors.temporary import TEMPORARY_ACTION_PROCESSORS
from bot_libs.queue_processor_errors import PermanentJobError

ActionProcessorFunc = Callable[..., Awaitable[Mapping[str, object]]]

DEFAULT_ACTION_PROCESSORS = {
    **TEMPORARY_ACTION_PROCESSORS,
    ACTION_ANSWER_QUESTION: process_answer_question,
    ACTION_CALCULATE_INCOME_EXPENSES: process_calculate_income_expenses,
    ACTION_SHOW_EXPENSES: process_show_income_expense_report,
    ACTION_SHOW_INCOME: process_show_income_expense_report,
    ACTION_SHOW_ALL: process_show_income_expense_report,
    ACTION_SHOW_ALL_DETAILED: process_show_income_expense_report,
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
