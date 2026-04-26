from __future__ import annotations

from collections.abc import Mapping

from telegram import Bot

from bot_libs.action_models import (
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_NONE,
    ACTION_SET_REMINDER,
)
from bot_libs.action_processing_context import ActionProcessingContext


async def process_log_expenses(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
) -> dict[str, object]:
    del bot, context
    return _temporary_action_result(row, processor="temporary_log_expenses")


async def process_log_income(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
) -> dict[str, object]:
    del bot, context
    return _temporary_action_result(row, processor="temporary_log_income")


async def process_set_reminder(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
) -> dict[str, object]:
    del bot, context
    return _temporary_action_result(row, processor="temporary_set_reminder")


async def process_none(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
) -> dict[str, object]:
    del bot, context
    return {
        "outcome": "processed",
        "processor": "none_noop",
        "action_code": str(row.get("action_code") or ACTION_NONE),
        "none": True,
    }


def _temporary_action_result(
    row: Mapping[str, object],
    *,
    processor: str,
) -> dict[str, object]:
    action_code = str(row.get("action_code") or "")
    result = {
        "outcome": "processed",
        "processor": processor,
        "action_code": action_code,
    }
    if action_code in {ACTION_LOG_EXPENSES, ACTION_LOG_INCOME}:
        source_label = "income" if action_code == ACTION_LOG_INCOME else "expense"
        result.update(
            {
                "ledger_state": "pending_calculation",
                "ledger_status_text": (
                    f"Logged as {source_label} source. Pending next /calculate."
                ),
            }
        )
    return result


TEMPORARY_ACTION_PROCESSORS = {
    ACTION_LOG_EXPENSES: process_log_expenses,
    ACTION_LOG_INCOME: process_log_income,
    ACTION_SET_REMINDER: process_set_reminder,
    ACTION_NONE: process_none,
}
