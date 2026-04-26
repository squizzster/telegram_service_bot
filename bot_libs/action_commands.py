from __future__ import annotations

from dataclasses import dataclass

from bot_libs.action_models import (
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_PROVIDER_CALCULATE_INCOME_EXPENSES,
    ACTION_PROVIDER_SHOW_ALL,
    ACTION_PROVIDER_SHOW_ALL_DETAILED,
    ACTION_PROVIDER_SHOW_EXPENSES,
    ACTION_PROVIDER_SHOW_INCOME,
    ACTION_SHOW_ALL,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
    ACTION_SHOW_INCOME,
)


@dataclass(frozen=True, slots=True)
class DirectActionCommand:
    command: str
    action_code: str
    provider_label: str


COMMAND_ACTIONS: dict[str, DirectActionCommand] = {
    "/calculate": DirectActionCommand(
        command="/calculate",
        action_code=ACTION_CALCULATE_INCOME_EXPENSES,
        provider_label=ACTION_PROVIDER_CALCULATE_INCOME_EXPENSES,
    ),
    "/calculate_force": DirectActionCommand(
        command="/calculate_force",
        action_code=ACTION_CALCULATE_INCOME_EXPENSES,
        provider_label=ACTION_PROVIDER_CALCULATE_INCOME_EXPENSES,
    ),
    "/calc": DirectActionCommand(
        command="/calc",
        action_code=ACTION_CALCULATE_INCOME_EXPENSES,
        provider_label=ACTION_PROVIDER_CALCULATE_INCOME_EXPENSES,
    ),
    "/show_expenses": DirectActionCommand(
        command="/show_expenses",
        action_code=ACTION_SHOW_EXPENSES,
        provider_label=ACTION_PROVIDER_SHOW_EXPENSES,
    ),
    "/show_income": DirectActionCommand(
        command="/show_income",
        action_code=ACTION_SHOW_INCOME,
        provider_label=ACTION_PROVIDER_SHOW_INCOME,
    ),
    "/show_all": DirectActionCommand(
        command="/show_all",
        action_code=ACTION_SHOW_ALL,
        provider_label=ACTION_PROVIDER_SHOW_ALL,
    ),
    "/show_all_detailed": DirectActionCommand(
        command="/show_all_detailed",
        action_code=ACTION_SHOW_ALL_DETAILED,
        provider_label=ACTION_PROVIDER_SHOW_ALL_DETAILED,
    ),
}


def direct_action_command_for_text(text: str | None) -> DirectActionCommand | None:
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    if not stripped.startswith("/"):
        return None

    first_token = stripped.split(maxsplit=1)[0].lower()
    command_name = first_token.split("@", 1)[0]
    return COMMAND_ACTIONS.get(command_name)
