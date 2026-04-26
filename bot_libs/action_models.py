from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ACTION_NONE = "NONE"
ACTION_LOG_EXPENSES = "LOG_EXPENSES"
ACTION_LOG_INCOME = "LOG_INCOME"
ACTION_SET_REMINDER = "SET_REMINDER"
ACTION_ANSWER_QUESTION = "ANSWER_QUESTION"
ACTION_CALCULATE_INCOME_EXPENSES = "CALCULATE_INCOME_EXPENSES"
ACTION_SHOW_EXPENSES = "SHOW_EXPENSES"
ACTION_SHOW_INCOME = "SHOW_INCOME"
ACTION_SHOW_ALL = "SHOW_ALL"
ACTION_SHOW_ALL_DETAILED = "SHOW_ALL_DETAILED"

ACTION_PROVIDER_NONE = "none"
ACTION_PROVIDER_LOG_EXPENSES = "reporting_expenses"
ACTION_PROVIDER_LOG_INCOME = "reporting_income"
ACTION_PROVIDER_SET_REMINDER = "set_a_reminder"
ACTION_PROVIDER_ASKING_A_QUESTION = "asking_a_question"
ACTION_PROVIDER_CALCULATE_INCOME_EXPENSES = "calculate_income_expenses"
ACTION_PROVIDER_SHOW_EXPENSES = "show_expenses"
ACTION_PROVIDER_SHOW_INCOME = "show_income"
ACTION_PROVIDER_SHOW_ALL = "show_all"
ACTION_PROVIDER_SHOW_ALL_DETAILED = "show_all_detailed"

ACTION_STATUS_QUEUED = "queued"
ACTION_STATUS_PROCESSING = "processing"
ACTION_STATUS_DONE = "done"
ACTION_STATUS_DEAD = "dead"
ACTION_STATUS_CANCELLED = "cancelled"

ACTION_DETECTION_NOT_APPLICABLE = "not_applicable"
ACTION_DETECTION_PENDING = "pending"
ACTION_DETECTION_PROCESSING = "processing"
ACTION_DETECTION_COMPLETE = "complete"
ACTION_DETECTION_FAILED = "failed"

ACTION_DETECTION_STATUSES = frozenset(
    {
        ACTION_DETECTION_NOT_APPLICABLE,
        ACTION_DETECTION_PENDING,
        ACTION_DETECTION_PROCESSING,
        ACTION_DETECTION_COMPLETE,
        ACTION_DETECTION_FAILED,
    }
)

TEXT_ACTION_CONTENT_TYPES = frozenset(
    {
        "text",
        "voice",
        "audio",
    }
)

ACTION_CATALOG_SEED: tuple[tuple[str, str, str, str, int, int], ...] = (
    (
        ACTION_NONE,
        ACTION_PROVIDER_NONE,
        "No action",
        "The text does not contain a supported action.",
        0,
        1,
    ),
    (
        ACTION_LOG_EXPENSES,
        ACTION_PROVIDER_LOG_EXPENSES,
        "Log expenses",
        "The user is reporting monetary outgoings or spending.",
        1,
        1,
    ),
    (
        ACTION_LOG_INCOME,
        ACTION_PROVIDER_LOG_INCOME,
        "Log income",
        "The user is reporting monetary incoming, earned funds, or paid work.",
        1,
        1,
    ),
    (
        ACTION_SET_REMINDER,
        ACTION_PROVIDER_SET_REMINDER,
        "Set reminder",
        "The user wants to be reminded about something in the future.",
        1,
        1,
    ),
    (
        ACTION_ANSWER_QUESTION,
        ACTION_PROVIDER_ASKING_A_QUESTION,
        "Answer question",
        "The user is asking a question that should be answered.",
        1,
        0,
    ),
    (
        ACTION_CALCULATE_INCOME_EXPENSES,
        ACTION_PROVIDER_CALCULATE_INCOME_EXPENSES,
        "Calculate income and expenses",
        "Normalize unprocessed income and expense source entries into ledger rows.",
        1,
        1,
    ),
    (
        ACTION_SHOW_EXPENSES,
        ACTION_PROVIDER_SHOW_EXPENSES,
        "Show expenses",
        "Show processed expense ledger rows.",
        1,
        1,
    ),
    (
        ACTION_SHOW_INCOME,
        ACTION_PROVIDER_SHOW_INCOME,
        "Show income",
        "Show processed income ledger rows.",
        1,
        1,
    ),
    (
        ACTION_SHOW_ALL,
        ACTION_PROVIDER_SHOW_ALL,
        "Show all income and expenses",
        "Show processed income and expense ledger rows.",
        1,
        1,
    ),
    (
        ACTION_SHOW_ALL_DETAILED,
        ACTION_PROVIDER_SHOW_ALL_DETAILED,
        "Show detailed income and expenses",
        "Show processed income and expense ledger rows with notes.",
        1,
        1,
    ),
)

KNOWN_PROVIDER_LABELS = frozenset(row[1] for row in ACTION_CATALOG_SEED)


@dataclass(frozen=True, slots=True)
class ActionCatalogEntry:
    id: int
    code: str
    provider_label: str
    name: str
    description: str
    is_executable: bool
    is_enabled: bool


@dataclass(frozen=True, slots=True)
class ProviderActionDetectionResult:
    provider: str
    prompt_id: str | None
    prompt_version: str | None
    raw_actions_text: str
    raw_response: dict[str, Any]


@dataclass(frozen=True, slots=True)
class NormalizedActionDetection:
    provider_labels: tuple[str, ...]
    action_codes: tuple[str, ...]
    none: bool

    def as_json_dict(self) -> dict[str, Any]:
        return {
            "provider_labels": list(self.provider_labels),
            "action_codes": list(self.action_codes),
            "none": self.none,
        }


def initial_action_detection_status(
    *,
    content_type: str,
    is_supported: bool,
    processing_text: str | None = None,
) -> str:
    del processing_text
    if not is_supported:
        return ACTION_DETECTION_NOT_APPLICABLE
    if content_type in TEXT_ACTION_CONTENT_TYPES:
        return ACTION_DETECTION_PENDING
    return ACTION_DETECTION_NOT_APPLICABLE
