from __future__ import annotations

import unittest
from dataclasses import replace

from bot_libs.action_detection_validation import validate_provider_actions
from bot_libs.action_models import (
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_NONE,
    ACTION_PROVIDER_LOG_EXPENSES,
    ACTION_PROVIDER_LOG_INCOME,
    ACTION_PROVIDER_NONE,
    ActionCatalogEntry,
)
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError


def entry(
    *,
    id: int,
    code: str,
    provider_label: str,
    is_executable: bool = True,
    is_enabled: bool = True,
) -> ActionCatalogEntry:
    return ActionCatalogEntry(
        id=id,
        code=code,
        provider_label=provider_label,
        name=code,
        description=code,
        is_executable=is_executable,
        is_enabled=is_enabled,
    )


def catalog() -> dict[str, ActionCatalogEntry]:
    none = entry(
        id=1,
        code=ACTION_NONE,
        provider_label=ACTION_PROVIDER_NONE,
        is_executable=False,
    )
    expenses = entry(
        id=2,
        code=ACTION_LOG_EXPENSES,
        provider_label=ACTION_PROVIDER_LOG_EXPENSES,
    )
    income = entry(
        id=3,
        code=ACTION_LOG_INCOME,
        provider_label=ACTION_PROVIDER_LOG_INCOME,
    )
    return {
        item.provider_label: item
        for item in (
            none,
            expenses,
            income,
        )
    }


class ActionDetectionValidationTests(unittest.TestCase):
    def test_valid_real_actions_map_to_internal_codes(self) -> None:
        result = validate_provider_actions(
            '["reporting_income","reporting_expenses"]',
            catalog_by_provider_label=catalog(),
        )

        self.assertEqual(
            result.provider_labels,
            (ACTION_PROVIDER_LOG_INCOME, ACTION_PROVIDER_LOG_EXPENSES),
        )
        self.assertEqual(
            result.action_codes,
            (ACTION_LOG_INCOME, ACTION_LOG_EXPENSES),
        )
        self.assertFalse(result.none)

    def test_none_is_valid_without_child_actions(self) -> None:
        result = validate_provider_actions(
            '["none"]',
            catalog_by_provider_label=catalog(),
        )

        self.assertEqual(result.provider_labels, (ACTION_PROVIDER_NONE,))
        self.assertEqual(result.action_codes, ())
        self.assertTrue(result.none)

    def test_invalid_provider_shapes_are_retryable(self) -> None:
        invalid_outputs = (
            "not JSON",
            "{}",
            "[]",
            '["unknown_action"]',
            '["none","reporting_income"]',
            '["reporting_income","reporting_income"]',
            '["reporting_income", 3]',
        )

        for raw_actions_text in invalid_outputs:
            with self.subTest(raw_actions_text=raw_actions_text):
                with self.assertRaises(RetryableJobError):
                    validate_provider_actions(
                        raw_actions_text,
                        catalog_by_provider_label=catalog(),
                    )

    def test_catalog_configuration_errors_are_permanent(self) -> None:
        current_catalog = catalog()
        current_catalog[ACTION_PROVIDER_LOG_EXPENSES] = replace(
            current_catalog[ACTION_PROVIDER_LOG_EXPENSES],
            is_enabled=False,
        )

        with self.assertRaises(PermanentJobError):
            validate_provider_actions(
                '["reporting_expenses"]',
                catalog_by_provider_label=current_catalog,
            )

        missing_catalog = catalog()
        del missing_catalog[ACTION_PROVIDER_LOG_EXPENSES]
        with self.assertRaises(PermanentJobError):
            validate_provider_actions(
                '["reporting_expenses"]',
                catalog_by_provider_label=missing_catalog,
            )
