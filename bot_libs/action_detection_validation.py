from __future__ import annotations

import json
from collections.abc import Mapping

from bot_libs.action_models import (
    ACTION_NONE,
    ACTION_PROVIDER_NONE,
    ActionCatalogEntry,
    KNOWN_PROVIDER_LABELS,
    NormalizedActionDetection,
)
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError


def validate_provider_actions(
    raw_actions_text: str,
    *,
    catalog_by_provider_label: Mapping[str, ActionCatalogEntry],
) -> NormalizedActionDetection:
    try:
        payload = json.loads(raw_actions_text)
    except json.JSONDecodeError as exc:
        raise RetryableJobError(f"action detection returned invalid JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise RetryableJobError("action detection response must be a JSON array")
    if not payload:
        raise RetryableJobError("action detection response must not be empty")

    provider_labels: list[str] = []
    seen_labels: set[str] = set()
    for item in payload:
        if not isinstance(item, str):
            raise RetryableJobError("action detection labels must be strings")
        label = item.strip()
        if not label:
            raise RetryableJobError("action detection labels must not be empty")
        if label in seen_labels:
            raise RetryableJobError(f"duplicate action detection label {label!r}")
        if label not in KNOWN_PROVIDER_LABELS:
            raise RetryableJobError(f"unknown action detection label {label!r}")
        if label not in catalog_by_provider_label:
            raise PermanentJobError(f"action catalog missing provider_label={label!r}")
        provider_labels.append(label)
        seen_labels.add(label)

    has_none = ACTION_PROVIDER_NONE in seen_labels
    if has_none and len(provider_labels) > 1:
        raise RetryableJobError("action detection label 'none' cannot be mixed")

    if has_none:
        none_entry = catalog_by_provider_label[ACTION_PROVIDER_NONE]
        if none_entry.code != ACTION_NONE or not none_entry.is_enabled:
            raise PermanentJobError("action catalog NONE entry is not usable")
        return NormalizedActionDetection(
            provider_labels=tuple(provider_labels),
            action_codes=(),
            none=True,
        )

    action_codes: list[str] = []
    for label in provider_labels:
        entry = catalog_by_provider_label[label]
        if not entry.is_enabled:
            raise PermanentJobError(f"action catalog entry disabled label={label!r}")
        if not entry.is_executable:
            raise PermanentJobError(
                f"action catalog real action is not executable label={label!r}"
            )
        action_codes.append(entry.code)

    return NormalizedActionDetection(
        provider_labels=tuple(provider_labels),
        action_codes=tuple(action_codes),
        none=False,
    )
