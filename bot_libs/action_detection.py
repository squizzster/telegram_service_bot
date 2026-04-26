from __future__ import annotations

import hashlib
import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from bot_libs.action_commands import direct_action_command_for_text
from bot_libs.action_detection_openai import (
    OPENAI_ACTION_DETECTION_PROVIDER,
    detect_actions_with_openai,
    resolve_openai_action_prompt_id,
)
from bot_libs.action_detection_validation import validate_provider_actions
from bot_libs.action_models import (
    ACTION_DETECTION_COMPLETE,
    ACTION_DETECTION_FAILED,
    ACTION_DETECTION_NOT_APPLICABLE,
    ACTION_DETECTION_PENDING,
    ACTION_DETECTION_PROCESSING,
    ProviderActionDetectionResult,
)
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError

ActionDetectionProvider = Callable[
    ...,
    Awaitable[ProviderActionDetectionResult],
]

log = logging.getLogger(__name__)


class ActionDetectionService:
    def __init__(
        self,
        *,
        queue_store: object,
        provider: ActionDetectionProvider = detect_actions_with_openai,
        provider_name: str = OPENAI_ACTION_DETECTION_PROVIDER,
        prompt_id: str | None = None,
        prompt_version: str | None = None,
    ) -> None:
        self.queue_store = queue_store
        self.provider = provider
        self.provider_name = provider_name
        self.prompt_id = prompt_id or resolve_openai_action_prompt_id()
        self.prompt_version = prompt_version

    async def detect_actions(self, row: Mapping[str, object]) -> dict[str, Any]:
        queue_id = _row_int(row, "id")
        status = _row_text(row.get("action_detection_status"))
        if status is None:
            status = ACTION_DETECTION_NOT_APPLICABLE

        if status == ACTION_DETECTION_NOT_APPLICABLE:
            return {"status": ACTION_DETECTION_NOT_APPLICABLE}
        if status == ACTION_DETECTION_COMPLETE:
            return _completed_result_from_row(row)
        if status == ACTION_DETECTION_FAILED:
            raise PermanentJobError("action detection is already failed")
        if status not in {ACTION_DETECTION_PENDING, ACTION_DETECTION_PROCESSING}:
            raise PermanentJobError(f"unknown action_detection_status={status!r}")

        incoming_text = _processing_text(row)
        if incoming_text is None:
            raise PermanentJobError("action detection requires processing_text")

        direct_command = direct_action_command_for_text(incoming_text)
        detection_provider_name = (
            "command" if direct_command is not None else self.provider_name
        )
        detection_prompt_id = (
            direct_command.command if direct_command is not None else self.prompt_id
        )
        detection_prompt_version = None if direct_command is not None else self.prompt_version
        incoming_text_sha256 = hashlib.sha256(incoming_text.encode("utf-8")).hexdigest()
        log.debug(
            "Action detection starting queue_id=%s provider=%s prompt_id=%s "
            "chars=%s sha256=%s",
            queue_id,
            detection_provider_name,
            detection_prompt_id,
            len(incoming_text),
            incoming_text_sha256,
        )
        await asyncio.to_thread(
            self.queue_store.mark_action_detection_processing,
            queue_id,
        )
        detection_run_id = await asyncio.to_thread(
                self.queue_store.start_action_detection_run,
                queue_id=queue_id,
                provider=detection_provider_name,
                prompt_id=detection_prompt_id,
                prompt_version=detection_prompt_version,
                incoming_text_chars=len(incoming_text),
                incoming_text_sha256=incoming_text_sha256,
            )

        provider_result: ProviderActionDetectionResult | None = None
        try:
            if direct_command is not None:
                normalized_json = {
                    "provider_labels": [direct_command.provider_label],
                    "action_codes": [direct_command.action_code],
                    "none": False,
                    "status": ACTION_DETECTION_COMPLETE,
                    "command": direct_command.command,
                }
                raw_response_json = {
                    "command": direct_command.command,
                    "provider_label": direct_command.provider_label,
                    "action_code": direct_command.action_code,
                }
                action_codes = (direct_command.action_code,)
            else:
                provider_result = await self.provider(incoming_text=incoming_text)
                catalog = await asyncio.to_thread(
                    self.queue_store.get_action_catalog_by_provider_label
                )
                normalized = validate_provider_actions(
                    provider_result.raw_actions_text,
                    catalog_by_provider_label=catalog,
                )
                normalized_json = normalized.as_json_dict()
                normalized_json["status"] = ACTION_DETECTION_COMPLETE
                raw_response_json = provider_result.raw_response
                action_codes = normalized.action_codes
            result_json = await asyncio.to_thread(
                self.queue_store.complete_action_detection,
                queue_id=queue_id,
                detection_run_id=detection_run_id,
                raw_response_json=raw_response_json,
                normalized_actions_json=normalized_json,
                action_codes=action_codes,
            )
        except RetryableJobError as exc:
            await asyncio.to_thread(
                self.queue_store.mark_action_detection_pending_after_retryable_failure,
                queue_id=queue_id,
                detection_run_id=detection_run_id,
                error=_error_text(exc),
                raw_response_json=_raw_response(provider_result),
            )
            raise
        except PermanentJobError as exc:
            await asyncio.to_thread(
                self.queue_store.mark_action_detection_failed,
                queue_id=queue_id,
                detection_run_id=detection_run_id,
                error=_error_text(exc),
                raw_response_json=_raw_response(provider_result),
            )
            raise

        log.info(
            "Action detection complete queue_id=%s detection_run_id=%s labels=%s "
            "actions=%s created_action_count=%s",
            queue_id,
            detection_run_id,
            ",".join(result_json.get("provider_labels", [])),
            ",".join(result_json.get("action_codes", [])),
            result_json.get("created_action_count"),
        )
        return result_json


def _completed_result_from_row(row: Mapping[str, object]) -> dict[str, Any]:
    raw_result = row.get("action_detection_result_json")
    if isinstance(raw_result, str) and raw_result.strip():
        try:
            parsed = json.loads(raw_result)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            parsed.setdefault("status", ACTION_DETECTION_COMPLETE)
            return parsed
    return {"status": ACTION_DETECTION_COMPLETE}


def _processing_text(row: Mapping[str, object]) -> str | None:
    value = row.get("processing_text")
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _row_int(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise PermanentJobError(f"queue row field {key!r} is missing")
    return int(value)


def _row_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _raw_response(
    provider_result: ProviderActionDetectionResult | None,
) -> dict[str, Any] | None:
    if provider_result is None:
        return None
    return provider_result.raw_response


def _error_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__
