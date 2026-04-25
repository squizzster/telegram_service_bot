from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from bot_libs.action_models import ProviderActionDetectionResult
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError

OPENAI_ACTION_DETECTION_PROVIDER = "openai"
DEFAULT_OPENAI_ACTION_PROMPT_ID = "pmpt_69ed0e37aab08193aef354f523d55a240171a57891089e80"
DEFAULT_OPENAI_ACTION_PROMPT_VERSION = "3"
PROMPT_ID_ENV_KEYS = (
    "OPENAI_ACTION_DETECTION_PROMPT_ID",
    "ACTION_DETECTION_OPENAI_PROMPT_ID",
)
PROMPT_VERSION_ENV_KEYS = (
    "OPENAI_ACTION_DETECTION_PROMPT_VERSION",
    "ACTION_DETECTION_OPENAI_PROMPT_VERSION",
)

log = logging.getLogger(__name__)


def resolve_openai_action_prompt_id() -> str:
    return _resolve_first_env(PROMPT_ID_ENV_KEYS, DEFAULT_OPENAI_ACTION_PROMPT_ID)


def resolve_openai_action_prompt_version() -> str:
    return _resolve_first_env(
        PROMPT_VERSION_ENV_KEYS,
        DEFAULT_OPENAI_ACTION_PROMPT_VERSION,
    )


async def detect_actions_with_openai(
    *,
    incoming_text: str,
) -> ProviderActionDetectionResult:
    prompt_id = resolve_openai_action_prompt_id()
    prompt_version = resolve_openai_action_prompt_version()
    log.debug(
        "OpenAI action detection request starting prompt_id=%s prompt_version=%s chars=%s",
        prompt_id,
        prompt_version,
        len(incoming_text),
    )

    try:
        response = await asyncio.to_thread(
            _detect_actions_sync,
            incoming_text,
            prompt_id,
            prompt_version,
        )
    except (PermanentJobError, RetryableJobError):
        raise
    except Exception as exc:
        raise _job_error_from_openai_error(exc) from exc

    response_text = _extract_response_text(response)
    raw_actions_text = _extract_actions_text(response_text)
    raw_response = _summarize_response(
        response,
        response_text=response_text,
        raw_actions_text=raw_actions_text,
    )
    log.debug(
        "OpenAI action detection request succeeded prompt_id=%s prompt_version=%s output_chars=%s",
        prompt_id,
        prompt_version,
        len(raw_actions_text),
    )
    return ProviderActionDetectionResult(
        provider=OPENAI_ACTION_DETECTION_PROVIDER,
        prompt_id=prompt_id,
        prompt_version=prompt_version,
        raw_actions_text=raw_actions_text,
        raw_response=raw_response,
    )


def _detect_actions_sync(
    incoming_text: str,
    prompt_id: str,
    prompt_version: str,
) -> object:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RetryableJobError("openai package is not installed") from exc

    client = OpenAI()
    return client.responses.create(
        prompt={
            "id": prompt_id,
            "version": prompt_version,
            "variables": {
                "incoming_text": incoming_text,
            },
        }
    )


def _extract_response_text(response: object) -> str:
    if isinstance(response, str):
        text = response
    else:
        text = _get_field(response, "output_text")
        if not isinstance(text, str):
            text = _extract_text_from_output(response)
    text = (text or "").strip()
    if not text:
        raise RetryableJobError("OpenAI action detection returned no output text")
    return text


def _extract_actions_text(response_text: object) -> str:
    text = str(response_text).strip()
    fenced_text = _strip_single_json_code_fence(text)
    return fenced_text if fenced_text is not None else text


def _strip_single_json_code_fence(text: str) -> str | None:
    lines = text.splitlines()
    if len(lines) < 3:
        return None
    first = lines[0].strip().lower()
    last = lines[-1].strip()
    if not first.startswith("```") or last != "```":
        return None
    fence_language = first[3:].strip()
    if fence_language not in {"", "json", "json_block"}:
        return None
    body = "\n".join(lines[1:-1]).strip()
    return body or None


def _extract_text_from_output(response: object) -> str:
    output = _get_field(response, "output")
    if not isinstance(output, list):
        return ""

    parts: list[str] = []
    for item in output:
        content = _get_field(item, "content")
        if not isinstance(content, list):
            continue
        for content_item in content:
            text = _get_field(content_item, "text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts)


def _summarize_response(
    response: object,
    *,
    response_text: str,
    raw_actions_text: str,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "output_text": response_text,
        "raw_actions_text": raw_actions_text,
    }
    for field_name in ("id", "model", "status", "created_at"):
        value = _json_safe(_get_field(response, field_name))
        if value is not None:
            summary[field_name] = value

    usage = _json_safe(_get_field(response, "usage"))
    if usage is not None:
        summary["usage"] = usage
    return summary


def _job_error_from_openai_error(exc: Exception) -> Exception:
    try:
        import openai
    except ImportError:
        return RetryableJobError(f"OpenAI action detection failed: {exc}")

    if isinstance(
        exc,
        (
            openai.APIConnectionError,
            openai.APITimeoutError,
            openai.RateLimitError,
            openai.InternalServerError,
        ),
    ):
        log.debug(
            "OpenAI action detection retryable failure error_class=%s error=%s",
            exc.__class__.__name__,
            exc,
        )
        return RetryableJobError(f"OpenAI action detection failed: {exc}")

    if isinstance(exc, openai.APIStatusError):
        status_code = exc.status_code
        log.debug(
            "OpenAI action detection status failure status_code=%s error_class=%s error=%s",
            status_code,
            exc.__class__.__name__,
            exc,
        )
        message = f"OpenAI action detection API error status={status_code}: {exc}"
        if status_code in {408, 409, 429} or status_code >= 500:
            return RetryableJobError(message)
        return PermanentJobError(message)

    log.debug(
        "OpenAI action detection unexpected failure error_class=%s error=%s",
        exc.__class__.__name__,
        exc,
    )
    return RetryableJobError(f"OpenAI action detection failed: {exc}")


def _get_field(value: object, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return getattr(value, field_name, None)


def _json_safe(value: object) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
            if _json_safe(item) is not None
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _json_safe(model_dump())
        except Exception:
            return None
    return str(value)


def _resolve_first_env(env_keys: tuple[str, ...], default: str) -> str:
    for env_key in env_keys:
        value = (os.getenv(env_key) or "").strip()
        if value:
            return value
    return default
