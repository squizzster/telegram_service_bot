from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError

OPENAI_QUESTION_ANSWERING_PROVIDER = "openai"
DEFAULT_OPENAI_QUESTION_ANSWERING_PROMPT_ID = (
    "pmpt_69ed2980b0988195b26594cc5467f26f07bc6d43e5ee5db1"
)
PROMPT_ID_ENV_KEYS = (
    "OPENAI_QUESTION_ANSWERING_PROMPT_ID",
    "QUESTION_ANSWERING_OPENAI_PROMPT_ID",
)

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class QuestionAnsweringResult:
    provider: str
    prompt_id: str
    answer_text: str
    raw_response: dict[str, Any]


def resolve_openai_question_answering_prompt_id() -> str:
    return _resolve_first_env(
        PROMPT_ID_ENV_KEYS,
        DEFAULT_OPENAI_QUESTION_ANSWERING_PROMPT_ID,
    )


async def answer_question_with_openai(*, question_to_answer: str) -> QuestionAnsweringResult:
    prompt_id = resolve_openai_question_answering_prompt_id()
    log.debug(
        "OpenAI question answering request starting prompt_id=%s chars=%s",
        prompt_id,
        len(question_to_answer),
    )

    try:
        response = await asyncio.to_thread(
            _answer_question_sync,
            question_to_answer,
            prompt_id,
        )
    except (PermanentJobError, RetryableJobError):
        raise
    except Exception as exc:
        raise _job_error_from_openai_error(exc) from exc

    response_text = _extract_response_text(response)
    answer_text = _extract_answer_block(response_text)
    raw_response = _summarize_response(
        response,
        response_text=response_text,
        answer_text=answer_text,
    )
    log.debug(
        "OpenAI question answering request succeeded prompt_id=%s answer_chars=%s",
        prompt_id,
        len(answer_text),
    )
    return QuestionAnsweringResult(
        provider=OPENAI_QUESTION_ANSWERING_PROVIDER,
        prompt_id=prompt_id,
        answer_text=answer_text,
        raw_response=raw_response,
    )


def _answer_question_sync(question_to_answer: str, prompt_id: str) -> object:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RetryableJobError("openai package is not installed") from exc

    client = OpenAI()
    return client.responses.create(
        prompt={
            "id": prompt_id,
            "variables": {
                "question_to_answer": question_to_answer,
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
        raise RetryableJobError("OpenAI question answering returned no output text")
    return text


def _extract_answer_block(response_text: object) -> str:
    text = str(response_text).strip()
    lines = text.splitlines()
    start_index: int | None = None
    for index, line in enumerate(lines):
        if line.strip().lower() == "```answer":
            start_index = index + 1
            break
    if start_index is None:
        raise RetryableJobError("OpenAI question answering response missing answer block")

    for end_index in range(start_index, len(lines)):
        if lines[end_index].strip() == "```":
            answer = "\n".join(lines[start_index:end_index]).strip()
            if not answer:
                raise RetryableJobError("OpenAI question answering returned empty answer block")
            return answer

    raise RetryableJobError("OpenAI question answering answer block was not closed")


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
    answer_text: str,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "output_text": response_text,
        "answer_text": answer_text,
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
        return RetryableJobError(f"OpenAI question answering failed: {exc}")

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
            "OpenAI question answering retryable failure error_class=%s error=%s",
            exc.__class__.__name__,
            exc,
        )
        return RetryableJobError(f"OpenAI question answering failed: {exc}")

    if isinstance(exc, openai.APIStatusError):
        status_code = exc.status_code
        log.debug(
            "OpenAI question answering status failure status_code=%s error_class=%s error=%s",
            status_code,
            exc.__class__.__name__,
            exc,
        )
        message = f"OpenAI question answering API error status={status_code}: {exc}"
        if status_code in {408, 409, 429} or status_code >= 500:
            return RetryableJobError(message)
        return PermanentJobError(message)

    log.debug(
        "OpenAI question answering unexpected failure error_class=%s error=%s",
        exc.__class__.__name__,
        exc,
    )
    return RetryableJobError(f"OpenAI question answering failed: {exc}")


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
