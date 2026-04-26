from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any

from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError

OPENAI_INCOME_EXPENSE_CALCULATION_PROVIDER = "openai"
DEFAULT_OPENAI_INCOME_EXPENSE_CALCULATION_PROMPT_ID = (
    "pmpt_69ee3c457fc081938576d3e02a80b6bd0569ee240260b2f9"
)
PROMPT_ID_ENV_KEYS = (
    "OPENAI_INCOME_EXPENSE_CALCULATION_PROMPT_ID",
    "INCOME_EXPENSE_CALCULATION_OPENAI_PROMPT_ID",
)

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IncomeExpenseCalculationResult:
    provider: str
    prompt_id: str
    entries: tuple[dict[str, object], ...]
    raw_response: dict[str, Any]


def resolve_openai_income_expense_calculation_prompt_id() -> str:
    return _resolve_first_env(
        PROMPT_ID_ENV_KEYS,
        DEFAULT_OPENAI_INCOME_EXPENSE_CALCULATION_PROMPT_ID,
    )


async def calculate_income_expenses_with_openai(
    *,
    messy_csv_data: str,
) -> IncomeExpenseCalculationResult:
    prompt_id = resolve_openai_income_expense_calculation_prompt_id()
    log.debug(
        "OpenAI income/expense calculation starting prompt_id=%s chars=%s",
        prompt_id,
        len(messy_csv_data),
    )

    try:
        response = await asyncio.to_thread(
            _calculate_income_expenses_sync,
            messy_csv_data,
            prompt_id,
        )
    except (PermanentJobError, RetryableJobError):
        raise
    except Exception as exc:
        raise _job_error_from_openai_error(exc) from exc

    response_text = _extract_response_text(response)
    entries = parse_income_expense_entries(response_text)
    raw_response = _summarize_response(
        response,
        response_text=response_text,
        entries=entries,
    )
    log.debug(
        "OpenAI income/expense calculation succeeded prompt_id=%s entries=%s",
        prompt_id,
        len(entries),
    )
    return IncomeExpenseCalculationResult(
        provider=OPENAI_INCOME_EXPENSE_CALCULATION_PROVIDER,
        prompt_id=prompt_id,
        entries=entries,
        raw_response=raw_response,
    )


def parse_income_expense_entries(response_text: str) -> tuple[dict[str, object], ...]:
    text = _strip_single_code_fence(str(response_text).strip()) or str(response_text).strip()
    if not text:
        raise RetryableJobError("income/expense calculation returned empty output")

    parsed_json = _try_parse_json_entries(text)
    if parsed_json is not None:
        return parsed_json

    parsed_csv = _try_parse_csv_entries(text)
    if parsed_csv is not None:
        return parsed_csv

    parsed_markdown = _try_parse_markdown_table_entries(text)
    if parsed_markdown is not None:
        return parsed_markdown

    raise RetryableJobError("income/expense calculation returned unparseable output")


def _calculate_income_expenses_sync(messy_csv_data: str, prompt_id: str) -> object:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RetryableJobError("openai package is not installed") from exc

    client = OpenAI()
    return client.responses.create(
        prompt={
            "id": prompt_id,
            "variables": {
                "messy_csv_data": messy_csv_data,
            },
        },
        input=[],
        reasoning={"summary": "auto"},
        store=True,
    )


def _try_parse_json_entries(text: str) -> tuple[dict[str, object], ...] | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        for key in ("entries", "rows", "data"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return _normalize_parsed_rows(rows)
        return None
    if isinstance(payload, list):
        return _normalize_parsed_rows(payload)
    return None


def _try_parse_csv_entries(text: str) -> tuple[dict[str, object], ...] | None:
    if "," not in text:
        return None
    try:
        rows = list(csv.DictReader(io.StringIO(text)))
    except csv.Error:
        return None
    if not rows or not rows[0]:
        return None
    if not _looks_like_income_expense_headers(rows[0].keys()):
        return None
    return _normalize_parsed_rows(rows)


def _try_parse_markdown_table_entries(text: str) -> tuple[dict[str, object], ...] | None:
    table_lines = [line.strip() for line in text.splitlines() if "|" in line]
    if len(table_lines) < 2:
        return None
    header_cells = _split_markdown_row(table_lines[0])
    if not _looks_like_income_expense_headers(header_cells):
        return None

    rows: list[dict[str, str]] = []
    for line in table_lines[1:]:
        cells = _split_markdown_row(line)
        if _is_markdown_separator(cells):
            continue
        if len(cells) != len(header_cells):
            continue
        rows.append(dict(zip(header_cells, cells)))
    if not rows:
        return None
    return _normalize_parsed_rows(rows)


def _normalize_parsed_rows(rows: list[object]) -> tuple[dict[str, object], ...]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise RetryableJobError("income/expense row is not an object")
        row_by_key = {_normalize_key(key): value for key, value in row.items()}
        normalized.append(
            {
                "entry_id": _required(row_by_key, "entry_id"),
                "date_time_utc": _required(
                    row_by_key,
                    "date_time_utc",
                    "entry_date_time_utc",
                    "entry_data_time",
                ),
                "direction": _required(row_by_key, "direction"),
                "description": _required(row_by_key, "description"),
                "notes": str(row_by_key.get("notes") or "").strip(),
                "price_value": _required(row_by_key, "price_value", "price"),
            }
        )
    return tuple(normalized)


def _required(row: dict[str, object], *keys: str) -> object:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return value
    raise RetryableJobError(f"income/expense row missing field {keys[0]!r}")


def _looks_like_income_expense_headers(headers: object) -> bool:
    normalized_headers = {_normalize_key(header) for header in headers}
    return {
        "entry_id",
        "direction",
        "description",
    }.issubset(normalized_headers) and (
        "date_time_utc" in normalized_headers
        or "entry_date_time_utc" in normalized_headers
        or "entry_data_time" in normalized_headers
    ) and ("price" in normalized_headers or "price_value" in normalized_headers)


def _normalize_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _split_markdown_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _is_markdown_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{1,}:?", cell.strip()) for cell in cells)


def _strip_single_code_fence(text: str) -> str | None:
    lines = text.splitlines()
    if len(lines) < 3:
        return None
    first = lines[0].strip().lower()
    last = lines[-1].strip()
    if not first.startswith("```") or last != "```":
        return None
    return "\n".join(lines[1:-1]).strip() or None


def _extract_response_text(response: object) -> str:
    if isinstance(response, str):
        text = response
    else:
        text = _get_field(response, "output_text")
        if not isinstance(text, str):
            text = _extract_text_from_output(response)
    text = (text or "").strip()
    if not text:
        raise RetryableJobError("OpenAI income/expense calculation returned no output text")
    return text


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
    entries: tuple[dict[str, object], ...],
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "output_text": response_text,
        "entries": list(entries),
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
        return RetryableJobError(f"OpenAI income/expense calculation failed: {exc}")

    if isinstance(
        exc,
        (
            openai.APIConnectionError,
            openai.APITimeoutError,
            openai.RateLimitError,
            openai.InternalServerError,
        ),
    ):
        return RetryableJobError(f"OpenAI income/expense calculation failed: {exc}")

    if isinstance(exc, openai.APIStatusError):
        status_code = exc.status_code
        message = f"OpenAI income/expense calculation API error status={status_code}: {exc}"
        if status_code in {408, 409, 429} or status_code >= 500:
            return RetryableJobError(message)
        return PermanentJobError(message)

    return RetryableJobError(f"OpenAI income/expense calculation failed: {exc}")


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
            return repr(value)
    return repr(value)


def _resolve_first_env(keys: tuple[str, ...], default: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    return default
