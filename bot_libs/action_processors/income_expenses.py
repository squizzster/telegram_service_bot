from __future__ import annotations

import asyncio
import csv
import html
import io
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from telegram import Bot
from telegram.error import TelegramError

from bot_libs.action_models import (
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_SHOW_ALL,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
    ACTION_SHOW_INCOME,
)
from bot_libs.action_processing_context import ActionProcessingContext
from bot_libs.income_expense_calculation_openai import (
    IncomeExpenseCalculationResult,
    calculate_income_expenses_with_openai,
)
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError
from bot_libs.stage_names import (
    STAGE_CALCULATING_INCOME_EXPENSES,
    STAGE_SENDING_ACTION_RESPONSE,
)
from bot_libs.telegram_error_policy import job_error_from_telegram_error
from bot_libs.telegram_message_utils import split_telegram_text

log = logging.getLogger(__name__)
RECALCULATION_LOOKBACK_DAYS = 7

IncomeExpenseCalculationProvider = Callable[
    ...,
    Awaitable[IncomeExpenseCalculationResult],
]


async def process_calculate_income_expenses(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
    provider: IncomeExpenseCalculationProvider = calculate_income_expenses_with_openai,
) -> dict[str, object]:
    queue_store = _queue_store(context)
    action_job_id = _row_int(row, "id")
    force_recalculation = _is_force_calculation_command(row)

    existing_rows = await asyncio.to_thread(
        queue_store.get_income_expense_rows_for_calculation_action,
        action_job_id,
    )
    if existing_rows:
        message_text = _format_calculation_summary(
            source_count=0,
            inserted_rows=existing_rows,
            reused=True,
        )
        message_ids = await _send_action_message(
            bot,
            row,
            context=context,
            outbound_kind="income_expense_calculation",
            message_text=message_text,
        )
        return {
            "outcome": "processed",
            "processor": "income_expense_calculation",
            "action_code": ACTION_CALCULATE_INCOME_EXPENSES,
            "source_action_count": 0,
            "inserted_count": len(existing_rows),
            "reused_existing_result": True,
            "message_ids": message_ids,
        }

    calculation_reference_utc = _calculation_reference_datetime(row)
    recalculation_window_start_utc = calculation_reference_utc - timedelta(
        days=RECALCULATION_LOOKBACK_DAYS
    )
    unprocessed_source_rows = await asyncio.to_thread(
        queue_store.get_recent_income_expense_source_actions,
        window_start_utc=recalculation_window_start_utc,
        only_unprocessed=True,
    )
    if not force_recalculation and not unprocessed_source_rows:
        message_ids = await _send_action_message(
            bot,
            row,
            context=context,
            outbound_kind="income_expense_calculation",
            message_text=(
                "No unprocessed income or expense voice entries found.\n"
                "Use /calculate_force to recalculate the last 7 days anyway."
            ),
        )
        return {
            "outcome": "processed",
            "processor": "income_expense_calculation",
            "action_code": ACTION_CALCULATE_INCOME_EXPENSES,
            "source_action_count": 0,
            "inserted_count": 0,
            "force_recalculation": False,
            "message_ids": message_ids,
        }

    source_rows = await asyncio.to_thread(
        queue_store.get_recent_income_expense_source_actions,
        window_start_utc=recalculation_window_start_utc,
    )
    if not source_rows:
        message_ids = await _send_action_message(
            bot,
            row,
            context=context,
            outbound_kind="income_expense_calculation",
            message_text="No income or expense entries found in the last 7 days.",
        )
        return {
            "outcome": "processed",
            "processor": "income_expense_calculation",
            "action_code": ACTION_CALCULATE_INCOME_EXPENSES,
            "source_action_count": 0,
            "inserted_count": 0,
            "message_ids": message_ids,
        }

    messy_csv_data = _build_messy_income_expense_csv(source_rows)
    await _set_stage(context, STAGE_CALCULATING_INCOME_EXPENSES)
    provider_result = await provider(messy_csv_data=messy_csv_data)
    _validate_calculated_entries(provider_result.entries, source_rows)
    inserted_rows = await asyncio.to_thread(
        queue_store.record_income_expense_calculation,
        calculation_action_id=action_job_id,
        source_action_ids=tuple(int(item["action_id"]) for item in source_rows),
        entries=provider_result.entries,
        recalculation_window_start_utc=recalculation_window_start_utc,
    )

    message_text = _format_calculation_summary(
        source_count=len(source_rows),
        inserted_rows=inserted_rows,
        reused=False,
    )
    message_ids = await _send_action_message(
        bot,
        row,
        context=context,
        outbound_kind="income_expense_calculation",
        message_text=message_text,
    )
    return {
        "outcome": "processed",
        "processor": "income_expense_calculation",
        "action_code": ACTION_CALCULATE_INCOME_EXPENSES,
        "provider": provider_result.provider,
        "prompt_id": provider_result.prompt_id,
        "source_action_count": len(source_rows),
        "inserted_count": len(inserted_rows),
        "unprocessed_source_count": len(unprocessed_source_rows),
        "force_recalculation": force_recalculation,
        "recalculation_lookback_days": RECALCULATION_LOOKBACK_DAYS,
        "recalculation_window_start_utc": _display_timestamp(
            recalculation_window_start_utc.isoformat()
        ),
        "messy_csv_data": messy_csv_data,
        "raw_response": provider_result.raw_response,
        "message_ids": message_ids,
    }


async def process_show_income_expense_report(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None = None,
) -> dict[str, object]:
    queue_store = _queue_store(context)
    action_code = str(row.get("action_code") or "")
    direction = _report_direction_for_action(action_code)
    report_style = _report_style_for_action(action_code)
    rows = await asyncio.to_thread(
        queue_store.get_income_expense_rows_for_week,
        direction=direction,
    )
    unprocessed_sources = await asyncio.to_thread(
        queue_store.get_unprocessed_income_expense_source_actions
    )
    if report_style == "detailed":
        message_text = _format_detailed_report_rows(rows, direction=direction)
        message_text = _prepend_out_of_date_warning(
            message_text,
            has_unprocessed_sources=bool(unprocessed_sources),
            html_format=False,
        )
        parse_mode = None
    else:
        message_text = _format_friendly_report_rows(rows, direction=direction)
        message_text = _prepend_out_of_date_warning(
            message_text,
            has_unprocessed_sources=bool(unprocessed_sources),
            html_format=True,
        )
        parse_mode = "HTML"
    message_ids = await _send_action_message(
        bot,
        row,
        context=context,
        outbound_kind="income_expense_report",
        message_text=message_text,
        parse_mode=parse_mode,
    )
    return {
        "outcome": "processed",
        "processor": "income_expense_report",
        "action_code": action_code,
        "direction": direction or "all",
        "report_style": report_style,
        "row_count": len(rows),
        "unprocessed_source_count": len(unprocessed_sources),
        "message_ids": message_ids,
    }


def _build_messy_income_expense_csv(source_rows: tuple[Mapping[str, object], ...]) -> str:
    grouped: OrderedDict[int, dict[str, object]] = OrderedDict()
    for row in source_rows:
        entry_id = int(row["entry_id"])
        group = grouped.setdefault(
            entry_id,
            {
                "entry_id": entry_id,
                "date_time_utc": _display_timestamp(str(row["date_time_utc"])),
                "directions": set(),
                "description": str(row.get("description") or ""),
            },
        )
        action_code = row.get("action_code")
        if action_code == ACTION_LOG_INCOME:
            group["directions"].add("incoming")
        elif action_code == ACTION_LOG_EXPENSES:
            group["directions"].add("outgoing")

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["entry_id", "date_time_utc", "incoming_outgoing", "description"])
    for group in grouped.values():
        directions = group["directions"]
        if directions == {"incoming", "outgoing"}:
            incoming_outgoing = "incoming_outgoing"
        elif directions == {"incoming"}:
            incoming_outgoing = "incoming"
        else:
            incoming_outgoing = "outgoing"
        writer.writerow(
            [
                group["entry_id"],
                group["date_time_utc"],
                incoming_outgoing,
                group["description"],
            ]
        )
    return output.getvalue().strip()


def _validate_calculated_entries(
    entries: tuple[Mapping[str, object], ...],
    source_rows: tuple[Mapping[str, object], ...],
) -> None:
    available = {
        (int(row["entry_id"]), _direction_for_source_action(str(row["action_code"])))
        for row in source_rows
    }
    for entry in entries:
        entry_id = int(entry.get("entry_id", 0))
        direction = str(entry.get("direction") or "").strip().lower()
        if direction not in {"incoming", "outgoing"}:
            raise RetryableJobError(
                f"income/expense result has invalid direction entry_id={entry_id}"
            )
        if (entry_id, direction) not in available:
            raise RetryableJobError(
                "income/expense result references source action not in batch "
                f"entry_id={entry_id} direction={direction}"
            )


def _direction_for_source_action(action_code: str) -> str:
    if action_code == ACTION_LOG_INCOME:
        return "incoming"
    if action_code == ACTION_LOG_EXPENSES:
        return "outgoing"
    raise RetryableJobError(f"unsupported income/expense source action {action_code!r}")


def _is_force_calculation_command(row: Mapping[str, object]) -> bool:
    processing_text = str(row.get("processing_text") or "").strip().lower()
    if not processing_text:
        return False
    first_token = processing_text.split(maxsplit=1)[0]
    command_name = first_token.split("@", 1)[0]
    return command_name == "/calculate_force"


def _calculation_reference_datetime(row: Mapping[str, object]) -> datetime:
    raw_value = row.get("source_telegram_date") or row.get("telegram_date")
    if raw_value is None:
        return datetime.now(timezone.utc)

    text = str(raw_value).strip()
    if not text:
        return datetime.now(timezone.utc)

    try:
        if "T" in text or text.endswith("Z") or "+" in text:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.now(timezone.utc)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _report_direction_for_action(action_code: str) -> str | None:
    if action_code == ACTION_SHOW_EXPENSES:
        return "outgoing"
    if action_code == ACTION_SHOW_INCOME:
        return "incoming"
    if action_code == ACTION_SHOW_ALL:
        return None
    if action_code == ACTION_SHOW_ALL_DETAILED:
        return None
    raise PermanentJobError(f"unsupported income/expense report action={action_code!r}")


def _report_style_for_action(action_code: str) -> str:
    if action_code == ACTION_SHOW_ALL_DETAILED:
        return "detailed"
    if action_code in {ACTION_SHOW_EXPENSES, ACTION_SHOW_INCOME, ACTION_SHOW_ALL}:
        return "friendly"
    raise PermanentJobError(f"unsupported income/expense report action={action_code!r}")


async def _send_action_message(
    bot: Bot,
    row: Mapping[str, object],
    *,
    context: ActionProcessingContext | None,
    outbound_kind: str,
    message_text: str,
    parse_mode: str | None = None,
) -> list[int]:
    outbound_state = _row_outbound_state(row)
    kind_state = _outbound_kind_state(outbound_state, outbound_kind)
    persisted_message_ids = _persisted_message_ids(kind_state)
    if persisted_message_ids:
        return [message_id for _, message_id in sorted(persisted_message_ids.items())]

    await _set_stage(context, STAGE_SENDING_ACTION_RESPONSE)
    chat_id = row.get("chat_id")
    if chat_id is None:
        raise RetryableJobError("chat_id missing for income/expense response send")

    message_thread_id = row.get("message_thread_id")
    source_message_id = row.get("message_id")
    sent_message_ids: list[int] = []
    messages = list(kind_state.get("messages", []))
    for index, chunk in enumerate(split_telegram_text(message_text)):
        send_kwargs: dict[str, object] = {
            "chat_id": chat_id,
            "text": chunk,
        }
        if parse_mode is not None:
            send_kwargs["parse_mode"] = parse_mode
        if message_thread_id is not None:
            send_kwargs["message_thread_id"] = int(message_thread_id)
        if index == 0 and source_message_id is not None:
            send_kwargs["reply_to_message_id"] = int(source_message_id)
            send_kwargs["allow_sending_without_reply"] = False
        try:
            sent_message = await bot.send_message(**send_kwargs)
        except TelegramError as exc:
            raise job_error_from_telegram_error(
                exc,
                action="send_income_expense_response",
            ) from exc

        message_id = int(sent_message.message_id)
        messages.append({"index": index, "message_id": message_id})
        kind_state = {
            **kind_state,
            "message_text": message_text,
            "messages": messages,
        }
        outbound_state[outbound_kind] = kind_state
        await _set_outbound_json(context, outbound_state, STAGE_SENDING_ACTION_RESPONSE)
        sent_message_ids.append(message_id)
    return sent_message_ids


def _format_calculation_summary(
    *,
    source_count: int,
    inserted_rows: tuple[Mapping[str, object], ...],
    reused: bool,
) -> str:
    prefix = "Calculation already recorded." if reused else "Calculation complete."
    incoming_total = _total_for_direction(inserted_rows, "incoming")
    outgoing_total = _total_for_direction(inserted_rows, "outgoing")
    return (
        f"{prefix}\n"
        f"Source action rows: {source_count}\n"
        f"Ledger rows: {len(inserted_rows)}\n"
        f"Incoming: {incoming_total:.2f}\n"
        f"Outgoing: {outgoing_total:.2f}\n"
        f"Net: {(incoming_total - outgoing_total):.2f}"
    )


def _format_detailed_report_rows(
    rows: tuple[Mapping[str, object], ...],
    *,
    direction: str | None,
) -> str:
    if not rows:
        label = direction or "income/expense"
        return f"No processed {label} rows found."

    week_year = int(rows[0]["week_year"])
    week_number = int(rows[0]["week_number"])
    title_direction = {
        "incoming": "income",
        "outgoing": "expenses",
        None: "income and expenses",
    }[direction]
    lines = [f"{title_direction.title()} for {week_year}-W{week_number:02d}"]
    for row in rows:
        lines.append(
            "{date} {direction} {price} - {description}".format(
                date=str(row["entry_date_time_utc"]),
                direction=str(row["direction"]),
                price=str(row["price_value"]),
                description=str(row["description"]),
            )
        )
        notes = str(row.get("notes") or "").strip()
        if notes:
            lines.append(f"  notes: {notes}")

    incoming_total = _total_for_direction(rows, "incoming")
    outgoing_total = _total_for_direction(rows, "outgoing")
    if direction == "incoming":
        lines.append(f"Total income: {incoming_total:.2f}")
    elif direction == "outgoing":
        lines.append(f"Total expenses: {outgoing_total:.2f}")
    else:
        lines.append(f"Total income: {incoming_total:.2f}")
        lines.append(f"Total expenses: {outgoing_total:.2f}")
        lines.append(f"Net: {(incoming_total - outgoing_total):.2f}")
    return "\n".join(lines)


def _prepend_out_of_date_warning(
    message_text: str,
    *,
    has_unprocessed_sources: bool,
    html_format: bool,
) -> str:
    if not has_unprocessed_sources:
        return message_text
    if html_format:
        warning = (
            "<b>The calculation below is out-of-date; there are unprocessed "
            "voice entries.</b>\n"
            "<i>Use /calculate if you wish. We auto-calculate on Sunday "
            "23:59:59 each week.</i>"
        )
    else:
        warning = (
            "The calculation below is out-of-date; there are unprocessed "
            "voice entries.\n"
            "Use /calculate if you wish. We auto-calculate on Sunday "
            "23:59:59 each week."
        )
    return f"{warning}\n\n{message_text}"


def _format_friendly_report_rows(
    rows: tuple[Mapping[str, object], ...],
    *,
    direction: str | None,
) -> str:
    if not rows:
        label = {
            "incoming": "income",
            "outgoing": "expenses",
            None: "income/expense",
        }[direction]
        return html.escape(f"No processed {label} rows found.")

    lines: list[str] = []
    current_month_key: tuple[int, int] | None = None
    for row in rows:
        entry_date = _parse_entry_datetime(str(row["entry_date_time_utc"]))
        month_key = (entry_date.year, entry_date.month)
        if month_key != current_month_key:
            if lines:
                lines.append("")
            lines.append(f"<b>{html.escape(entry_date.strftime('%B %Y').upper())}:</b>")
            current_month_key = month_key

        short_direction = "in" if str(row["direction"]) == "incoming" else "out"
        lines.append(
            "{day} - {direction} - {price} - {description}".format(
                day=html.escape(_friendly_day_label(entry_date)),
                direction=short_direction,
                price=html.escape(_format_currency(row["price_value"])),
                description=html.escape(str(row["description"])),
            )
        )

    incoming_total = _total_for_direction(rows, "incoming")
    outgoing_total = _total_for_direction(rows, "outgoing")
    lines.append("")
    if direction == "incoming":
        lines.append(f"<b>Total income:</b> {html.escape(_format_currency(incoming_total))}")
    elif direction == "outgoing":
        lines.append(
            f"<b>Total expenses:</b> {html.escape(_format_currency(outgoing_total))}"
        )
    else:
        lines.append(f"<b>Total income:</b> {html.escape(_format_currency(incoming_total))}")
        lines.append(
            f"<b>Total expenses:</b> {html.escape(_format_currency(outgoing_total))}"
        )
        lines.append(
            f"<b>Net:</b> {html.escape(_format_currency(incoming_total - outgoing_total))}"
        )
    return "\n".join(lines)


def _total_for_direction(
    rows: tuple[Mapping[str, object], ...],
    direction: str,
) -> Decimal:
    total = Decimal("0.00")
    for row in rows:
        if str(row.get("direction") or "") != direction:
            continue
        try:
            total += Decimal(str(row["price_value"]))
        except (InvalidOperation, KeyError) as exc:
            raise PermanentJobError("stored income/expense price is invalid") from exc
    return total


def _format_currency(value: object) -> str:
    amount = Decimal(str(value))
    prefix = "-" if amount < 0 else ""
    return f"{prefix}\u00a3{abs(amount):.2f}"


def _friendly_day_label(value: datetime) -> str:
    return f"{value.strftime('%a')} {value.day:02d}{_ordinal_suffix(value.day)}"


def _ordinal_suffix(day: int) -> str:
    if 10 <= day % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def _parse_entry_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise PermanentJobError("stored income/expense timestamp is empty")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _queue_store(context: ActionProcessingContext | None) -> object:
    queue_store = getattr(context, "queue_store", None)
    if queue_store is None:
        raise PermanentJobError("income/expense action requires queue_store context")
    return queue_store


async def _set_stage(context: ActionProcessingContext | None, stage: str) -> None:
    if context is not None:
        await context.set_stage(stage)


async def _set_outbound_json(
    context: ActionProcessingContext | None,
    outbound_json: dict[str, Any],
    stage: str | None,
) -> None:
    if context is not None:
        await context.set_outbound_json(outbound_json, stage)


def _row_int(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise PermanentJobError(f"action row field {key!r} is missing")
    return int(value)


def _row_outbound_state(row: Mapping[str, object]) -> dict[str, object]:
    value = row.get("outbound_json")
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        log.warning("action_job=%s outbound_json_invalid ignored=true", row.get("id"))
        return {}
    if not isinstance(decoded, dict):
        return {}
    return decoded


def _outbound_kind_state(
    outbound_state: dict[str, object],
    outbound_kind: str,
) -> dict[str, object]:
    value = outbound_state.get(outbound_kind)
    return dict(value) if isinstance(value, dict) else {}


def _persisted_message_ids(kind_state: Mapping[str, object]) -> dict[int, int]:
    value = kind_state.get("messages")
    if not isinstance(value, list):
        return {}
    result: dict[int, int] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        try:
            result[int(item["index"])] = int(item["message_id"])
        except (KeyError, TypeError, ValueError):
            continue
    return result


def _display_timestamp(value: str) -> str:
    text = value.strip()
    if "T" in text:
        text = text.replace("T", " ", 1)
    for suffix in ("+00:00", "Z"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text
