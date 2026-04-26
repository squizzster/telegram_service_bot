from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot_libs.action_models import (
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
)
from bot_libs.action_processing_context import ActionProcessingContext
from bot_libs.action_processors.income_expenses import (
    process_calculate_income_expenses,
    process_show_income_expense_report,
)
from bot_libs.income_expense_calculation_openai import (
    IncomeExpenseCalculationResult,
)
from bot_libs.sql import SQLiteQueueStore, SQLiteSettings
from tests.test_sql import make_job


class FakeIncomeExpenseProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, *, messy_csv_data: str) -> IncomeExpenseCalculationResult:
        self.calls.append(messy_csv_data)
        return IncomeExpenseCalculationResult(
            provider="fake",
            prompt_id="prompt-calc",
            entries=(
                {
                    "entry_id": 1,
                    "date_time_utc": "2026-04-26 14:43:23",
                    "direction": "incoming",
                    "description": "Putting up TVs",
                    "notes": "",
                    "price_value": "160",
                },
                {
                    "entry_id": 1,
                    "date_time_utc": "2026-04-26 14:43:23",
                    "direction": "outgoing",
                    "description": "Aquarium",
                    "notes": "Pet supplies.",
                    "price_value": "56",
                },
            ),
            raw_response={"entries": []},
        )


class CrossMonthIncomeExpenseProvider:
    async def __call__(self, *, messy_csv_data: str) -> IncomeExpenseCalculationResult:
        del messy_csv_data
        return IncomeExpenseCalculationResult(
            provider="fake",
            prompt_id="prompt-calc",
            entries=(
                {
                    "entry_id": 1,
                    "date_time_utc": "2026-04-30 18:00:00",
                    "direction": "outgoing",
                    "description": "April expense",
                    "notes": "",
                    "price_value": "10",
                },
                {
                    "entry_id": 1,
                    "date_time_utc": "2026-05-01 09:00:00",
                    "direction": "incoming",
                    "description": "May income",
                    "notes": "",
                    "price_value": "25",
                },
            ),
            raw_response={"entries": []},
        )


class RemovingIncomeProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, *, messy_csv_data: str) -> IncomeExpenseCalculationResult:
        self.calls.append(messy_csv_data)
        return IncomeExpenseCalculationResult(
            provider="fake",
            prompt_id="prompt-calc",
            entries=(
                {
                    "entry_id": 1,
                    "date_time_utc": "2026-04-26 14:43:23",
                    "direction": "outgoing",
                    "description": "Aquarium",
                    "notes": "",
                    "price_value": "56",
                },
            ),
            raw_response={"entries": []},
        )


class IncomeExpenseProcessorTests(unittest.IsolatedAsyncioTestCase):
    async def test_calculate_records_source_rows_and_sends_summary(self) -> None:
        with TemporaryDirectory() as tmpdir:
            store, calculation_row = _store_with_mixed_source_and_action(tmpdir)
            provider = FakeIncomeExpenseProvider()
            bot = SimpleNamespace(
                send_message=AsyncMock(return_value=SimpleNamespace(message_id=9001))
            )
            context = _context(store)

            result = await process_calculate_income_expenses(
                bot,
                calculation_row,
                context=context,
                provider=provider,
            )

            self.assertIn("incoming_outgoing", provider.calls[0])
            self.assertEqual(result["inserted_count"], 2)
            self.assertEqual(
                store.get_unprocessed_income_expense_source_actions(),
                (),
            )
            bot.send_message.assert_awaited_once()
            self.assertIn("Calculation complete", bot.send_message.await_args.kwargs["text"])

    async def test_calculate_reprocesses_recent_sources_for_corrections(self) -> None:
        with TemporaryDirectory() as tmpdir:
            store, first_calculation_row = _store_with_mixed_source_and_action(tmpdir)
            await process_calculate_income_expenses(
                SimpleNamespace(
                    send_message=AsyncMock(return_value=SimpleNamespace(message_id=9001))
                ),
                first_calculation_row,
                context=_context(store),
                provider=FakeIncomeExpenseProvider(),
            )

            correction = store.insert_queue_job(
                make_job(
                    update_id=3,
                    message_id=458,
                    processing_text="Yeah, the earnings 160 should be removed.",
                )
            )
            command = store.insert_queue_job(
                make_job(update_id=4, message_id=459, processing_text="/calculate")
            )
            assert correction.queue_id is not None
            assert command.queue_id is not None
            store.insert_incoming_message_actions(
                queue_id=correction.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_INCOME,),
            )
            store.insert_incoming_message_actions(
                queue_id=command.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_CALCULATE_INCOME_EXPENSES,),
            )
            store.mark_job_done(correction.queue_id, result_json={"ok": True})
            store.mark_job_done(command.queue_id, result_json={"ok": True})
            correction_action = store.claim_next_action(worker_name="worker")
            second_calculation_row = store.claim_next_action(worker_name="worker")
            assert correction_action is not None
            assert second_calculation_row is not None
            store.mark_action_done(int(correction_action["id"]), result_json={"ok": True})
            provider = RemovingIncomeProvider()

            result = await process_calculate_income_expenses(
                SimpleNamespace(
                    send_message=AsyncMock(return_value=SimpleNamespace(message_id=9002))
                ),
                second_calculation_row,
                context=_context(store),
                provider=provider,
            )

            self.assertEqual(result["inserted_count"], 1)
            self.assertIn(
                "Earned 160 putting up TVs, spent 56 on my aquarium.",
                provider.calls[0],
            )
            self.assertIn(
                "Yeah, the earnings 160 should be removed.",
                provider.calls[0],
            )
            income_rows = store.get_income_expense_rows_for_week(direction="incoming")
            expense_rows = store.get_income_expense_rows_for_week(direction="outgoing")
            self.assertEqual(income_rows, ())
            self.assertEqual(len(expense_rows), 1)
            self.assertEqual(expense_rows[0]["description"], "Aquarium")

    async def test_show_expenses_reads_processed_table_only(self) -> None:
        with TemporaryDirectory() as tmpdir:
            store, calculation_row = _store_with_mixed_source_and_action(tmpdir)
            await process_calculate_income_expenses(
                SimpleNamespace(
                    send_message=AsyncMock(return_value=SimpleNamespace(message_id=9001))
                ),
                calculation_row,
                context=_context(store),
                provider=FakeIncomeExpenseProvider(),
            )
            bot = SimpleNamespace(
                send_message=AsyncMock(return_value=SimpleNamespace(message_id=9002))
            )

            result = await process_show_income_expense_report(
                bot,
                {
                    "id": 99,
                    "action_code": ACTION_SHOW_EXPENSES,
                    "chat_id": 123,
                    "message_id": 789,
                },
                context=_context(store),
            )

            self.assertEqual(result["row_count"], 1)
            sent_text = bot.send_message.await_args.kwargs["text"]
            self.assertIn("<b>APRIL 2026:</b>", sent_text)
            self.assertIn("Sun 26th - out - \u00a356.00 - Aquarium", sent_text)
            self.assertIn("Aquarium", sent_text)
            self.assertNotIn("Putting up TVs", sent_text)
            self.assertNotIn("Pet supplies.", sent_text)
            self.assertEqual(bot.send_message.await_args.kwargs["parse_mode"], "HTML")

    async def test_show_all_detailed_keeps_notes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            store, calculation_row = _store_with_mixed_source_and_action(tmpdir)
            await process_calculate_income_expenses(
                SimpleNamespace(
                    send_message=AsyncMock(return_value=SimpleNamespace(message_id=9001))
                ),
                calculation_row,
                context=_context(store),
                provider=FakeIncomeExpenseProvider(),
            )
            bot = SimpleNamespace(
                send_message=AsyncMock(return_value=SimpleNamespace(message_id=9002))
            )

            result = await process_show_income_expense_report(
                bot,
                {
                    "id": 100,
                    "action_code": ACTION_SHOW_ALL_DETAILED,
                    "chat_id": 123,
                    "message_id": 789,
                },
                context=_context(store),
            )

            self.assertEqual(result["row_count"], 2)
            self.assertEqual(result["report_style"], "detailed")
            sent_text = bot.send_message.await_args.kwargs["text"]
            self.assertIn("Income And Expenses for 2026-W17", sent_text)
            self.assertIn(
                "2026-04-26 14:43:23 incoming 160.00 - Putting up TVs",
                sent_text,
            )
            self.assertIn("  notes: Pet supplies.", sent_text)
            self.assertNotIn("parse_mode", bot.send_message.await_args.kwargs)

    async def test_show_all_friendly_groups_one_week_across_months(self) -> None:
        with TemporaryDirectory() as tmpdir:
            store, calculation_row = _store_with_mixed_source_and_action(tmpdir)
            await process_calculate_income_expenses(
                SimpleNamespace(
                    send_message=AsyncMock(return_value=SimpleNamespace(message_id=9001))
                ),
                calculation_row,
                context=_context(store),
                provider=CrossMonthIncomeExpenseProvider(),
            )
            bot = SimpleNamespace(
                send_message=AsyncMock(return_value=SimpleNamespace(message_id=9002))
            )

            await process_show_income_expense_report(
                bot,
                {
                    "id": 101,
                    "action_code": "SHOW_ALL",
                    "chat_id": 123,
                    "message_id": 789,
                },
                context=_context(store),
            )

            sent_text = bot.send_message.await_args.kwargs["text"]
            self.assertIn("<b>APRIL 2026:</b>", sent_text)
            self.assertIn("<b>MAY 2026:</b>", sent_text)
            self.assertLess(
                sent_text.index("<b>APRIL 2026:</b>"),
                sent_text.index("<b>MAY 2026:</b>"),
            )
            self.assertIn("Thu 30th - out - \u00a310.00 - April expense", sent_text)
            self.assertIn("Fri 01st - in - \u00a325.00 - May income", sent_text)


def _store_with_mixed_source_and_action(tmpdir: str) -> tuple[SQLiteQueueStore, dict[str, object]]:
    db_path = Path(tmpdir) / "queue.sqlite"
    store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
    store.create_schema(create_parent_dir=True)
    source = store.insert_queue_job(
        make_job(
            update_id=1,
            message_id=456,
            processing_text="Earned 160 putting up TVs, spent 56 on my aquarium.",
        )
    )
    command = store.insert_queue_job(
        make_job(update_id=2, message_id=457, processing_text="/calculate")
    )
    assert source.queue_id is not None
    assert command.queue_id is not None
    store.insert_incoming_message_actions(
        queue_id=source.queue_id,
        detection_run_id=None,
        action_codes=(ACTION_LOG_INCOME, ACTION_LOG_EXPENSES),
    )
    store.insert_incoming_message_actions(
        queue_id=command.queue_id,
        detection_run_id=None,
        action_codes=(ACTION_CALCULATE_INCOME_EXPENSES,),
    )
    store.mark_job_done(source.queue_id, result_json={"ok": True})
    store.mark_job_done(command.queue_id, result_json={"ok": True})
    income_source = store.claim_next_action(worker_name="worker")
    expense_source = store.claim_next_action(worker_name="worker")
    calculation_row = store.claim_next_action(worker_name="worker")
    assert income_source is not None
    assert expense_source is not None
    assert calculation_row is not None
    store.mark_action_done(int(income_source["id"]), result_json={"ok": True})
    store.mark_action_done(int(expense_source["id"]), result_json={"ok": True})
    return store, calculation_row


def _context(store: SQLiteQueueStore) -> ActionProcessingContext:
    async def set_stage(stage: str) -> None:
        del stage

    async def set_outbound_json(
        outbound_json: dict[str, object],
        stage: str | None,
    ) -> None:
        del outbound_json, stage

    return ActionProcessingContext(
        set_stage=set_stage,
        set_outbound_json=set_outbound_json,
        queue_store=store,
    )
