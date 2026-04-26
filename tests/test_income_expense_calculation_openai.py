from __future__ import annotations

import unittest

from bot_libs.income_expense_calculation_openai import parse_income_expense_entries


class IncomeExpenseCalculationOpenAITests(unittest.TestCase):
    def test_parse_markdown_table_output(self) -> None:
        entries = parse_income_expense_entries(
            """
| id | entry_id | date_time_utc       | direction | description | notes | price |
| -: | -------: | ------------------- | --------- | ----------- | ----- | ----: |
|  1 |       23 | 2026-04-26 11:01:30 | outgoing  | Fish        | Fixed |    30 |
|  2 |       26 | 2026-04-26 14:42:58 | incoming  | Earnings    |       |   300 |
""".strip()
        )

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["entry_id"], "23")
        self.assertEqual(entries[0]["direction"], "outgoing")
        self.assertEqual(entries[0]["price_value"], "30")
        self.assertEqual(entries[1]["notes"], "")

    def test_parse_csv_code_fence_output(self) -> None:
        entries = parse_income_expense_entries(
            """```csv
entry_id,date_time_utc,direction,description,notes,price_value
30,2026-04-26 14:43:23,incoming,Putting up TVs,,160
```"""
        )

        self.assertEqual(
            entries,
            (
                {
                    "entry_id": "30",
                    "date_time_utc": "2026-04-26 14:43:23",
                    "direction": "incoming",
                    "description": "Putting up TVs",
                    "notes": "",
                    "price_value": "160",
                },
            ),
        )
