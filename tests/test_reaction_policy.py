from __future__ import annotations

import unittest

from bot_libs.reaction_policy import (
    REACTION_AI_PROMPT,
    reaction_for_row_state,
    reaction_for_stage,
)
from bot_libs.stage_names import (
    STAGE_ANSWERING_QUESTION,
    STAGE_CALCULATING_INCOME_EXPENSES,
    STAGE_CALLING_STT_API,
    STAGE_DETECTING_ACTIONS,
)


class ReactionPolicyTests(unittest.TestCase):
    def test_ai_prompt_stages_use_lightning_reaction(self) -> None:
        for stage in (
            STAGE_ANSWERING_QUESTION,
            STAGE_CALCULATING_INCOME_EXPENSES,
            STAGE_CALLING_STT_API,
            STAGE_DETECTING_ACTIONS,
        ):
            with self.subTest(stage=stage):
                self.assertEqual(reaction_for_stage(stage), REACTION_AI_PROMPT)
                self.assertEqual(
                    reaction_for_row_state({"status": "processing", "stage": stage}),
                    REACTION_AI_PROMPT,
                )

