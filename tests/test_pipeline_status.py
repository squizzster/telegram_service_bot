from __future__ import annotations

import asyncio
import unittest

from bot_libs.pipeline_definitions import StageStatusDefinition
from bot_libs.pipeline_status import PipelineStatusController
from bot_libs.queue_processor_errors import OriginalMessageUnavailableError
from bot_libs.reaction_policy import (
    REACTION_CALLING_STT_API,
    REACTION_DOWNLOADING_FILE,
    RowReactionResult,
)
from bot_libs.stage_names import STAGE_CALLING_STT_API
from bot_libs.status_feedback import ReactionFeedbackClient


class FakeFeedbackClient:
    def __init__(
        self,
        *,
        results: tuple[RowReactionResult, ...] = (RowReactionResult(ok=True),),
    ) -> None:
        self.results = results
        self.reactions: list[tuple[str, str]] = []

    async def send_reaction(self, emoji: str, *, reason: str) -> RowReactionResult:
        self.reactions.append((emoji, reason))
        index = min(len(self.reactions) - 1, len(self.results) - 1)
        return self.results[index]


class PipelineStatusControllerTests(unittest.IsolatedAsyncioTestCase):
    def make_controller(
        self,
        feedback: FakeFeedbackClient,
        stages: list[tuple[str, str | None]],
    ) -> PipelineStatusController:
        async def set_stage(stage: str, stage_detail: str | None) -> None:
            stages.append((stage, stage_detail))

        return PipelineStatusController(
            set_stage=set_stage,
            feedback_client=feedback,
            stage_definitions={
                STAGE_CALLING_STT_API: StageStatusDefinition(
                    stage=STAGE_CALLING_STT_API,
                    reaction=REACTION_CALLING_STT_API,
                    pulse_reactions=(
                        REACTION_CALLING_STT_API,
                        REACTION_DOWNLOADING_FILE,
                    ),
                    pulse_interval_seconds=0.01,
                )
            },
        )

    async def test_activity_sets_stage_and_alternates_reactions(self) -> None:
        feedback = FakeFeedbackClient()
        stages: list[tuple[str, str | None]] = []
        controller = self.make_controller(feedback, stages)

        async with controller.activity(STAGE_CALLING_STT_API):
            await asyncio.sleep(0.025)

        self.assertEqual(stages, [(STAGE_CALLING_STT_API, None)])
        self.assertGreaterEqual(len(feedback.reactions), 3)
        self.assertEqual(
            [item[0] for item in feedback.reactions[:3]],
            [
                REACTION_CALLING_STT_API,
                REACTION_DOWNLOADING_FILE,
                REACTION_CALLING_STT_API,
            ],
        )

    async def test_activity_stops_pulse_on_exit(self) -> None:
        feedback = FakeFeedbackClient()
        stages: list[tuple[str, str | None]] = []
        controller = self.make_controller(feedback, stages)

        async with controller.activity(STAGE_CALLING_STT_API):
            await asyncio.sleep(0.015)

        reaction_count_after_exit = len(feedback.reactions)
        await asyncio.sleep(0.025)

        self.assertEqual(len(feedback.reactions), reaction_count_after_exit)

    async def test_activity_does_not_pulse_after_initial_reaction_failure(self) -> None:
        feedback = FakeFeedbackClient(results=(RowReactionResult(ok=False),))
        stages: list[tuple[str, str | None]] = []
        controller = self.make_controller(feedback, stages)

        async with controller.activity(STAGE_CALLING_STT_API):
            await asyncio.sleep(0.025)

        self.assertEqual(
            feedback.reactions,
            [(REACTION_CALLING_STT_API, f"stage_enter:{STAGE_CALLING_STT_API}")],
        )

    async def test_activity_raises_on_initial_original_message_unavailable(self) -> None:
        feedback = FakeFeedbackClient(
            results=(
                RowReactionResult(
                    ok=False,
                    original_message_unavailable=True,
                    error_text="Message to react not found",
                ),
            )
        )
        stages: list[tuple[str, str | None]] = []
        controller = self.make_controller(feedback, stages)

        with self.assertRaisesRegex(
            OriginalMessageUnavailableError,
            "Message to react not found",
        ):
            async with controller.activity(STAGE_CALLING_STT_API):
                pass

    async def test_activity_cancels_body_when_pulse_sees_original_message_gone(self) -> None:
        feedback = FakeFeedbackClient(
            results=(
                RowReactionResult(ok=True),
                RowReactionResult(
                    ok=False,
                    original_message_unavailable=True,
                    error_text="Message to react not found",
                ),
            )
        )
        stages: list[tuple[str, str | None]] = []
        controller = self.make_controller(feedback, stages)

        with self.assertRaisesRegex(
            OriginalMessageUnavailableError,
            "Message to react not found",
        ):
            async with controller.activity(STAGE_CALLING_STT_API):
                await asyncio.sleep(60)


class ReactionFeedbackClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_reaction_emoji_is_not_swallowed(self) -> None:
        client = ReactionFeedbackClient(
            bot=object(),
            row={
                "chat_id": 123,
                "message_id": 456,
                "chat_type": "supergroup",
            },
        )

        with self.assertRaisesRegex(ValueError, "allowed Telegram set"):
            await client.send_reaction("not-valid", reason="test_invalid")
