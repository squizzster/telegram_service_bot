from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import TracebackType

from bot_libs.pipeline_definitions import (
    DEFAULT_STAGE_STATUS_DEFINITIONS,
    StageStatusDefinition,
)
from bot_libs.queue_processor_errors import OriginalMessageUnavailableError
from bot_libs.reaction_policy import RowReactionResult
from bot_libs.status_feedback import ReactionFeedbackClient

log = logging.getLogger(__name__)

SetStageCallback = Callable[[str, str | None], Awaitable[None]]
SleepCallback = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class PipelineStatusController:
    set_stage: SetStageCallback
    feedback_client: ReactionFeedbackClient
    stage_definitions: Mapping[str, StageStatusDefinition] | None = None
    sleep: SleepCallback = asyncio.sleep

    def activity(
        self,
        stage: str,
        stage_detail: str | None = None,
    ) -> "PipelineStageActivity":
        definitions = (
            DEFAULT_STAGE_STATUS_DEFINITIONS
            if self.stage_definitions is None
            else self.stage_definitions
        )
        return PipelineStageActivity(
            stage=stage,
            stage_detail=stage_detail,
            definition=definitions.get(stage),
            set_stage=self.set_stage,
            feedback_client=self.feedback_client,
            sleep=self.sleep,
        )


@dataclass(slots=True)
class PipelineStageActivity:
    stage: str
    stage_detail: str | None
    definition: StageStatusDefinition | None
    set_stage: SetStageCallback
    feedback_client: ReactionFeedbackClient
    sleep: SleepCallback
    _pulse_task: asyncio.Task[None] | None = None
    _owner_task: asyncio.Task[object] | None = None
    _original_message_unavailable: bool = False
    _original_message_unavailable_detail: str | None = None

    async def __aenter__(self) -> "PipelineStageActivity":
        self._owner_task = asyncio.current_task()
        await self.set_stage(self.stage, self.stage_detail)

        if self.definition is None or self.definition.reaction is None:
            return self

        reaction_set = await self.feedback_client.send_reaction(
            self.definition.reaction,
            reason=f"stage_enter:{self.stage}",
        )
        if self._mark_original_message_unavailable(reaction_set):
            self.raise_if_original_message_unavailable()
        if not reaction_set:
            log.debug(
                "Pipeline activity reaction disabled after initial send failed "
                "stage=%s emoji=%s",
                self.stage,
                self.definition.reaction,
            )
            return self

        if len(self.definition.pulse_reactions) < 2:
            return self

        self._pulse_task = asyncio.create_task(
            self._pulse_reactions(),
            name=f"pipeline-status-pulse:{self.stage}",
        )
        log.debug(
            "Pipeline activity pulse started stage=%s emojis=%s interval=%.3f",
            self.stage,
            ",".join(self.definition.pulse_reactions),
            self.definition.pulse_interval_seconds,
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        if self._pulse_task is not None:
            self._pulse_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pulse_task
            self._pulse_task = None
            log.debug("Pipeline activity pulse stopped stage=%s", self.stage)
        self.raise_if_original_message_unavailable()

    async def _pulse_reactions(self) -> None:
        assert self.definition is not None
        reactions = self.definition.pulse_reactions
        next_index = self._initial_pulse_index(reactions, self.definition.reaction)

        while True:
            await self.sleep(self.definition.pulse_interval_seconds)
            emoji = reactions[next_index]
            reaction_set = await self.feedback_client.send_reaction(
                emoji,
                reason=f"stage_pulse:{self.stage}",
            )
            if self._mark_original_message_unavailable(reaction_set):
                log.debug(
                    "Pipeline activity pulse stopped after source message vanished "
                    "stage=%s emoji=%s",
                    self.stage,
                    emoji,
                )
                self._cancel_owner_task()
                return
            if not reaction_set:
                log.debug(
                    "Pipeline activity pulse disabled after reaction failed "
                    "stage=%s emoji=%s",
                    self.stage,
                    emoji,
                )
                return
            next_index = (next_index + 1) % len(reactions)

    def raise_if_original_message_unavailable(self) -> None:
        if not self._original_message_unavailable:
            return
        detail = self._original_message_unavailable_detail or "unknown Telegram error"
        raise OriginalMessageUnavailableError(
            f"original message unavailable during stage {self.stage}: {detail}"
        )

    def _mark_original_message_unavailable(
        self,
        reaction_set: RowReactionResult,
    ) -> bool:
        if not reaction_set.original_message_unavailable:
            return False
        self._original_message_unavailable = True
        self._original_message_unavailable_detail = reaction_set.error_text
        return True

    def _cancel_owner_task(self) -> None:
        if self._owner_task is None:
            return
        if self._owner_task.done():
            return
        self._owner_task.cancel()

    @staticmethod
    def _initial_pulse_index(
        reactions: tuple[str, ...],
        initial_reaction: str | None,
    ) -> int:
        if initial_reaction in reactions:
            return (reactions.index(initial_reaction) + 1) % len(reactions)
        return 0
