from __future__ import annotations

from dataclasses import dataclass

from bot_libs.reaction_policy import (
    REACTION_CALLING_STT_API,
    REACTION_DOWNLOADING_FILE,
    VALID_REACTION_EMOJIS,
)
from bot_libs.stage_names import (
    STAGE_CALLING_STT_API,
    STAGE_DOWNLOADING_FILE,
)


def _validate_reaction(reaction: str) -> None:
    if reaction not in VALID_REACTION_EMOJIS:
        raise ValueError(
            f"stage reaction emoji is not in the allowed Telegram set: {reaction!r}"
        )


@dataclass(frozen=True, slots=True)
class StageStatusDefinition:
    stage: str
    reaction: str | None = None
    pulse_reactions: tuple[str, ...] = ()
    pulse_interval_seconds: float = 4.0

    def __post_init__(self) -> None:
        if not self.stage.strip():
            raise ValueError("stage must not be empty")
        if self.reaction is not None:
            _validate_reaction(self.reaction)
        for reaction in self.pulse_reactions:
            _validate_reaction(reaction)
        if self.pulse_reactions and self.pulse_interval_seconds <= 0:
            raise ValueError("pulse_interval_seconds must be > 0")


DEFAULT_STAGE_STATUS_DEFINITIONS: dict[str, StageStatusDefinition] = {
    STAGE_DOWNLOADING_FILE: StageStatusDefinition(
        stage=STAGE_DOWNLOADING_FILE,
        reaction=REACTION_DOWNLOADING_FILE,
    ),
    STAGE_CALLING_STT_API: StageStatusDefinition(
        stage=STAGE_CALLING_STT_API,
        reaction=REACTION_CALLING_STT_API,
        pulse_reactions=(REACTION_CALLING_STT_API, REACTION_DOWNLOADING_FILE),
        pulse_interval_seconds=7.0,
    ),
}
