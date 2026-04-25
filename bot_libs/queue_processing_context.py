from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncContextManager


@asynccontextmanager
async def _noop_activity(
    stage: str,
    stage_detail: str | None = None,
) -> AsyncIterator[None]:
    del stage, stage_detail
    yield


ActivityFactory = Callable[[str, str | None], AsyncContextManager[Any]]


@dataclass(frozen=True, slots=True)
class QueueProcessingContext:
    set_stage: Callable[[str, str | None], Awaitable[None]]
    set_processing_text: Callable[[str, str | None, str | None], Awaitable[None]]
    set_outbound_json: Callable[[dict[str, Any], str | None, str | None], Awaitable[None]]
    activity: ActivityFactory = _noop_activity
