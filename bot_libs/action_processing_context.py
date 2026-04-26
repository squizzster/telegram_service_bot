from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ActionProcessingContext:
    set_stage: Callable[[str], Awaitable[None]]
    set_outbound_json: Callable[[dict[str, Any], str | None], Awaitable[None]]
    queue_store: Any | None = None
