from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from app.core.contracts import Event, EventHandler


class EventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        if handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)

    async def publish(self, event: Event) -> None:
        for handler in self._matching_handlers(event.type):
            await handler(event)

    async def publish_and_wait(self, event: Event, timeout: float = 5.0) -> list[Any]:
        handlers = self._matching_handlers(event.type)

        if not handlers:
            return []

        tasks = [handler(event) for handler in handlers]

        return await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=False),
            timeout=timeout,
        )

    def _matching_handlers(self, event_type: str) -> list[EventHandler]:
        matched: list[EventHandler] = []

        matched.extend(self._handlers.get(event_type, []))

        for pattern, handlers in self._handlers.items():
            if pattern.endswith(".*"):
                prefix = pattern[:-2]
                if event_type.startswith(prefix + "."):
                    matched.extend(handlers)

        return matched