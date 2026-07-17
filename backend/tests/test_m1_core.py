from typing import Any

import pytest

from app.core.contracts import BaseModule, BaseTool, Event
from app.core.event_bus import EventBus
from app.core.module_registry import ModuleRegistry
from app.core.tool_registry import ToolRegistry


class MockModule(BaseModule):
    module_id = "mock_module"
    version = "0.1.0"

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def health_check(self) -> bool:
        return self.started and not self.stopped


class MockTool(BaseTool):
    tool_id = "mock_tool"
    description = "Mock tool for testing"
    capabilities = ["query", "test"]
    input_schema = {}
    output_schema = {}

    async def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": True,
            "params": params,
        }


@pytest.mark.asyncio
async def test_eventbus_publish_subscribe() -> None:
    bus = EventBus()
    received: list[Event] = []

    async def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe("topic.created", handler)

    await bus.publish(
        Event(
            type="topic.created",
            data={"topic_id": "t1"},
            source="test",
        )
    )

    assert len(received) == 1
    assert received[0].data["topic_id"] == "t1"


@pytest.mark.asyncio
async def test_eventbus_wildcard_subscription() -> None:
    bus = EventBus()
    received: list[Event] = []

    async def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe("topic.*", handler)

    await bus.publish(Event(type="topic.created", data={}))
    await bus.publish(Event(type="topic.updated", data={}))

    assert len(received) == 2


@pytest.mark.asyncio
async def test_eventbus_unsubscribe() -> None:
    bus = EventBus()
    received: list[Event] = []

    async def handler(event: Event) -> None:
        received.append(event)

    bus.subscribe("topic.created", handler)
    bus.unsubscribe("topic.created", handler)

    await bus.publish(Event(type="topic.created", data={}))

    assert len(received) == 0


@pytest.mark.asyncio
async def test_eventbus_publish_and_wait_returns_results() -> None:
    bus = EventBus()

    async def handler(event: Event) -> dict[str, str]:
        return {"handled": event.type}

    bus.subscribe("system.test", handler)

    results = await bus.publish_and_wait(Event(type="system.test", data={}))

    assert len(results) == 1
    assert results[0]["handled"] == "system.test"


@pytest.mark.asyncio
async def test_module_registry_register_and_start() -> None:
    registry = ModuleRegistry()
    module = MockModule()

    registry.register(module)

    assert registry.get("mock_module") is module
    assert len(registry.list_all()) == 1

    await registry.start_all()

    assert module.started is True
    assert module.health_check() is True
    assert len(registry.list_healthy()) == 1


@pytest.mark.asyncio
async def test_module_registry_stop_all() -> None:
    registry = ModuleRegistry()
    module = MockModule()

    registry.register(module)
    await registry.start_all()
    await registry.stop_all()

    assert module.stopped is True
    assert module.health_check() is False


@pytest.mark.asyncio
async def test_tool_registry_register_and_execute() -> None:
    registry = ToolRegistry()
    tool = MockTool()

    registry.register(tool)

    assert registry.get("mock_tool") is tool
    assert len(registry.list_all()) == 1
    assert len(registry.list_by_capability("query")) == 1

    result = await registry.execute(
        "mock_tool",
        {"topic_id": "t1"},
    )

    assert result["ok"] is True
    assert result["params"]["topic_id"] == "t1"
