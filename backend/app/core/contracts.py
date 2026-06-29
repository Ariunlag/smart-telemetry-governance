from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal


SourceType = Literal["mqtt", "rest", "file", "kafka", "opcua"]


@dataclass(slots=True)
class FieldMapping:
    topic_field: str | None = None
    timestamp_field: str | None = None
    value_field: str | None = None
    unit_field: str | None = None
    metric_fields: list[str] = field(default_factory=list)
    metadata_fields: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SourceConfig:
    source_id: str
    name: str
    source_type: SourceType
    config: dict[str, Any]
    mapping: FieldMapping | None = None


@dataclass(slots=True)
class RawMessage:
    source_id: str
    source_type: SourceType
    received_at: datetime
    payload: Any
    topic_hint: str | None = None


@dataclass(slots=True)
class TelemetryPoint:
    source_id: str
    source_type: SourceType
    topic: str
    metric: str
    timestamp: datetime
    value: float | int | str | bool
    unit: str | None = None
    quality: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    raw: Any | None = None


@dataclass(slots=True)
class Event:
    type: str
    data: dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source: str | None = None


class BaseModule(ABC):
    module_id: str
    version: str

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    def health_check(self) -> bool:
        pass


class BaseTool(ABC):
    tool_id: str
    description: str
    capabilities: list[str]
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]

    @abstractmethod
    async def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        pass


EventHandler = Callable[[Event], Awaitable[Any]]