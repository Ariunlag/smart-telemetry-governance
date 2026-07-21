from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import isfinite
from types import MappingProxyType
from typing import Any, Literal, Protocol

SourceType = Literal["mqtt", "rest", "file", "kafka", "opcua"]
type TransportMetadataValue = str | int | float | bool | None

MAX_SOURCE_IDENTIFIER_LENGTH = 255
MAX_EXTERNAL_STREAM_IDENTIFIER_LENGTH = 1024
MAX_CONTENT_TYPE_LENGTH = 255
MAX_TRANSPORT_METADATA_ENTRIES = 16
MAX_TRANSPORT_METADATA_KEY_LENGTH = 128
MAX_TRANSPORT_METADATA_STRING_LENGTH = 1024
MAX_TRANSPORT_METADATA_ENCODED_BYTES = 4096

SENSITIVE_TRANSPORT_METADATA_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "clientsecret",
        "apikey",
        "accesstoken",
        "refreshtoken",
        "authorization",
        "credential",
        "credentials",
        "cookie",
        "privatekey",
    }
)
RAW_CONTENT_TRANSPORT_METADATA_KEYS = frozenset(
    {
        "payload",
        "rawpayload",
        "messagebody",
        "rawbody",
        "requestbody",
        "responsebody",
        "rawcontent",
    }
)


@dataclass(frozen=True, slots=True)
class RawObservation:
    """Immutable, transport-neutral source evidence accepted by ingestion."""

    source_id: str
    source_type: str
    external_stream_id: str
    payload: bytes
    received_at: datetime
    content_type: str | None = None
    transport_metadata: Mapping[str, TransportMetadataValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate_identifier("source_id", self.source_id, MAX_SOURCE_IDENTIFIER_LENGTH)
        self._validate_identifier("source_type", self.source_type, MAX_SOURCE_IDENTIFIER_LENGTH)
        self._validate_identifier(
            "external_stream_id", self.external_stream_id, MAX_EXTERNAL_STREAM_IDENTIFIER_LENGTH
        )
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be bytes")
        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        if self.content_type is not None and (
            not self.content_type.strip() or len(self.content_type) > MAX_CONTENT_TYPE_LENGTH
        ):
            raise ValueError("content_type must be a bounded non-empty string when provided")
        if not isinstance(self.transport_metadata, Mapping):
            raise TypeError("transport_metadata must be a mapping")
        if len(self.transport_metadata) > MAX_TRANSPORT_METADATA_ENTRIES:
            raise ValueError("transport_metadata exceeds the entry limit")

        metadata: dict[str, TransportMetadataValue] = {}
        for key, value in self.transport_metadata.items():
            if not isinstance(key, str) or not key or len(key) > MAX_TRANSPORT_METADATA_KEY_LENGTH:
                raise ValueError("transport_metadata keys must be bounded non-empty strings")
            normalized_key = re.sub(r"[^a-z0-9]+", "", key.casefold())
            if normalized_key in SENSITIVE_TRANSPORT_METADATA_KEYS:
                raise ValueError("transport_metadata must not contain credential material")
            if normalized_key in RAW_CONTENT_TRANSPORT_METADATA_KEYS:
                raise ValueError("transport_metadata must not duplicate raw content")
            if isinstance(value, str) and len(value) > MAX_TRANSPORT_METADATA_STRING_LENGTH:
                raise ValueError("transport_metadata string values exceed the length limit")
            if not isinstance(value, str | int | float | bool | type(None)):
                raise TypeError("transport_metadata values must be JSON scalar values")
            if isinstance(value, float) and not isfinite(value):
                raise ValueError("transport_metadata float values must be finite")
            metadata[key] = value
        encoded_metadata = json.dumps(metadata, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        if len(encoded_metadata) > MAX_TRANSPORT_METADATA_ENCODED_BYTES:
            raise ValueError("transport_metadata exceeds the encoded size limit")
        object.__setattr__(self, "transport_metadata", MappingProxyType(metadata))

    @staticmethod
    def _validate_identifier(name: str, value: str, maximum_length: int) -> None:
        if not isinstance(value, str) or not value.strip() or len(value) > maximum_length:
            raise ValueError(f"{name} must be a bounded non-empty string")


RawObservationHandler = Callable[[RawObservation], Awaitable[None]]


class SourceAdapter(Protocol):
    @property
    def status(self) -> str: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


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
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
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
