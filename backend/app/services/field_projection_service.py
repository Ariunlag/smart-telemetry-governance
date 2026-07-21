from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.domain.streams.models import (
    ObservationOutbox,
    ObservationProcessingTask,
    RawObservationRecord,
    Stream,
)
from app.services.field_projection_contract import (
    FIELD_POINT_SCHEMA_VERSION,
    FIELD_PROJECTION_PROCESSOR_TYPE,
    FIELD_PROJECTION_PROCESSOR_VERSION,
)
from app.services.field_value_extractor import (
    FieldValueExtractionFailure,
    FieldValueExtractor,
    ScalarValue,
    ScalarValueType,
)
from app.services.observation_time import (
    TimestampSource,
    normalize_observation_time,
    select_observation_timestamp,
)
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.schema_observation_service import StaleProcessingClaim

__all__ = [
    "FIELD_POINT_SCHEMA_VERSION",
    "FIELD_PROJECTION_PROCESSOR_TYPE",
    "FIELD_PROJECTION_PROCESSOR_VERSION",
    "FieldProjectionFailure",
    "FieldProjectionService",
]


class FieldProjectionFailure(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


RawObservationLoader = Callable[[AsyncSession, UUID], Awaitable[RawObservationRecord | None]]


class FieldProjectionService:
    def __init__(
        self,
        extractor: FieldValueExtractor | None = None,
        tasks: ProcessingTaskRepository | None = None,
        raw_loader: RawObservationLoader | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._extractor = extractor or FieldValueExtractor()
        self._tasks = tasks or ProcessingTaskRepository()
        self._raw_loader = raw_loader or self._load_raw
        self._future_skew_seconds = (settings or Settings()).observation_future_skew_seconds

    async def process_claim(self, session: AsyncSession, item: ProcessingTaskItem) -> int:
        task = await self._claimed_task(session, item)
        raw = await self._raw_loader(session, task.raw_observation_id)
        if raw is None:
            raise FieldProjectionFailure("raw_observation_missing")
        if raw.content_type != "application/json":
            raise FieldProjectionFailure("unsupported_persisted_input")
        try:
            extraction = self._extractor.extract(raw.payload)
        except FieldValueExtractionFailure as error:
            raise FieldProjectionFailure(error.code) from error
        stream_query = select(Stream).where(Stream.id == raw.stream_id)
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            stream_query = stream_query.with_for_update()
        stream = await session.scalar(stream_query)
        if stream is None:
            raise FieldProjectionFailure("stream_missing")
        source_timestamp = self._source_timestamp(raw)
        timestamp, timestamp_source = select_observation_timestamp(
            source_timestamp,
            raw.transport_metadata,
            raw.received_at,
            future_skew_seconds=self._future_skew_seconds,
        )
        for field in extraction.values:
            await self._persist_outbox(
                session,
                raw,
                stream,
                field.field_path,
                field.value_type,
                field.value,
                timestamp,
                timestamp_source,
            )
        if not await self._tasks.finalize(session, item, "completed"):
            raise StaleProcessingClaim()
        return len(extraction.values)

    async def _claimed_task(
        self, session: AsyncSession, item: ProcessingTaskItem
    ) -> ObservationProcessingTask:
        if (
            item.processor_type != FIELD_PROJECTION_PROCESSOR_TYPE
            or item.processor_version != FIELD_PROJECTION_PROCESSOR_VERSION
            or item.attempt_count < 1
        ):
            raise StaleProcessingClaim()
        try:
            task_id = UUID(item.id)
        except ValueError as error:
            raise StaleProcessingClaim() from error
        task = await session.scalar(
            select(ObservationProcessingTask).where(
                ObservationProcessingTask.id == task_id,
                ObservationProcessingTask.state == "processing",
                ObservationProcessingTask.attempt_count == item.attempt_count,
                ObservationProcessingTask.processing_started_at == item.processing_started_at,
                ObservationProcessingTask.processor_type == FIELD_PROJECTION_PROCESSOR_TYPE,
                ObservationProcessingTask.processor_version == FIELD_PROJECTION_PROCESSOR_VERSION,
            )
        )
        if task is None or str(task.raw_observation_id) != item.raw_observation_id:
            raise StaleProcessingClaim()
        return task

    @staticmethod
    async def _load_raw(session: AsyncSession, raw_id: UUID) -> RawObservationRecord | None:
        return await session.get(RawObservationRecord, raw_id)

    @staticmethod
    def _source_timestamp(raw: RawObservationRecord) -> object:
        try:
            envelope = json.loads(raw.payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return envelope.get("timestamp") if isinstance(envelope, dict) else None

    async def _persist_outbox(
        self,
        session: AsyncSession,
        raw: RawObservationRecord,
        stream: Stream,
        field_path: str,
        value_type: ScalarValueType,
        value: ScalarValue,
        timestamp: datetime,
        timestamp_source: TimestampSource,
    ) -> None:
        delivery_key = self._delivery_key(raw.observation_key, field_path)
        received_at = normalize_observation_time(raw.received_at)
        point_payload: dict[str, str | int | float | bool] = {
            "stream_id": str(stream.id),
            "source_id": stream.source_id,
            "topic": stream.topic,
            "observation_timestamp": timestamp.isoformat(),
            "received_timestamp": received_at.isoformat(),
            "timestamp_source": timestamp_source,
            "field_path": field_path,
            "value_type": value_type,
            "value": value,
            "content_schema_version": FIELD_POINT_SCHEMA_VERSION,
            "quality_status": "unassessed",
            "provenance_reference": str(raw.evidence_id),
        }
        if stream.tenant is not None:
            point_payload["tenant"] = stream.tenant
        values = {
            "delivery_key": delivery_key,
            "stream_id": stream.id,
            "evidence_id": raw.evidence_id,
            "state": "pending",
            "point_payload": point_payload,
            "attempt_count": 0,
            "available_at": raw.received_at,
        }
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            await session.execute(
                pg_insert(ObservationOutbox)
                .values(**values)
                .on_conflict_do_nothing(constraint="uq_observation_outbox_delivery_key")
            )
        elif (
            await session.scalar(
                select(ObservationOutbox.id).where(ObservationOutbox.delivery_key == delivery_key)
            )
            is None
        ):
            session.add(ObservationOutbox(**values))

    @staticmethod
    def _delivery_key(observation_key: str, field_path: str) -> str:
        material = "\x1f".join((observation_key, FIELD_PROJECTION_PROCESSOR_VERSION, field_path))
        return hashlib.sha256(material.encode("utf-8")).hexdigest()
