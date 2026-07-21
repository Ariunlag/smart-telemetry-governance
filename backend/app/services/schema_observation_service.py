from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.streams.models import (
    ObservationProcessingTask,
    ObservedField,
    ObservedSchema,
    RawObservationRecord,
    SchemaDriftEvent,
    SchemaObservationRecord,
    Stream,
)
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.schema_extractor import (
    FINGERPRINT_VERSION,
    SchemaExtractionFailure,
    SchemaExtractor,
)
from app.services.stream_catalog import (
    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
)


class SchemaObservationFailure(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class StaleProcessingClaim(SchemaObservationFailure):
    def __init__(self) -> None:
        super().__init__("stale_processing_claim")


RawObservationLoader = Callable[[AsyncSession, UUID], Awaitable[RawObservationRecord | None]]


class SchemaObservationService:
    def __init__(
        self,
        extractor: SchemaExtractor | None = None,
        tasks: ProcessingTaskRepository | None = None,
        raw_loader: RawObservationLoader | None = None,
    ) -> None:
        self._extractor = extractor or SchemaExtractor()
        self._tasks = tasks or ProcessingTaskRepository()
        self._raw_loader = raw_loader or self._load_raw

    async def process_claim(
        self, session: AsyncSession, item: ProcessingTaskItem
    ) -> ObservedSchema:
        if (
            item.processor_type != SCHEMA_OBSERVATION_PROCESSOR_TYPE
            or item.processor_version != SCHEMA_OBSERVATION_PROCESSOR_VERSION
        ):
            raise StaleProcessingClaim()
        task = await session.scalar(
            select(ObservationProcessingTask).where(
                ObservationProcessingTask.id == UUID(item.id),
                ObservationProcessingTask.state == "processing",
                ObservationProcessingTask.attempt_count == item.attempt_count,
                ObservationProcessingTask.processing_started_at == item.processing_started_at,
                ObservationProcessingTask.processor_type == SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                ObservationProcessingTask.processor_version == SCHEMA_OBSERVATION_PROCESSOR_VERSION,
            )
        )
        if task is None:
            raise StaleProcessingClaim()
        if str(task.raw_observation_id) != item.raw_observation_id:
            raise StaleProcessingClaim()
        raw = await self._raw_loader(session, task.raw_observation_id)
        if raw is None:
            raise SchemaObservationFailure("raw_observation_missing")
        if raw.content_type != "application/json":
            raise SchemaObservationFailure("unsupported_persisted_input")
        try:
            observation = self._extractor.extract(raw.payload)
        except SchemaExtractionFailure as error:
            raise SchemaObservationFailure(error.code) from error
        stream_query = select(Stream).where(Stream.id == raw.stream_id)
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            stream_query = stream_query.with_for_update()
        stream = await session.scalar(stream_query)
        if stream is None:
            raise SchemaObservationFailure("raw_observation_missing")
        schema = await session.scalar(
            select(ObservedSchema).where(
                ObservedSchema.stream_id == stream.id,
                ObservedSchema.fingerprint_version == FINGERPRINT_VERSION,
                ObservedSchema.fingerprint == observation.fingerprint,
            )
        )
        now = datetime.now(UTC)
        if schema is None:
            previous = await session.scalar(
                select(ObservedSchema)
                .where(ObservedSchema.stream_id == stream.id)
                .order_by(ObservedSchema.version_number.desc())
                .limit(1)
            )
            schema = ObservedSchema(
                stream_id=stream.id,
                fingerprint=observation.fingerprint,
                fingerprint_version=FINGERPRINT_VERSION,
                version_number=1 if previous is None else previous.version_number + 1,
                root_type=observation.root_type,
                field_count=len(observation.fields),
                schema_document=observation.document,
                first_observed_at=raw.received_at,
                last_observed_at=raw.received_at,
                observation_count=1,
            )
            session.add(schema)
            await session.flush()
            session.add_all(
                ObservedField(
                    observed_schema_id=schema.id,
                    path=field.path,
                    value_type=field.value_type,
                    depth=field.depth,
                    nullable=field.nullable,
                )
                for field in observation.fields
            )
            if previous is not None:
                await self._drift(session, stream.id, previous, schema, now)
        else:
            schema.observation_count += 1
            schema.last_observed_at = raw.received_at
        session.add(
            SchemaObservationRecord(
                processing_task_id=task.id,
                raw_observation_id=raw.id,
                observed_schema_id=schema.id,
                processor_version=task.processor_version,
                fingerprint_version=FINGERPRINT_VERSION,
                observed_at=now,
            )
        )
        if not await self._tasks.finalize(session, item, "completed"):
            raise StaleProcessingClaim()
        return schema

    @staticmethod
    async def _load_raw(session: AsyncSession, raw_id: UUID) -> RawObservationRecord | None:
        return await session.get(RawObservationRecord, raw_id)

    async def _drift(
        self,
        session: AsyncSession,
        stream_id: object,
        previous: ObservedSchema,
        current: ObservedSchema,
        now: datetime,
    ) -> None:
        old = {
            field.path: field
            for field in (
                await session.scalars(
                    select(ObservedField).where(ObservedField.observed_schema_id == previous.id)
                )
            ).all()
        }
        new = {
            field.path: field
            for field in (
                await session.scalars(
                    select(ObservedField).where(ObservedField.observed_schema_id == current.id)
                )
            ).all()
        }
        added = sorted(set(new) - set(old))
        removed = sorted(set(old) - set(new))
        changed = [
            {
                "path": path,
                "previous_type": old[path].value_type,
                "current_type": new[path].value_type,
            }
            for path in sorted(set(old) & set(new))
            if old[path].value_type != new[path].value_type
            or old[path].nullable != new[path].nullable
        ]
        session.add(
            SchemaDriftEvent(
                stream_id=stream_id,
                previous_schema_id=previous.id,
                current_schema_id=current.id,
                detected_at=now,
                added_paths=added,
                removed_paths=removed,
                type_changed_paths=changed,
            )
        )
