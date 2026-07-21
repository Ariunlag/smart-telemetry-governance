from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.contracts import RawObservation
from app.domain.streams.models import (
    ObservationProcessingTask,
    ObservedField,
    ObservedSchema,
    SchemaDriftEvent,
    SchemaObservationRecord,
)
from app.services.field_projection_contract import (
    FIELD_PROJECTION_PROCESSOR_TYPE,
    FIELD_PROJECTION_PROCESSOR_VERSION,
)
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.schema_observation_service import SchemaObservationService, StaleProcessingClaim
from app.services.stream_catalog import (
    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
    StreamCatalogService,
)


def observation(payload: bytes, second: int) -> RawObservation:
    return RawObservation(
        source_id="r2-postgresql-source",
        source_type="mqtt",
        external_stream_id="r2/postgresql/telemetry",
        payload=payload,
        received_at=datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC),
        content_type="application/json",
        transport_metadata={},
    )


async def create_and_claim(
    sessions: async_sessionmaker[AsyncSession], payload: bytes, second: int
) -> ProcessingTaskItem:
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["r2/#"]))
    tasks = ProcessingTaskRepository()
    async with sessions.begin() as session:
        await catalog.record_raw(session, observation(payload, second))
    async with sessions.begin() as session:
        claimed = await tasks.claim(
            session,
            SCHEMA_OBSERVATION_PROCESSOR_TYPE,
            SCHEMA_OBSERVATION_PROCESSOR_VERSION,
            1,
            60,
        )
    assert len(claimed) == 1
    return claimed[0]


async def process_in_transaction(
    sessions: async_sessionmaker[AsyncSession], item: ProcessingTaskItem
) -> None:
    async with sessions.begin() as session:
        await SchemaObservationService().process_claim(session, item)


async def schema_state(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[
    list[ObservedSchema], list[ObservedField], list[SchemaDriftEvent], list[SchemaObservationRecord]
]:
    async with sessions() as session:
        schemas = list(
            (
                await session.scalars(
                    select(ObservedSchema).order_by(ObservedSchema.version_number)
                )
            ).all()
        )
        fields = list((await session.scalars(select(ObservedField))).all())
        drift_events = list((await session.scalars(select(SchemaDriftEvent))).all())
        records = list((await session.scalars(select(SchemaObservationRecord))).all())
    return schemas, fields, drift_events, records


@pytest.mark.postgresql
@pytest.mark.asyncio
async def test_postgresql_concurrent_identical_schemas_reuse_one_version(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    first = await create_and_claim(postgresql_sessions, b'{"temperature":20}', 1)
    second = await create_and_claim(postgresql_sessions, b'{"temperature":999}', 2)

    await asyncio.gather(
        process_in_transaction(postgresql_sessions, first),
        process_in_transaction(postgresql_sessions, second),
    )

    schemas, fields, drift_events, records = await schema_state(postgresql_sessions)
    assert len(schemas) == 1
    schema = schemas[0]
    assert schema.version_number == 1
    assert schema.observation_count == 2
    assert len(records) == 2
    assert len({record.raw_observation_id for record in records}) == 2
    assert (
        len({(item.stream_id, item.fingerprint_version, item.fingerprint) for item in schemas}) == 1
    )
    assert len({(item.stream_id, item.version_number) for item in schemas}) == 1
    assert len({(field.observed_schema_id, field.path) for field in fields}) == len(fields)
    assert not drift_events
    assert "20" not in str(schema.schema_document)
    assert "999" not in str(schema.schema_document)
    async with postgresql_sessions() as session:
        tasks = list((await session.scalars(select(ObservationProcessingTask))).all())
    schema_tasks = [
        task
        for task in tasks
        if task.processor_type == SCHEMA_OBSERVATION_PROCESSOR_TYPE
        and task.processor_version == SCHEMA_OBSERVATION_PROCESSOR_VERSION
    ]
    field_tasks = [
        task
        for task in tasks
        if task.processor_type == FIELD_PROJECTION_PROCESSOR_TYPE
        and task.processor_version == FIELD_PROJECTION_PROCESSOR_VERSION
    ]
    assert len(tasks) == 4
    assert len(schema_tasks) == 2
    assert len(field_tasks) == 2
    assert all(task.state == "completed" for task in schema_tasks)
    assert all(task.state == "pending" for task in field_tasks)


@pytest.mark.postgresql
@pytest.mark.asyncio
async def test_postgresql_concurrent_distinct_schemas_get_serial_versions_and_drift(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    first = await create_and_claim(postgresql_sessions, b'{"temperature":20}', 1)
    second = await create_and_claim(postgresql_sessions, b'{"temperature":20,"humidity":40}', 2)

    await asyncio.gather(
        process_in_transaction(postgresql_sessions, first),
        process_in_transaction(postgresql_sessions, second),
    )

    schemas, _, drift_events, _ = await schema_state(postgresql_sessions)
    assert [schema.version_number for schema in schemas] == [1, 2]
    assert (
        len({(item.stream_id, item.fingerprint_version, item.fingerprint) for item in schemas}) == 2
    )
    assert len({(item.stream_id, item.version_number) for item in schemas}) == 2
    assert len(drift_events) == 1
    earlier, later = schemas
    drift = drift_events[0]
    assert drift.previous_schema_id == earlier.id
    assert drift.current_schema_id == later.id
    earlier_paths = {
        field.path
        for field in (await schema_state(postgresql_sessions))[1]
        if field.observed_schema_id == earlier.id
    }
    later_paths = {
        field.path
        for field in (await schema_state(postgresql_sessions))[1]
        if field.observed_schema_id == later.id
    }
    assert drift.added_paths == sorted(later_paths - earlier_paths)
    assert drift.removed_paths == sorted(earlier_paths - later_paths)
    assert len(drift_events) == 1
    async with postgresql_sessions() as session:
        tasks = list((await session.scalars(select(ObservationProcessingTask))).all())
    schema_tasks = [
        task
        for task in tasks
        if task.processor_type == SCHEMA_OBSERVATION_PROCESSOR_TYPE
        and task.processor_version == SCHEMA_OBSERVATION_PROCESSOR_VERSION
    ]
    field_tasks = [
        task
        for task in tasks
        if task.processor_type == FIELD_PROJECTION_PROCESSOR_TYPE
        and task.processor_version == FIELD_PROJECTION_PROCESSOR_VERSION
    ]
    assert len(tasks) == 4
    assert len(schema_tasks) == 2
    assert len(field_tasks) == 2
    assert all(task.state == "completed" for task in schema_tasks)
    assert all(task.state == "pending" for task in field_tasks)


@pytest.mark.postgresql
@pytest.mark.asyncio
async def test_postgresql_stale_schema_claim_commits_no_schema_output(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    item = await create_and_claim(
        postgresql_sessions, b'{"temperature":"PRIVATE_STALE_SCHEMA_SECRET"}', 1
    )
    later = item.processing_started_at + timedelta(seconds=5)
    async with postgresql_sessions.begin() as session:
        task = await session.get(ObservationProcessingTask, UUID(item.id))
        assert task is not None
        task.attempt_count += 1
        task.processing_started_at = later

    with pytest.raises(StaleProcessingClaim) as raised:
        await process_in_transaction(postgresql_sessions, item)
    assert "PRIVATE_STALE_SCHEMA_SECRET" not in str(raised.value)
    assert "PRIVATE_STALE_SCHEMA_SECRET" not in repr(raised.value)
    schemas, fields, drift_events, records = await schema_state(postgresql_sessions)
    assert not schemas
    assert not fields
    assert not drift_events
    assert not records
    async with postgresql_sessions() as session:
        task = await session.get(ObservationProcessingTask, UUID(item.id))
    assert task is not None
    assert task.state == "processing"
    assert task.attempt_count == item.attempt_count + 1
    assert task.processing_started_at == later
