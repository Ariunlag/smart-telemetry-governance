from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings
from app.core.contracts import RawObservation
from app.domain.streams.models import (
    ObservationEvidence,
    ObservationOutbox,
    ObservationProcessingTask,
    ObservedField,
    ObservedSchema,
    RawObservationRecord,
    SchemaDriftEvent,
    SchemaObservationRecord,
    Stream,
)
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.schema_observation_service import (
    SchemaObservationFailure,
    SchemaObservationService,
    StaleProcessingClaim,
)
from app.services.stream_catalog import (
    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
    StreamCatalogService,
)

pytestmark = pytest.mark.schema_observation


async def create_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        for model in (
            Stream,
            ObservationEvidence,
            ObservationOutbox,
            RawObservationRecord,
            ObservationProcessingTask,
            ObservedSchema,
            ObservedField,
            SchemaDriftEvent,
            SchemaObservationRecord,
        ):
            await connection.run_sync(cast(Table, model.__table__).create)


def raw(payload: bytes, second: int) -> RawObservation:
    return RawObservation(
        source_id="source-a",
        source_type="mqtt",
        external_stream_id="site/one/telemetry",
        payload=payload,
        received_at=datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC),
        content_type="application/json",
        transport_metadata={},
    )


@pytest.mark.asyncio
async def test_first_and_repeat_structure_create_task_provenance() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService()
    try:
        await create_tables(engine)
        for payload, second in ((b'{"a":1}', 1), (b'{"a":99}', 2)):
            async with sessions.begin() as session:
                await catalog.record_raw(session, raw(payload, second))
            async with sessions.begin() as session:
                item = (
                    await tasks.claim(
                        session,
                        SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                        SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                        1,
                        60,
                    )
                )[0]
                await service.process_claim(session, item)
        async with sessions() as session:
            schemas = (await session.scalars(select(ObservedSchema))).all()
            provenance = (await session.scalars(select(SchemaObservationRecord))).all()
        assert (
            len(schemas) == 1
            and schemas[0].version_number == 1
            and schemas[0].observation_count == 2
        )
        assert len(provenance) == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "second", "added", "removed", "changed"),
    [
        (b'{"temperature":20}', b'{"temperature":21,"humidity":40}', ['$["humidity"]'], [], []),
        (b'{"temperature":20,"humidity":40}', b'{"temperature":21}', [], ['$["humidity"]'], []),
        (
            b'{"temperature":20}',
            b'{"temperature":20.5}',
            [],
            [],
            [{"path": '$["temperature"]', "previous_type": "integer", "current_type": "number"}],
        ),
    ],
)
async def test_structure_changes_create_deterministic_drift(
    first: bytes, second: bytes, added: list[str], removed: list[str], changed: list[dict[str, str]]
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService()
    try:
        await create_tables(engine)
        for payload, second_value in ((first, 1), (second, 2)):
            async with sessions.begin() as session:
                await catalog.record_raw(session, raw(payload, second_value))
            async with sessions.begin() as session:
                item = (
                    await tasks.claim(
                        session,
                        SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                        SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                        1,
                        60,
                    )
                )[0]
                await service.process_claim(session, item)
        async with sessions() as session:
            schemas = (
                await session.scalars(
                    select(ObservedSchema).order_by(ObservedSchema.version_number)
                )
            ).all()
            drift = await session.scalar(select(SchemaDriftEvent))
        assert [schema.version_number for schema in schemas] == [1, 2]
        assert (
            drift is not None
            and drift.added_paths == added
            and drift.removed_paths == removed
            and drift.type_changed_paths == changed
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_completed_task_is_rejected_without_duplicate_provenance() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(session, raw(b'{"secret":"PRIVATE_SENSOR_SECRET_7319"}', 1))
        async with sessions.begin() as session:
            item = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
            schema = await service.process_claim(session, item)
        count, last = schema.observation_count, schema.last_observed_at
        with pytest.raises(StaleProcessingClaim):
            async with sessions.begin() as session:
                await service.process_claim(session, item)
        async with sessions() as session:
            stored = await session.get(ObservedSchema, schema.id)
            records = (await session.scalars(select(SchemaObservationRecord))).all()
        assert (
            stored is not None
            and stored.observation_count == count
            and stored.last_observed_at == last
            and len(records) == 1
        )
    finally:
        await engine.dispose()


class FailingFinalizeRepository(ProcessingTaskRepository):
    async def finalize(self, *args: object, **kwargs: object) -> bool:
        del args, kwargs
        return False


@pytest.mark.asyncio
async def test_stale_finalization_rolls_back_new_schema() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService(tasks=FailingFinalizeRepository())
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(session, raw(b'{"a":"initial"}', 1))
        async with sessions.begin() as session:
            item = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
            with pytest.raises(StaleProcessingClaim):
                await service.process_claim(session, item)
            await session.rollback()
        async with sessions() as session:
            assert await session.scalar(select(ObservedSchema)) is None
            assert await session.scalar(select(SchemaObservationRecord)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "variant",
    ["unknown", "attempt", "started", "type", "version", "pending", "retryable", "dead_letter"],
)
async def test_stale_claim_tokens_do_not_persist_schema(variant: str) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(
                session, raw(b'{"value":"PRIVATE_STALE_PAYLOAD_SECRET_4307"}', 1)
            )
        async with sessions.begin() as session:
            item = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
            stored = await session.get(ObservationProcessingTask, UUID(item.id))
            assert stored is not None
            stale = item
            if variant == "unknown":
                stale = ProcessingTaskItem(
                    "00000000-0000-0000-0000-000000000000",
                    item.raw_observation_id,
                    item.processor_type,
                    item.processor_version,
                    item.attempt_count,
                    item.processing_started_at,
                )
            elif variant == "attempt":
                stale = ProcessingTaskItem(
                    item.id,
                    item.raw_observation_id,
                    item.processor_type,
                    item.processor_version,
                    item.attempt_count + 1,
                    item.processing_started_at,
                )
            elif variant == "started":
                stale = ProcessingTaskItem(
                    item.id,
                    item.raw_observation_id,
                    item.processor_type,
                    item.processor_version,
                    item.attempt_count,
                    item.processing_started_at + timedelta(seconds=1),
                )
            elif variant == "type":
                stale = ProcessingTaskItem(
                    item.id,
                    item.raw_observation_id,
                    "other",
                    item.processor_version,
                    item.attempt_count,
                    item.processing_started_at,
                )
            elif variant == "version":
                stale = ProcessingTaskItem(
                    item.id,
                    item.raw_observation_id,
                    item.processor_type,
                    "other",
                    item.attempt_count,
                    item.processing_started_at,
                )
            else:
                stored.state = variant
        with pytest.raises(StaleProcessingClaim) as raised:
            async with sessions.begin() as session:
                await service.process_claim(session, stale)
        assert "PRIVATE_STALE_PAYLOAD_SECRET_4307" not in str(raised.value)
        async with sessions() as session:
            assert await session.scalar(select(ObservedSchema)) is None
            assert await session.scalar(select(SchemaObservationRecord)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content_type", "payload", "code", "token"),
    [
        (
            "application/json",
            b'{"secret": PRIVATE_INVALID_JSON_SECRET_9281',
            "invalid_json",
            "PRIVATE_INVALID_JSON_SECRET_9281",
        ),
        (
            "text/plain",
            b'{"secret":"PRIVATE_TEXT_SECRET_6103"}',
            "unsupported_persisted_input",
            "PRIVATE_TEXT_SECRET_6103",
        ),
    ],
)
async def test_persisted_input_failures_are_bounded_and_leave_task_processing(
    content_type: str, payload: bytes, code: str, token: str
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(session, raw(b'{"a":1}', 1))
            stored_raw = await session.scalar(select(RawObservationRecord))
            assert stored_raw is not None
            stored_raw.content_type, stored_raw.payload = content_type, payload
        async with sessions.begin() as session:
            item = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
        with pytest.raises(SchemaObservationFailure) as raised:
            async with sessions.begin() as session:
                await service.process_claim(session, item)
        assert (
            raised.value.code == code
            and token not in str(raised.value)
            and token not in repr(raised.value)
        )
        async with sessions() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert task is not None and task.state == "processing"
            assert await session.scalar(select(ObservedSchema)) is None
            assert await session.scalar(select(ObservedField)) is None
            assert await session.scalar(select(SchemaDriftEvent)) is None
            assert await session.scalar(select(SchemaObservationRecord)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_stale_claim_before_persistence_creates_no_schema() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(
                session, raw(b'{"value":"PRIVATE_STALE_BEFORE_SECRET_6103"}', 1)
            )
        async with sessions.begin() as session:
            item = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
        later = item.processing_started_at + timedelta(seconds=5)
        async with sessions.begin() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert task is not None
            task.attempt_count += 1
            task.processing_started_at = later
        with pytest.raises(StaleProcessingClaim) as raised:
            async with sessions.begin() as session:
                await service.process_claim(session, item)
        assert "PRIVATE_STALE_BEFORE_SECRET_6103" not in str(raised.value)
        async with sessions() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert task is not None
            assert task.attempt_count == item.attempt_count + 1
            assert task.processing_started_at is not None
            assert task.processing_started_at.replace(tzinfo=UTC) == later
            assert await session.scalar(select(ObservedSchema)) is None
            assert await session.scalar(select(SchemaObservationRecord)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_missing_raw_is_bounded_and_leaves_task_processing() -> None:
    async def missing_loader(session: AsyncSession, raw_id: UUID) -> RawObservationRecord | None:
        del session, raw_id
        return None

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    service = SchemaObservationService(raw_loader=missing_loader)
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(session, raw(b'{"value":"PRIVATE_MISSING_RAW_SECRET"}', 1))
        async with sessions.begin() as session:
            item = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
        with pytest.raises(SchemaObservationFailure) as raised:
            async with sessions.begin() as session:
                await service.process_claim(session, item)
        assert (
            raised.value.code == "raw_observation_missing"
            and "PRIVATE_MISSING_RAW_SECRET" not in str(raised.value)
            and "PRIVATE_MISSING_RAW_SECRET" not in repr(raised.value)
            and "ProcessingTaskItem" not in repr(raised.value)
        )
        async with sessions() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert (
                task is not None
                and task.state == "processing"
                and task.attempt_count == item.attempt_count
                and task.processing_started_at is not None
            )
            assert await session.scalar(select(ObservedSchema)) is None
            assert await session.scalar(select(ObservedField)) is None
            assert await session.scalar(select(SchemaDriftEvent)) is None
            assert await session.scalar(select(SchemaObservationRecord)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_stale_finalization_rolls_back_schema_reuse() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    catalog = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    tasks = ProcessingTaskRepository()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await catalog.record_raw(session, raw(b'{"a":1}', 1))
        async with sessions.begin() as session:
            first = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
            await SchemaObservationService().process_claim(session, first)
        async with sessions() as session:
            schema = await session.scalar(select(ObservedSchema))
            assert schema is not None
            baseline = (
                schema.id,
                schema.version_number,
                schema.fingerprint,
                schema.observation_count,
                schema.last_observed_at,
                len((await session.scalars(select(SchemaObservationRecord))).all()),
                len((await session.scalars(select(ObservedField))).all()),
                len((await session.scalars(select(SchemaDriftEvent))).all()),
            )
        async with sessions.begin() as session:
            await catalog.record_raw(session, raw(b'{"a": "PRIVATE_REUSED_SCHEMA_SECRET_3814"}', 2))
        async with sessions.begin() as session:
            second = (
                await tasks.claim(
                    session,
                    SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
        with pytest.raises(StaleProcessingClaim) as raised:
            async with sessions.begin() as session:
                await SchemaObservationService(tasks=FailingFinalizeRepository()).process_claim(
                    session, second
                )
        assert "PRIVATE_REUSED_SCHEMA_SECRET_3814" not in str(raised.value)
        assert "PRIVATE_REUSED_SCHEMA_SECRET_3814" not in repr(raised.value)
        async with sessions() as session:
            stored = await session.scalar(select(ObservedSchema))
            records = (await session.scalars(select(SchemaObservationRecord))).all()
            fields = (await session.scalars(select(ObservedField))).all()
            drift_events = (await session.scalars(select(SchemaDriftEvent))).all()
            task = await session.get(ObservationProcessingTask, UUID(second.id))
        assert (
            stored is not None
            and (
                stored.id,
                stored.version_number,
                stored.fingerprint,
                stored.observation_count,
                stored.last_observed_at,
                len(records),
                len(fields),
                len(drift_events),
            )
            == baseline
        )
        assert (
            task is not None
            and task.state == "processing"
            and task.attempt_count == second.attempt_count
            and task.processing_started_at is not None
        )
    finally:
        await engine.dispose()
