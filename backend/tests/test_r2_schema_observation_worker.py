from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.core.config import Settings
from app.core.contracts import RawObservation
from app.db.session import Database
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
)
from app.services.schema_observation_worker import SchemaObservationWorker
from app.services.stream_catalog import StreamCatalogService

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


async def database(tmp_path: Path, enabled: bool = True) -> Database:
    instance = Database(
        Settings(
            database_url=f"sqlite+aiosqlite:///{tmp_path / 'schema-worker.db'}",
            schema_observation_worker_enabled=enabled,
        )
    )
    await instance.initialize()
    await create_tables(instance.get_engine())
    return instance


def raw(payload: bytes, second: int) -> RawObservation:
    return RawObservation(
        source_id="schema-worker-source",
        source_type="mqtt",
        external_stream_id="schema/worker/telemetry",
        payload=payload,
        received_at=datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC),
        content_type="application/json",
        transport_metadata={},
    )


async def add_raw(database: Database, payload: bytes, second: int) -> None:
    async with database.transaction() as session:
        await StreamCatalogService(Settings(mqtt_topic_allowlist=["schema/#"])).record_raw(
            session, raw(payload, second)
        )


class FailingService(SchemaObservationService):
    def __init__(self, code: str) -> None:
        self._code = code

    async def process_claim(
        self, session: AsyncSession, item: ProcessingTaskItem
    ) -> ObservedSchema:
        del session, item
        raise SchemaObservationFailure(self._code)


@pytest.mark.asyncio
async def test_disabled_worker_does_not_start(tmp_path: Path) -> None:
    instance = await database(tmp_path, enabled=False)
    worker = SchemaObservationWorker(instance._settings, instance)
    try:
        await worker.start()
        assert worker._task is None
        assert not worker.running
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_enabled_worker_starts_once_and_stops_cleanly(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = SchemaObservationWorker(instance._settings, instance)
    try:
        await worker.start()
        first_task = worker._task
        await asyncio.sleep(0)
        await worker.start()
        assert first_task is not None and worker._task is first_task
        assert worker.running
        await worker.stop()
        await worker.stop()
        assert worker._task is None and not worker.running
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_worker_completes_schema_task_without_changing_outbox(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = SchemaObservationWorker(instance._settings, instance)
    try:
        await add_raw(instance, b'{"temperature":20}', 1)
        async with instance.session() as session:
            before = list((await session.scalars(select(ObservationOutbox))).all())
        assert await worker.run_once() == 1
        async with instance.session() as session:
            task = await session.scalar(select(ObservationProcessingTask))
            schema = await session.scalar(select(ObservedSchema))
            fields = list((await session.scalars(select(ObservedField))).all())
            records = list((await session.scalars(select(SchemaObservationRecord))).all())
            after = list((await session.scalars(select(ObservationOutbox))).all())
        assert task is not None and task.state == "completed"
        assert schema is not None and fields and len(records) == 1
        assert [(item.id, item.state, item.attempt_count) for item in after] == [
            (item.id, item.state, item.attempt_count) for item in before
        ]
    finally:
        await instance.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "raw_observation_missing",
        "invalid_json",
        "unsupported_persisted_input",
        "schema_depth_exceeded",
        "schema_field_limit_exceeded",
        "schema_node_limit_exceeded",
        "schema_path_too_long",
        "schema_document_too_large",
    ],
)
async def test_terminal_schema_failures_dead_letter_without_schema_output(
    tmp_path: Path, code: str
) -> None:
    instance = await database(tmp_path)
    worker = SchemaObservationWorker(
        instance._settings, instance, service_factory=lambda: FailingService(code)
    )
    try:
        await add_raw(instance, b'{"value":"PRIVATE_WORKER_SECRET"}', 1)
        assert await worker.run_once() == 0
        async with instance.session() as session:
            task = await session.scalar(select(ObservationProcessingTask))
            assert task is not None
            assert task.state == "dead_letter"
            assert task.last_error_code == code
            assert task.last_error_detail == code
            assert await session.scalar(select(ObservedSchema)) is None
            assert await session.scalar(select(SchemaObservationRecord)) is None
        assert "PRIVATE_WORKER_SECRET" not in str(worker.last_error_code)
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_transient_failure_retries_with_bounded_backoff(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = SchemaObservationWorker(
        instance._settings,
        instance,
        service_factory=lambda: FailingService("temporary_infrastructure_error"),
    )
    before = datetime.now(UTC)
    try:
        await add_raw(instance, b'{"value":1}', 1)
        assert await worker.run_once() == 0
        async with instance.session() as session:
            task = await session.scalar(select(ObservationProcessingTask))
        assert task is not None
        assert task.state == "retryable"
        assert task.attempt_count == 1
        assert task.available_at.replace(tzinfo=UTC) >= before + timedelta(seconds=2)
        assert task.last_error_code == "temporary_infrastructure_error"
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_transient_failure_at_max_attempts_dead_letters(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = SchemaObservationWorker(
        instance._settings,
        instance,
        service_factory=lambda: FailingService("temporary_infrastructure_error"),
    )
    try:
        await add_raw(instance, b'{"value":1}', 1)
        async with instance.transaction() as session:
            task = await session.scalar(select(ObservationProcessingTask))
            assert task is not None
            task.attempt_count = instance._settings.schema_observation_worker_max_attempts - 1
        await worker.run_once()
        async with instance.session() as session:
            task = await session.scalar(select(ObservationProcessingTask))
        assert task is not None
        assert task.state == "dead_letter"
        assert task.attempt_count == instance._settings.schema_observation_worker_max_attempts
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_stale_failure_finalization_preserves_newer_claim(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    repository = ProcessingTaskRepository()
    worker = SchemaObservationWorker(instance._settings, instance, repository)
    try:
        await add_raw(instance, b'{"value":1}', 1)
        async with instance.transaction() as session:
            item = (await repository.claim(session, "schema_observation", "r2.schema.v1", 1, 30))[0]
        later = item.processing_started_at + timedelta(seconds=1)
        async with instance.transaction() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert task is not None
            task.attempt_count += 1
            task.processing_started_at = later
        await worker._finalize_failure(item, "unexpected_error", False)
        async with instance.session() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
        assert task is not None
        assert task.state == "processing"
        assert task.attempt_count == item.attempt_count + 1
        assert task.processing_started_at is not None
        assert task.processing_started_at.replace(tzinfo=UTC) == later
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_batch_is_bounded_and_unrelated_tasks_are_untouched(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = SchemaObservationWorker(
        instance._settings.model_copy(update={"schema_observation_worker_batch_size": 1}), instance
    )
    try:
        await add_raw(instance, b'{"one":1}', 1)
        await add_raw(instance, b'{"two":2}', 2)
        async with instance.transaction() as session:
            tasks = [
                task
                for task in (await session.scalars(select(ObservationProcessingTask))).all()
                if task.processor_type == "schema_observation"
            ]
            tasks[1].processor_version = "other"
        assert await worker.run_once() == 1
        async with instance.session() as session:
            tasks = [
                task
                for task in (await session.scalars(select(ObservationProcessingTask))).all()
                if task.processor_type == "schema_observation"
            ]
        assert [task.state for task in tasks].count("completed") == 1
        assert [task.state for task in tasks].count("pending") == 1
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_one_terminal_failure_does_not_prevent_later_success(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    try:
        await add_raw(instance, b'{"value":1}', 1)
        await add_raw(instance, b'{"value":2}', 2)
        async with instance.transaction() as session:
            first = await session.scalar(
                select(RawObservationRecord).order_by(RawObservationRecord.created_at)
            )
            assert first is not None
            first.payload = b'{"invalid": PRIVATE_WORKER_LOOP_SECRET'
        worker = SchemaObservationWorker(instance._settings, instance)
        assert await worker.run_once() == 1
        async with instance.session() as session:
            tasks = [
                task
                for task in (await session.scalars(select(ObservationProcessingTask))).all()
                if task.processor_type == "schema_observation"
            ]
        assert sorted(task.state for task in tasks) == ["completed", "dead_letter"]
    finally:
        await instance.dispose()


@pytest.mark.postgresql
@pytest.mark.asyncio
async def test_postgresql_workers_claim_distinct_tasks_and_serialize_schema_updates(
    postgresql_engine: AsyncEngine,
) -> None:
    del postgresql_engine
    database_url = os.environ.get("TEST_DATABASE_URL")
    assert database_url is not None
    instance = Database(
        Settings(
            database_url=database_url,
            schema_observation_worker_enabled=True,
            schema_observation_worker_batch_size=1,
        )
    )
    await instance.initialize()
    first = SchemaObservationWorker(instance._settings, instance)
    second = SchemaObservationWorker(instance._settings, instance)
    try:
        await add_raw(instance, b'{"temperature":20}', 1)
        await add_raw(instance, b'{"temperature":999}', 2)
        results = await asyncio.gather(first.run_once(), second.run_once())
        assert sorted(results) == [1, 1]
        async with instance.session() as session:
            tasks = [
                task
                for task in (await session.scalars(select(ObservationProcessingTask))).all()
                if task.processor_type == "schema_observation"
            ]
            schemas = list((await session.scalars(select(ObservedSchema))).all())
            records = list((await session.scalars(select(SchemaObservationRecord))).all())
        assert len(tasks) == 2 and all(task.state == "completed" for task in tasks)
        assert len(schemas) == 1 and schemas[0].observation_count == 2
        assert len(records) == 2
    finally:
        await instance.dispose()
