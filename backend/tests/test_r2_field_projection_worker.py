from __future__ import annotations

import asyncio
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
    RawObservationRecord,
    Stream,
)
from app.services.field_projection_contract import (
    FIELD_POINT_SCHEMA_VERSION,
    FIELD_PROJECTION_PROCESSOR_TYPE,
    FIELD_PROJECTION_PROCESSOR_VERSION,
)
from app.services.field_projection_service import FieldProjectionFailure, FieldProjectionService
from app.services.field_projection_worker import FieldProjectionWorker
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.stream_catalog import StreamCatalogService


async def create_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        for model in (
            Stream,
            ObservationEvidence,
            ObservationOutbox,
            RawObservationRecord,
            ObservationProcessingTask,
        ):
            await connection.run_sync(cast(Table, model.__table__).create)


async def database(
    tmp_path: Path, enabled: bool = True, name: str = "field-projection-worker.db"
) -> Database:
    instance = Database(
        Settings(
            database_url=f"sqlite+aiosqlite:///{tmp_path / name}",
            field_projection_worker_enabled=enabled,
        )
    )
    await instance.initialize()
    await create_tables(instance.get_engine())
    return instance


async def add_raw(database: Database, payload: bytes, second: int = 1) -> None:
    observation = RawObservation(
        source_id="field-worker-source",
        source_type="mqtt",
        external_stream_id="field/worker/telemetry",
        payload=payload,
        received_at=datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC),
        content_type="application/json",
        transport_metadata={},
    )
    async with database.transaction() as session:
        await StreamCatalogService(Settings(mqtt_topic_allowlist=["field/#"])).record_raw(
            session, observation
        )


async def field_task(session: AsyncSession) -> ObservationProcessingTask:
    task = await session.scalar(
        select(ObservationProcessingTask).where(
            ObservationProcessingTask.processor_type == FIELD_PROJECTION_PROCESSOR_TYPE,
            ObservationProcessingTask.processor_version == FIELD_PROJECTION_PROCESSOR_VERSION,
        )
    )
    assert task is not None
    return task


class FailingService(FieldProjectionService):
    def __init__(self, code: str) -> None:
        self._code = code

    async def process_claim(self, session: AsyncSession, item: ProcessingTaskItem) -> int:
        del session, item
        raise FieldProjectionFailure(self._code)


@pytest.mark.asyncio
async def test_worker_claims_completes_and_creates_field_rows(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = FieldProjectionWorker(instance._settings, instance)
    try:
        await add_raw(instance, b'{"nested":{"temperature":20,"active":true}}')
        assert await worker.run_once() == 1
        async with instance.session() as session:
            task = await field_task(session)
            rows = list((await session.scalars(select(ObservationOutbox))).all())
        projected = [
            row
            for row in rows
            if row.point_payload.get("content_schema_version") == FIELD_POINT_SCHEMA_VERSION
        ]
        assert task.state == "completed"
        assert len(projected) == 2
        assert worker.processed_count == 1 and worker.failed_count == 0
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_worker_completes_empty_object_without_field_rows(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = FieldProjectionWorker(instance._settings, instance)
    try:
        await add_raw(instance, b'{"empty":{},"items":[],"missing":null}')
        assert await worker.run_once() == 1
        async with instance.session() as session:
            task = await field_task(session)
            rows = list((await session.scalars(select(ObservationOutbox))).all())
        assert task.state == "completed"
        assert not [
            row
            for row in rows
            if row.point_payload.get("content_schema_version") == FIELD_POINT_SCHEMA_VERSION
        ]
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_worker_terminal_extraction_failure_dead_letters(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = FieldProjectionWorker(instance._settings, instance)
    try:
        await add_raw(instance, b'{"value":1}')
        async with instance.transaction() as session:
            raw = await session.scalar(select(RawObservationRecord))
            assert raw is not None
            raw.payload = b'{"invalid": PRIVATE_FIELD_WORKER_SECRET'
            raw.payload_size = len(raw.payload)
        assert await worker.run_once() == 0
        async with instance.session() as session:
            task = await field_task(session)
            rows = list((await session.scalars(select(ObservationOutbox))).all())
        assert task.state == "dead_letter" and task.last_error_code == "invalid_json"
        assert worker.last_error_code == "invalid_json"
        assert not [
            row
            for row in rows
            if row.point_payload.get("content_schema_version") == FIELD_POINT_SCHEMA_VERSION
        ]
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_worker_transient_service_failure_becomes_retryable(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    worker = FieldProjectionWorker(
        instance._settings,
        instance,
        service_factory=lambda: FailingService("temporary_infrastructure_error"),
    )
    before = datetime.now(UTC)
    try:
        await add_raw(instance, b'{"value":1}')
        assert await worker.run_once() == 0
        async with instance.session() as session:
            task = await field_task(session)
        assert task.state == "retryable" and task.attempt_count == 1
        assert task.available_at.replace(tzinfo=UTC) >= before + timedelta(seconds=2)
        assert task.last_error_code == "temporary_infrastructure_error"
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_worker_stale_claim_does_not_overwrite_newer_state(tmp_path: Path) -> None:
    instance = await database(tmp_path)
    repository = ProcessingTaskRepository()
    worker = FieldProjectionWorker(instance._settings, instance, repository)
    try:
        await add_raw(instance, b'{"value":1}')
        async with instance.transaction() as session:
            item = (
                await repository.claim(
                    session,
                    FIELD_PROJECTION_PROCESSOR_TYPE,
                    FIELD_PROJECTION_PROCESSOR_VERSION,
                    1,
                    30,
                )
            )[0]
        later = item.processing_started_at + timedelta(seconds=1)
        async with instance.transaction() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert task is not None
            task.attempt_count += 1
            task.processing_started_at = later
        await worker._finalize_failure(item, "unexpected_error", False)
        async with instance.session() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
        assert task is not None and task.state == "processing"
        assert task.attempt_count == item.attempt_count + 1
        assert task.processing_started_at is not None
        assert task.processing_started_at.replace(tzinfo=UTC) == later
    finally:
        await instance.dispose()


@pytest.mark.asyncio
async def test_worker_lifecycle_respects_enabled_setting(tmp_path: Path) -> None:
    disabled = await database(tmp_path, enabled=False)
    enabled = await database(tmp_path, enabled=True, name="field-projection-worker-enabled.db")
    try:
        disabled_worker = FieldProjectionWorker(disabled._settings, disabled)
        await disabled_worker.start()
        assert disabled_worker._task is None and not disabled_worker.running

        enabled_worker = FieldProjectionWorker(enabled._settings, enabled)
        await enabled_worker.start()
        first_task = enabled_worker._task
        await asyncio.sleep(0)
        await enabled_worker.start()
        assert (
            first_task is not None and enabled_worker._task is first_task and enabled_worker.running
        )
        await enabled_worker.stop()
        await enabled_worker.stop()
        assert enabled_worker._task is None and not enabled_worker.running
    finally:
        await disabled.dispose()
        await enabled.dispose()
