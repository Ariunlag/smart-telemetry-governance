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
    RawObservationRecord,
    Stream,
)
from app.services.processing_task_repository import ProcessingTaskRepository
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


async def add_task(
    session: AsyncSession,
    *,
    suffix: int,
    processor_type: str = "schema_observation",
    processor_version: str = "r2.schema.v1",
    state: str = "pending",
    available_at: datetime | None = None,
) -> ObservationProcessingTask:
    received = datetime(2026, 1, 1, 0, 0, suffix, tzinfo=UTC)
    service = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    await service.record_raw(
        session,
        RawObservation(
            source_id="source-a",
            source_type="mqtt",
            external_stream_id=f"site/one/telemetry/{suffix}",
            payload=(
                '{"metric":"temperature","value":21,"timestamp":"2026-01-01T00:00:'
                f'{suffix:02d}+00:00"}}'
            ).encode(),
            received_at=received,
            content_type="application/json",
            transport_metadata={},
        ),
    )
    raw = await session.scalar(
        select(RawObservationRecord).where(
            RawObservationRecord.external_stream_id == f"site/one/telemetry/{suffix}"
        )
    )
    assert raw is not None
    task = await session.scalar(
        select(ObservationProcessingTask).where(
            ObservationProcessingTask.raw_observation_id == raw.id,
            ObservationProcessingTask.processor_type == "schema_observation",
            ObservationProcessingTask.processor_version == "r2.schema.v1",
        )
    )
    assert task is not None
    task.processor_type = processor_type
    task.processor_version = processor_version
    task.state = state
    task.available_at = available_at or datetime.now(UTC) - timedelta(seconds=1)
    if state == "processing":
        task.processing_started_at = datetime.now(UTC) - timedelta(seconds=120)
        task.attempt_count = 1
    await session.flush()
    return task


@pytest.mark.asyncio
async def test_claim_selects_eligible_processor_tasks_in_order_and_limit() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = ProcessingTaskRepository()
    now = datetime.now(UTC)
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            first = await add_task(session, suffix=1, available_at=now - timedelta(seconds=2))
            second = await add_task(
                session, suffix=2, state="retryable", available_at=now - timedelta(seconds=1)
            )
            await add_task(session, suffix=3, processor_type="other")
            await add_task(session, suffix=4, available_at=now + timedelta(hours=1))
        async with sessions.begin() as session:
            claimed = await repository.claim(session, "schema_observation", "r2.schema.v1", 2, 60)
        assert [item.id for item in claimed] == [str(first.id), str(second.id)]
        assert all(
            item.attempt_count == 1 and item.processing_started_at.tzinfo for item in claimed
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lease_recovery_and_optimistic_finalization() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = ProcessingTaskRepository()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            stale = await add_task(session, suffix=10, state="processing")
            live = await add_task(session, suffix=11, state="processing")
            live.processing_started_at = datetime.now(UTC)
        async with sessions.begin() as session:
            claimed = await repository.claim(session, "schema_observation", "r2.schema.v1", 5, 60)
            assert [item.id for item in claimed] == [str(stale.id)]
            item = claimed[0]
            assert item.attempt_count == 2
            assert not await repository.finalize(
                session,
                item.__class__(
                    item.id,
                    item.raw_observation_id,
                    item.processor_type,
                    item.processor_version,
                    1,
                    item.processing_started_at,
                ),
                "completed",
            )
            assert await repository.finalize(session, item, "completed")
        async with sessions() as session:
            completed = await session.get(ObservationProcessingTask, stale.id)
            live_task = await session.get(ObservationProcessingTask, live.id)
            assert completed is not None and completed.completed_at is not None
            assert completed.processing_started_at is None and completed.last_error_code is None
            assert live_task is not None and live_task.state == "processing"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_retry_dead_letter_and_counts_are_isolated() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = ProcessingTaskRepository()
    try:
        await create_tables(engine)
        async with sessions.begin() as session:
            await add_task(session, suffix=20)
            await add_task(session, suffix=21)
            await add_task(session, suffix=22, processor_version="other")
        async with sessions.begin() as session:
            first, second = await repository.claim(
                session, "schema_observation", "r2.schema.v1", 2, 60
            )
            assert await repository.finalize(
                session, first, "retryable", datetime.now(UTC) + timedelta(minutes=1), "transient"
            )
            assert await repository.finalize(session, second, "dead_letter", code="permanent")
        async with sessions() as session:
            counts = await repository.counts(session, "schema_observation", "r2.schema.v1")
            assert counts == {"dead_letter": 1, "retryable": 1}
            retry = await session.get(ObservationProcessingTask, UUID(first.id))
            dead = await session.get(ObservationProcessingTask, UUID(second.id))
            assert (
                retry is not None
                and retry.processing_started_at is None
                and retry.last_error_code == "transient"
            )
            assert (
                dead is not None
                and dead.processing_started_at is None
                and dead.state == "dead_letter"
            )
    finally:
        await engine.dispose()


@pytest.mark.postgresql
@pytest.mark.asyncio
async def test_postgresql_concurrent_claims_do_not_overlap(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    repository = ProcessingTaskRepository()
    async with postgresql_sessions.begin() as session:
        await add_task(session, suffix=30)
        await add_task(session, suffix=31)
    async with postgresql_sessions.begin() as first_session:
        first = await repository.claim(first_session, "schema_observation", "r2.schema.v1", 1, 60)
        async with postgresql_sessions.begin() as second_session:
            second = await repository.claim(
                second_session, "schema_observation", "r2.schema.v1", 1, 60
            )
    assert len(first) == len(second) == 1
    assert first[0].id != second[0].id
