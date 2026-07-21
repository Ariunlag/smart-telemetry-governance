from __future__ import annotations

import hashlib
import json
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
from app.services.field_projection_service import (
    FIELD_POINT_SCHEMA_VERSION,
    FIELD_PROJECTION_PROCESSOR_TYPE,
    FIELD_PROJECTION_PROCESSOR_VERSION,
    FieldProjectionFailure,
    FieldProjectionService,
)
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.schema_observation_service import StaleProcessingClaim
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


def source_observation(
    payload: bytes,
    received_at: datetime,
    transport_metadata: dict[str, str] | None = None,
) -> RawObservation:
    return RawObservation(
        source_id="source-a",
        source_type="mqtt",
        external_stream_id="site/one/telemetry",
        payload=payload,
        received_at=received_at,
        content_type="application/json",
        transport_metadata=transport_metadata or {},
    )


async def create_claim(
    sessions: async_sessionmaker[AsyncSession],
    payload: bytes,
    received_at: datetime,
    transport_metadata: dict[str, str] | None = None,
    tenant: str | None = None,
) -> tuple[ProcessingTaskItem, FieldProjectionService, ProcessingTaskRepository]:
    settings = Settings(mqtt_topic_allowlist=["site/#"])
    catalog = StreamCatalogService(settings)
    service = FieldProjectionService(settings=settings)
    tasks = ProcessingTaskRepository()
    async with sessions.begin() as session:
        await catalog.record_raw(
            session, source_observation(payload, received_at, transport_metadata)
        )
        raw = await session.scalar(select(RawObservationRecord))
        stream = await session.scalar(select(Stream))
        assert raw is not None and stream is not None
        stream.tenant = tenant
    async with sessions.begin() as session:
        items = await tasks.claim(
            session,
            FIELD_PROJECTION_PROCESSOR_TYPE,
            FIELD_PROJECTION_PROCESSOR_VERSION,
            1,
            60,
        )
    assert len(items) == 1
    return items[0], service, tasks


async def field_rows(session: AsyncSession) -> list[ObservationOutbox]:
    rows = (await session.scalars(select(ObservationOutbox))).all()
    return [
        row
        for row in rows
        if row.point_payload.get("content_schema_version") == FIELD_POINT_SCHEMA_VERSION
    ]


def point_path(row: ObservationOutbox) -> str:
    return cast(str, row.point_payload["field_path"])


def point_snapshot(row: ObservationOutbox) -> str:
    return json.dumps(row.point_payload, sort_keys=True, separators=(",", ":"))


@pytest.mark.asyncio
async def test_field_projection_creates_versioned_outbox_rows() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    received_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    broker_timestamp = "2026-02-03T04:05:05Z"
    try:
        await create_tables(engine)
        item, service, _ = await create_claim(
            sessions,
            b'{"values":{"temperature":21,"humidity":45.5},"meta":{"label":"west"},"enabled":true}',
            received_at,
            {"timestamp": broker_timestamp},
            tenant="tenant-a",
        )
        async with sessions.begin() as session:
            assert await service.process_claim(session, item) == 4

        async with sessions() as session:
            rows = sorted(await field_rows(session), key=point_path)
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            raw = await session.get(RawObservationRecord, UUID(item.raw_observation_id))
            stream = await session.scalar(select(Stream))
        expected_paths = [
            '$["enabled"]',
            '$["meta"]["label"]',
            '$["values"]["humidity"]',
            '$["values"]["temperature"]',
        ]
        assert [point_path(row) for row in rows] == expected_paths
        assert [row.point_payload["value_type"] for row in rows] == [
            "boolean",
            "string",
            "float",
            "integer",
        ]
        assert [row.point_payload["value"] for row in rows] == [True, "west", 45.5, 21]
        assert raw is not None and stream is not None and task is not None
        expected_keys = {
            "stream_id",
            "source_id",
            "tenant",
            "topic",
            "observation_timestamp",
            "received_timestamp",
            "timestamp_source",
            "field_path",
            "value_type",
            "value",
            "content_schema_version",
            "quality_status",
            "provenance_reference",
        }
        for row in rows:
            assert set(row.point_payload) == expected_keys
            assert row.point_payload["stream_id"] == str(stream.id)
            assert row.point_payload["source_id"] == "source-a"
            assert row.point_payload["tenant"] == "tenant-a"
            assert row.point_payload["topic"] == "site/one/telemetry"
            assert row.point_payload["observation_timestamp"] == broker_timestamp.replace(
                "Z", "+00:00"
            )
            assert row.point_payload["received_timestamp"] == received_at.isoformat()
            assert row.point_payload["timestamp_source"] == "broker"
            assert row.point_payload["content_schema_version"] == FIELD_POINT_SCHEMA_VERSION
            assert row.point_payload["quality_status"] == "unassessed"
            assert row.point_payload["provenance_reference"] == str(raw.evidence_id)
            assert not {"payload", "transport_metadata", "database_url"} & set(row.point_payload)
        assert task.state == "completed"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_field_projection_delivery_keys_are_deterministic_and_idempotent() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    received_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    try:
        await create_tables(engine)
        first, service, tasks = await create_claim(
            sessions, b'{"nested":{"first":1,"second":2}}', received_at
        )
        async with sessions.begin() as session:
            assert await service.process_claim(session, first) == 2
        async with sessions() as session:
            raw = await session.get(RawObservationRecord, UUID(first.raw_observation_id))
            first_rows = sorted(await field_rows(session), key=point_path)
        assert raw is not None
        first_keys = {row.delivery_key for row in first_rows}
        expected_keys = {
            hashlib.sha256(
                "\x1f".join(
                    (raw.observation_key, FIELD_PROJECTION_PROCESSOR_VERSION, point_path(row))
                ).encode("utf-8")
            ).hexdigest()
            for row in first_rows
        }
        assert first_keys == expected_keys
        assert all(len(key) == 64 and int(key, 16) >= 0 for key in first_keys)

        async with sessions.begin() as session:
            task = await session.get(ObservationProcessingTask, UUID(first.id))
            assert task is not None
            task.state = "pending"
            task.processing_started_at = None
            task.completed_at = None
            task.available_at = received_at
        async with sessions.begin() as session:
            second = (
                await tasks.claim(
                    session,
                    FIELD_PROJECTION_PROCESSOR_TYPE,
                    FIELD_PROJECTION_PROCESSOR_VERSION,
                    1,
                    60,
                )
            )[0]
            assert await service.process_claim(session, second) == 2
        async with sessions() as session:
            repeated_rows = await field_rows(session)
        assert {row.delivery_key for row in repeated_rows} == first_keys
        assert len(repeated_rows) == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_field_projection_empty_object_completes_without_outbox_rows() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    received_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    try:
        await create_tables(engine)
        item, service, _ = await create_claim(
            sessions, b'{"empty":{},"items":[],"missing":null}', received_at
        )
        async with sessions.begin() as session:
            assert await service.process_claim(session, item) == 0
        async with sessions() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            rows = await field_rows(session)
        assert task is not None and task.state == "completed"
        assert rows == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_field_projection_failure_persists_no_partial_rows() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    received_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    secret = "PRIVATE_INVALID_FIELD_PAYLOAD_7281"
    try:
        await create_tables(engine)
        item, service, _ = await create_claim(sessions, b'{"safe":1}', received_at)
        async with sessions.begin() as session:
            raw = await session.get(RawObservationRecord, UUID(item.raw_observation_id))
            assert raw is not None
            raw.payload = f'{{"secret": {secret}'.encode()
            raw.payload_size = len(raw.payload)
        async with sessions() as session:
            raw = await session.get(RawObservationRecord, UUID(item.raw_observation_id))
            evidence = await session.get(ObservationEvidence, raw.evidence_id if raw else None)
            stream = await session.get(Stream, raw.stream_id if raw else None)
        assert raw is not None and evidence is not None and stream is not None
        baseline = (raw.payload, evidence.id, stream.id, stream.observation_count)

        with pytest.raises(FieldProjectionFailure) as raised:
            async with sessions.begin() as session:
                await service.process_claim(session, item)
        assert raised.value.code == "invalid_json"
        assert secret not in str(raised.value)
        async with sessions() as session:
            stored_raw = await session.get(RawObservationRecord, UUID(item.raw_observation_id))
            stored_evidence = await session.get(ObservationEvidence, evidence.id)
            stored_stream = await session.get(Stream, stream.id)
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            rows = await field_rows(session)
        assert stored_raw is not None and stored_evidence is not None and stored_stream is not None
        assert (
            stored_raw.payload,
            stored_evidence.id,
            stored_stream.id,
            stored_stream.observation_count,
        ) == baseline
        assert task is not None and task.state == "processing"
        assert rows == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_field_projection_rejects_stale_claim_without_writes() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    received_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    try:
        await create_tables(engine)
        item, service, _ = await create_claim(sessions, b'{"value":1}', received_at)
        newer_started_at = item.processing_started_at + timedelta(seconds=1)
        async with sessions.begin() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            assert task is not None
            task.attempt_count += 1
            task.processing_started_at = newer_started_at
        with pytest.raises(StaleProcessingClaim):
            async with sessions.begin() as session:
                await service.process_claim(session, item)
        async with sessions() as session:
            task = await session.get(ObservationProcessingTask, UUID(item.id))
            rows = await field_rows(session)
        assert task is not None and task.state == "processing"
        assert task.attempt_count == item.attempt_count + 1
        assert task.processing_started_at is not None
        assert task.processing_started_at.replace(tzinfo=UTC) == newer_started_at
        assert rows == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_field_projection_preserves_legacy_outbox_rows() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    received_at = datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    try:
        await create_tables(engine)
        item, service, _ = await create_claim(
            sessions,
            b'{"metric":"temperature","value":21,"unit":"C","nested":{"humidity":40}}',
            received_at,
        )
        async with sessions() as session:
            legacy = await session.scalar(select(ObservationOutbox))
        assert legacy is not None
        legacy_identity = (legacy.id, legacy.delivery_key, point_snapshot(legacy))

        async with sessions.begin() as session:
            assert await service.process_claim(session, item) == 4
        async with sessions() as session:
            rows = (await session.scalars(select(ObservationOutbox))).all()
            preserved_legacy = next(
                row
                for row in rows
                if row.point_payload.get("content_schema_version") == "r1.normalized-point.v1"
            )
            projected = await field_rows(session)
        assert (
            preserved_legacy.id,
            preserved_legacy.delivery_key,
            point_snapshot(preserved_legacy),
        ) == legacy_identity
        assert len(projected) == 4
        assert all(
            row.point_payload["content_schema_version"] == FIELD_POINT_SCHEMA_VERSION
            for row in projected
        )
        assert legacy.delivery_key not in {row.delivery_key for row in projected}
    finally:
        await engine.dispose()
