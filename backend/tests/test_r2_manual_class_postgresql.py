from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.classes.models import ClassMembership, SavedClassQuery, TelemetryClass
from app.domain.sources.models import Site, TelemetrySource, Tenant
from app.domain.streams.models import (
    ObservationEvidence,
    ObservationOutbox,
    ObservationProcessingTask,
    RawObservationRecord,
    Stream,
)
from app.services.manual_class_service import ManualClassError, ManualClassService, SavedQuerySpec

pytestmark = pytest.mark.postgresql


def query_spec(stream_id: UUID) -> SavedQuerySpec:
    return SavedQuerySpec.model_validate(
        {
            "spec_version": "saved-class-query.v1",
            "series": [
                {"stream_id": str(stream_id), "field_path": '$["temperature"]', "alias": None}
            ],
            "time_window": {"mode": "relative", "lookback_seconds": 3600},
            "aggregation": {"function": "raw", "bucket_seconds": None},
            "live_append": False,
            "visualization": {"kind": "line"},
        }
    )


async def seed_tenant_streams(
    sessions: async_sessionmaker[AsyncSession], tenant_key: str, count: int = 1
) -> tuple[UUID, list[UUID]]:
    async with sessions() as session:
        async with session.begin():
            tenant = Tenant(tenant_key=tenant_key, display_name=tenant_key)
            session.add(tenant)
            await session.flush()
            site = Site(tenant_id=tenant.id, site_key=f"{tenant_key}-site", display_name="Site")
            session.add(site)
            await session.flush()
            source = TelemetrySource(
                tenant_id=tenant.id,
                site_id=site.id,
                source_key=f"{tenant_key}-source",
                display_name="Source",
            )
            session.add(source)
            await session.flush()
            observed_at = datetime.now(UTC)
            streams = [
                Stream(
                    stream_key=f"{tenant_key}-stream-{number}",
                    source_id=str(source.id),
                    topic=f"telemetry/{tenant_key}/{number}",
                    tenant=tenant_key,
                    first_observed_at=observed_at,
                    last_observed_at=observed_at,
                    observation_count=1,
                )
                for number in range(count)
            ]
            session.add_all(streams)
            await session.flush()
            return tenant.id, [stream.id for stream in streams]


async def create_class(
    sessions: async_sessionmaker[AsyncSession], tenant_id: UUID, name: str
) -> TelemetryClass:
    async with sessions() as session:
        async with session.begin():
            return await ManualClassService().create_class(session, tenant_id, name, "Description")


async def create_query(
    sessions: async_sessionmaker[AsyncSession],
    class_id: UUID,
    tenant_id: UUID,
    stream_id: UUID,
    name: str,
) -> SavedClassQuery:
    async with sessions() as session:
        async with session.begin():
            service = ManualClassService()
            return await service.create_query(
                session, tenant_id, class_id, name, "Description", query_spec(stream_id)
            )


async def add_member(
    sessions: async_sessionmaker[AsyncSession], class_id: UUID, tenant_id: UUID, stream_id: UUID
) -> None:
    async with sessions() as session:
        async with session.begin():
            await ManualClassService().add_members(session, tenant_id, class_id, [stream_id])


async def count_rows(
    sessions: async_sessionmaker[AsyncSession],
    model: type[TelemetryClass] | type[ClassMembership] | type[SavedClassQuery],
) -> int:
    async with sessions() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


@pytest.mark.asyncio
async def test_postgresql_manual_class_model_migration_parity(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with postgresql_sessions() as session:
        tables = set(
            (
                await session.scalars(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
            ).all()
        )
        constraints = set(
            (
                await session.scalars(
                    text(
                        "SELECT conname FROM pg_constraint WHERE conname IN "
                        "('uq_telemetry_classes_tenant_name_key', "
                        "'uq_class_memberships_class_stream', "
                        "'uq_saved_class_queries_class_name_key')"
                    )
                )
            ).all()
        )
        indexes = set(
            (
                await session.scalars(
                    text(
                        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' "
                        "AND indexname IN ('ix_telemetry_classes_tenant_name', "
                        "'ix_class_memberships_class', 'ix_saved_class_queries_class_name')"
                    )
                )
            ).all()
        )
        columns = set(
            (
                await session.scalars(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'saved_class_queries'"
                    )
                )
            ).all()
        )
    assert {"telemetry_classes", "class_memberships", "saved_class_queries"} <= tables
    assert constraints == {
        "uq_telemetry_classes_tenant_name_key",
        "uq_class_memberships_class_stream",
        "uq_saved_class_queries_class_name_key",
    }
    assert indexes == {
        "ix_telemetry_classes_tenant_name",
        "ix_class_memberships_class",
        "ix_saved_class_queries_class_name",
    }
    assert {"tenant_id", "telemetry_class_id", "name_key", "query_spec"} <= columns


@pytest.mark.asyncio
async def test_postgresql_concurrent_normalized_class_name_uniqueness(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    tenant_id, _ = await seed_tenant_streams(postgresql_sessions, "tenant-a")

    async def attempt(name: str) -> bool:
        try:
            await create_class(postgresql_sessions, tenant_id, name)
        except ManualClassError as error:
            assert error.code == "duplicate_class_name"
            return False
        return True

    results = await asyncio.gather(
        attempt("Building Temperature"), attempt(" building temperature")
    )
    assert sorted(results) == [False, True]
    assert await count_rows(postgresql_sessions, TelemetryClass) == 1
    other_tenant, _ = await seed_tenant_streams(postgresql_sessions, "tenant-b")
    await create_class(postgresql_sessions, other_tenant, "BUILDING TEMPERATURE")
    assert await count_rows(postgresql_sessions, TelemetryClass) == 2


@pytest.mark.asyncio
async def test_postgresql_concurrent_saved_query_and_membership_uniqueness(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    tenant_id, streams = await seed_tenant_streams(postgresql_sessions, "tenant-a")
    telemetry_class = await create_class(postgresql_sessions, tenant_id, "Class A")
    await add_member(postgresql_sessions, telemetry_class.id, tenant_id, streams[0])

    async def query_attempt(name: str) -> bool:
        try:
            await create_query(postgresql_sessions, telemetry_class.id, tenant_id, streams[0], name)
        except ManualClassError as error:
            assert error.code == "duplicate_query_name"
            return False
        return True

    query_results = await asyncio.gather(
        query_attempt("Temperature Overview"), query_attempt(" temperature overview")
    )
    assert sorted(query_results) == [False, True]
    assert await count_rows(postgresql_sessions, SavedClassQuery) == 1

    membership_class = await create_class(postgresql_sessions, tenant_id, "Membership Class")

    async def membership_attempt() -> bool:
        try:
            await add_member(postgresql_sessions, membership_class.id, tenant_id, streams[0])
        except (IntegrityError, ManualClassError):
            return False
        return True

    membership_results = await asyncio.gather(membership_attempt(), membership_attempt())
    assert sorted(membership_results) == [False, True]
    async with postgresql_sessions() as session:
        member_count = await session.scalar(
            select(func.count())
            .select_from(ClassMembership)
            .where(ClassMembership.telemetry_class_id == membership_class.id)
        )
    assert member_count == 1
    other_class = await create_class(postgresql_sessions, tenant_id, "Class B")
    await add_member(postgresql_sessions, other_class.id, tenant_id, streams[0])
    await create_query(
        postgresql_sessions, other_class.id, tenant_id, streams[0], "Temperature Overview"
    )
    assert await count_rows(postgresql_sessions, SavedClassQuery) == 2


@pytest.mark.asyncio
async def test_postgresql_manual_class_rollback_and_deletion_preservation(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    tenant_id, streams = await seed_tenant_streams(postgresql_sessions, "tenant-a", count=2)
    other_tenant, other_streams = await seed_tenant_streams(postgresql_sessions, "tenant-b")
    telemetry_class = await create_class(postgresql_sessions, tenant_id, "Class A")
    with pytest.raises(ManualClassError, match="stream_not_found"):
        async with postgresql_sessions() as session:
            async with session.begin():
                await ManualClassService().add_members(
                    session, tenant_id, telemetry_class.id, [streams[0], other_streams[0]]
                )
    assert await count_rows(postgresql_sessions, ClassMembership) == 0
    await add_member(postgresql_sessions, telemetry_class.id, tenant_id, streams[0])
    await add_member(postgresql_sessions, telemetry_class.id, tenant_id, streams[1])
    await create_query(
        postgresql_sessions, telemetry_class.id, tenant_id, streams[0], "First Query"
    )
    await create_query(
        postgresql_sessions, telemetry_class.id, tenant_id, streams[1], "Second Query"
    )

    async with postgresql_sessions() as session:
        async with session.begin():
            now = datetime.now(UTC)
            evidence = ObservationEvidence(
                stream_id=streams[0],
                received_at=now,
                outcome="accepted",
                payload_size=2,
                payload_fingerprint="a" * 64,
            )
            session.add(evidence)
            await session.flush()
            raw = RawObservationRecord(
                observation_key=f"raw-{uuid4()}",
                stream_id=streams[0],
                evidence_id=evidence.id,
                source_id="postgres-test-source",
                source_type="mqtt",
                external_stream_id="postgres/test",
                received_at=now,
                payload=b"{}",
                payload_size=2,
                payload_fingerprint="b" * 64,
                retention_until=now + timedelta(days=1),
            )
            session.add(raw)
            await session.flush()
            task = ObservationProcessingTask(
                raw_observation_id=raw.id,
                processor_type="schema_observation",
                processor_version="test-v1",
                state="pending",
                available_at=now,
            )
            outbox = ObservationOutbox(
                delivery_key=f"delivery-{uuid4()}",
                stream_id=streams[0],
                evidence_id=evidence.id,
                state="pending",
                point_payload={"value": 1},
                available_at=now,
            )
            session.add_all((task, outbox))
            await session.flush()
            raw_id, task_id, outbox_id = raw.id, task.id, outbox.id
    unrelated = await create_class(postgresql_sessions, other_tenant, "Class B")
    await add_member(postgresql_sessions, unrelated.id, other_tenant, other_streams[0])
    await create_query(
        postgresql_sessions, unrelated.id, other_tenant, other_streams[0], "Other Query"
    )

    async with postgresql_sessions() as session:
        async with session.begin():
            await ManualClassService().delete_class(session, tenant_id, telemetry_class.id)
    async with postgresql_sessions() as session:
        assert await session.get(TelemetryClass, telemetry_class.id) is None
        assert await session.scalar(select(func.count()).select_from(ClassMembership)) == 1
        assert await session.scalar(select(func.count()).select_from(SavedClassQuery)) == 1
        assert await session.get(Stream, streams[0]) is not None
        assert await session.get(RawObservationRecord, raw_id) is not None
        stored_task = await session.get(ObservationProcessingTask, task_id)
        stored_outbox = await session.get(ObservationOutbox, outbox_id)
    assert stored_task is not None and stored_task.state == "pending"
    assert stored_outbox is not None and stored_outbox.state == "pending"
