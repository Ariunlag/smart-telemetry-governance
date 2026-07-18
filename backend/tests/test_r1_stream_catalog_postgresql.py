from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.domain.streams.models import ObservationEvidence, Stream
from app.services.stream_catalog import ObservationCommand, StreamCatalogService

pytestmark = pytest.mark.postgresql


def build_service() -> StreamCatalogService:
    return StreamCatalogService(
        Settings(
            database_url="postgresql+psycopg://test:test@localhost/test",
            mqtt_topic_allowlist=["site/#"],
        )
    )


async def record(factory: async_sessionmaker[AsyncSession], command: ObservationCommand) -> None:
    async with factory() as session:
        async with session.begin():
            await build_service().record(session, command)


@pytest.mark.asyncio
async def test_concurrent_first_discovery(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    command = ObservationCommand("broker", "site/one", b"{}", "tenant", "application/json")
    await asyncio.gather(record(postgresql_sessions, command), record(postgresql_sessions, command))
    async with postgresql_sessions() as session:
        streams = list((await session.scalars(select(Stream))).all())
        evidence = await session.scalar(select(func.count()).select_from(ObservationEvidence))
    assert len(streams) == 1
    assert streams[0].observation_count == 2
    assert evidence == 2
    assert streams[0].first_observed_at is not None
    assert streams[0].last_observed_at >= streams[0].first_observed_at
    assert streams[0].created_at is not None and streams[0].updated_at is not None


@pytest.mark.asyncio
async def test_sequential_redelivery_and_identity_boundaries(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    command = ObservationCommand("broker", "site/one", b"{}", "tenant", "application/json")
    await record(postgresql_sessions, command)
    await record(postgresql_sessions, command)
    await record(postgresql_sessions, ObservationCommand("other", "site/one", b"{}", "tenant"))
    await record(postgresql_sessions, ObservationCommand("broker", "site/one", b"{}", "other"))
    async with postgresql_sessions() as session:
        streams = list((await session.scalars(select(Stream))).all())
        evidence = await session.scalar(select(func.count()).select_from(ObservationEvidence))
    assert len(streams) == 3
    assert sorted(stream.observation_count for stream in streams) == [1, 1, 2]
    assert evidence == 4


class FailingEvidenceService(StreamCatalogService):
    async def _evidence(self, *args: object, **kwargs: object) -> ObservationEvidence:
        del args, kwargs
        raise RuntimeError("forced evidence failure")


async def record_failing(
    factory: async_sessionmaker[AsyncSession], command: ObservationCommand
) -> None:
    service = FailingEvidenceService(
        Settings(
            database_url="postgresql+psycopg://test:test@localhost/test",
            mqtt_topic_allowlist=["site/#"],
        )
    )
    async with factory() as session:
        async with session.begin():
            await service.record(session, command)


@pytest.mark.asyncio
async def test_new_stream_rollback(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    command = ObservationCommand("broker", "site/rollback", b"{}", "tenant")
    with pytest.raises(RuntimeError, match="forced evidence failure"):
        await record_failing(postgresql_sessions, command)
    async with postgresql_sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Stream)) == 0
        evidence_count = await session.scalar(select(func.count()).select_from(ObservationEvidence))
        assert evidence_count == 0
    await record(postgresql_sessions, command)


@pytest.mark.asyncio
async def test_existing_stream_rollback(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    command = ObservationCommand("broker", "site/existing", b"{}", "tenant")
    await record(postgresql_sessions, command)
    with pytest.raises(RuntimeError, match="forced evidence failure"):
        await record_failing(postgresql_sessions, command)
    async with postgresql_sessions() as session:
        stream = await session.scalar(select(Stream))
        evidence = await session.scalar(select(func.count()).select_from(ObservationEvidence))
    assert stream is not None and stream.observation_count == 1
    assert evidence == 1
    await record(postgresql_sessions, command)


@pytest.mark.asyncio
async def test_postgresql_schema_defaults(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with postgresql_sessions() as session:
        tables = (
            (
                await session.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
            )
            .scalars()
            .all()
        )
        columns = (
            await session.execute(
                text(
                    "SELECT column_name, is_nullable, column_default "
                    "FROM information_schema.columns WHERE table_name = 'streams'"
                )
            )
        ).all()
        constraints = (
            (
                await session.execute(
                    text(
                        "SELECT conname FROM pg_constraint WHERE conname = 'uq_streams_stream_key'"
                    )
                )
            )
            .scalars()
            .all()
        )
    details = {name: (nullable, default) for name, nullable, default in columns}
    assert {"streams", "observation_evidence"}.issubset(tables)
    assert constraints == ["uq_streams_stream_key"]
    for column in ("created_at", "updated_at", "lifecycle_status", "observation_count"):
        assert details[column][0] == "NO" and details[column][1] is not None
