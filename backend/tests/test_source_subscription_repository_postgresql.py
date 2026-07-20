from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.sources.models import IngestionRun, MqttSubscription, TelemetrySource
from app.services.source_subscription_repository import SourceSubscriptionRepository

pytestmark = pytest.mark.postgresql


async def create_owned_records(
    sessions: async_sessionmaker[AsyncSession], *, tenant_key: str = "tenant-one"
) -> tuple[UUID, UUID, UUID, UUID]:
    repository = SourceSubscriptionRepository()
    async with sessions() as session:
        async with session.begin():
            tenant = await repository.create_tenant(session, tenant_key, "Tenant")
            site = await repository.create_site(session, tenant.id, "site-one", "Site")
            source = await repository.create_source(
                session,
                tenant.id,
                site.id,
                "source-one",
                "Source",
                "secret://broker/tenant-one/source-one",
                configuration_status="enabled",
            )
            subscription = await repository.create_subscription(
                session, tenant.id, site.id, source.id, "site/one/#", 1
            )
    return tenant.id, site.id, source.id, subscription.id


@pytest.mark.asyncio
async def test_tenant_and_site_identity_constraints(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    repository = SourceSubscriptionRepository()
    async with postgresql_sessions() as session:
        async with session.begin():
            first = await repository.create_tenant(session, "tenant-one", "Tenant One")
            await repository.create_site(session, first.id, "site", "Site")
            second = await repository.create_tenant(session, "tenant-two", "Tenant Two")
            await repository.create_site(session, second.id, "site", "Other Site")
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await repository.create_tenant(session, "tenant-one", "Duplicate")
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await repository.create_site(session, first.id, "site", "Duplicate site")


@pytest.mark.asyncio
async def test_source_and_subscription_ownership_constraints(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    tenant_id, site_id, source_id, _ = await create_owned_records(postgresql_sessions)
    other_tenant_id, other_site_id, _, _ = await create_owned_records(
        postgresql_sessions, tenant_key="tenant-two"
    )
    repository = SourceSubscriptionRepository()
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await repository.create_source(
                    session,
                    tenant_id,
                    other_site_id,
                    "cross-tenant-site",
                    "Invalid",
                    "secret://reference",
                )
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await repository.create_subscription(
                    session, other_tenant_id, other_site_id, source_id, "site/two/#", 1
                )
    async with postgresql_sessions() as session:
        source = await session.get(TelemetrySource, source_id)
    assert (
        source is not None
        and source.credential_reference == "secret://broker/tenant-one/source-one"
    )
    assert "password" not in source.credential_reference
    assert site_id != other_site_id


@pytest.mark.asyncio
async def test_subscription_constraints_and_active_definition_uniqueness(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    tenant_id, site_id, source_id, _ = await create_owned_records(postgresql_sessions)
    repository = SourceSubscriptionRepository()
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    MqttSubscription(
                        tenant_id=tenant_id,
                        site_id=site_id,
                        source_id=source_id,
                        topic_filter="#",
                        qos=0,
                    )
                )
                await session.flush()
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                await repository.create_subscription(
                    session, tenant_id, site_id, source_id, "site/one/#", 1
                )
    async with postgresql_sessions() as session:
        async with session.begin():
            duplicate_disabled = await repository.create_subscription(
                session, tenant_id, site_id, source_id, "site/one/#", 1, enabled=False
            )
    assert not duplicate_disabled.enabled


@pytest.mark.asyncio
async def test_ingestion_run_constraints_and_finalization(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    tenant_id, site_id, source_id, subscription_id = await create_owned_records(postgresql_sessions)
    repository = SourceSubscriptionRepository()
    async with postgresql_sessions() as session:
        async with session.begin():
            run = await repository.start_ingestion_run(
                session, tenant_id, site_id, source_id, subscription_id
            )
            finalized = await repository.finalize_ingestion_run(
                session,
                tenant_id,
                run.id,
                "completed",
                accepted_count=3,
                malformed_count=1,
                reconnect_count=2,
                error_code="bounded_code",
            )
    assert finalized.ended_at is not None and finalized.accepted_count == 3
    async with postgresql_sessions() as session:
        async with session.begin():
            failed_run = await repository.start_ingestion_run(
                session, tenant_id, site_id, source_id, subscription_id
            )
            failed = await repository.finalize_ingestion_run(
                session, tenant_id, failed_run.id, "failed", error_code="broker_unavailable"
            )
    assert failed.ended_at is not None and failed.status == "failed"
    assert failed.error_code == "broker_unavailable"
    async with postgresql_sessions() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(
                    IngestionRun(
                        tenant_id=tenant_id,
                        site_id=site_id,
                        source_id=source_id,
                        subscription_id=subscription_id,
                        started_at=datetime.now(UTC),
                        status="running",
                        accepted_count=-1,
                    )
                )
                await session.flush()
    async with postgresql_sessions() as session:
        with pytest.raises(DataError):
            async with session.begin():
                session.add(
                    IngestionRun(
                        tenant_id=tenant_id,
                        site_id=site_id,
                        source_id=source_id,
                        subscription_id=subscription_id,
                        started_at=datetime.now(UTC),
                        status="failed",
                        error_code="x" * 65,
                    )
                )
                await session.flush()


@pytest.mark.asyncio
async def test_postgresql_schema_has_control_plane_constraints(
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
                        "('uq_tenants_tenant_key', 'uq_sites_tenant_id_site_key', "
                        "'fk_sources_tenant_site', 'fk_subscriptions_tenant_site_source', "
                        "'ck_ingestion_runs_accepted_count')"
                    )
                )
            ).all()
        )
        source_count = await session.scalar(select(func.count()).select_from(TelemetrySource))
    assert {
        "tenants",
        "sites",
        "telemetry_sources",
        "mqtt_subscriptions",
        "ingestion_runs",
    } <= tables
    assert constraints == {
        "uq_tenants_tenant_key",
        "uq_sites_tenant_id_site_key",
        "fk_sources_tenant_site",
        "fk_subscriptions_tenant_site_source",
        "ck_ingestion_runs_accepted_count",
    }
    assert source_count == 0
