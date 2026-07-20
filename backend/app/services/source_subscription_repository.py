from __future__ import annotations

from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.sources.models import IngestionRun, MqttSubscription, Site, TelemetrySource, Tenant


class SourceSubscriptionRepository:
    """Tenant-aware persistence operations for the R1 control-plane foundation."""

    async def create_tenant(
        self, session: AsyncSession, tenant_key: str, display_name: str
    ) -> Tenant:
        tenant = Tenant(tenant_key=tenant_key, display_name=display_name)
        session.add(tenant)
        await session.flush()
        return tenant

    async def create_site(
        self, session: AsyncSession, tenant_id: UUID, site_key: str, display_name: str
    ) -> Site:
        site = Site(tenant_id=tenant_id, site_key=site_key, display_name=display_name)
        session.add(site)
        await session.flush()
        return site

    async def create_source(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        site_id: UUID,
        source_key: str,
        display_name: str,
        credential_reference: str | None,
        *,
        protocol: str = "mqtt",
        configuration_status: str = "disabled",
    ) -> TelemetrySource:
        if protocol != "mqtt":
            raise ValueError("Only MQTT telemetry sources are supported")
        source = TelemetrySource(
            tenant_id=tenant_id,
            site_id=site_id,
            source_key=source_key,
            display_name=display_name,
            protocol=protocol,
            configuration_status=configuration_status,
            credential_reference=credential_reference,
        )
        session.add(source)
        await session.flush()
        return source

    async def create_subscription(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        site_id: UUID,
        source_id: UUID,
        topic_filter: str,
        qos: int,
        *,
        enabled: bool = True,
        retained_message_policy: str = "accept",
        sample_every_n: int | None = None,
    ) -> MqttSubscription:
        if qos not in {0, 1, 2}:
            raise ValueError("MQTT QoS must be 0, 1, or 2")
        if topic_filter == "#":
            raise ValueError("An unrestricted MQTT wildcard is not allowed")
        subscription = MqttSubscription(
            tenant_id=tenant_id,
            site_id=site_id,
            source_id=source_id,
            topic_filter=topic_filter,
            qos=qos,
            enabled=enabled,
            retained_message_policy=retained_message_policy,
            sample_every_n=sample_every_n,
        )
        session.add(subscription)
        await session.flush()
        return subscription

    async def start_ingestion_run(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        site_id: UUID,
        source_id: UUID,
        subscription_id: UUID,
        *,
        started_at: datetime | None = None,
    ) -> IngestionRun:
        run = IngestionRun(
            tenant_id=tenant_id,
            site_id=site_id,
            source_id=source_id,
            subscription_id=subscription_id,
            started_at=started_at or datetime.now(UTC),
            status="starting",
        )
        session.add(run)
        await session.flush()
        return run

    async def finalize_ingestion_run(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        run_id: UUID,
        status: str,
        *,
        accepted_count: int = 0,
        rejected_count: int = 0,
        malformed_count: int = 0,
        oversized_count: int = 0,
        reconnect_count: int = 0,
        error_code: str | None = None,
    ) -> IngestionRun:
        run = await session.scalar(
            select(IngestionRun).where(
                IngestionRun.id == run_id, IngestionRun.tenant_id == tenant_id
            )
        )
        if run is None:
            raise LookupError("Ingestion run was not found for the tenant")
        run.status = status
        run.ended_at = datetime.now(UTC)
        run.accepted_count = accepted_count
        run.rejected_count = rejected_count
        run.malformed_count = malformed_count
        run.oversized_count = oversized_count
        run.reconnect_count = reconnect_count
        run.error_code = error_code
        await session.flush()
        return run

    async def get_source_by_key(
        self, session: AsyncSession, tenant_id: UUID, site_id: UUID, source_key: str
    ) -> TelemetrySource | None:
        return cast(
            TelemetrySource | None,
            await session.scalar(
                select(TelemetrySource).where(
                    TelemetrySource.tenant_id == tenant_id,
                    TelemetrySource.site_id == site_id,
                    TelemetrySource.source_key == source_key,
                )
            ),
        )
