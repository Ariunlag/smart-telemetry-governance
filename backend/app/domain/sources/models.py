from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.streams.models import Base


class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (
        UniqueConstraint("tenant_key", name="uq_tenants_tenant_key"),
        UniqueConstraint("id", "tenant_key", name="uq_tenants_id_tenant_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_key: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class Site(Base):
    __tablename__ = "sites"
    __table_args__ = (
        UniqueConstraint("tenant_id", "site_key", name="uq_sites_tenant_id_site_key"),
        UniqueConstraint("tenant_id", "id", name="uq_sites_tenant_id_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    site_key: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class TelemetrySource(Base):
    __tablename__ = "telemetry_sources"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "site_id"], ["sites.tenant_id", "sites.id"], name="fk_sources_tenant_site"
        ),
        UniqueConstraint(
            "tenant_id", "site_id", "source_key", name="uq_sources_tenant_site_source_key"
        ),
        UniqueConstraint("tenant_id", "site_id", "id", name="uq_sources_tenant_site_id"),
        CheckConstraint("protocol = 'mqtt'", name="ck_sources_protocol"),
        CheckConstraint(
            "configuration_status IN ('disabled', 'enabled')",
            name="ck_sources_configuration_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    site_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    source_key: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    protocol: Mapped[str] = mapped_column(
        String(16), nullable=False, default="mqtt", server_default="mqtt"
    )
    configuration_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="disabled", server_default="disabled"
    )
    credential_reference: Mapped[str | None] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class MqttSubscription(Base):
    __tablename__ = "mqtt_subscriptions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "site_id"],
            ["sites.tenant_id", "sites.id"],
            name="fk_subscriptions_tenant_site",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "site_id", "source_id"],
            ["telemetry_sources.tenant_id", "telemetry_sources.site_id", "telemetry_sources.id"],
            name="fk_subscriptions_tenant_site_source",
        ),
        UniqueConstraint(
            "tenant_id", "site_id", "source_id", "id", name="uq_subscriptions_tenant_site_source_id"
        ),
        CheckConstraint("qos IN (0, 1, 2)", name="ck_subscriptions_qos"),
        CheckConstraint(
            "topic_filter <> '#'", name="ck_subscriptions_topic_filter_not_unrestricted"
        ),
        CheckConstraint(
            "retained_message_policy IN ('accept', 'ignore')",
            name="ck_subscriptions_retained_message_policy",
        ),
        CheckConstraint(
            "sample_every_n IS NULL OR sample_every_n > 0", name="ck_subscriptions_sample_every_n"
        ),
        Index(
            "uq_subscriptions_active_definition",
            "tenant_id",
            "site_id",
            "source_id",
            "topic_filter",
            "qos",
            unique=True,
            postgresql_where="enabled",
            sqlite_where="enabled",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    site_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    topic_filter: Mapped[str] = mapped_column(String(1024), nullable=False)
    qos: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    retained_message_policy: Mapped[str] = mapped_column(
        String(16), nullable=False, default="accept", server_default="accept"
    )
    sample_every_n: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "site_id"], ["sites.tenant_id", "sites.id"], name="fk_runs_tenant_site"
        ),
        ForeignKeyConstraint(
            ["tenant_id", "site_id", "source_id"],
            ["telemetry_sources.tenant_id", "telemetry_sources.site_id", "telemetry_sources.id"],
            name="fk_runs_tenant_site_source",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "site_id", "source_id", "subscription_id"],
            [
                "mqtt_subscriptions.tenant_id",
                "mqtt_subscriptions.site_id",
                "mqtt_subscriptions.source_id",
                "mqtt_subscriptions.id",
            ],
            name="fk_runs_tenant_site_source_subscription",
        ),
        CheckConstraint(
            "status IN ('starting', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_ingestion_runs_status",
        ),
        CheckConstraint("accepted_count >= 0", name="ck_ingestion_runs_accepted_count"),
        CheckConstraint("rejected_count >= 0", name="ck_ingestion_runs_rejected_count"),
        CheckConstraint("malformed_count >= 0", name="ck_ingestion_runs_malformed_count"),
        CheckConstraint("oversized_count >= 0", name="ck_ingestion_runs_oversized_count"),
        CheckConstraint("reconnect_count >= 0", name="ck_ingestion_runs_reconnect_count"),
        CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name="ck_ingestion_runs_ended_after_started",
        ),
        CheckConstraint(
            "status NOT IN ('completed', 'failed', 'cancelled') OR ended_at IS NOT NULL",
            name="ck_ingestion_runs_terminal_end",
        ),
        Index("ix_ingestion_runs_tenant_started_at", "tenant_id", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    site_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    subscription_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="starting", server_default="starting"
    )
    accepted_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    rejected_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    malformed_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    oversized_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    reconnect_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    error_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
