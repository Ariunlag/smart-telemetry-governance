"""Add tenant-aware R1 source and subscription persistence.

Revision ID: f4c5d6e7f8a9
Revises: e3b4c5d6f7a8
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f4c5d6e7f8a9"
down_revision: str | None = "e3b4c5d6f7a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def timestamps() -> list[sa.Column[object]]:
    return [
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    ]


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_key", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        *timestamps(),
        sa.UniqueConstraint("tenant_key", name="uq_tenants_tenant_key"),
        sa.UniqueConstraint("id", "tenant_key", name="uq_tenants_id_tenant_key"),
    )
    op.create_table(
        "sites",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("site_key", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        *timestamps(),
        sa.UniqueConstraint("tenant_id", "site_key", name="uq_sites_tenant_id_site_key"),
        sa.UniqueConstraint("tenant_id", "id", name="uq_sites_tenant_id_id"),
    )
    op.create_table(
        "telemetry_sources",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("site_id", sa.Uuid(), nullable=False),
        sa.Column("source_key", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("protocol", sa.String(16), nullable=False, server_default="mqtt"),
        sa.Column("configuration_status", sa.String(16), nullable=False, server_default="disabled"),
        sa.Column("credential_reference", sa.String(512)),
        *timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id", "site_id"], ["sites.tenant_id", "sites.id"], name="fk_sources_tenant_site"
        ),
        sa.UniqueConstraint(
            "tenant_id", "site_id", "source_key", name="uq_sources_tenant_site_source_key"
        ),
        sa.UniqueConstraint("tenant_id", "site_id", "id", name="uq_sources_tenant_site_id"),
        sa.CheckConstraint("protocol = 'mqtt'", name="ck_sources_protocol"),
        sa.CheckConstraint(
            "configuration_status IN ('disabled', 'enabled')",
            name="ck_sources_configuration_status",
        ),
    )
    op.create_table(
        "mqtt_subscriptions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("site_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("topic_filter", sa.String(1024), nullable=False),
        sa.Column("qos", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "retained_message_policy", sa.String(16), nullable=False, server_default="accept"
        ),
        sa.Column("sample_every_n", sa.Integer()),
        *timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id", "site_id"],
            ["sites.tenant_id", "sites.id"],
            name="fk_subscriptions_tenant_site",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "site_id", "source_id"],
            ["telemetry_sources.tenant_id", "telemetry_sources.site_id", "telemetry_sources.id"],
            name="fk_subscriptions_tenant_site_source",
        ),
        sa.UniqueConstraint(
            "tenant_id", "site_id", "source_id", "id", name="uq_subscriptions_tenant_site_source_id"
        ),
        sa.CheckConstraint("qos IN (0, 1, 2)", name="ck_subscriptions_qos"),
        sa.CheckConstraint(
            "topic_filter <> '#'", name="ck_subscriptions_topic_filter_not_unrestricted"
        ),
        sa.CheckConstraint(
            "retained_message_policy IN ('accept', 'ignore')",
            name="ck_subscriptions_retained_message_policy",
        ),
        sa.CheckConstraint(
            "sample_every_n IS NULL OR sample_every_n > 0", name="ck_subscriptions_sample_every_n"
        ),
    )
    op.create_index(
        "uq_subscriptions_active_definition",
        "mqtt_subscriptions",
        ["tenant_id", "site_id", "source_id", "topic_filter", "qos"],
        unique=True,
        postgresql_where=sa.text("enabled"),
        sqlite_where=sa.text("enabled"),
    )
    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("site_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("subscription_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(16), nullable=False, server_default="starting"),
        sa.Column("accepted_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("malformed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("oversized_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reconnect_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_code", sa.String(64)),
        *timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id", "site_id"], ["sites.tenant_id", "sites.id"], name="fk_runs_tenant_site"
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "site_id", "source_id"],
            ["telemetry_sources.tenant_id", "telemetry_sources.site_id", "telemetry_sources.id"],
            name="fk_runs_tenant_site_source",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "site_id", "source_id", "subscription_id"],
            [
                "mqtt_subscriptions.tenant_id",
                "mqtt_subscriptions.site_id",
                "mqtt_subscriptions.source_id",
                "mqtt_subscriptions.id",
            ],
            name="fk_runs_tenant_site_source_subscription",
        ),
        sa.CheckConstraint(
            "status IN ('starting', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_ingestion_runs_status",
        ),
        sa.CheckConstraint("accepted_count >= 0", name="ck_ingestion_runs_accepted_count"),
        sa.CheckConstraint("rejected_count >= 0", name="ck_ingestion_runs_rejected_count"),
        sa.CheckConstraint("malformed_count >= 0", name="ck_ingestion_runs_malformed_count"),
        sa.CheckConstraint("oversized_count >= 0", name="ck_ingestion_runs_oversized_count"),
        sa.CheckConstraint("reconnect_count >= 0", name="ck_ingestion_runs_reconnect_count"),
        sa.CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name="ck_ingestion_runs_ended_after_started",
        ),
        sa.CheckConstraint(
            "status NOT IN ('completed', 'failed', 'cancelled') OR ended_at IS NOT NULL",
            name="ck_ingestion_runs_terminal_end",
        ),
    )
    op.create_index(
        "ix_ingestion_runs_tenant_started_at", "ingestion_runs", ["tenant_id", "started_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_ingestion_runs_tenant_started_at", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")
    op.drop_index("uq_subscriptions_active_definition", table_name="mqtt_subscriptions")
    op.drop_table("mqtt_subscriptions")
    op.drop_table("telemetry_sources")
    op.drop_table("sites")
    op.drop_table("tenants")
