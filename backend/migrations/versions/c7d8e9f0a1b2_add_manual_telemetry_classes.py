"""Add tenant-owned manual telemetry classes.

Revision ID: c7d8e9f0a1b2
Revises: b6c7d8e9f0a1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c7d8e9f0a1b2"
down_revision: str | None = "b6c7d8e9f0a1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "telemetry_classes",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("name_key", sa.String(120), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("tenant_id", "name_key", name="uq_telemetry_classes_tenant_name_key"),
        sa.CheckConstraint("name <> ''", name="ck_telemetry_classes_name_not_empty"),
    )
    op.create_index(
        "ix_telemetry_classes_tenant_name", "telemetry_classes", ["tenant_id", "name_key"]
    )
    op.create_table(
        "class_memberships",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "telemetry_class_id",
            sa.Uuid(),
            sa.ForeignKey("telemetry_classes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("stream_id", sa.Uuid(), sa.ForeignKey("streams.id"), nullable=False),
        sa.Column("membership_source", sa.String(32), nullable=False, server_default="manual"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "telemetry_class_id", "stream_id", name="uq_class_memberships_class_stream"
        ),
        sa.CheckConstraint(
            "membership_source IN ('manual', 'approved_recommendation')",
            name="ck_class_memberships_source",
        ),
    )
    op.create_index("ix_class_memberships_class", "class_memberships", ["telemetry_class_id"])
    op.create_table(
        "saved_class_queries",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column(
            "telemetry_class_id",
            sa.Uuid(),
            sa.ForeignKey("telemetry_classes.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("name_key", sa.String(120), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column(
            "spec_version", sa.String(64), nullable=False, server_default="saved-class-query.v1"
        ),
        sa.Column("query_spec", sa.JSON(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "telemetry_class_id", "name_key", name="uq_saved_class_queries_class_name_key"
        ),
        sa.CheckConstraint(
            "spec_version = 'saved-class-query.v1'", name="ck_saved_class_queries_spec_version"
        ),
    )
    op.create_index(
        "ix_saved_class_queries_class_name",
        "saved_class_queries",
        ["telemetry_class_id", "name_key"],
    )


def downgrade() -> None:
    op.drop_index("ix_saved_class_queries_class_name", table_name="saved_class_queries")
    op.drop_table("saved_class_queries")
    op.drop_index("ix_class_memberships_class", table_name="class_memberships")
    op.drop_table("class_memberships")
    op.drop_index("ix_telemetry_classes_tenant_name", table_name="telemetry_classes")
    op.drop_table("telemetry_classes")
