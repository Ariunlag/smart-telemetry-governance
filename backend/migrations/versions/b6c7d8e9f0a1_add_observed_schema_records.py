"""Add deterministic observed schema records.

Revision ID: b6c7d8e9f0a1
Revises: a5b6c7d8e9f0
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b6c7d8e9f0a1"
down_revision: str | None = "a5b6c7d8e9f0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "observed_schemas",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("stream_id", sa.Uuid(), sa.ForeignKey("streams.id"), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("fingerprint_version", sa.String(64), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("root_type", sa.String(16), nullable=False),
        sa.Column("field_count", sa.Integer(), nullable=False),
        sa.Column("schema_document", sa.JSON(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observation_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "stream_id",
            "fingerprint_version",
            "fingerprint",
            name="uq_observed_schemas_stream_fingerprint",
        ),
        sa.UniqueConstraint(
            "stream_id", "version_number", name="uq_observed_schemas_stream_version"
        ),
        sa.CheckConstraint("version_number >= 1", name="ck_observed_schemas_version"),
        sa.CheckConstraint("field_count >= 0", name="ck_observed_schemas_field_count"),
        sa.CheckConstraint("observation_count >= 0", name="ck_observed_schemas_observation_count"),
    )
    op.create_index(
        "ix_observed_schemas_stream_version", "observed_schemas", ["stream_id", "version_number"]
    )
    op.create_table(
        "schema_observation_records",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "processing_task_id",
            sa.Uuid(),
            sa.ForeignKey("observation_processing_tasks.id"),
            nullable=False,
        ),
        sa.Column(
            "raw_observation_id", sa.Uuid(), sa.ForeignKey("raw_observations.id"), nullable=False
        ),
        sa.Column(
            "observed_schema_id", sa.Uuid(), sa.ForeignKey("observed_schemas.id"), nullable=False
        ),
        sa.Column("processor_version", sa.String(64), nullable=False),
        sa.Column("fingerprint_version", sa.String(64), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("processing_task_id", name="uq_schema_observation_records_task"),
        sa.UniqueConstraint(
            "raw_observation_id",
            "processor_version",
            name="uq_schema_observation_records_raw_processor",
        ),
    )
    op.create_index(
        "ix_schema_observation_records_raw", "schema_observation_records", ["raw_observation_id"]
    )
    op.create_table(
        "observed_fields",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "observed_schema_id",
            sa.Uuid(),
            sa.ForeignKey("observed_schemas.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("path", sa.String(1024), nullable=False),
        sa.Column("value_type", sa.String(16), nullable=False),
        sa.Column("depth", sa.Integer(), nullable=False),
        sa.Column("nullable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("observed_schema_id", "path", name="uq_observed_fields_schema_path"),
        sa.CheckConstraint("depth >= 0", name="ck_observed_fields_depth"),
    )
    op.create_table(
        "schema_drift_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("stream_id", sa.Uuid(), sa.ForeignKey("streams.id"), nullable=False),
        sa.Column(
            "previous_schema_id", sa.Uuid(), sa.ForeignKey("observed_schemas.id"), nullable=False
        ),
        sa.Column(
            "current_schema_id", sa.Uuid(), sa.ForeignKey("observed_schemas.id"), nullable=False
        ),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("added_paths", sa.JSON(), nullable=False),
        sa.Column("removed_paths", sa.JSON(), nullable=False),
        sa.Column("type_changed_paths", sa.JSON(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("current_schema_id", name="uq_schema_drift_events_current_schema"),
    )


def downgrade() -> None:
    op.drop_table("schema_drift_events")
    op.drop_index("ix_schema_observation_records_raw", table_name="schema_observation_records")
    op.drop_table("schema_observation_records")
    op.drop_table("observed_fields")
    op.drop_index("ix_observed_schemas_stream_version", table_name="observed_schemas")
    op.drop_table("observed_schemas")
