"""Add durable R2 raw observations and processor work records.

Revision ID: a5b6c7d8e9f0
Revises: f4c5d6e7f8a9
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a5b6c7d8e9f0"
down_revision: str | None = "f4c5d6e7f8a9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_observations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("observation_key", sa.String(64), nullable=False),
        sa.Column("stream_id", sa.Uuid(), sa.ForeignKey("streams.id"), nullable=False),
        sa.Column(
            "evidence_id", sa.Uuid(), sa.ForeignKey("observation_evidence.id"), nullable=False
        ),
        sa.Column("source_id", sa.String(255), nullable=False),
        sa.Column("source_type", sa.String(255), nullable=False),
        sa.Column("external_stream_id", sa.String(1024), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_type", sa.String(255)),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("payload_size", sa.Integer(), nullable=False),
        sa.Column("payload_fingerprint", sa.String(64), nullable=False),
        sa.Column("transport_metadata", sa.JSON()),
        sa.Column("retention_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("observation_key", name="uq_raw_observations_observation_key"),
        sa.CheckConstraint("payload_size >= 0", name="ck_raw_observations_payload_size"),
        sa.CheckConstraint("source_id <> ''", name="ck_raw_observations_source_id"),
        sa.CheckConstraint("source_type <> ''", name="ck_raw_observations_source_type"),
        sa.CheckConstraint(
            "external_stream_id <> ''", name="ck_raw_observations_external_stream_id"
        ),
    )
    op.create_index("ix_raw_observations_retention_until", "raw_observations", ["retention_until"])
    op.create_table(
        "observation_processing_tasks",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "raw_observation_id",
            sa.Uuid(),
            sa.ForeignKey("raw_observations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("processor_type", sa.String(64), nullable=False),
        sa.Column("processor_version", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processing_started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("last_error_code", sa.String(64)),
        sa.Column("last_error_detail", sa.String(1024)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "raw_observation_id",
            "processor_type",
            "processor_version",
            name="uq_processing_tasks_processor_identity",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'processing', 'completed', 'retryable', 'dead_letter')",
            name="ck_processing_tasks_state",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_processing_tasks_attempt_count"),
        sa.CheckConstraint("processor_type <> ''", name="ck_processing_tasks_processor_type"),
        sa.CheckConstraint("processor_version <> ''", name="ck_processing_tasks_processor_version"),
    )
    op.create_index(
        "ix_processing_tasks_claim",
        "observation_processing_tasks",
        ["processor_type", "processor_version", "state", "available_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_processing_tasks_claim", table_name="observation_processing_tasks")
    op.drop_table("observation_processing_tasks")
    op.drop_index("ix_raw_observations_retention_until", table_name="raw_observations")
    op.drop_table("raw_observations")
