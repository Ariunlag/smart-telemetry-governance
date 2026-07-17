"""Add the R1 stream catalog and bounded observation evidence.

Revision ID: d2a1b9c3e4f5
Revises: c916a10cc59c
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d2a1b9c3e4f5"
down_revision: str | None = "c916a10cc59c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "streams",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("stream_key", sa.String(64), nullable=False),
        sa.Column("source_id", sa.String(255), nullable=False),
        sa.Column("topic", sa.String(1024), nullable=False),
        sa.Column("tenant", sa.String(255)),
        sa.Column("lifecycle_status", sa.String(32), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observation_count", sa.Integer(), nullable=False),
        sa.Column("payload_format", sa.String(64)),
        sa.Column("schema_summary", sa.JSON()),
        sa.Column("provenance", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("stream_key", name="uq_streams_stream_key"),
    )
    op.create_table(
        "observation_evidence",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("stream_id", sa.Uuid(), sa.ForeignKey("streams.id", ondelete="CASCADE")),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("payload_size", sa.Integer(), nullable=False),
        sa.Column("content_type", sa.String(128)),
        sa.Column("payload_preview", sa.Text()),
        sa.Column("payload_fingerprint", sa.String(64), nullable=False),
        sa.Column("broker_metadata", sa.JSON()),
    )


def downgrade() -> None:
    op.drop_table("observation_evidence")
    op.drop_table("streams")
