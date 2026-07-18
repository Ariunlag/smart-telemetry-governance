"""Add the R1 PostgreSQL observation outbox.

Revision ID: e3b4c5d6f7a8
Revises: d2a1b9c3e4f5
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e3b4c5d6f7a8"
down_revision: str | None = "d2a1b9c3e4f5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "observation_outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("delivery_key", sa.String(64), nullable=False),
        sa.Column("stream_id", sa.Uuid(), sa.ForeignKey("streams.id"), nullable=False),
        sa.Column(
            "evidence_id", sa.Uuid(), sa.ForeignKey("observation_evidence.id"), nullable=False
        ),
        sa.Column("state", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("point_payload", sa.JSON(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processing_started_at", sa.DateTime(timezone=True)),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("last_error_code", sa.String(64)),
        sa.Column("last_error_detail", sa.String(1024)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("delivery_key", name="uq_observation_outbox_delivery_key"),
        sa.CheckConstraint(
            "state IN ('pending', 'processing', 'delivered', 'retryable', 'dead_letter')",
            name="ck_observation_outbox_state",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_observation_outbox_attempt_count"),
    )
    op.create_index(
        "ix_observation_outbox_state_available_at", "observation_outbox", ["state", "available_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_observation_outbox_state_available_at", table_name="observation_outbox")
    op.drop_table("observation_outbox")
