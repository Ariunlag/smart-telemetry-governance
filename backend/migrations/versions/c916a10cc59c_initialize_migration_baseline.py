"""Initialize the non-destructive migration baseline.

Revision ID: c916a10cc59c
Revises:
Create Date: 2026-07-17
"""

from collections.abc import Sequence

revision: str = "c916a10cc59c"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Establish Alembic history without creating or changing domain tables."""


def downgrade() -> None:
    """Baseline migration is intentionally non-destructive."""
