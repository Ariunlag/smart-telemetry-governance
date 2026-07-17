from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.db.session import metadata


class Base(DeclarativeBase):
    metadata = metadata


class Stream(Base):
    __tablename__ = "streams"
    __table_args__ = (UniqueConstraint("stream_key", name="uq_streams_stream_key"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    stream_key: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(1024), nullable=False)
    tenant: Mapped[str | None] = mapped_column(String(255))
    lifecycle_status: Mapped[str] = mapped_column(String(32), default="discovered")
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observation_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    payload_format: Mapped[str | None] = mapped_column(String(64))
    schema_summary: Mapped[dict[str, object] | None] = mapped_column(JSON)
    provenance: Mapped[dict[str, object] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ObservationEvidence(Base):
    __tablename__ = "observation_evidence"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    stream_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("streams.id", ondelete="CASCADE")
    )
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    payload_size: Mapped[int] = mapped_column(Integer, nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(128))
    payload_preview: Mapped[str | None] = mapped_column(Text)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    broker_metadata: Mapped[dict[str, object] | None] = mapped_column(JSON)
