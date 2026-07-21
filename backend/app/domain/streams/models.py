from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
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
    lifecycle_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="discovered", server_default="discovered"
    )
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observation_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    payload_format: Mapped[str | None] = mapped_column(String(64))
    schema_summary: Mapped[dict[str, object] | None] = mapped_column(JSON)
    provenance: Mapped[dict[str, object] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
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


class ObservationOutbox(Base):
    __tablename__ = "observation_outbox"
    __table_args__ = (
        UniqueConstraint("delivery_key", name="uq_observation_outbox_delivery_key"),
        CheckConstraint(
            "state IN ('pending', 'processing', 'delivered', 'retryable', 'dead_letter')",
            name="ck_observation_outbox_state",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_observation_outbox_attempt_count"),
        Index("ix_observation_outbox_state_available_at", "state", "available_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    delivery_key: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("streams.id"), nullable=False)
    evidence_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("observation_evidence.id"), nullable=False
    )
    state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending", server_default="pending"
    )
    point_payload: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(64))
    last_error_detail: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class RawObservationRecord(Base):
    """Append-oriented accepted source-native evidence for later replayable processing."""

    __tablename__ = "raw_observations"
    __table_args__ = (
        UniqueConstraint("observation_key", name="uq_raw_observations_observation_key"),
        CheckConstraint("payload_size >= 0", name="ck_raw_observations_payload_size"),
        CheckConstraint("source_id <> ''", name="ck_raw_observations_source_id"),
        CheckConstraint("source_type <> ''", name="ck_raw_observations_source_type"),
        CheckConstraint("external_stream_id <> ''", name="ck_raw_observations_external_stream_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    observation_key: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("streams.id"), nullable=False)
    evidence_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("observation_evidence.id"), nullable=False
    )
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str] = mapped_column(String(255), nullable=False)
    external_stream_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(255))
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    payload_size: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    transport_metadata: Mapped[dict[str, object] | None] = mapped_column(JSON)
    retention_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ObservationProcessingTask(Base):
    __tablename__ = "observation_processing_tasks"
    __table_args__ = (
        UniqueConstraint(
            "raw_observation_id",
            "processor_type",
            "processor_version",
            name="uq_processing_tasks_processor_identity",
        ),
        CheckConstraint(
            "state IN ('pending', 'processing', 'completed', 'retryable', 'dead_letter')",
            name="ck_processing_tasks_state",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_processing_tasks_attempt_count"),
        CheckConstraint("processor_type <> ''", name="ck_processing_tasks_processor_type"),
        CheckConstraint("processor_version <> ''", name="ck_processing_tasks_processor_version"),
        Index(
            "ix_processing_tasks_claim",
            "processor_type",
            "processor_version",
            "state",
            "available_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    raw_observation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("raw_observations.id", ondelete="CASCADE"), nullable=False
    )
    processor_type: Mapped[str] = mapped_column(String(64), nullable=False)
    processor_version: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending", server_default="pending"
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(64))
    last_error_detail: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
