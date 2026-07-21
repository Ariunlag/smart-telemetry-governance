from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.streams.models import Base


class TelemetryClass(Base):
    __tablename__ = "telemetry_classes"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name_key", name="uq_telemetry_classes_tenant_name_key"),
        CheckConstraint("name <> ''", name="ck_telemetry_classes_name_not_empty"),
        Index("ix_telemetry_classes_tenant_name", "tenant_id", "name_key"),
    )
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    name_key: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ClassMembership(Base):
    __tablename__ = "class_memberships"
    __table_args__ = (
        UniqueConstraint(
            "telemetry_class_id", "stream_id", name="uq_class_memberships_class_stream"
        ),
        CheckConstraint(
            "membership_source IN ('manual', 'approved_recommendation')",
            name="ck_class_memberships_source",
        ),
        Index("ix_class_memberships_class", "telemetry_class_id"),
    )
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    telemetry_class_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("telemetry_classes.id", ondelete="CASCADE"), nullable=False
    )
    stream_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("streams.id"), nullable=False)
    membership_source: Mapped[str] = mapped_column(
        String(32), nullable=False, default="manual", server_default="manual"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SavedClassQuery(Base):
    __tablename__ = "saved_class_queries"
    __table_args__ = (
        UniqueConstraint(
            "telemetry_class_id", "name_key", name="uq_saved_class_queries_class_name_key"
        ),
        CheckConstraint(
            "spec_version = 'saved-class-query.v1'", name="ck_saved_class_queries_spec_version"
        ),
        Index("ix_saved_class_queries_class_name", "telemetry_class_id", "name_key"),
    )
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    telemetry_class_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("telemetry_classes.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    name_key: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    spec_version: Mapped[str] = mapped_column(
        String(64), nullable=False, default="saved-class-query.v1"
    )
    query_spec: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
