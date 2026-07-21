from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.classes.models import ClassMembership, SavedClassQuery, TelemetryClass
from app.domain.sources.models import Tenant
from app.domain.streams.models import Stream

SPEC_VERSION = "saved-class-query.v1"


class ManualClassError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def normalized(value: str) -> tuple[str, str]:
    display = value.strip()
    if not display or len(display) > 120:
        raise ManualClassError("invalid_name")
    return display, display.lower()


class SeriesItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    stream_id: UUID
    field_path: str = Field(min_length=1, max_length=1024)
    alias: str | None = Field(default=None, max_length=120)

    @field_validator("field_path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        if not value.startswith("$") or "[" not in value:
            raise ValueError("invalid field path")
        return value


class TimeWindow(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: str
    lookback_seconds: int = Field(ge=1, le=31536000)

    @field_validator("mode")
    @classmethod
    def relative_only(cls, value: str) -> str:
        if value != "relative":
            raise ValueError("unsupported time window")
        return value


class Aggregation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    function: str
    bucket_seconds: int | None = Field(default=None, ge=1, le=86400)

    @model_validator(mode="after")
    def valid_bucket(self) -> Aggregation:
        if self.function not in {"raw", "mean", "min", "max", "sum", "count", "last"}:
            raise ValueError("unsupported aggregation")
        if (self.function == "raw") != (self.bucket_seconds is None):
            raise ValueError("invalid aggregation bucket")
        return self


class Visualization(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str

    @field_validator("kind")
    @classmethod
    def valid_kind(cls, value: str) -> str:
        if value not in {"line", "area", "table"}:
            raise ValueError("unsupported visualization")
        return value


class SavedQuerySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    spec_version: str
    series: list[SeriesItem] = Field(min_length=1, max_length=100)
    time_window: TimeWindow
    aggregation: Aggregation
    live_append: bool = False
    visualization: Visualization

    @field_validator("spec_version")
    @classmethod
    def valid_version(cls, value: str) -> str:
        if value != SPEC_VERSION:
            raise ValueError("unsupported specification version")
        return value


class ManualClassService:
    async def _class(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID
    ) -> TelemetryClass:
        result = await session.scalar(
            select(TelemetryClass).where(
                TelemetryClass.id == class_id, TelemetryClass.tenant_id == tenant_id
            )
        )
        if result is None:
            raise ManualClassError("class_not_found")
        return result

    async def create_class(
        self, session: AsyncSession, tenant_id: UUID, name: str, description: str | None
    ) -> TelemetryClass:
        display, key = normalized(name)
        if description is not None and len(description) > 1000:
            raise ManualClassError("invalid_description")
        item = TelemetryClass(
            tenant_id=tenant_id, name=display, name_key=key, description=description
        )
        session.add(item)
        try:
            await session.flush()
            await session.refresh(item)
        except IntegrityError as error:
            raise ManualClassError("duplicate_class_name") from error
        return item

    async def list_classes(
        self, session: AsyncSession, tenant_id: UUID, limit: int, offset: int
    ) -> list[TelemetryClass]:
        return list(
            (
                await session.scalars(
                    select(TelemetryClass)
                    .where(TelemetryClass.tenant_id == tenant_id)
                    .order_by(TelemetryClass.name_key, TelemetryClass.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).all()
        )

    async def update_class(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        class_id: UUID,
        name: str | None,
        description: str | None,
    ) -> TelemetryClass:
        item = await self._class(session, tenant_id, class_id)
        if name is not None:
            item.name, item.name_key = normalized(name)
        if description is not None:
            if len(description) > 1000:
                raise ManualClassError("invalid_description")
            item.description = description
        try:
            await session.flush()
            await session.refresh(item)
        except IntegrityError as error:
            raise ManualClassError("duplicate_class_name") from error
        return item

    async def delete_class(self, session: AsyncSession, tenant_id: UUID, class_id: UUID) -> None:
        item = await self._class(session, tenant_id, class_id)
        await session.execute(
            delete(ClassMembership).where(ClassMembership.telemetry_class_id == item.id)
        )
        await session.execute(
            delete(SavedClassQuery).where(SavedClassQuery.telemetry_class_id == item.id)
        )
        await session.delete(item)

    async def add_members(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID, stream_ids: list[UUID]
    ) -> list[ClassMembership]:
        item = await self._class(session, tenant_id, class_id)
        tenant_key = await session.scalar(select(Tenant.tenant_key).where(Tenant.id == tenant_id))
        streams = list(
            (
                await session.scalars(
                    select(Stream).where(Stream.id.in_(stream_ids), Stream.tenant == tenant_key)
                )
            ).all()
        )
        if len(streams) != len(set(stream_ids)):
            raise ManualClassError("stream_not_found")
        existing = await session.scalar(
            select(ClassMembership.id).where(
                ClassMembership.telemetry_class_id == item.id,
                ClassMembership.stream_id.in_(stream_ids),
            )
        )
        if existing is not None:
            raise ManualClassError("duplicate_membership")
        memberships = [
            ClassMembership(telemetry_class_id=item.id, stream_id=stream.id) for stream in streams
        ]
        session.add_all(memberships)
        await session.flush()
        return memberships

    async def list_members(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID, limit: int, offset: int
    ) -> list[tuple[ClassMembership, Stream]]:
        item = await self._class(session, tenant_id, class_id)
        rows = (
            await session.execute(
                select(ClassMembership, Stream)
                .join(Stream, ClassMembership.stream_id == Stream.id)
                .where(ClassMembership.telemetry_class_id == item.id)
                .order_by(Stream.topic, Stream.id)
                .limit(limit)
                .offset(offset)
            )
        ).all()
        return [(row[0], row[1]) for row in rows]

    async def remove_member(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID, stream_id: UUID
    ) -> None:
        item = await self._class(session, tenant_id, class_id)
        membership = await session.scalar(
            select(ClassMembership).where(
                ClassMembership.telemetry_class_id == item.id,
                ClassMembership.stream_id == stream_id,
            )
        )
        if membership is None:
            raise ManualClassError("membership_not_found")
        await session.delete(membership)

    async def create_query(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        class_id: UUID,
        name: str,
        description: str | None,
        spec: SavedQuerySpec,
    ) -> SavedClassQuery:
        item = await self._class(session, tenant_id, class_id)
        display, key = normalized(name)
        member_ids = set(
            (
                await session.scalars(
                    select(ClassMembership.stream_id).where(
                        ClassMembership.telemetry_class_id == item.id
                    )
                )
            ).all()
        )
        if not {series.stream_id for series in spec.series} <= member_ids:
            raise ManualClassError("query_stream_not_member")
        query = SavedClassQuery(
            tenant_id=tenant_id,
            telemetry_class_id=item.id,
            name=display,
            name_key=key,
            description=description,
            spec_version=SPEC_VERSION,
            query_spec=spec.model_dump(mode="json"),
        )
        session.add(query)
        try:
            await session.flush()
        except IntegrityError as error:
            raise ManualClassError("duplicate_query_name") from error
        return query

    async def list_queries(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID, limit: int, offset: int
    ) -> list[SavedClassQuery]:
        item = await self._class(session, tenant_id, class_id)
        return list(
            (
                await session.scalars(
                    select(SavedClassQuery)
                    .where(SavedClassQuery.telemetry_class_id == item.id)
                    .order_by(SavedClassQuery.name_key, SavedClassQuery.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).all()
        )

    async def get_query(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID, query_id: UUID
    ) -> SavedClassQuery:
        await self._class(session, tenant_id, class_id)
        query = await session.scalar(
            select(SavedClassQuery).where(
                SavedClassQuery.id == query_id,
                SavedClassQuery.telemetry_class_id == class_id,
                SavedClassQuery.tenant_id == tenant_id,
            )
        )
        if query is None:
            raise ManualClassError("query_not_found")
        return query

    async def update_query(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        class_id: UUID,
        query_id: UUID,
        name: str | None,
        description: str | None,
        spec: SavedQuerySpec | None,
    ) -> SavedClassQuery:
        query = await self.get_query(session, tenant_id, class_id, query_id)
        if name is not None:
            query.name, query.name_key = normalized(name)
        if description is not None:
            query.description = description
        if spec is not None:
            member_ids = set(
                (
                    await session.scalars(
                        select(ClassMembership.stream_id).where(
                            ClassMembership.telemetry_class_id == class_id
                        )
                    )
                ).all()
            )
            if not {series.stream_id for series in spec.series} <= member_ids:
                raise ManualClassError("query_stream_not_member")
            query.query_spec = spec.model_dump(mode="json")
        try:
            await session.flush()
            await session.refresh(query)
        except IntegrityError as error:
            raise ManualClassError("duplicate_query_name") from error
        return query

    async def delete_query(
        self, session: AsyncSession, tenant_id: UUID, class_id: UUID, query_id: UUID
    ) -> None:
        await session.delete(await self.get_query(session, tenant_id, class_id, query_id))
