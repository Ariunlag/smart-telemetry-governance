from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.classes.models import ClassMembership, SavedClassQuery, TelemetryClass
from app.services.manual_class_service import ManualClassError, ManualClassService, SavedQuerySpec

router = APIRouter(prefix="/api/classes", tags=["classes"])


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TelemetryClassCreate(StrictModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)


class TelemetryClassUpdate(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)


class TelemetryClassSummary(StrictModel):
    id: UUID
    name: str
    description: str | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TelemetryClassDetail(TelemetryClassSummary):
    member_count: int
    query_count: int


class ClassMemberAddRequest(StrictModel):
    stream_ids: list[UUID] = Field(min_length=1, max_length=100)


class ClassMemberResponse(StrictModel):
    id: UUID
    stream_id: UUID
    membership_source: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ClassMemberListResponse(StrictModel):
    items: list[ClassMemberResponse]


class SavedClassQueryCreate(StrictModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    query_spec: SavedQuerySpec


class SavedClassQueryUpdate(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    query_spec: SavedQuerySpec | None = None


class SavedClassQueryResponse(StrictModel):
    id: UUID
    name: str
    description: str | None
    spec_version: str
    query_spec: dict[str, object]
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


class SavedClassQueryListResponse(StrictModel):
    items: list[SavedClassQueryResponse]


def tenant_id(value: UUID | None = Header(default=None, alias="X-Tenant-ID")) -> UUID:
    if value is None:
        raise HTTPException(400, "tenant context is required")
    return value


def error(error: ManualClassError) -> HTTPException:
    if error.code in {
        "class_not_found",
        "query_not_found",
        "membership_not_found",
        "stream_not_found",
        "query_stream_not_member",
    }:
        return HTTPException(404, "resource not found")
    if error.code.startswith("duplicate"):
        return HTTPException(409, "conflict")
    return HTTPException(400, "invalid request")


async def detail(session: AsyncSession, item: TelemetryClass) -> TelemetryClassDetail:
    counts = await session.execute(
        select(func.count(ClassMembership.id.distinct()), func.count(SavedClassQuery.id.distinct()))
        .select_from(TelemetryClass)
        .outerjoin(ClassMembership, ClassMembership.telemetry_class_id == TelemetryClass.id)
        .outerjoin(SavedClassQuery, SavedClassQuery.telemetry_class_id == TelemetryClass.id)
        .where(TelemetryClass.id == item.id)
        .group_by(TelemetryClass.id)
    )
    members, queries = counts.one()
    summary = TelemetryClassSummary.model_validate(item, from_attributes=True)
    return TelemetryClassDetail(**summary.model_dump(), member_count=members, query_count=queries)


@router.post("", response_model=TelemetryClassDetail, status_code=201)
async def create(
    payload: TelemetryClassCreate, request: Request, tenant: UUID = Header(alias="X-Tenant-ID")
) -> TelemetryClassDetail:
    service = ManualClassService()
    try:
        async with request.app.state.database.transaction() as session:
            return await detail(
                session,
                await service.create_class(session, tenant, payload.name, payload.description),
            )
    except ManualClassError as exc:
        raise error(exc)


@router.get("", response_model=list[TelemetryClassSummary])
async def list_classes(
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> list[TelemetryClass]:
    async with request.app.state.database.session() as session:
        return await ManualClassService().list_classes(session, tenant, limit, offset)


@router.get("/{class_id}", response_model=TelemetryClassDetail)
async def get_class(
    class_id: UUID, request: Request, tenant: UUID = Header(alias="X-Tenant-ID")
) -> TelemetryClassDetail:
    service = ManualClassService()
    try:
        async with request.app.state.database.session() as session:
            return await detail(session, await service._class(session, tenant, class_id))
    except ManualClassError as exc:
        raise error(exc)


@router.patch("/{class_id}", response_model=TelemetryClassDetail)
async def update_class(
    class_id: UUID,
    payload: TelemetryClassUpdate,
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
) -> TelemetryClassDetail:
    service = ManualClassService()
    try:
        async with request.app.state.database.transaction() as session:
            return await detail(
                session,
                await service.update_class(
                    session, tenant, class_id, payload.name, payload.description
                ),
            )
    except ManualClassError as exc:
        raise error(exc)


@router.delete("/{class_id}", status_code=204)
async def delete_class(
    class_id: UUID, request: Request, tenant: UUID = Header(alias="X-Tenant-ID")
) -> Response:
    try:
        async with request.app.state.database.transaction() as session:
            await ManualClassService().delete_class(session, tenant, class_id)
    except ManualClassError as exc:
        raise error(exc)
    return Response(status_code=204)


@router.get("/{class_id}/members", response_model=ClassMemberListResponse)
async def list_members(
    class_id: UUID,
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> ClassMemberListResponse:
    try:
        async with request.app.state.database.session() as session:
            rows = await ManualClassService().list_members(session, tenant, class_id, limit, offset)
        return ClassMemberListResponse(
            items=[ClassMemberResponse.model_validate(row[0]) for row in rows]
        )
    except ManualClassError as exc:
        raise error(exc)


@router.post("/{class_id}/members", response_model=list[ClassMemberResponse], status_code=201)
async def add_members(
    class_id: UUID,
    payload: ClassMemberAddRequest,
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
) -> list[ClassMembership]:
    try:
        async with request.app.state.database.transaction() as session:
            return await ManualClassService().add_members(
                session, tenant, class_id, payload.stream_ids
            )
    except ManualClassError as exc:
        raise error(exc)


@router.delete("/{class_id}/members/{stream_id}", status_code=204)
async def remove_member(
    class_id: UUID, stream_id: UUID, request: Request, tenant: UUID = Header(alias="X-Tenant-ID")
) -> Response:
    try:
        async with request.app.state.database.transaction() as session:
            await ManualClassService().remove_member(session, tenant, class_id, stream_id)
    except ManualClassError as exc:
        raise error(exc)
    return Response(status_code=204)


@router.get("/{class_id}/queries", response_model=SavedClassQueryListResponse)
async def list_queries(
    class_id: UUID,
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> SavedClassQueryListResponse:
    try:
        async with request.app.state.database.session() as session:
            items = await ManualClassService().list_queries(
                session, tenant, class_id, limit, offset
            )
        return SavedClassQueryListResponse(
            items=[SavedClassQueryResponse.model_validate(item) for item in items]
        )
    except ManualClassError as exc:
        raise error(exc)


@router.post("/{class_id}/queries", response_model=SavedClassQueryResponse, status_code=201)
async def create_query(
    class_id: UUID,
    payload: SavedClassQueryCreate,
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
) -> SavedClassQuery:
    try:
        async with request.app.state.database.transaction() as session:
            return await ManualClassService().create_query(
                session, tenant, class_id, payload.name, payload.description, payload.query_spec
            )
    except ManualClassError as exc:
        raise error(exc)


@router.get("/{class_id}/queries/{query_id}", response_model=SavedClassQueryResponse)
async def get_query(
    class_id: UUID, query_id: UUID, request: Request, tenant: UUID = Header(alias="X-Tenant-ID")
) -> SavedClassQuery:
    try:
        async with request.app.state.database.session() as session:
            return await ManualClassService().get_query(session, tenant, class_id, query_id)
    except ManualClassError as exc:
        raise error(exc)


@router.patch("/{class_id}/queries/{query_id}", response_model=SavedClassQueryResponse)
async def update_query(
    class_id: UUID,
    query_id: UUID,
    payload: SavedClassQueryUpdate,
    request: Request,
    tenant: UUID = Header(alias="X-Tenant-ID"),
) -> SavedClassQuery:
    try:
        async with request.app.state.database.transaction() as session:
            return await ManualClassService().update_query(
                session,
                tenant,
                class_id,
                query_id,
                payload.name,
                payload.description,
                payload.query_spec,
            )
    except ManualClassError as exc:
        raise error(exc)


@router.delete("/{class_id}/queries/{query_id}", status_code=204)
async def delete_query(
    class_id: UUID, query_id: UUID, request: Request, tenant: UUID = Header(alias="X-Tenant-ID")
) -> Response:
    try:
        async with request.app.state.database.transaction() as session:
            await ManualClassService().delete_query(session, tenant, class_id, query_id)
    except ManualClassError as exc:
        raise error(exc)
    return Response(status_code=204)
