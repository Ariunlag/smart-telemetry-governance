from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import select

from app.domain.streams.models import Stream

router = APIRouter(prefix="/streams", tags=["streams"])


class StreamResponse(BaseModel):
    id: UUID
    stream_key: str
    source_id: str
    topic: str
    tenant: str | None
    lifecycle_status: str
    first_observed_at: datetime
    last_observed_at: datetime
    observation_count: int
    payload_format: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


@router.get("", response_model=list[StreamResponse])
async def list_streams(
    request: Request, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)
) -> list[Stream]:
    database = request.app.state.database
    async with database.session() as session:
        return list(
            (
                await session.scalars(
                    select(Stream)
                    .order_by(Stream.last_observed_at.desc())
                    .offset(offset)
                    .limit(limit)
                )
            ).all()
        )


@router.get("/{stream_id}", response_model=StreamResponse)
async def get_stream(stream_id: UUID, request: Request) -> Stream:
    async with request.app.state.database.session() as session:
        stream = await session.get(Stream, stream_id)
    if stream is None:
        raise HTTPException(status_code=404, detail="stream not found")
    return cast(Stream, stream)
