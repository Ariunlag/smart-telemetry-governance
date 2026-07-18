from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.db.session import DatabaseNotInitializedError
from app.services.observation_outbox_repository import ObservationOutboxRepository

router = APIRouter(prefix="/api/delivery", tags=["delivery"])


class DeliveryStatusResponse(BaseModel):
    enabled: bool
    database_available: bool
    worker_running: bool
    pending: int = 0
    processing: int = 0
    delivered: int = 0
    retryable: int = 0
    dead_letter: int = 0
    stale_processing_count: int = 0
    oldest_eligible_available_at: datetime | None = None
    batch_size: int
    maximum_attempts: int
    last_successful_cycle: datetime | None = None
    last_worker_error_code: str | None = None


@router.get("/status", response_model=DeliveryStatusResponse)
async def delivery_status(request: Request) -> DeliveryStatusResponse:
    settings = request.app.state.settings
    worker = request.app.state.delivery_worker
    result = DeliveryStatusResponse(
        enabled=settings.influxdb_enabled,
        database_available=False,
        worker_running=worker.running,
        batch_size=settings.outbox_worker_batch_size,
        maximum_attempts=settings.outbox_max_attempts,
        last_successful_cycle=worker.last_successful_cycle,
        last_worker_error_code=worker.last_error_code,
    )
    if not settings.influxdb_enabled or not request.app.state.database.configured:
        return result
    try:
        async with request.app.state.database.session() as session:
            counts, stale, oldest = await ObservationOutboxRepository().status(
                session, settings.outbox_processing_lease_seconds
            )
    except (DatabaseNotInitializedError, Exception):
        return result
    result.database_available = True
    for state, count in counts.items():
        setattr(result, state, count)
    result.stale_processing_count = stale
    result.oldest_eligible_available_at = oldest
    return result
