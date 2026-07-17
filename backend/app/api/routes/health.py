from typing import Literal

from fastapi import APIRouter, Request, Response, status
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: Literal["ok"]
    version: str
    service: str


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    database: Literal["ready", "not_configured", "unavailable"]
    mqtt: Literal["disabled", "running"] = "disabled"


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(status="ok", version="0.1.0", service="smart-telemetry-governance")


@router.get("/ready", response_model=ReadinessResponse)
async def readiness_check(request: Request, response: Response) -> ReadinessResponse:
    database = request.app.state.database
    is_ready = await database.check_connection()
    if is_ready and not database.configured:
        return ReadinessResponse(
            status="ready", database="not_configured", mqtt=request.app.state.mqtt_adapter.status
        )
    if is_ready:
        return ReadinessResponse(
            status="ready", database="ready", mqtt=request.app.state.mqtt_adapter.status
        )
    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(
        status="not_ready", database="unavailable", mqtt=request.app.state.mqtt_adapter.status
    )
