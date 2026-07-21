from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.classes.models import ClassMembership, SavedClassQuery, TelemetryClass
from app.services.influx_class_query_reader import (
    InfluxClassQueryError,
    InfluxClassQueryReaderProtocol,
    InfluxFieldRecord,
    InfluxQuerySeries,
)
from app.services.manual_class_service import SPEC_VERSION, SavedQuerySpec, SeriesItem

MAX_POINTS_PER_SERIES = 5_000
MAX_POINTS_PER_RESPONSE = 20_000

ValueType = Literal["boolean", "integer", "float", "string"]
ScalarValue = bool | int | float | str
VALUE_TYPES: dict[str, ValueType] = {
    "value_boolean": "boolean",
    "value_integer": "integer",
    "value_float": "float",
    "value_string": "string",
}


class SavedClassQueryExecutionError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class QueryPoint:
    timestamp: datetime
    value_type: ValueType
    value: ScalarValue


@dataclass(frozen=True)
class QuerySeriesResult:
    stream_id: UUID
    field_path: str
    alias: str | None
    points: list[QueryPoint]


@dataclass(frozen=True)
class SavedClassQueryExecutionResult:
    class_id: UUID
    query_id: UUID
    spec_version: str
    executed_at: datetime
    start: datetime
    stop: datetime
    live_append_requested: bool
    truncated: bool
    series: list[QuerySeriesResult]


class SavedClassQueryExecutionService:
    def __init__(self, reader: InfluxClassQueryReaderProtocol) -> None:
        self._reader = reader

    async def execute(
        self,
        session: AsyncSession,
        tenant_id: UUID,
        class_id: UUID,
        query_id: UUID,
        *,
        now: datetime | None = None,
    ) -> SavedClassQueryExecutionResult:
        telemetry_class = await session.scalar(
            select(TelemetryClass).where(
                TelemetryClass.id == class_id,
                TelemetryClass.tenant_id == tenant_id,
            )
        )
        if telemetry_class is None:
            raise SavedClassQueryExecutionError("class_not_found")
        query = await session.scalar(
            select(SavedClassQuery).where(
                SavedClassQuery.id == query_id,
                SavedClassQuery.telemetry_class_id == telemetry_class.id,
                SavedClassQuery.tenant_id == tenant_id,
            )
        )
        if query is None:
            raise SavedClassQueryExecutionError("query_not_found")

        spec = _spec(query)
        if spec.time_window.mode != "relative" or spec.aggregation.function != "raw":
            raise SavedClassQueryExecutionError("unsupported_query_execution")
        member_ids = set(
            (
                await session.scalars(
                    select(ClassMembership.stream_id).where(
                        ClassMembership.telemetry_class_id == telemetry_class.id
                    )
                )
            ).all()
        )
        if not {item.stream_id for item in spec.series} <= member_ids:
            raise SavedClassQueryExecutionError("query_stream_not_member")

        stop = _utc(now or datetime.now(UTC))
        start = stop - timedelta(seconds=spec.time_window.lookback_seconds)
        query_series = [InfluxQuerySeries(item.stream_id, item.field_path) for item in spec.series]
        try:
            records = await self._reader.read(
                series=query_series,
                start=start,
                stop=stop,
                per_series_limit=MAX_POINTS_PER_SERIES + 1,
                total_limit=MAX_POINTS_PER_RESPONSE + 1,
            )
        except InfluxClassQueryError as error:
            raise SavedClassQueryExecutionError(error.code) from error
        series, truncated = _normalize(spec.series, records)
        return SavedClassQueryExecutionResult(
            class_id=telemetry_class.id,
            query_id=query.id,
            spec_version=SPEC_VERSION,
            executed_at=stop,
            start=start,
            stop=stop,
            live_append_requested=spec.live_append,
            truncated=truncated,
            series=series,
        )


def _spec(query: SavedClassQuery) -> SavedQuerySpec:
    if query.spec_version != SPEC_VERSION:
        raise SavedClassQueryExecutionError("invalid_persisted_query")
    try:
        spec = SavedQuerySpec.model_validate(query.query_spec)
    except ValidationError as error:
        raise SavedClassQueryExecutionError("invalid_persisted_query") from error
    if spec.spec_version != query.spec_version:
        raise SavedClassQueryExecutionError("invalid_persisted_query")
    return spec


def _normalize(
    requested_series: Sequence[SeriesItem], records: Sequence[InfluxFieldRecord]
) -> tuple[list[QuerySeriesResult], bool]:
    points_by_key: dict[tuple[UUID, str], list[QueryPoint]] = {}
    requested_keys = {(item.stream_id, item.field_path) for item in requested_series}
    for record in records:
        key = (record.stream_id, record.field_path)
        if key not in requested_keys:
            continue
        point = _point(record)
        if point is not None:
            points_by_key.setdefault(key, []).append(point)

    total = 0
    truncated = False
    normalized: list[QuerySeriesResult] = []
    for item in requested_series:
        points = sorted(points_by_key.get((item.stream_id, item.field_path), []), key=_point_key)
        if len(points) > MAX_POINTS_PER_SERIES:
            points = points[:MAX_POINTS_PER_SERIES]
            truncated = True
        remaining = MAX_POINTS_PER_RESPONSE - total
        if len(points) > remaining:
            points = points[: max(remaining, 0)]
            truncated = True
        total += len(points)
        normalized.append(QuerySeriesResult(item.stream_id, item.field_path, item.alias, points))
    return normalized, truncated


def _point(record: InfluxFieldRecord) -> QueryPoint | None:
    value_type = VALUE_TYPES.get(record.field_name)
    if value_type == "boolean" and isinstance(record.value, bool):
        return QueryPoint(record.timestamp, value_type, record.value)
    if (
        value_type == "integer"
        and isinstance(record.value, int)
        and not isinstance(record.value, bool)
    ):
        return QueryPoint(record.timestamp, value_type, record.value)
    if value_type == "float" and isinstance(record.value, float):
        return QueryPoint(record.timestamp, value_type, record.value)
    if value_type == "string" and isinstance(record.value, str):
        return QueryPoint(record.timestamp, value_type, record.value)
    return None


def _point_key(point: QueryPoint) -> tuple[datetime, int, str]:
    type_order = {"boolean": 0, "integer": 1, "float": 2, "string": 3}
    return point.timestamp, type_order[point.value_type], json.dumps(point.value, ensure_ascii=True)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
