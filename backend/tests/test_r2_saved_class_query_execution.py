from __future__ import annotations

import asyncio
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import Table, func, select

import app.main as main_module
import app.services.saved_class_query_execution_service as execution_module
from app.core.config import get_settings
from app.domain.classes.models import ClassMembership, SavedClassQuery, TelemetryClass
from app.domain.sources.models import Site, TelemetrySource, Tenant
from app.domain.streams.models import (
    ObservationEvidence,
    ObservationOutbox,
    ObservationProcessingTask,
    RawObservationRecord,
    Stream,
)
from app.services.influx_class_query_reader import (
    InfluxClassQueryError,
    InfluxFieldRecord,
    InfluxQuerySeries,
    build_flux_query,
    flux_string_literal,
)


@dataclass(frozen=True)
class StreamFixture:
    stream_id: UUID


@dataclass(frozen=True)
class ReaderCall:
    series: tuple[InfluxQuerySeries, ...]
    start: datetime
    stop: datetime
    per_series_limit: int
    total_limit: int


@dataclass
class FakeInfluxReader:
    records: list[InfluxFieldRecord] = field(default_factory=list)
    error_code: str | None = None
    calls: list[ReaderCall] = field(default_factory=list)

    async def read(
        self,
        *,
        series: Sequence[InfluxQuerySeries],
        start: datetime,
        stop: datetime,
        per_series_limit: int,
        total_limit: int,
    ) -> list[InfluxFieldRecord]:
        self.calls.append(ReaderCall(tuple(series), start, stop, per_series_limit, total_limit))
        if self.error_code is not None:
            raise InfluxClassQueryError(self.error_code)
        return list(self.records)


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[TestClient]:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'queries.db'}")
    get_settings.cache_clear()
    result = TestClient(main_module.create_app())
    result.__enter__()

    async def create_tables() -> None:
        application = result.app
        assert isinstance(application, FastAPI)
        async with application.state.database.get_engine().begin() as connection:
            for model in (
                Tenant,
                Site,
                TelemetrySource,
                Stream,
                ObservationEvidence,
                RawObservationRecord,
                ObservationProcessingTask,
                ObservationOutbox,
                TelemetryClass,
                ClassMembership,
                SavedClassQuery,
            ):
                await connection.run_sync(cast(Table, model.__table__).create)

    asyncio.run(create_tables())
    application = result.app
    assert isinstance(application, FastAPI)
    application.state.influx_class_query_reader = FakeInfluxReader()
    try:
        yield result
    finally:
        result.__exit__(None, None, None)
        get_settings.cache_clear()


def tenant_headers(tenant_id: UUID) -> dict[str, str]:
    return {"X-Tenant-ID": str(tenant_id)}


def reader(app: TestClient) -> FakeInfluxReader:
    application = app.app
    assert isinstance(application, FastAPI)
    return cast(FakeInfluxReader, application.state.influx_class_query_reader)


def create_tenant(app: TestClient, key: str) -> UUID:
    async def insert() -> UUID:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.transaction() as session:
            item = Tenant(tenant_key=key, display_name=key)
            session.add(item)
            await session.flush()
            return item.id

    return asyncio.run(insert())


def create_stream(app: TestClient, tenant_id: UUID, tenant_key: str) -> StreamFixture:
    async def insert() -> StreamFixture:
        application = app.app
        assert isinstance(application, FastAPI)
        unique = str(uuid4())
        now = datetime.now(UTC)
        async with application.state.database.transaction() as session:
            site = Site(
                tenant_id=tenant_id,
                site_key=f"site-{unique}",
                display_name=f"Site {unique}",
            )
            session.add(site)
            await session.flush()
            source = TelemetrySource(
                tenant_id=tenant_id,
                site_id=site.id,
                source_key=f"source-{unique}",
                display_name=f"Source {unique}",
            )
            session.add(source)
            await session.flush()
            stream = Stream(
                stream_key=f"stream-{unique}",
                source_id=str(source.id),
                topic=f"telemetry/{unique}",
                tenant=tenant_key,
                first_observed_at=now,
                last_observed_at=now,
                observation_count=1,
                schema_summary={},
            )
            session.add(stream)
            await session.flush()
            return StreamFixture(stream.id)

    return asyncio.run(insert())


def create_class(app: TestClient, tenant_id: UUID) -> UUID:
    response = app.post(
        "/api/classes",
        headers=tenant_headers(tenant_id),
        json={"name": f"Class {uuid4()}", "description": "Historical query test"},
    )
    assert response.status_code == 201
    return UUID(cast(str, response.json()["id"]))


def add_members(
    app: TestClient, tenant_id: UUID, class_id: UUID, streams: Sequence[StreamFixture]
) -> None:
    response = app.post(
        f"/api/classes/{class_id}/members",
        headers=tenant_headers(tenant_id),
        json={"stream_ids": [str(item.stream_id) for item in streams]},
    )
    assert response.status_code == 201


def query_spec(
    series: Sequence[tuple[StreamFixture, str, str | None]],
    *,
    aggregation: str = "raw",
    bucket_seconds: int | None = None,
    live_append: bool = False,
) -> dict[str, object]:
    return {
        "spec_version": "saved-class-query.v1",
        "series": [
            {"stream_id": str(item.stream_id), "field_path": path, "alias": alias}
            for item, path, alias in series
        ],
        "time_window": {"mode": "relative", "lookback_seconds": 600},
        "aggregation": {"function": aggregation, "bucket_seconds": bucket_seconds},
        "live_append": live_append,
        "visualization": {"kind": "line"},
    }


def create_query(
    app: TestClient,
    tenant_id: UUID,
    class_id: UUID,
    spec: dict[str, object],
) -> UUID:
    response = app.post(
        f"/api/classes/{class_id}/queries",
        headers=tenant_headers(tenant_id),
        json={"name": f"Query {uuid4()}", "description": "Historical query", "query_spec": spec},
    )
    assert response.status_code == 201
    return UUID(cast(str, response.json()["id"]))


def execute(app: TestClient, tenant_id: UUID, class_id: UUID, query_id: UUID) -> Response:
    return app.post(
        f"/api/classes/{class_id}/queries/{query_id}/execute",
        headers=tenant_headers(tenant_id),
    )


def record(
    stream: StreamFixture,
    field_path: str,
    timestamp: datetime,
    field_name: str,
    value: bool | int | float | str,
) -> InfluxFieldRecord:
    return InfluxFieldRecord(stream.stream_id, field_path, timestamp, field_name, value)


def api_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def query_counts(app: TestClient) -> tuple[int, int, int, int, int]:
    async def counts() -> tuple[int, int, int, int, int]:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            values = (
                await session.scalar(select(func.count()).select_from(TelemetryClass)),
                await session.scalar(select(func.count()).select_from(ClassMembership)),
                await session.scalar(select(func.count()).select_from(SavedClassQuery)),
                await session.scalar(select(func.count()).select_from(ObservationProcessingTask)),
                await session.scalar(select(func.count()).select_from(ObservationOutbox)),
            )
            return cast(tuple[int, int, int, int, int], values)

    return asyncio.run(counts())


def corrupt_query_spec(app: TestClient, query_id: UUID) -> None:
    async def corrupt() -> None:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.transaction() as session:
            query = await session.get(SavedClassQuery, query_id)
            assert query is not None
            query.query_spec = {"spec_version": "saved-class-query.v1", "series": "invalid"}

    asyncio.run(corrupt())


def test_raw_single_series_execution_returns_historical_window_and_alias(app: TestClient) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    stream = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [stream])
    path = '$["temperature"]'
    query_id = create_query(
        app, tenant_id, class_id, query_spec([(stream, path, "Room temperature")], live_append=True)
    )
    observed = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    reader(app).records = [record(stream, path, observed, "value_float", 22.5)]

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 200
    body = response.json()
    assert body["class_id"] == str(class_id)
    assert body["query_id"] == str(query_id)
    assert body["spec_version"] == "saved-class-query.v1"
    assert body["live_append_requested"] is True
    assert body["truncated"] is False
    assert body["series"] == [
        {
            "stream_id": str(stream.stream_id),
            "field_path": path,
            "alias": "Room temperature",
            "points": [
                {"timestamp": api_timestamp(observed), "value_type": "float", "value": 22.5}
            ],
        }
    ]
    call = reader(app).calls[0]
    assert call.series == (InfluxQuerySeries(stream.stream_id, path),)
    assert call.stop - call.start == timedelta(seconds=600)


def test_multi_series_scalar_normalization_and_ordering_are_deterministic(app: TestClient) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    first = create_stream(app, tenant_id, "tenant-a")
    second = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [first, second])
    first_path, second_path = '$["temperature"]', '$["status"]'
    query_id = create_query(
        app,
        tenant_id,
        class_id,
        query_spec(
            [
                (second, second_path, "Status"),
                (first, first_path, None),
            ]
        ),
    )
    earliest = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    reader(app).records = [
        record(first, first_path, earliest + timedelta(seconds=1), "value_string", "warm"),
        record(second, second_path, earliest, "value_float", 22.5),
        record(first, first_path, earliest, "value_integer", 7),
        record(first, first_path, earliest, "value_boolean", True),
    ]

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 200
    series = response.json()["series"]
    assert [item["stream_id"] for item in series] == [str(second.stream_id), str(first.stream_id)]
    assert series[0]["alias"] == "Status"
    assert series[0]["points"] == [
        {"timestamp": api_timestamp(earliest), "value_type": "float", "value": 22.5}
    ]
    assert series[1]["points"] == [
        {"timestamp": api_timestamp(earliest), "value_type": "boolean", "value": True},
        {"timestamp": api_timestamp(earliest), "value_type": "integer", "value": 7},
        {
            "timestamp": api_timestamp(earliest + timedelta(seconds=1)),
            "value_type": "string",
            "value": "warm",
        },
    ]


def test_empty_result_series_is_returned_without_database_side_effects(app: TestClient) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    stream = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [stream])
    query_id = create_query(app, tenant_id, class_id, query_spec([(stream, '$["empty"]', None)]))
    before = query_counts(app)

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 200
    assert response.json()["series"][0]["points"] == []
    assert query_counts(app) == before
    assert len(reader(app).calls) == 1


def test_execute_uses_bounded_not_found_behavior_for_tenant_and_unknown_resources(
    app: TestClient,
) -> None:
    tenant_a = create_tenant(app, "tenant-a")
    tenant_b = create_tenant(app, "tenant-b")
    stream = create_stream(app, tenant_a, "tenant-a")
    class_id = create_class(app, tenant_a)
    add_members(app, tenant_a, class_id, [stream])
    query_id = create_query(app, tenant_a, class_id, query_spec([(stream, '$["value"]', None)]))

    for response in (
        execute(app, tenant_b, class_id, query_id),
        execute(app, tenant_a, class_id, uuid4()),
        execute(app, tenant_a, uuid4(), query_id),
    ):
        assert response.status_code == 404
        assert response.json() == {"detail": "resource not found"}
    assert not reader(app).calls


def test_removed_member_is_rejected_before_influx_query(app: TestClient) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    stream = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [stream])
    query_id = create_query(app, tenant_id, class_id, query_spec([(stream, '$["value"]', None)]))
    removed = app.delete(
        f"/api/classes/{class_id}/members/{stream.stream_id}", headers=tenant_headers(tenant_id)
    )
    assert removed.status_code == 204

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 404
    assert response.json() == {"detail": "resource not found"}
    assert not reader(app).calls


def test_unsupported_aggregation_is_rejected_without_influx_execution(app: TestClient) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    stream = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [stream])
    query_id = create_query(
        app,
        tenant_id,
        class_id,
        query_spec([(stream, '$["value"]', None)], aggregation="mean", bucket_seconds=60),
    )

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 400
    assert response.json() == {"detail": "unsupported_query_execution"}
    assert not reader(app).calls


def test_invalid_persisted_query_is_bounded_and_not_executed(app: TestClient) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    stream = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [stream])
    query_id = create_query(app, tenant_id, class_id, query_spec([(stream, '$["value"]', None)]))
    corrupt_query_spec(app, query_id)

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 400
    assert response.json() == {"detail": "invalid_persisted_query"}
    assert not reader(app).calls


def test_flux_string_literals_preserve_field_path_as_literal_data() -> None:
    stream_id = uuid4()
    path = '$["room \\\\ " \\u03bb"] |> drop(columns: ["_value"]) //'
    query = build_flux_query(
        bucket='telemetry") |> drop()',
        series=[InfluxQuerySeries(stream_id, path)],
        start=datetime(2026, 1, 1, tzinfo=UTC),
        stop=datetime(2026, 1, 1, 1, tzinfo=UTC),
        per_series_limit=5_001,
        total_limit=20_001,
    )

    assert f"r.field_path == {flux_string_literal(path)}" in query
    assert f"from(bucket: {flux_string_literal('telemetry") |> drop()')})" in query
    assert '\n  |> drop(columns: ["_value"])' not in query
    assert "telemetry_field" in query
    assert "value_boolean" in query and "value_string" in query
    unicode_path = '$["λ"]'
    unicode_query = build_flux_query(
        bucket="telemetry",
        series=[InfluxQuerySeries(stream_id, unicode_path)],
        start=datetime(2026, 1, 1, tzinfo=UTC),
        stop=datetime(2026, 1, 1, 1, tzinfo=UTC),
        per_series_limit=5_001,
        total_limit=20_001,
    )
    assert flux_string_literal(unicode_path) in unicode_query


@pytest.mark.parametrize("error_code", ("influx_not_configured", "influx_query_failed"))
def test_influx_failures_are_bounded_and_do_not_expose_details(
    app: TestClient, error_code: str
) -> None:
    tenant_id = create_tenant(app, "tenant-a")
    stream = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [stream])
    query_id = create_query(app, tenant_id, class_id, query_spec([(stream, '$["value"]', None)]))
    reader(app).error_code = error_code

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 503
    assert response.json() == {"detail": error_code}
    for forbidden in ("PRIVATE_QUERY_FAILURE_SECRET", "token", "http://", "https://", "Traceback"):
        assert forbidden not in response.text


def test_point_limits_set_truncated_with_deterministic_series_allocation(
    app: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(execution_module, "MAX_POINTS_PER_SERIES", 2)
    monkeypatch.setattr(execution_module, "MAX_POINTS_PER_RESPONSE", 3)
    tenant_id = create_tenant(app, "tenant-a")
    first = create_stream(app, tenant_id, "tenant-a")
    second = create_stream(app, tenant_id, "tenant-a")
    class_id = create_class(app, tenant_id)
    add_members(app, tenant_id, class_id, [first, second])
    first_path, second_path = '$["first"]', '$["second"]'
    query_id = create_query(
        app,
        tenant_id,
        class_id,
        query_spec([(first, first_path, None), (second, second_path, None)]),
    )
    base = datetime(2026, 1, 1, tzinfo=UTC)
    reader(app).records = [
        record(first, first_path, base + timedelta(seconds=index), "value_integer", index)
        for index in range(3)
    ] + [
        record(second, second_path, base + timedelta(seconds=index), "value_integer", index)
        for index in range(2)
    ]

    response = execute(app, tenant_id, class_id, query_id)

    assert response.status_code == 200
    body = response.json()
    assert body["truncated"] is True
    assert [len(item["points"]) for item in body["series"]] == [2, 1]
    call = reader(app).calls[0]
    assert (call.per_series_limit, call.total_limit) == (3, 4)


def test_execute_route_requires_tenant_context(app: TestClient) -> None:
    response = app.post(f"/api/classes/{uuid4()}/queries/{uuid4()}/execute")

    assert response.status_code == 422
