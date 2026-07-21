from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.main as main_module
from app.core.config import Settings, get_settings
from app.domain.sources.models import Site, TelemetrySource, Tenant
from app.domain.streams.models import Stream
from app.services.influx_class_query_reader import InfluxClassQueryReader, InfluxQuerySeries
from app.services.influx_observation_writer import DeliveryItem, InfluxObservationWriter

pytestmark = [pytest.mark.influxdb, pytest.mark.postgresql]


def integration_settings() -> Settings:
    required = (
        "TEST_DATABASE_URL",
        "TEST_INFLUXDB_URL",
        "TEST_INFLUXDB_ORG",
        "TEST_INFLUXDB_BUCKET",
        "TEST_INFLUXDB_TOKEN",
    )
    if any(not os.getenv(name) for name in required):
        pytest.skip("TEST_DATABASE_URL and TEST_INFLUXDB_* are required for integration tests")
    return Settings(
        app_env="test",
        database_url=os.environ["TEST_DATABASE_URL"],
        influxdb_enabled=True,
        influxdb_url=os.environ["TEST_INFLUXDB_URL"],
        influxdb_org=os.environ["TEST_INFLUXDB_ORG"],
        influxdb_bucket=os.environ["TEST_INFLUXDB_BUCKET"],
        influxdb_token=os.environ["TEST_INFLUXDB_TOKEN"],
        influxdb_verify_ssl=False,
        influxdb_timeout_ms=1_000,
    )


async def seed_streams(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[UUID, str, UUID, UUID]:
    primary_key = f"query-tenant-{uuid4()}"
    other_key = f"query-other-tenant-{uuid4()}"
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        primary_tenant = Tenant(tenant_key=primary_key, display_name="Query tenant")
        other_tenant = Tenant(tenant_key=other_key, display_name="Other tenant")
        session.add_all((primary_tenant, other_tenant))
        await session.flush()
        primary_site = Site(
            tenant_id=primary_tenant.id,
            site_key=f"site-{uuid4()}",
            display_name="Query site",
        )
        other_site = Site(
            tenant_id=other_tenant.id,
            site_key=f"site-{uuid4()}",
            display_name="Other site",
        )
        session.add_all((primary_site, other_site))
        await session.flush()
        primary_source = TelemetrySource(
            tenant_id=primary_tenant.id,
            site_id=primary_site.id,
            source_key=f"source-{uuid4()}",
            display_name="Query source",
        )
        other_source = TelemetrySource(
            tenant_id=other_tenant.id,
            site_id=other_site.id,
            source_key=f"source-{uuid4()}",
            display_name="Other source",
        )
        session.add_all((primary_source, other_source))
        await session.flush()
        primary_stream = Stream(
            stream_key=f"stream-{uuid4()}",
            source_id=str(primary_source.id),
            topic="query/integration/primary",
            tenant=primary_key,
            first_observed_at=now,
            last_observed_at=now,
            observation_count=1,
            schema_summary={},
        )
        other_stream = Stream(
            stream_key=f"stream-{uuid4()}",
            source_id=str(other_source.id),
            topic="query/integration/other",
            tenant=other_key,
            first_observed_at=now,
            last_observed_at=now,
            observation_count=1,
            schema_summary={},
        )
        session.add_all((primary_stream, other_stream))
        await session.flush()
        return primary_tenant.id, primary_key, primary_stream.id, other_stream.id


def headers(tenant_id: UUID) -> dict[str, str]:
    return {"X-Tenant-ID": str(tenant_id)}


def field_item(
    *,
    stream_id: UUID,
    tenant: str,
    field_path: str,
    value_type: str,
    value: bool | int | float | str,
    timestamp: datetime,
) -> DeliveryItem:
    payload: dict[str, object] = {
        "stream_id": str(stream_id),
        "source_id": f"source-{uuid4()}",
        "topic": "query/integration/telemetry",
        "observation_timestamp": timestamp.isoformat(),
        "received_timestamp": timestamp.isoformat(),
        "timestamp_source": "payload",
        "field_path": field_path,
        "value_type": value_type,
        "value": value,
        "content_schema_version": "r2.field-point.v1",
        "quality_status": "accepted",
        "provenance_reference": str(uuid4()),
        "tenant": tenant,
    }
    return DeliveryItem(str(uuid4()), str(uuid4()), payload, 1, timestamp)


async def write_points(settings: Settings, items: list[DeliveryItem]) -> None:
    writer = InfluxObservationWriter(settings)
    await writer.initialize()
    try:
        for item in items:
            await writer.write(item)
    finally:
        await writer.close()


def create_query(
    app: TestClient,
    tenant_id: UUID,
    class_id: str,
    stream_id: UUID,
    field_path: str,
    lookback_seconds: int,
) -> str:
    response = app.post(
        f"/api/classes/{class_id}/queries",
        headers=headers(tenant_id),
        json={
            "name": f"Historical query {uuid4()}",
            "description": "Real InfluxDB query integration",
            "query_spec": {
                "spec_version": "saved-class-query.v1",
                "series": [
                    {
                        "stream_id": str(stream_id),
                        "field_path": field_path,
                        "alias": "Quoted field",
                    }
                ],
                "time_window": {"mode": "relative", "lookback_seconds": lookback_seconds},
                "aggregation": {"function": "raw", "bucket_seconds": None},
                "live_append": False,
                "visualization": {"kind": "line"},
            },
        },
    )
    assert response.status_code == 201
    return cast(str, response.json()["id"])


async def execute_until_points(
    app: TestClient, tenant_id: UUID, class_id: str, query_id: str
) -> dict[str, object]:
    for _ in range(10):
        response = app.post(
            f"/api/classes/{class_id}/queries/{query_id}/execute",
            headers=headers(tenant_id),
        )
        assert response.status_code == 200, response.text
        body = cast(dict[str, object], response.json())
        series = cast(list[dict[str, object]], body["series"])
        if series and cast(list[object], series[0]["points"]):
            return body
        await asyncio.sleep(0.1)
    raise AssertionError("real telemetry_field points were not returned by the execute endpoint")


async def read_until_points(
    reader: InfluxClassQueryReader, stream_id: UUID, field_path: str
) -> None:
    for _ in range(10):
        records = await reader.read(
            series=[InfluxQuerySeries(stream_id, field_path)],
            start=datetime.now(UTC) - timedelta(minutes=10),
            stop=datetime.now(UTC),
            per_series_limit=5_001,
            total_limit=20_001,
        )
        if records:
            assert len(records) == 4
            return
        await asyncio.sleep(0.1)
    raise AssertionError("real telemetry_field points were not returned by the reader")


@pytest.mark.asyncio
async def test_real_field_points_execute_through_postgresql_and_influxdb(
    monkeypatch: pytest.MonkeyPatch,
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    settings = integration_settings()
    tenant_id, tenant_key, stream_id, other_stream_id = await seed_streams(postgresql_sessions)
    field_path = '$["device"]["quoted\\" field"]'
    now = datetime.now(UTC)
    primary_points = [
        field_item(
            stream_id=stream_id,
            tenant=tenant_key,
            field_path=field_path,
            value_type="boolean",
            value=True,
            timestamp=now - timedelta(seconds=20),
        ),
        field_item(
            stream_id=stream_id,
            tenant=tenant_key,
            field_path=field_path,
            value_type="integer",
            value=7,
            timestamp=now - timedelta(seconds=15),
        ),
        field_item(
            stream_id=stream_id,
            tenant=tenant_key,
            field_path=field_path,
            value_type="float",
            value=22.5,
            timestamp=now - timedelta(seconds=10),
        ),
        field_item(
            stream_id=stream_id,
            tenant=tenant_key,
            field_path=field_path,
            value_type="string",
            value="warm",
            timestamp=now - timedelta(seconds=5),
        ),
    ]
    excluded_point = field_item(
        stream_id=stream_id,
        tenant=tenant_key,
        field_path='$["device"]["excluded"]',
        value_type="string",
        value="excluded",
        timestamp=now - timedelta(seconds=4),
    )
    other_tenant_point = field_item(
        stream_id=other_stream_id,
        tenant=f"other-{uuid4()}",
        field_path=field_path,
        value_type="string",
        value="other-tenant",
        timestamp=now - timedelta(seconds=3),
    )
    await write_points(settings, [*primary_points, excluded_point, other_tenant_point])

    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", os.environ["TEST_DATABASE_URL"])
    monkeypatch.setenv("INFLUXDB_ENABLED", "true")
    monkeypatch.setenv("INFLUXDB_URL", os.environ["TEST_INFLUXDB_URL"])
    monkeypatch.setenv("INFLUXDB_ORG", os.environ["TEST_INFLUXDB_ORG"])
    monkeypatch.setenv("INFLUXDB_BUCKET", os.environ["TEST_INFLUXDB_BUCKET"])
    monkeypatch.setenv("INFLUXDB_TOKEN", os.environ["TEST_INFLUXDB_TOKEN"])
    monkeypatch.setenv("INFLUXDB_VERIFY_SSL", "false")
    monkeypatch.setenv("MQTT_ENABLED", "false")
    monkeypatch.setenv("SCHEMA_OBSERVATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("FIELD_PROJECTION_WORKER_ENABLED", "false")
    get_settings.cache_clear()
    try:
        with TestClient(main_module.create_app()) as app:
            application = app.app
            assert isinstance(application, FastAPI)
            assert isinstance(application.state.influx_class_query_reader, InfluxClassQueryReader)
            await read_until_points(
                application.state.influx_class_query_reader, stream_id, field_path
            )
            created_class = app.post(
                "/api/classes",
                headers=headers(tenant_id),
                json={"name": f"Real integration class {uuid4()}", "description": "Integration"},
            )
            assert created_class.status_code == 201
            class_id = cast(str, created_class.json()["id"])
            membership = app.post(
                f"/api/classes/{class_id}/members",
                headers=headers(tenant_id),
                json={"stream_ids": [str(stream_id)]},
            )
            assert membership.status_code == 201
            query_id = create_query(app, tenant_id, class_id, stream_id, field_path, 600)

            body = await execute_until_points(app, tenant_id, class_id, query_id)
            series = cast(list[dict[str, object]], body["series"])
            assert len(series) == 1
            returned = series[0]
            assert returned["stream_id"] == str(stream_id)
            assert returned["field_path"] == field_path
            assert returned["alias"] == "Quoted field"
            points = cast(list[dict[str, object]], returned["points"])
            timestamps = [
                datetime.fromisoformat(cast(str, item["timestamp"]).replace("Z", "+00:00"))
                for item in points
            ]
            assert timestamps == sorted(timestamps)
            assert [(item["value_type"], item["value"]) for item in points] == [
                ("boolean", True),
                ("integer", 7),
                ("float", 22.5),
                ("string", "warm"),
            ]
            assert "excluded" not in str(body)
            assert "other-tenant" not in str(body)

            empty_query_id = create_query(app, tenant_id, class_id, stream_id, field_path, 1)
            empty = app.post(
                f"/api/classes/{class_id}/queries/{empty_query_id}/execute",
                headers=headers(tenant_id),
            )
            assert empty.status_code == 200
            empty_series = cast(list[dict[str, object]], empty.json()["series"])
            assert len(empty_series) == 1 and empty_series[0]["points"] == []
    finally:
        get_settings.cache_clear()
