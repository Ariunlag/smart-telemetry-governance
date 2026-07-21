from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.session import Database
from app.domain.streams.models import ObservationEvidence, ObservationOutbox, Stream
from app.services.influx_observation_writer import (
    DeliveryFailure,
    DeliveryItem,
    InfluxObservationWriter,
)
from app.services.observation_delivery_worker import ObservationDeliveryWorker

pytestmark = pytest.mark.influxdb


def integration_settings(*, url: str | None = None, token: str | None = None) -> Settings:
    required = (
        "TEST_INFLUXDB_URL",
        "TEST_INFLUXDB_ORG",
        "TEST_INFLUXDB_BUCKET",
        "TEST_INFLUXDB_TOKEN",
    )
    if any(not os.getenv(name) for name in required):
        pytest.skip("TEST_INFLUXDB_* variables are required for InfluxDB integration tests")
    database_url = os.getenv("TEST_DATABASE_URL", "postgresql+psycopg://test:test@localhost/test")
    return Settings(
        app_env="test",
        database_url=database_url,
        influxdb_enabled=True,
        influxdb_url=url or os.environ["TEST_INFLUXDB_URL"],
        influxdb_org=os.environ["TEST_INFLUXDB_ORG"],
        influxdb_bucket=os.environ["TEST_INFLUXDB_BUCKET"],
        influxdb_token=token or os.environ["TEST_INFLUXDB_TOKEN"],
        influxdb_verify_ssl=False,
        influxdb_timeout_ms=500,
        outbox_worker_batch_size=10,
        outbox_backoff_base_seconds=1,
        outbox_backoff_max_seconds=2,
    )


def delivery_item(
    value_type: str,
    value: object,
    *,
    stream_id: str | None = None,
    tenant: str | None = None,
    unit: str | None = None,
) -> DeliveryItem:
    unique = stream_id or str(uuid4())
    timestamp = datetime.now(UTC) - timedelta(seconds=2)
    payload: dict[str, object] = {
        "stream_id": unique,
        "source_id": f"source-{uuid4()}",
        "topic": "integration/temperature",
        "observation_timestamp": timestamp.isoformat(),
        "received_timestamp": timestamp.isoformat(),
        "timestamp_source": "payload",
        "metric": f"metric-{uuid4()}",
        "value_type": value_type,
        "value": value,
        "content_schema_version": "r1.normalized-point.v1",
        "quality_status": "accepted",
        "provenance_reference": f"evidence-{uuid4()}",
    }
    if tenant is not None:
        payload["tenant"] = tenant
    if unit is not None:
        payload["unit"] = unit
    return DeliveryItem(str(uuid4()), f"delivery-{uuid4()}", payload, 1, timestamp)


def field_delivery_item(
    value_type: str, value: object, *, tenant: str | None = None
) -> DeliveryItem:
    unique = str(uuid4())
    timestamp = datetime.now(UTC) - timedelta(seconds=2)
    payload: dict[str, object] = {
        "stream_id": unique,
        "source_id": f"source-{uuid4()}",
        "topic": "integration/field",
        "observation_timestamp": timestamp.isoformat(),
        "received_timestamp": timestamp.isoformat(),
        "timestamp_source": "source",
        "field_path": '$["sensors"]["value"]',
        "value_type": value_type,
        "value": value,
        "content_schema_version": "r2.field-point.v1",
        "quality_status": "unassessed",
        "provenance_reference": f"evidence-{uuid4()}",
    }
    if tenant is not None:
        payload["tenant"] = tenant
    return DeliveryItem(str(uuid4()), f"delivery-{uuid4()}", payload, 1, timestamp)


async def query_records(stream_id: str, start: datetime) -> list[dict[str, object]]:
    client_type: Any = import_module(
        "influxdb_client.client.influxdb_client_async"
    ).InfluxDBClientAsync
    client: Any = client_type(
        url=os.environ["TEST_INFLUXDB_URL"],
        token=os.environ["TEST_INFLUXDB_TOKEN"],
        org=os.environ["TEST_INFLUXDB_ORG"],
        verify_ssl=False,
    )
    query = (
        f'from(bucket: "{os.environ["TEST_INFLUXDB_BUCKET"]}") '
        f"|> range(start: {start.astimezone(UTC).isoformat()}) "
        '|> filter(fn: (r) => r._measurement == "telemetry_observation") '
        f'|> filter(fn: (r) => r.stream_id == "{stream_id}")'
    )
    try:
        for _ in range(10):
            tables: Any = await client.query_api().query(query=query)
            records = [dict(record.values) for table in tables for record in table.records]
            if records:
                return cast(list[dict[str, object]], records)
            await asyncio.sleep(0.1)
        raise AssertionError("InfluxDB point was not visible within the bounded retry window")
    finally:
        await client.close()


async def query_field_records(
    stream_id: str, start: datetime, *, wait_for_record: bool = True
) -> list[dict[str, object]]:
    client_type: Any = import_module(
        "influxdb_client.client.influxdb_client_async"
    ).InfluxDBClientAsync
    client: Any = client_type(
        url=os.environ["TEST_INFLUXDB_URL"],
        token=os.environ["TEST_INFLUXDB_TOKEN"],
        org=os.environ["TEST_INFLUXDB_ORG"],
        verify_ssl=False,
    )
    query = (
        f'from(bucket: "{os.environ["TEST_INFLUXDB_BUCKET"]}") '
        f"|> range(start: {start.astimezone(UTC).isoformat()}) "
        '|> filter(fn: (r) => r._measurement == "telemetry_field") '
        f'|> filter(fn: (r) => r.stream_id == "{stream_id}")'
    )
    try:
        for _ in range(10):
            tables: Any = await client.query_api().query(query=query)
            records = [dict(record.values) for table in tables for record in table.records]
            if records or not wait_for_record:
                return cast(list[dict[str, object]], records)
            await asyncio.sleep(0.1)
        raise AssertionError("InfluxDB field point was not visible within the bounded retry window")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_writer_initializes_writes_and_closes_idempotently() -> None:
    writer = InfluxObservationWriter(integration_settings())
    await writer.initialize()
    await writer.initialize()
    await writer.close()
    await writer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("value_type", "value", "field"),
    [
        ("integer", 42, "value_integer"),
        ("float", 1.5, "value_float"),
        ("boolean", True, "value_boolean"),
        ("string", "warm", "value_string"),
    ],
)
async def test_typed_point_write_and_query(value_type: str, value: object, field: str) -> None:
    item = delivery_item(value_type, value, tenant="tenant-integration", unit="celsius")
    writer = InfluxObservationWriter(integration_settings())
    await writer.initialize()
    try:
        await writer.write(item)
    finally:
        await writer.close()

    payload = item.point_payload
    records = await query_records(
        str(payload["stream_id"]), item.processing_started_at - timedelta(seconds=1)
    )
    record = next(record for record in records if record.get("_field") == field)
    assert record["_measurement"] == "telemetry_observation"
    assert record["_value"] == value
    assert record["stream_id"] == payload["stream_id"]
    assert record["source_id"] == payload["source_id"]
    assert record["metric"] == payload["metric"]
    assert record["tenant"] == "tenant-integration" and record["unit"] == "celsius"
    assert record["timestamp_source"] == "payload"
    assert record["quality_status"] == "accepted"
    assert record["content_schema_version"] == "r1.normalized-point.v1"
    assert cast(datetime, record["_time"]).astimezone(UTC) == datetime.fromisoformat(
        str(payload["observation_timestamp"])
    ).astimezone(UTC)
    assert {"topic", "delivery_key", "provenance_reference"}.isdisjoint(record)
    stored_fields = {str(stored["_field"]): stored["_value"] for stored in records}
    assert set(stored_fields) == {
        field,
        "topic",
        "received_timestamp",
        "provenance_reference",
        "delivery_key",
    }
    assert stored_fields["topic"] == payload["topic"]
    assert stored_fields["delivery_key"] == item.delivery_key
    assert stored_fields["received_timestamp"] == payload["received_timestamp"]
    assert stored_fields["provenance_reference"] == payload["provenance_reference"]
    assert {
        "raw_payload",
        "payload_fingerprint",
        "broker_metadata",
        "token",
        "database_url",
    }.isdisjoint(stored_fields)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("value_type", "value", "field", "tenant"),
    [
        ("integer", 42, "value_integer", "tenant-integration"),
        ("float", 1.5, "value_float", "tenant-integration"),
        ("boolean", True, "value_boolean", "tenant-integration"),
        ("string", "warm", "value_string", None),
    ],
)
async def test_field_point_write_and_query(
    value_type: str, value: object, field: str, tenant: str | None
) -> None:
    item = field_delivery_item(value_type, value, tenant=tenant)
    writer = InfluxObservationWriter(integration_settings())
    await writer.initialize()
    try:
        await writer.write(item)
    finally:
        await writer.close()

    payload = item.point_payload
    records = await query_field_records(
        str(payload["stream_id"]), item.processing_started_at - timedelta(seconds=1)
    )
    record = next(record for record in records if record.get("_field") == field)
    assert record["_measurement"] == "telemetry_field"
    assert record["_value"] == value
    assert record["field_path"] == payload["field_path"]
    if tenant is None:
        assert "tenant" not in record
    else:
        assert record["tenant"] == tenant
    assert record["timestamp_source"] == "source"
    assert record["quality_status"] == "unassessed"
    assert record["content_schema_version"] == "r2.field-point.v1"
    assert cast(datetime, record["_time"]).astimezone(UTC) == datetime.fromisoformat(
        str(payload["observation_timestamp"])
    ).astimezone(UTC)
    stored_fields = {str(stored["_field"]): stored["_value"] for stored in records}
    assert set(stored_fields) == {
        field,
        "topic",
        "received_timestamp",
        "provenance_reference",
        "delivery_key",
    }
    assert stored_fields["provenance_reference"] == payload["provenance_reference"]
    assert stored_fields["delivery_key"] == item.delivery_key


@pytest.mark.asyncio
async def test_invalid_field_payload_does_not_write_a_point() -> None:
    item = field_delivery_item("integer", 7)
    item.point_payload["value_type"] = "unsupported"
    writer = InfluxObservationWriter(integration_settings())
    await writer.initialize()
    try:
        with pytest.raises(DeliveryFailure, match="invalid_point"):
            await writer.write(item)
    finally:
        await writer.close()

    records = await query_field_records(
        str(item.point_payload["stream_id"]),
        item.processing_started_at - timedelta(seconds=1),
        wait_for_record=False,
    )
    assert not records


@pytest.mark.asyncio
async def test_repeat_write_is_one_logical_point_and_omits_optional_tags() -> None:
    item = delivery_item("integer", 7)
    writer = InfluxObservationWriter(integration_settings())
    await writer.initialize()
    try:
        await writer.write(item)
        await writer.write(item)
    finally:
        await writer.close()
    records = await query_records(
        str(item.point_payload["stream_id"]), item.processing_started_at - timedelta(seconds=1)
    )
    values = [record for record in records if record.get("_field") == "value_integer"]
    assert len(values) == 1 and values[0]["_value"] == 7
    assert "tenant" not in values[0] and "unit" not in values[0]


async def seed_outbox(sessions: async_sessionmaker[AsyncSession], item: DeliveryItem) -> UUID:
    now = datetime.now(UTC)
    stream_id, evidence_id, outbox_id = uuid4(), uuid4(), uuid4()
    async with sessions() as session:
        async with session.begin():
            session.add(
                Stream(
                    id=stream_id,
                    stream_key=f"stream-{uuid4()}",
                    source_id="source",
                    topic="integration/topic",
                    first_observed_at=now,
                    last_observed_at=now,
                )
            )
            await session.flush()
            session.add(
                ObservationEvidence(
                    id=evidence_id,
                    stream_id=stream_id,
                    received_at=now,
                    outcome="accepted",
                    payload_size=1,
                    payload_fingerprint="0" * 64,
                )
            )
            await session.flush()
            session.add(
                ObservationOutbox(
                    id=outbox_id,
                    delivery_key=item.delivery_key,
                    stream_id=stream_id,
                    evidence_id=evidence_id,
                    point_payload=item.point_payload,
                    available_at=now,
                )
            )
    return outbox_id


async def read_outbox(
    sessions: async_sessionmaker[AsyncSession], row_id: UUID
) -> ObservationOutbox:
    async with sessions() as session:
        row = await session.get(ObservationOutbox, row_id)
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_real_worker_delivery_outage_and_recovery(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    item = delivery_item("integer", 11)
    row_id = await seed_outbox(postgresql_sessions, item)
    healthy_settings = integration_settings()
    database = Database(healthy_settings)
    await database.initialize()
    writer = InfluxObservationWriter(healthy_settings)
    await writer.initialize()
    try:
        await ObservationDeliveryWorker(healthy_settings, database, writer).run_cycle()
    finally:
        await writer.close()
        await database.dispose()
    delivered = await read_outbox(postgresql_sessions, row_id)
    assert delivered.state == "delivered" and delivered.delivered_at is not None
    assert delivered.processing_started_at is None and delivered.last_error_code is None
    assert await query_records(
        str(item.point_payload["stream_id"]), item.processing_started_at - timedelta(seconds=1)
    )

    retry_item = delivery_item("integer", 12)
    retry_row_id = await seed_outbox(postgresql_sessions, retry_item)
    unavailable_settings = integration_settings(url="http://127.0.0.1:1")
    unavailable_database = Database(unavailable_settings)
    await unavailable_database.initialize()
    unavailable_writer = InfluxObservationWriter(unavailable_settings)
    await unavailable_writer.initialize()
    try:
        await ObservationDeliveryWorker(
            unavailable_settings, unavailable_database, unavailable_writer
        ).run_cycle()
    finally:
        await unavailable_writer.close()
        await unavailable_database.dispose()
    retried = await read_outbox(postgresql_sessions, retry_row_id)
    assert retried.state == "retryable" and retried.available_at > datetime.now(UTC)
    assert retried.delivered_at is None and retried.last_error_code in {"network_error", "timeout"}

    async with postgresql_sessions() as session:
        async with session.begin():
            row = await session.get(ObservationOutbox, retry_row_id)
            assert row is not None
            row.available_at = datetime.now(UTC) - timedelta(seconds=1)
    recovery_database = Database(healthy_settings)
    await recovery_database.initialize()
    recovery_writer = InfluxObservationWriter(healthy_settings)
    await recovery_writer.initialize()
    try:
        await ObservationDeliveryWorker(
            healthy_settings, recovery_database, recovery_writer
        ).run_cycle()
    finally:
        await recovery_writer.close()
        await recovery_database.dispose()
    recovered = await read_outbox(postgresql_sessions, retry_row_id)
    assert recovered.state == "delivered" and recovered.attempt_count == 2


@pytest.mark.asyncio
async def test_invalid_token_is_permanent_and_sanitized() -> None:
    item = delivery_item("integer", 13)
    writer = InfluxObservationWriter(integration_settings(token="invalid-integration-token"))
    await writer.initialize()
    try:
        with pytest.raises(DeliveryFailure) as raised:
            await writer.write(item)
    finally:
        await writer.close()
    assert raised.value.code == "http_401" and raised.value.retryable is False
    assert "invalid-integration-token" not in str(raised.value)


@pytest.mark.asyncio
async def test_invalid_token_dead_letters_worker_item(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    item = delivery_item("integer", 14)
    row_id = await seed_outbox(postgresql_sessions, item)
    settings = integration_settings(token="invalid-integration-token")
    database = Database(settings)
    await database.initialize()
    writer = InfluxObservationWriter(settings)
    await writer.initialize()
    try:
        await ObservationDeliveryWorker(settings, database, writer).run_cycle()
    finally:
        await writer.close()
        await database.dispose()
    stored = await read_outbox(postgresql_sessions, row_id)
    assert stored.state == "dead_letter" and stored.last_error_code == "http_401"
    assert stored.last_error_detail == "http_401"
