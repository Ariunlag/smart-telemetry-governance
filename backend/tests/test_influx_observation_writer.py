from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from typing import cast

import pytest

from app.core.config import Settings
from app.services import influx_observation_writer as writer_module
from app.services.influx_observation_writer import (
    DeliveryFailure,
    DeliveryItem,
    InfluxConfigurationFailure,
    InfluxObservationWriter,
    point_mapping,
)


def make_item(*, delivery_key: str = "delivery-1", **overrides: object) -> DeliveryItem:
    point: dict[str, object] = {
        "stream_id": "stream-1",
        "source_id": "source-1",
        "topic": "site/temperature",
        "observation_timestamp": "2026-01-02T03:04:05-05:00",
        "received_timestamp": "2026-01-02T08:04:06Z",
        "timestamp_source": "payload",
        "metric": "temperature",
        "value_type": "integer",
        "value": 42,
        "content_schema_version": "r1.normalized-point.v1",
        "quality_status": "accepted",
        "provenance_reference": "evidence-1",
    }
    point.update(overrides)
    return DeliveryItem(
        id="outbox-1",
        delivery_key=delivery_key,
        point_payload=point,
        attempt_count=1,
        processing_started_at=datetime(2026, 1, 2, tzinfo=UTC),
    )


def enabled_settings() -> Settings:
    return Settings(
        app_env="test",
        database_url="sqlite+aiosqlite:///writer.db",
        influxdb_enabled=True,
        influxdb_url="http://influx.test:8086",
        influxdb_org="test-org",
        influxdb_bucket="test-bucket",
        influxdb_token="test-only-token",
        influxdb_verify_ssl=False,
        influxdb_timeout_ms=1234,
    )


@pytest.mark.parametrize(
    ("value_type", "value", "field"),
    [
        ("integer", 42, "value_integer"),
        ("float", 1.5, "value_float"),
        ("boolean", True, "value_boolean"),
        ("string", "warm", "value_string"),
    ],
)
def test_point_mapping_uses_one_typed_value_field(
    value_type: str, value: object, field: str
) -> None:
    mapped = point_mapping(make_item(value_type=value_type, value=value))

    assert mapped["measurement"] == "telemetry_observation"
    assert mapped["fields"] == {
        field: value,
        "topic": "site/temperature",
        "received_timestamp": "2026-01-02T08:04:06Z",
        "provenance_reference": "evidence-1",
        "delivery_key": "delivery-1",
    }
    assert mapped["tags"] == {
        "stream_id": "stream-1",
        "source_id": "source-1",
        "metric": "temperature",
        "timestamp_source": "payload",
        "quality_status": "accepted",
        "content_schema_version": "r1.normalized-point.v1",
    }
    assert mapped["time"] == datetime(2026, 1, 2, 8, 4, 5, tzinfo=UTC)


def test_boolean_is_not_treated_as_integer() -> None:
    with pytest.raises(DeliveryFailure, match="invalid_point"):
        point_mapping(make_item(value_type="integer", value=True))


def test_optional_tenant_and_unit_are_tags_only_when_present() -> None:
    without_optional = point_mapping(make_item())
    with_optional = point_mapping(make_item(tenant="tenant-1", unit="celsius"))

    assert "tenant" not in cast(dict[str, object], without_optional["tags"])
    assert "unit" not in cast(dict[str, object], without_optional["tags"])
    assert cast(dict[str, object], with_optional["tags"])["tenant"] == "tenant-1"
    assert cast(dict[str, object], with_optional["tags"])["unit"] == "celsius"


def test_point_mapping_is_deterministic_and_excludes_sensitive_tags() -> None:
    item = make_item()
    first = point_mapping(item)
    second = point_mapping(item)
    tags = cast(dict[str, object], first["tags"])

    assert first == second
    assert {
        "topic",
        "delivery_key",
        "evidence_id",
        "payload_fingerprint",
        "broker_metadata",
    }.isdisjoint(tags)
    assert "raw_payload" not in cast(dict[str, object], first["fields"])


@pytest.mark.parametrize(
    "overrides",
    [
        {"content_schema_version": "r2"},
        {"metric": ""},
        {"observation_timestamp": "not-a-timestamp"},
        {"value_type": "array", "value": []},
        {"value_type": "integer", "value": "42"},
        {"value_type": "float", "value": 42},
        {"value_type": "boolean", "value": "true"},
        {"value_type": "string", "value": 42},
        {"value_type": "float", "value": float("inf")},
        {"value_type": "string", "value": {"nested": "value"}},
    ],
)
def test_invalid_points_are_permanent_and_sanitized(overrides: dict[str, object]) -> None:
    item = make_item()
    item.point_payload.update(overrides)
    with pytest.raises(DeliveryFailure) as raised:
        point_mapping(item)

    assert raised.value.code == "invalid_point"
    assert raised.value.retryable is False
    assert "test-only-token" not in str(raised.value)
    assert "site/temperature" not in str(raised.value)


def test_malformed_point_payload_is_permanent() -> None:
    malformed = DeliveryItem(
        id="outbox-1",
        delivery_key="delivery-1",
        point_payload={},
        attempt_count=1,
        processing_started_at=datetime.now(UTC),
    )
    with pytest.raises(DeliveryFailure, match="invalid_point") as raised:
        point_mapping(malformed)
    assert raised.value.retryable is False


class FakePoint:
    def __init__(self, measurement: str) -> None:
        self.measurement = measurement
        self.tags: dict[str, object] = {}
        self.fields: dict[str, object] = {}
        self.timestamp: tuple[datetime, object] | None = None

    def tag(self, key: str, value: object) -> None:
        self.tags[key] = value

    def field(self, key: str, value: object) -> None:
        self.fields[key] = value

    def time(self, value: datetime, precision: object) -> None:
        self.timestamp = (value, precision)


class FakeWriteApi:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, object]] = []

    async def write(self, *, bucket: str, record: object) -> None:
        self.calls.append((bucket, record))
        if self.error is not None:
            raise self.error


class FakeClient:
    def __init__(self, write_api: FakeWriteApi) -> None:
        self._write_api = write_api
        self.closed = 0

    def write_api(self) -> FakeWriteApi:
        return self._write_api

    async def close(self) -> None:
        self.closed += 1


def install_fake_influx_client(
    monkeypatch: pytest.MonkeyPatch, client: FakeClient
) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> FakeClient:
        calls.append(kwargs)
        return client

    package = ModuleType("influxdb_client")
    client_package = ModuleType("influxdb_client.client")
    async_module = ModuleType("influxdb_client.client.influxdb_client_async")
    setattr(async_module, "InfluxDBClientAsync", factory)
    monkeypatch.setitem(sys.modules, "influxdb_client", package)
    monkeypatch.setitem(sys.modules, "influxdb_client.client", client_package)
    monkeypatch.setitem(sys.modules, "influxdb_client.client.influxdb_client_async", async_module)
    return calls


def install_fake_point_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import_module(name: str) -> object:
        if name == "influxdb_client.client.write.point":
            return SimpleNamespace(Point=FakePoint)
        if name == "influxdb_client.domain.write_precision":
            return SimpleNamespace(WritePrecision=SimpleNamespace(NS="ns"))
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(writer_module, "import_module", fake_import_module)


@pytest.mark.asyncio
async def test_disabled_writer_never_creates_a_client() -> None:
    writer = InfluxObservationWriter(Settings())
    await writer.initialize()
    await writer.close()


@pytest.mark.asyncio
async def test_writer_lifecycle_reuses_one_configured_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_api = FakeWriteApi()
    client = FakeClient(write_api)
    calls = install_fake_influx_client(monkeypatch, client)
    install_fake_point_builder(monkeypatch)
    writer = InfluxObservationWriter(enabled_settings())

    await writer.initialize()
    await writer.initialize()
    await writer.write(make_item())
    await writer.write(make_item(delivery_key="delivery-2"))
    await writer.close()
    await writer.close()

    assert len(calls) == 1
    assert calls[0] == {
        "url": "http://influx.test:8086",
        "token": "test-only-token",
        "org": "test-org",
        "timeout": 1234,
        "verify_ssl": False,
    }
    assert len(write_api.calls) == 2
    assert all(bucket == "test-bucket" for bucket, _ in write_api.calls)
    assert client.closed == 1
    first_point = cast(FakePoint, write_api.calls[0][1])
    second_point = cast(FakePoint, write_api.calls[1][1])
    assert first_point.measurement == second_point.measurement == "telemetry_observation"
    assert first_point.tags == second_point.tags
    assert first_point.fields["delivery_key"] != second_point.fields["delivery_key"]


@pytest.mark.asyncio
async def test_write_before_initialization_and_missing_token_are_sanitized() -> None:
    writer = InfluxObservationWriter(Settings())
    with pytest.raises(DeliveryFailure, match="configuration_error") as not_initialized:
        await writer.write(make_item())
    assert not_initialized.value.retryable is False

    settings = enabled_settings()
    settings.influxdb_token = None
    with pytest.raises(InfluxConfigurationFailure) as missing_token:
        await InfluxObservationWriter(settings).initialize()
    assert "test-only-token" not in str(missing_token.value)


class HttpFailure(Exception):
    def __init__(self, status: int) -> None:
        super().__init__("response includes token=test-only-token and raw telemetry")
        self.status = status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "code", "retryable"),
    [
        (TimeoutError(), "timeout", True),
        (ConnectionError("network failure"), "network_error", True),
        (OSError("dns failure"), "network_error", True),
        (HttpFailure(408), "http_408", True),
        (HttpFailure(429), "http_429", True),
        (HttpFailure(500), "http_500", True),
        (HttpFailure(502), "http_502", True),
        (HttpFailure(503), "http_503", True),
        (HttpFailure(400), "http_400", False),
        (HttpFailure(401), "http_401", False),
        (HttpFailure(403), "http_403", False),
    ],
)
async def test_write_failure_classification_is_bounded_and_sanitized(
    monkeypatch: pytest.MonkeyPatch, error: Exception, code: str, retryable: bool
) -> None:
    write_api = FakeWriteApi(error)
    client = FakeClient(write_api)
    install_fake_influx_client(monkeypatch, client)
    install_fake_point_builder(monkeypatch)
    writer = InfluxObservationWriter(enabled_settings())
    await writer.initialize()

    with pytest.raises(DeliveryFailure) as raised:
        await writer.write(make_item())

    assert raised.value.code == code
    assert raised.value.retryable is retryable
    assert str(raised.value) == code
    assert "test-only-token" not in str(raised.value)
    assert "raw telemetry" not in str(raised.value)


@pytest.mark.asyncio
async def test_invalid_point_is_permanent_before_network_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_api = FakeWriteApi()
    client = FakeClient(write_api)
    install_fake_influx_client(monkeypatch, client)
    install_fake_point_builder(monkeypatch)
    writer = InfluxObservationWriter(enabled_settings())
    await writer.initialize()

    with pytest.raises(DeliveryFailure, match="invalid_point"):
        await writer.write(make_item(value_type="float", value=float("nan")))
    assert write_api.calls == []
