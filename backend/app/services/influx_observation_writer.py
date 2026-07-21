from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from math import isfinite
from typing import Any, Protocol, cast

from app.core.config import Settings


class DeliveryFailure(RuntimeError):
    def __init__(self, code: str, retryable: bool) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class InfluxConfigurationFailure(DeliveryFailure):
    def __init__(self, message: str) -> None:
        super().__init__("configuration_error", False)


@dataclass(frozen=True)
class DeliveryItem:
    id: str
    delivery_key: str
    point_payload: dict[str, object]
    attempt_count: int
    processing_started_at: datetime


class ObservationWriter(Protocol):
    async def initialize(self) -> None: ...
    async def write(self, item: DeliveryItem) -> None: ...
    async def close(self) -> None: ...


class AsyncInfluxWriteApi(Protocol):
    async def write(self, *, bucket: str, record: object) -> None: ...


class AsyncInfluxClient(Protocol):
    def write_api(self) -> AsyncInfluxWriteApi: ...
    async def close(self) -> None: ...


def point_mapping(item: DeliveryItem) -> dict[str, object]:
    schema_version = item.point_payload.get("content_schema_version")
    if schema_version == "r1.normalized-point.v1":
        return _normalized_point_mapping(item)
    if schema_version == "r2.field-point.v1":
        return _field_point_mapping(item)
    raise DeliveryFailure("invalid_point", False)


def _normalized_point_mapping(item: DeliveryItem) -> dict[str, object]:
    point = item.point_payload
    required = {
        "stream_id",
        "source_id",
        "topic",
        "observation_timestamp",
        "received_timestamp",
        "timestamp_source",
        "metric",
        "value_type",
        "value",
        "content_schema_version",
        "quality_status",
        "provenance_reference",
    }
    if (
        not required <= point.keys()
        or not isinstance(point["metric"], str)
        or not point["metric"]
        or point["content_schema_version"] != "r1.normalized-point.v1"
    ):
        raise DeliveryFailure("invalid_point", False)
    try:
        timestamp = datetime.fromisoformat(str(point["observation_timestamp"])).astimezone(UTC)
    except ValueError as error:
        raise DeliveryFailure("invalid_point", False) from error
    value_type = point["value_type"]
    value = point["value"]
    fields: dict[str, object]
    if value_type == "boolean" and isinstance(value, bool):
        fields = {"value_boolean": value}
    elif value_type == "integer" and isinstance(value, int) and not isinstance(value, bool):
        fields = {"value_integer": value}
    elif value_type == "float" and isinstance(value, float) and isfinite(value):
        fields = {"value_float": value}
    elif value_type == "string" and isinstance(value, str):
        fields = {"value_string": value}
    else:
        raise DeliveryFailure("invalid_point", False)
    tags = {
        key: str(point[key])
        for key in (
            "stream_id",
            "source_id",
            "metric",
            "timestamp_source",
            "quality_status",
            "content_schema_version",
        )
    }
    if point.get("tenant") is not None:
        tags["tenant"] = str(point["tenant"])
    if point.get("unit") is not None:
        tags["unit"] = str(point["unit"])
    fields.update(
        {
            "topic": point["topic"],
            "received_timestamp": point["received_timestamp"],
            "provenance_reference": point["provenance_reference"],
            "delivery_key": item.delivery_key,
        }
    )
    return {
        "measurement": "telemetry_observation",
        "tags": tags,
        "fields": fields,
        "time": timestamp,
    }


def _field_point_mapping(item: DeliveryItem) -> dict[str, object]:
    point = item.point_payload
    required = {
        "stream_id",
        "source_id",
        "topic",
        "observation_timestamp",
        "received_timestamp",
        "timestamp_source",
        "field_path",
        "value_type",
        "value",
        "content_schema_version",
        "quality_status",
        "provenance_reference",
    }
    required_strings = required - {"value", "content_schema_version"}
    if (
        not required <= point.keys()
        or point["content_schema_version"] != "r2.field-point.v1"
        or any(not isinstance(point[key], str) or not point[key] for key in required_strings)
    ):
        raise DeliveryFailure("invalid_point", False)
    try:
        parsed_timestamp = datetime.fromisoformat(str(point["observation_timestamp"]))
    except ValueError as error:
        raise DeliveryFailure("invalid_point", False) from error
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise DeliveryFailure("invalid_point", False)
    timestamp = parsed_timestamp.astimezone(UTC)
    value_type = point["value_type"]
    value = point["value"]
    fields: dict[str, object]
    if value_type == "boolean" and isinstance(value, bool):
        fields = {"value_boolean": value}
    elif value_type == "integer" and isinstance(value, int) and not isinstance(value, bool):
        fields = {"value_integer": value}
    elif value_type == "float" and isinstance(value, float) and isfinite(value):
        fields = {"value_float": value}
    elif value_type == "string" and isinstance(value, str):
        fields = {"value_string": value}
    else:
        raise DeliveryFailure("invalid_point", False)
    tags = {
        key: str(point[key])
        for key in (
            "stream_id",
            "source_id",
            "field_path",
            "timestamp_source",
            "quality_status",
            "content_schema_version",
        )
    }
    if point.get("tenant") is not None:
        if not isinstance(point["tenant"], str) or not point["tenant"]:
            raise DeliveryFailure("invalid_point", False)
        tags["tenant"] = point["tenant"]
    fields.update(
        {
            "topic": point["topic"],
            "received_timestamp": point["received_timestamp"],
            "provenance_reference": point["provenance_reference"],
            "delivery_key": item.delivery_key,
        }
    )
    return {
        "measurement": "telemetry_field",
        "tags": tags,
        "fields": fields,
        "time": timestamp,
    }


class InfluxObservationWriter:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: object | None = None

    async def initialize(self) -> None:
        if not self._settings.influxdb_enabled or self._client is not None:
            return
        token = self._settings.influxdb_token
        if not token:
            raise InfluxConfigurationFailure("InfluxDB token is not configured")

        import influxdb_client.client.influxdb_client_async as influx_async

        self._client = influx_async.InfluxDBClientAsync(
            url=self._settings.influxdb_url,
            token=token,
            org=self._settings.influxdb_org,
            timeout=self._settings.influxdb_timeout_ms,
            verify_ssl=self._settings.influxdb_verify_ssl,
        )

    async def write(self, item: DeliveryItem) -> None:
        if self._client is None:
            raise DeliveryFailure("configuration_error", False)
        mapped = point_mapping(item)
        try:
            point_factory: Any = import_module("influxdb_client.client.write.point").Point
            precision_module: Any = import_module("influxdb_client.domain.write_precision")
            write_precision: Any = precision_module.WritePrecision
            point: Any = point_factory(cast(str, mapped["measurement"]))
            for key, value in cast(dict[str, object], mapped["tags"]).items():
                point.tag(key, value)
            for key, value in cast(dict[str, object], mapped["fields"]).items():
                point.field(key, value)
            point.time(cast(datetime, mapped["time"]), write_precision.NS)
            client = cast("AsyncInfluxClient", self._client)
            await client.write_api().write(bucket=self._settings.influxdb_bucket, record=point)
        except DeliveryFailure:
            raise
        except TimeoutError as error:
            raise DeliveryFailure("timeout", True) from error
        except Exception as error:
            raise _classify_write_error(error) from error

    async def close(self) -> None:
        if self._client is not None:
            await cast("AsyncInfluxClient", self._client).close()
        self._client = None


def _classify_write_error(error: Exception) -> DeliveryFailure:
    status_code = getattr(error, "status", getattr(error, "status_code", None))
    if status_code in {400, 401, 403}:
        return DeliveryFailure(f"http_{status_code}", False)
    if status_code in {408, 429, 500, 502, 503}:
        return DeliveryFailure(f"http_{status_code}", True)
    return DeliveryFailure("network_error", True)
