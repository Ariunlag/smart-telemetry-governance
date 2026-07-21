from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from math import isfinite
from typing import Any, Protocol, cast
from uuid import UUID

from app.core.config import Settings

MEASUREMENT = "telemetry_field"
VALUE_FIELDS = (
    "value_boolean",
    "value_integer",
    "value_float",
    "value_string",
)


class InfluxClassQueryError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class InfluxQuerySeries:
    stream_id: UUID
    field_path: str


@dataclass(frozen=True)
class InfluxFieldRecord:
    stream_id: UUID
    field_path: str
    timestamp: datetime
    field_name: str
    value: bool | int | float | str


class AsyncInfluxQueryApi(Protocol):
    async def query(self, *, query: str) -> object: ...


class AsyncInfluxQueryClient(Protocol):
    def query_api(self) -> AsyncInfluxQueryApi: ...

    async def close(self) -> None: ...


class InfluxClassQueryReaderProtocol(Protocol):
    async def read(
        self,
        *,
        series: Sequence[InfluxQuerySeries],
        start: datetime,
        stop: datetime,
        per_series_limit: int,
        total_limit: int,
    ) -> list[InfluxFieldRecord]: ...


def flux_string_literal(value: str) -> str:
    """Encode a Flux string literal without allowing query-fragment interpolation."""

    return json.dumps(value, ensure_ascii=False)


def build_flux_query(
    *,
    bucket: str,
    series: Sequence[InfluxQuerySeries],
    start: datetime,
    stop: datetime,
    per_series_limit: int,
    total_limit: int,
) -> str:
    pair_filters = " or ".join(
        "(r.stream_id == "
        f"{flux_string_literal(str(item.stream_id))} and r.field_path == "
        f"{flux_string_literal(item.field_path)})"
        for item in series
    )
    value_filters = " or ".join(
        f"r._field == {flux_string_literal(value_field)}" for value_field in VALUE_FIELDS
    )
    start_literal = flux_string_literal(_utc_timestamp(start))
    stop_literal = flux_string_literal(_utc_timestamp(stop))
    return "\n".join(
        (
            f"from(bucket: {flux_string_literal(bucket)})",
            f"  |> range(start: time(v: {start_literal}), stop: time(v: {stop_literal}))",
            f"  |> filter(fn: (r) => r._measurement == {flux_string_literal(MEASUREMENT)})",
            f"  |> filter(fn: (r) => {pair_filters})",
            f"  |> filter(fn: (r) => {value_filters})",
            "  |> map(fn: (r) => ({r with _value: string(v: r._value)}))",
            '  |> group(columns: ["stream_id", "field_path"])',
            '  |> sort(columns: ["_time", "_field"], desc: false)',
            f"  |> limit(n: {per_series_limit})",
            "  |> group()",
            '  |> sort(columns: ["_time", "stream_id", "field_path", "_field"], desc: false)',
            f"  |> limit(n: {total_limit})",
        )
    )


class InfluxClassQueryReader:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def read(
        self,
        *,
        series: Sequence[InfluxQuerySeries],
        start: datetime,
        stop: datetime,
        per_series_limit: int,
        total_limit: int,
    ) -> list[InfluxFieldRecord]:
        token = self._settings.influxdb_token
        if not self._settings.influxdb_enabled or not token:
            raise InfluxClassQueryError("influx_not_configured")

        query = build_flux_query(
            bucket=self._settings.influxdb_bucket,
            series=series,
            start=start,
            stop=stop,
            per_series_limit=per_series_limit,
            total_limit=total_limit,
        )
        client: AsyncInfluxQueryClient | None = None
        try:
            client_type: Any = import_module(
                "influxdb_client.client.influxdb_client_async"
            ).InfluxDBClientAsync
            client = cast(
                AsyncInfluxQueryClient,
                client_type(
                    url=self._settings.influxdb_url,
                    token=token,
                    org=self._settings.influxdb_org,
                    timeout=self._settings.influxdb_timeout_ms,
                    verify_ssl=self._settings.influxdb_verify_ssl,
                ),
            )
            result = await client.query_api().query(query=query)
            return _records(result)
        except InfluxClassQueryError:
            raise
        except Exception as error:
            raise InfluxClassQueryError("influx_query_failed") from error
        finally:
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass


def _records(result: object) -> list[InfluxFieldRecord]:
    records: list[InfluxFieldRecord] = []
    if not isinstance(result, Sequence):
        return records
    for table in result:
        table_records = getattr(table, "records", ())
        if not isinstance(table_records, Sequence):
            continue
        for record in table_records:
            values = getattr(record, "values", None)
            if not isinstance(values, Mapping):
                continue
            parsed = _record(cast(Mapping[str, object], values))
            if parsed is not None:
                records.append(parsed)
    return records


def _record(values: Mapping[str, object]) -> InfluxFieldRecord | None:
    stream_value = values.get("stream_id")
    field_path = values.get("field_path")
    timestamp = values.get("_time")
    field_name = values.get("_field")
    value = values.get("_value")
    if not isinstance(stream_value, str) or not isinstance(field_path, str):
        return None
    if not isinstance(field_name, str) or field_name not in VALUE_FIELDS:
        return None
    parsed_timestamp = _timestamp(timestamp)
    if parsed_timestamp is None:
        return None
    parsed_value = _field_value(field_name, value)
    if parsed_value is None:
        return None
    try:
        stream_id = UUID(stream_value)
    except ValueError:
        return None
    return InfluxFieldRecord(stream_id, field_path, parsed_timestamp, field_name, parsed_value)


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value).astimezone(UTC)
    except ValueError:
        return None


def _field_value(field_name: str, value: object) -> bool | int | float | str | None:
    if field_name == "value_boolean":
        if isinstance(value, bool):
            return value
        if value == "true":
            return True
        if value == "false":
            return False
        return None
    if field_name == "value_integer":
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None
    if field_name == "value_float":
        if isinstance(value, int | float) and not isinstance(value, bool):
            parsed = float(value)
        elif isinstance(value, str):
            try:
                parsed = float(value)
            except ValueError:
                return None
        else:
            return None
        return parsed if isfinite(parsed) else None
    if field_name == "value_string" and isinstance(value, str):
        return value
    return None


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
