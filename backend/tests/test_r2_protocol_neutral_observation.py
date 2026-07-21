from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

import pytest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.core.contracts import (
    MAX_TRANSPORT_METADATA_ENCODED_BYTES,
    MAX_TRANSPORT_METADATA_ENTRIES,
    MAX_TRANSPORT_METADATA_KEY_LENGTH,
    MAX_TRANSPORT_METADATA_STRING_LENGTH,
    RawObservation,
)
from app.domain.streams.models import ObservationEvidence, ObservationOutbox, Stream
from app.services.stream_catalog import StreamCatalogService


def observation(**overrides: object) -> RawObservation:
    return RawObservation(
        source_id=cast(str, overrides.get("source_id", "source-a")),
        source_type=cast(str, overrides.get("source_type", "mqtt")),
        external_stream_id=cast(str, overrides.get("external_stream_id", "site/one/telemetry")),
        payload=cast(bytes, overrides.get("payload", b'{"metric":"temperature","value":21}')),
        received_at=cast(datetime, overrides.get("received_at", datetime.now(UTC))),
        content_type=cast(str | None, overrides.get("content_type", "application/json")),
        transport_metadata=cast(
            Mapping[str, str | int | float | bool | None],
            overrides.get("transport_metadata", {"qos": 1, "retain": False}),
        ),
    )


def test_raw_observation_is_immutable_and_preserves_bytes() -> None:
    payload = b"\x00source-native\xff"
    metadata = {"mqtt_topic": "site/one", "qos": 1, "retain": False}
    raw = observation(payload=payload, transport_metadata=metadata)
    metadata["qos"] = 2

    assert raw.payload == payload
    assert raw.transport_metadata == {"mqtt_topic": "site/one", "qos": 1, "retain": False}
    assert not any(hasattr(raw, name) for name in ("topic", "qos", "retain", "broker"))
    with pytest.raises(AttributeError):
        setattr(raw, "source_id", "other")
    with pytest.raises(TypeError):
        cast(dict[str, object], raw.transport_metadata)["qos"] = 2


def test_raw_observation_accepts_bounded_transport_scalars() -> None:
    raw = observation(
        transport_metadata={
            "mqtt_topic": "site/one",
            "qos": 1,
            "retain": False,
            "content_encoding": "utf-8",
            "content_length": 24,
            "message_id": None,
            "ratio": 1.5,
        }
    )

    assert raw.transport_metadata["mqtt_topic"] == "site/one"
    assert raw.transport_metadata["qos"] == 1
    assert raw.transport_metadata["retain"] is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_id": ""},
        {"external_stream_id": ""},
        {"received_at": datetime(2026, 1, 1)},
        {"transport_metadata": {"nested": {"mqtt": "only"}}},
        {"transport_metadata": {"item": "x" * 1025}},
    ],
)
def test_raw_observation_rejects_unbounded_or_nonportable_values(
    overrides: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        observation(**overrides)


@pytest.mark.parametrize(
    "key",
    ["authorization", "clientSecret", "API_KEY", "raw_payload", "message-body"],
)
def test_raw_observation_rejects_sensitive_or_raw_content_metadata_keys(key: str) -> None:
    with pytest.raises(ValueError):
        observation(transport_metadata={key: "placeholder"})


@pytest.mark.parametrize(
    "value",
    [{"nested": "value"}, ["value"], b"value", object(), float("nan"), float("inf")],
)
def test_raw_observation_rejects_non_scalar_metadata_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        observation(transport_metadata={"transport_value": value})


def test_raw_observation_rejects_metadata_bounds() -> None:
    with pytest.raises(ValueError):
        observation(
            transport_metadata={
                str(index): index for index in range(MAX_TRANSPORT_METADATA_ENTRIES + 1)
            }
        )
    with pytest.raises(ValueError):
        observation(transport_metadata={"x" * (MAX_TRANSPORT_METADATA_KEY_LENGTH + 1): "value"})
    with pytest.raises(ValueError):
        observation(transport_metadata={"item": "x" * (MAX_TRANSPORT_METADATA_STRING_LENGTH + 1)})
    with pytest.raises(ValueError):
        observation(
            transport_metadata={
                f"item-{index}": "x" * (MAX_TRANSPORT_METADATA_ENCODED_BYTES // 4)
                for index in range(4)
            }
        )


@pytest.mark.asyncio
async def test_raw_observation_preserves_existing_catalog_evidence_and_outbox_flow() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(cast(Table, Stream.__table__).create)
            await connection.run_sync(cast(Table, ObservationEvidence.__table__).create)
            await connection.run_sync(cast(Table, ObservationOutbox.__table__).create)
        async with session_factory.begin() as session:
            await service.record_raw(session, observation())

        async with session_factory() as session:
            stream = await session.scalar(select(Stream))
            evidence = await session.scalar(select(ObservationEvidence))
            outbox = await session.scalar(select(ObservationOutbox))

        assert stream is not None and stream.topic == "site/one/telemetry"
        assert evidence is not None and evidence.broker_metadata == {"qos": 1, "retain": False}
        assert outbox is not None and outbox.point_payload["topic"] == "site/one/telemetry"
    finally:
        await engine.dispose()
