from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.core.config import Settings
from app.domain.streams.models import Stream
from app.services.stream_catalog import ObservationCommand, StreamCatalogService


@pytest.fixture
def service() -> StreamCatalogService:
    return StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))


def stream() -> Stream:
    return Stream(
        id=uuid4(),
        stream_key="a" * 64,
        source_id="source",
        topic="site/topic",
        tenant=None,
        first_observed_at=datetime.now(UTC),
        last_observed_at=datetime.now(UTC),
        observation_count=1,
    )


@pytest.mark.parametrize(
    ("payload", "value_type"),
    [
        (b'{"metric":"temperature","value":1}', "integer"),
        (b'{"metric":"temperature","value":1.5}', "float"),
        (b'{"metric":"enabled","value":true}', "boolean"),
        (b'{"metric":"label","value":"ok","unit":"state"}', "string"),
    ],
)
def test_explicit_json_envelope_creates_normalized_point(
    service: StreamCatalogService, payload: bytes, value_type: str
) -> None:
    command = ObservationCommand("source", "site/topic", payload, content_type="application/json")
    point = service._normalized_point(stream(), uuid4(), command, datetime.now(UTC))
    assert point is not None
    assert point.value_type == value_type
    assert point.content_schema_version == "r1.normalized-point.v1"
    assert "payload" not in point.payload()


@pytest.mark.parametrize(
    "payload",
    [
        b'{"metric":"","value":1}',
        b'{"metric":"x","value":null}',
        b'{"metric":"x","value":[]}',
        b'{"metric":"x","value":{}}',
    ],
)
def test_non_normalizable_accepted_json_has_no_point(
    service: StreamCatalogService, payload: bytes
) -> None:
    command = ObservationCommand("source", "site/topic", payload, content_type="application/json")
    assert service._normalized_point(stream(), uuid4(), command, datetime.now(UTC)) is None
