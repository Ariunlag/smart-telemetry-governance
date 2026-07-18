import pytest

from app.core.config import Settings
from app.services.stream_catalog import StreamCatalogService


@pytest.fixture
def service() -> StreamCatalogService:
    return StreamCatalogService(
        Settings(mqtt_topic_allowlist=["site/+/telemetry/#"], mqtt_max_payload_bytes=4)
    )


@pytest.mark.parametrize(
    ("payload", "content_type", "outcome"),
    [
        (b"{}", "application/json", "accepted"),
        (b"text", "text/plain", "accepted"),
        (b"{", "application/json", "malformed"),
        (b"\xff", "text/plain", "unsupported_encoding"),
        (b"text", "application/xml", "unsupported_encoding"),
        (b"12345", "text/plain", "oversized"),
    ],
)
def test_payload_outcomes(
    service: StreamCatalogService, payload: bytes, content_type: str, outcome: str
) -> None:
    assert service.classify(payload, content_type) == outcome


def test_unauthorized_topic_is_rejected(service: StreamCatalogService) -> None:
    with pytest.raises(PermissionError):
        service.authorize("unapproved/topic")
