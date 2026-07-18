import pytest
from pydantic import ValidationError

from app.core.config import Settings


@pytest.mark.parametrize(
    "values",
    [
        {
            "mqtt_enabled": True,
            "mqtt_host": "broker",
            "mqtt_source_id": "source",
            "mqtt_topic_allowlist": ["a/#"],
        },
        {
            "mqtt_enabled": True,
            "database_url": "sqlite+aiosqlite:///test.db",
            "mqtt_source_id": "source",
            "mqtt_topic_allowlist": ["a/#"],
        },
        {
            "mqtt_enabled": True,
            "database_url": "sqlite+aiosqlite:///test.db",
            "mqtt_host": "broker",
            "mqtt_topic_allowlist": ["a/#"],
        },
        {
            "mqtt_enabled": True,
            "database_url": "sqlite+aiosqlite:///test.db",
            "mqtt_host": "broker",
            "mqtt_source_id": "source",
        },
    ],
)
def test_enabled_mqtt_requires_safe_configuration(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate(values)


def test_enabled_mqtt_accepts_complete_configuration() -> None:
    settings = Settings(
        mqtt_enabled=True,
        database_url="sqlite+aiosqlite:///test.db",
        mqtt_host="broker",
        mqtt_source_id="source",
        mqtt_topic_allowlist=["a/#"],
    )
    assert settings.mqtt_enabled
