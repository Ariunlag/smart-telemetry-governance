from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application configuration loaded from environment variables only."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Smart Telemetry Governance"
    app_env: Literal["development", "test", "production"] = "development"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    debug: bool = False
    log_level: str = "INFO"
    database_url: str | None = None
    database_required: bool = False
    mqtt_enabled: bool = False
    mqtt_host: str | None = None
    mqtt_port: int = Field(default=8883, ge=1, le=65535)
    mqtt_source_id: str | None = None
    mqtt_client_id: str = "smart-telemetry-r1"
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls_enabled: bool = True
    mqtt_tls_verify: bool = True
    mqtt_topic_allowlist: list[str] = Field(default_factory=list)
    mqtt_max_payload_bytes: int = Field(default=65536, ge=1, le=1048576)
    evidence_preview_bytes: int = Field(default=512, ge=0, le=4096)
    cors_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ]
    )

    @model_validator(mode="after")
    def validate_required_database(self) -> Settings:
        if self.database_required and self.database_url is None:
            raise ValueError("DATABASE_REQUIRED is true but DATABASE_URL is not configured")
        if self.mqtt_enabled and self.database_url is None:
            raise ValueError("DATABASE_URL must be configured when MQTT_ENABLED is true")
        if self.mqtt_enabled and not self.mqtt_topic_allowlist:
            raise ValueError("MQTT_TOPIC_ALLOWLIST must not be empty when MQTT_ENABLED is true")
        if self.mqtt_enabled and (not self.mqtt_host or not self.mqtt_source_id):
            raise ValueError("MQTT_HOST and MQTT_SOURCE_ID are required when MQTT_ENABLED is true")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
