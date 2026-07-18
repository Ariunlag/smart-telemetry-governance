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
    observation_future_skew_seconds: int = Field(default=300, ge=0, le=86400)
    observation_fallback_window_seconds: int = Field(default=60, ge=1, le=3600)
    influxdb_enabled: bool = False
    influxdb_url: str = "http://localhost:8086"
    influxdb_org: str = "smarttelemetry"
    influxdb_bucket: str = "telemetry"
    influxdb_token: str | None = None
    influxdb_verify_ssl: bool = True
    influxdb_timeout_ms: int = Field(default=10000, ge=100, le=120000)
    outbox_worker_poll_interval_ms: int = Field(default=1000, ge=100, le=60000)
    outbox_worker_batch_size: int = Field(default=25, ge=1, le=500)
    outbox_processing_lease_seconds: int = Field(default=60, ge=1, le=3600)
    outbox_max_attempts: int = Field(default=5, ge=1, le=100)
    outbox_backoff_base_seconds: int = Field(default=5, ge=1, le=3600)
    outbox_backoff_max_seconds: int = Field(default=300, ge=1, le=86400)
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
        if self.outbox_backoff_max_seconds < self.outbox_backoff_base_seconds:
            raise ValueError(
                "OUTBOX_BACKOFF_MAX_SECONDS must be at least OUTBOX_BACKOFF_BASE_SECONDS"
            )
        if self.influxdb_enabled:
            if (
                not self.database_url
                or not self.influxdb_url
                or not self.influxdb_org
                or not self.influxdb_bucket
                or not self.influxdb_token
            ):
                raise ValueError(
                    "InfluxDB delivery requires database, URL, organization, bucket, and token"
                )
            if self.app_env not in {"development", "test"} and (
                not self.influxdb_url.startswith("https://") or not self.influxdb_verify_ssl
            ):
                raise ValueError(
                    "Production InfluxDB delivery requires HTTPS with TLS verification"
                )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
