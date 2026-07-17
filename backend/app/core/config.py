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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
