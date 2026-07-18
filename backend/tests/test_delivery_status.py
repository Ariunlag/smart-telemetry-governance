from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import delivery as delivery_route
from app.core.config import Settings


class FakeDatabase:
    def __init__(self, configured: bool = True) -> None:
        self.configured = configured
        self.session_calls = 0

    @asynccontextmanager
    async def session(self) -> AsyncIterator[object]:
        self.session_calls += 1
        yield object()


def make_client(
    settings: Settings,
    database: FakeDatabase | None = None,
    *,
    running: bool = False,
    last_cycle: datetime | None = None,
    last_error: str | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(delivery_route.router)
    app.state.settings = settings
    app.state.database = database or FakeDatabase()
    app.state.delivery_worker = SimpleNamespace(
        running=running, last_successful_cycle=last_cycle, last_error_code=last_error
    )
    return TestClient(app)


def enabled_settings() -> Settings:
    return Settings(
        app_env="test",
        database_url="sqlite+aiosqlite:///status.db",
        influxdb_enabled=True,
        influxdb_token="test-token",
    )


def test_disabled_delivery_skips_database_query(monkeypatch: pytest.MonkeyPatch) -> None:
    database = FakeDatabase()
    with make_client(Settings(), database) as client:
        response = client.get("/api/delivery/status")
    assert response.status_code == 200
    assert response.json()["enabled"] is False
    assert response.json()["worker_running"] is False
    assert database.session_calls == 0


def test_unconfigured_database_is_unavailable() -> None:
    with make_client(Settings(influxdb_enabled=False), FakeDatabase(configured=False)) as client:
        response = client.get("/api/delivery/status")
    assert response.status_code == 200
    assert response.json()["database_available"] is False
    assert response.json()["pending"] == 0


def test_enabled_worker_status_and_repository_values(monkeypatch: pytest.MonkeyPatch) -> None:
    oldest = datetime(2026, 1, 2, tzinfo=UTC)
    last_cycle = datetime(2026, 1, 3, tzinfo=UTC)

    class FakeRepository:
        async def status(
            self, session: object, lease_seconds: int
        ) -> tuple[dict[str, int], int, datetime]:
            del session, lease_seconds
            return (
                {"pending": 1, "processing": 2, "delivered": 3, "retryable": 4, "dead_letter": 5},
                6,
                oldest,
            )

    monkeypatch.setattr(delivery_route, "ObservationOutboxRepository", FakeRepository)
    with make_client(
        enabled_settings(), running=True, last_cycle=last_cycle, last_error="timeout"
    ) as client:
        response = client.get("/api/delivery/status")
    data = response.json()
    assert data["enabled"] is True and data["worker_running"] is True
    assert [
        data[state] for state in ("pending", "processing", "delivered", "retryable", "dead_letter")
    ] == [1, 2, 3, 4, 5]
    assert data["stale_processing_count"] == 6
    assert data["oldest_eligible_available_at"] == oldest.isoformat().replace("+00:00", "Z")
    assert data["batch_size"] == 25 and data["maximum_attempts"] == 5
    assert data["last_successful_cycle"] == last_cycle.isoformat().replace("+00:00", "Z")
    assert data["last_worker_error_code"] == "timeout"


def test_stopped_worker_and_repository_failure_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingRepository:
        async def status(
            self, session: object, lease_seconds: int
        ) -> tuple[dict[str, int], int, datetime | None]:
            del session, lease_seconds
            raise RuntimeError("database failed token=DO_NOT_EXPOSE")

    monkeypatch.setattr(delivery_route, "ObservationOutboxRepository", FailingRepository)
    with make_client(enabled_settings(), running=False) as client:
        response = client.get("/api/delivery/status")
    text = response.text.lower()
    assert response.status_code == 200
    assert response.json()["database_available"] is False
    assert response.json()["worker_running"] is False
    for value in (
        "do_not_expose",
        "token",
        "database failed",
        "sqlite",
        "topic",
        "tenant",
        "delivery_key",
        "evidence",
    ):
        assert value not in text
    assert set(response.json()) == {
        "enabled",
        "database_available",
        "worker_running",
        "pending",
        "processing",
        "delivered",
        "retryable",
        "dead_letter",
        "stale_processing_count",
        "oldest_eligible_available_at",
        "batch_size",
        "maximum_attempts",
        "last_successful_cycle",
        "last_worker_error_code",
    }
