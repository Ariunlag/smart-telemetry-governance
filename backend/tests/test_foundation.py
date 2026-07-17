from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import cast

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.main as main_module
from app.core.config import Settings, get_settings
from app.core.contracts import BaseModule
from app.core.module_registry import ModuleRegistry
from app.db.session import Database, DatabaseNotInitializedError


def create_test_app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_REQUIRED", "false")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    return main_module.create_app()


def build_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    return TestClient(create_test_app(monkeypatch))


def test_health_is_live_and_includes_correlation_id(monkeypatch: pytest.MonkeyPatch) -> None:
    with build_client(monkeypatch) as client:
        response = client.get("/health", headers={"X-Correlation-ID": "test-request"})

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.headers["X-Correlation-ID"] == "test-request"


def test_readiness_is_ready_without_an_optional_database(monkeypatch: pytest.MonkeyPatch) -> None:
    with build_client(monkeypatch) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "not_configured"}


def test_readiness_fails_when_a_required_database_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_REQUIRED", "true")
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///C:/missing/foundation.db")
    get_settings.cache_clear()

    with TestClient(main_module.create_app()) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"


def test_settings_rejects_an_invalid_port() -> None:
    with pytest.raises(ValidationError):
        Settings(app_port=70000)


def test_settings_require_a_database_url_when_database_is_required() -> None:
    with pytest.raises(ValidationError, match="DATABASE_REQUIRED"):
        Settings(database_required=True)


@pytest.mark.asyncio
async def test_session_factory_fails_clearly_before_initialization() -> None:
    database = Database(Settings())

    with pytest.raises(DatabaseNotInitializedError, match="session factory"):
        database.get_session_factory()


@pytest.mark.asyncio
async def test_async_session_acquisition_and_repeated_disposal_are_safe(tmp_path: Path) -> None:
    database = Database(Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'session.db'}"))
    await database.initialize()

    async with database.session() as session:
        result = await session.execute(text("SELECT 1"))
        assert result.scalar_one() == 1

    await database.dispose()
    await database.dispose()
    with pytest.raises(DatabaseNotInitializedError, match="engine"):
        database.get_engine()


@pytest.mark.asyncio
async def test_transaction_commits_only_at_an_explicit_boundary(tmp_path: Path) -> None:
    database = Database(Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'transaction.db'}"))
    await database.initialize()

    async with database.transaction() as session:
        await session.execute(text("CREATE TABLE records (value INTEGER NOT NULL)"))
        await session.execute(text("INSERT INTO records (value) VALUES (1)"))

    async with database.session() as session:
        result = await session.execute(text("SELECT value FROM records"))
        assert result.scalar_one() == 1

    await database.dispose()


class TrackingSession:
    def __init__(self) -> None:
        self.rollback_called = False
        self.close_called = False

    async def rollback(self) -> None:
        self.rollback_called = True

    async def close(self) -> None:
        self.close_called = True


@pytest.mark.asyncio
async def test_session_closes_after_success_and_rolls_back_after_exception() -> None:
    database = Database(Settings())
    successful_session = TrackingSession()
    database._session_factory = cast(async_sessionmaker[AsyncSession], lambda: successful_session)

    async with database.session():
        pass

    assert successful_session.close_called
    assert not successful_session.rollback_called

    failing_session = TrackingSession()
    database._session_factory = cast(async_sessionmaker[AsyncSession], lambda: failing_session)
    with pytest.raises(RuntimeError, match="work failed"):
        async with database.session():
            raise RuntimeError("work failed")

    assert failing_session.rollback_called
    assert failing_session.close_called


class TrackingModule(BaseModule):
    module_id = "tracking"
    version = "0.1.0"

    def __init__(self, fail_start: bool = False) -> None:
        self.fail_start = fail_start
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        if self.fail_start:
            raise RuntimeError("module startup failed")
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def health_check(self) -> bool:
        return self.started


@pytest.mark.asyncio
async def test_module_registry_stops_modules_started_before_a_failure() -> None:
    registry = ModuleRegistry()
    started = TrackingModule()
    failing = TrackingModule(fail_start=True)
    failing.module_id = "failing"
    registry.register(started)
    registry.register(failing)

    with pytest.raises(RuntimeError, match="module startup failed"):
        await registry.start_all()
    await registry.stop_all()

    assert started.stopped


def test_lifespan_stops_modules_on_normal_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_test_app(monkeypatch)
    with TestClient(app):
        module = app.state.module_registry.get("system_status")
        assert module.health_check()

    assert not module.health_check()


def test_lifespan_preserves_startup_error_and_disposes_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingDatabase:
        disposed = False

        def __init__(self, settings: Settings) -> None:
            del settings

        async def initialize(self) -> None:
            return None

        async def dispose(self) -> None:
            RecordingDatabase.disposed = True

    class FailingModule(TrackingModule):
        async def start(self) -> None:
            raise RuntimeError("original startup error")

    monkeypatch.setattr(main_module, "Database", RecordingDatabase)
    monkeypatch.setattr(main_module, "SystemStatusModule", FailingModule)
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()

    with pytest.raises(RuntimeError, match="original startup error"):
        with TestClient(main_module.create_app()):
            pass

    assert RecordingDatabase.disposed


def test_migration_head_upgrade_downgrade_and_reupgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "migration.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{database_path}")
    monkeypatch.setenv("DATABASE_REQUIRED", "false")
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option("script_location", str(Path(__file__).parents[1] / "migrations"))
    script = ScriptDirectory.from_config(config)

    assert script.get_heads() == ["c916a10cc59c"]
    command.upgrade(config, "head")
    with sqlite3.connect(database_path) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    assert tables == [("alembic_version",)]
    command.downgrade(config, "base")
    command.upgrade(config, "head")
