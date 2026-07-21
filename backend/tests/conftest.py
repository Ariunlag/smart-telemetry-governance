from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture
async def postgresql_engine() -> AsyncGenerator[AsyncEngine, None]:
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL tests")
    engine = create_async_engine(url)
    async with engine.begin() as connection:
        await connection.execute(text("DELETE FROM saved_class_queries"))
        await connection.execute(text("DELETE FROM class_memberships"))
        await connection.execute(text("DELETE FROM telemetry_classes"))
        await connection.execute(text("DELETE FROM schema_observation_records"))
        await connection.execute(text("DELETE FROM schema_drift_events"))
        await connection.execute(text("DELETE FROM observed_fields"))
        await connection.execute(text("DELETE FROM observed_schemas"))
        await connection.execute(text("DELETE FROM observation_processing_tasks"))
        await connection.execute(text("DELETE FROM raw_observations"))
        await connection.execute(text("DELETE FROM ingestion_runs"))
        await connection.execute(text("DELETE FROM mqtt_subscriptions"))
        await connection.execute(text("DELETE FROM telemetry_sources"))
        await connection.execute(text("DELETE FROM sites"))
        await connection.execute(text("DELETE FROM tenants"))
        await connection.execute(text("DELETE FROM observation_outbox"))
        await connection.execute(text("DELETE FROM observation_evidence"))
        await connection.execute(text("DELETE FROM streams"))
    yield engine
    await engine.dispose()


@pytest.fixture
async def postgresql_sessions(
    postgresql_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    async with postgresql_engine.begin() as connection:
        await connection.execute(text("DELETE FROM saved_class_queries"))
        await connection.execute(text("DELETE FROM class_memberships"))
        await connection.execute(text("DELETE FROM telemetry_classes"))
        await connection.execute(text("DELETE FROM schema_observation_records"))
        await connection.execute(text("DELETE FROM schema_drift_events"))
        await connection.execute(text("DELETE FROM observed_fields"))
        await connection.execute(text("DELETE FROM observed_schemas"))
        await connection.execute(text("DELETE FROM observation_processing_tasks"))
        await connection.execute(text("DELETE FROM raw_observations"))
        await connection.execute(text("DELETE FROM ingestion_runs"))
        await connection.execute(text("DELETE FROM mqtt_subscriptions"))
        await connection.execute(text("DELETE FROM telemetry_sources"))
        await connection.execute(text("DELETE FROM sites"))
        await connection.execute(text("DELETE FROM tenants"))
        await connection.execute(text("DELETE FROM observation_outbox"))
        await connection.execute(text("DELETE FROM observation_evidence"))
        await connection.execute(text("DELETE FROM streams"))
    return async_sessionmaker(postgresql_engine, expire_on_commit=False)
