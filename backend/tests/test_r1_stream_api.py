from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.main as main_module
from app.core.config import get_settings
from app.domain.streams.models import Stream


def test_stream_routes_return_503_without_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    with TestClient(main_module.create_app()) as client:
        for path in ("/streams", f"/streams/{uuid4()}"):
            response = client.get(path)
            assert response.status_code == 503
            assert response.json() == {"detail": "database unavailable"}
            response_text = response.text
            assert "DatabaseNotInitializedError" not in response_text
            assert "DATABASE_URL" not in response_text
            assert "postgresql" not in response_text


@pytest.mark.postgresql
def test_stream_list_empty_and_detail_errors(
    postgresql_sessions: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    database_url = postgresql_sessions.kw["bind"].url.render_as_string(hide_password=False)
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("APP_ENV", "test")
    get_settings.cache_clear()
    with TestClient(main_module.create_app()) as client:
        response = client.get("/streams")
        assert response.status_code == 200
        assert response.json() == []
        assert client.get(f"/streams/{uuid4()}").status_code == 404
        assert client.get("/streams/not-a-uuid").status_code == 422


@pytest.mark.postgresql
def test_stream_list_order_pagination_and_detail(
    postgresql_sessions: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)

    async def insert() -> list[Stream]:
        async with postgresql_sessions() as session:
            async with session.begin():
                streams = [
                    Stream(
                        stream_key=f"key-{index}",
                        source_id=f"source-{index}",
                        topic=f"site/{index}",
                        tenant=None if index == 0 else f"tenant-{index}",
                        first_observed_at=now,
                        last_observed_at=now + timedelta(seconds=index),
                        observation_count=index + 1,
                    )
                    for index in range(3)
                ]
                session.add_all(streams)
            return streams

    streams = asyncio.run(insert())
    monkeypatch.setenv(
        "DATABASE_URL", postgresql_sessions.kw["bind"].url.render_as_string(hide_password=False)
    )
    monkeypatch.setenv("APP_ENV", "test")
    get_settings.cache_clear()
    with TestClient(main_module.create_app()) as client:
        response = client.get("/streams")
        assert response.status_code == 200
        data = response.json()
        required_fields = {
            "id",
            "stream_key",
            "source_id",
            "topic",
            "tenant",
            "lifecycle_status",
            "first_observed_at",
            "last_observed_at",
            "observation_count",
            "payload_format",
            "created_at",
            "updated_at",
        }
        assert all(required_fields <= item.keys() for item in data)
        assert all(UUID(item["id"]) for item in data)
        assert all(datetime.fromisoformat(item["first_observed_at"]) for item in data)
        assert all(datetime.fromisoformat(item["last_observed_at"]) for item in data)
        assert all(datetime.fromisoformat(item["created_at"]) for item in data)
        assert all(datetime.fromisoformat(item["updated_at"]) for item in data)
        assert [item["observation_count"] for item in data] == [3, 2, 1]
        assert [item["last_observed_at"] for item in data] == sorted(
            (item["last_observed_at"] for item in data), reverse=True
        )
        assert data[-1]["tenant"] is None
        assert data[0]["tenant"] == "tenant-2"
        prohibited_fields = {
            "payload_preview",
            "payload_fingerprint",
            "broker_metadata",
            "mqtt_hostname",
            "mqtt_username",
            "mqtt_password",
            "database_url",
        }
        assert all(not (prohibited_fields & item.keys()) for item in data)
        assert len(client.get("/streams?limit=1").json()) == 1
        assert len(client.get("/streams?offset=1").json()) == 2
        paginated = client.get("/streams?limit=1&offset=1")
        assert paginated.status_code == 200
        assert [item["id"] for item in paginated.json()] == [str(streams[1].id)]
        for invalid_query in ("limit=0", "limit=-1", "limit=101", "offset=-1"):
            assert client.get(f"/streams?{invalid_query}").status_code == 422
        detail = client.get(f"/streams/{streams[0].id}")
        assert detail.status_code == 200
        detail_data = detail.json()
        assert detail_data["id"] == str(streams[0].id)
        assert required_fields <= detail_data.keys()
        assert datetime.fromisoformat(detail_data["first_observed_at"])
        assert datetime.fromisoformat(detail_data["last_observed_at"])
        assert not (prohibited_fields & detail_data.keys())
