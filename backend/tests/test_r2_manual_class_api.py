from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import Table, func, select

import app.main as main_module
from app.core.config import get_settings
from app.domain.classes.models import ClassMembership, SavedClassQuery, TelemetryClass
from app.domain.sources.models import Site, TelemetrySource, Tenant
from app.domain.streams.models import (
    ObservationEvidence,
    ObservationOutbox,
    ObservationProcessingTask,
    RawObservationRecord,
    Stream,
)


@dataclass(frozen=True)
class StreamFixture:
    stream_id: UUID
    source_id: UUID
    site_id: UUID


@dataclass(frozen=True)
class OperationalEvidenceFixture:
    raw_observation_id: UUID
    processing_task_id: UUID
    outbox_id: UUID


def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    return TestClient(main_module.create_app())


def sqlite_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'classes.db'}")
    get_settings.cache_clear()
    result = TestClient(main_module.create_app())
    result.__enter__()

    async def tables() -> None:
        application = result.app
        assert isinstance(application, FastAPI)
        async with application.state.database.get_engine().begin() as connection:
            for model in (
                Tenant,
                Site,
                TelemetrySource,
                Stream,
                ObservationEvidence,
                RawObservationRecord,
                ObservationProcessingTask,
                ObservationOutbox,
                TelemetryClass,
                ClassMembership,
                SavedClassQuery,
            ):
                await connection.run_sync(cast(Table, model.__table__).create)

    asyncio.run(tables())
    return result


def tenant_headers(tenant_id: UUID) -> dict[str, str]:
    return {"X-Tenant-ID": str(tenant_id)}


def create_tenant(app: TestClient, key: str) -> UUID:
    async def insert() -> UUID:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.transaction() as session:
            item = Tenant(tenant_key=key, display_name=key)
            session.add(item)
            await session.flush()
            return item.id

    return asyncio.run(insert())


def create_class(
    app: TestClient, tenant_id: UUID, name: str, description: str
) -> dict[str, object]:
    response = app.post(
        "/api/classes",
        headers=tenant_headers(tenant_id),
        json={"name": name, "description": description},
    )
    assert response.status_code == 201
    return dict(response.json())


def create_stream(
    app: TestClient,
    tenant_id: UUID,
    tenant_key: str,
    stream_key: str,
    description: str = "Stream description",
) -> StreamFixture:
    async def insert() -> StreamFixture:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.transaction() as session:
            site = Site(
                tenant_id=tenant_id,
                site_key=f"{stream_key}-site",
                display_name=f"{stream_key} site",
            )
            session.add(site)
            await session.flush()
            source = TelemetrySource(
                tenant_id=tenant_id,
                site_id=site.id,
                source_key=f"{stream_key}-source",
                display_name=f"{stream_key} source",
            )
            session.add(source)
            await session.flush()
            observed_at = datetime.now(UTC)
            stream = Stream(
                stream_key=stream_key,
                source_id=str(source.id),
                topic=f"telemetry/{stream_key}",
                tenant=tenant_key,
                first_observed_at=observed_at,
                last_observed_at=observed_at,
                observation_count=1,
                schema_summary={"description": description},
            )
            session.add(stream)
            await session.flush()
            return StreamFixture(stream.id, source.id, site.id)

    return asyncio.run(insert())


def member_ids(app: TestClient, tenant_id: UUID, class_id: object) -> list[str]:
    response = app.get(f"/api/classes/{class_id}/members", headers=tenant_headers(tenant_id))
    assert response.status_code == 200
    return [str(member["stream_id"]) for member in response.json()["items"]]


def class_detail(app: TestClient, tenant_id: UUID, class_id: object) -> dict[str, object]:
    response = app.get(f"/api/classes/{class_id}", headers=tenant_headers(tenant_id))
    assert response.status_code == 200
    return dict(response.json())


def add_members(
    app: TestClient, tenant_id: UUID, class_id: object, stream_ids: list[UUID]
) -> Response:
    return app.post(
        f"/api/classes/{class_id}/members",
        headers=tenant_headers(tenant_id),
        json={"stream_ids": [str(stream_id) for stream_id in stream_ids]},
    )


def stream_source_site_exist(app: TestClient, stream: StreamFixture) -> bool:
    async def exists() -> bool:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            return all(
                (
                    await session.get(Stream, stream.stream_id),
                    await session.get(TelemetrySource, stream.source_id),
                    await session.get(Site, stream.site_id),
                )
            )

    return asyncio.run(exists())


def create_operational_evidence(
    app: TestClient,
    stream: StreamFixture,
    payload: bytes = b'{"value": 21.5}',
) -> OperationalEvidenceFixture:
    async def insert() -> OperationalEvidenceFixture:
        application = app.app
        assert isinstance(application, FastAPI)
        observed_at = datetime.now(UTC)
        async with application.state.database.transaction() as session:
            evidence = ObservationEvidence(
                stream_id=stream.stream_id,
                received_at=observed_at,
                outcome="accepted",
                payload_size=len(payload),
                content_type="application/json",
                payload_fingerprint="a" * 64,
                broker_metadata={"qos": 1},
            )
            session.add(evidence)
            await session.flush()
            raw_observation = RawObservationRecord(
                observation_key=f"raw-{uuid4()}",
                stream_id=stream.stream_id,
                evidence_id=evidence.id,
                source_id=str(stream.source_id),
                source_type="mqtt",
                external_stream_id=f"stream:{stream.stream_id}",
                received_at=observed_at,
                content_type="application/json",
                payload=payload,
                payload_size=len(payload),
                payload_fingerprint="b" * 64,
                transport_metadata={"qos": 1},
                retention_until=observed_at + timedelta(days=1),
            )
            session.add(raw_observation)
            await session.flush()
            task = ObservationProcessingTask(
                raw_observation_id=raw_observation.id,
                processor_type="schema_observation",
                processor_version="test-v1",
                state="pending",
                attempt_count=0,
                available_at=observed_at,
            )
            outbox = ObservationOutbox(
                delivery_key=f"delivery-{uuid4()}",
                stream_id=stream.stream_id,
                evidence_id=evidence.id,
                state="pending",
                point_payload={"measurement": "telemetry", "value": 21.5},
                attempt_count=0,
                available_at=observed_at,
            )
            session.add_all((task, outbox))
            await session.flush()
            return OperationalEvidenceFixture(raw_observation.id, task.id, outbox.id)

    return asyncio.run(insert())


def class_owned_record_counts(app: TestClient, class_id: UUID) -> tuple[int, int, int]:
    async def counts() -> tuple[int, int, int]:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            telemetry_class_count = cast(
                int,
                await session.scalar(
                    select(func.count())
                    .select_from(TelemetryClass)
                    .where(TelemetryClass.id == class_id)
                ),
            )
            membership_count = cast(
                int,
                await session.scalar(
                    select(func.count())
                    .select_from(ClassMembership)
                    .where(ClassMembership.telemetry_class_id == class_id)
                ),
            )
            query_count = cast(
                int,
                await session.scalar(
                    select(func.count())
                    .select_from(SavedClassQuery)
                    .where(SavedClassQuery.telemetry_class_id == class_id)
                ),
            )
            return telemetry_class_count, membership_count, query_count

    return asyncio.run(counts())


def operational_evidence_records(
    app: TestClient, evidence: OperationalEvidenceFixture
) -> tuple[RawObservationRecord | None, ObservationProcessingTask | None, ObservationOutbox | None]:
    async def records() -> tuple[
        RawObservationRecord | None, ObservationProcessingTask | None, ObservationOutbox | None
    ]:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            return (
                await session.get(RawObservationRecord, evidence.raw_observation_id),
                await session.get(ObservationProcessingTask, evidence.processing_task_id),
                await session.get(ObservationOutbox, evidence.outbox_id),
            )

    return asyncio.run(records())


def membership_record(app: TestClient, class_id: UUID, stream_id: UUID) -> ClassMembership | None:
    async def record() -> ClassMembership | None:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            return cast(
                ClassMembership | None,
                await session.scalar(
                    select(ClassMembership).where(
                        ClassMembership.telemetry_class_id == class_id,
                        ClassMembership.stream_id == stream_id,
                    )
                ),
            )

    return asyncio.run(record())


def valid_query_spec(stream_id: UUID) -> dict[str, object]:
    return {
        "spec_version": "saved-class-query.v1",
        "series": [{"stream_id": str(stream_id), "field_path": '$["temperature"]', "alias": None}],
        "time_window": {"mode": "relative", "lookback_seconds": 3600},
        "aggregation": {"function": "raw", "bucket_seconds": None},
        "live_append": False,
        "visualization": {"kind": "line"},
    }


def create_saved_query(
    app: TestClient,
    tenant_id: UUID,
    class_id: object,
    name: str,
    description: str,
    stream_id: UUID,
) -> Response:
    return app.post(
        f"/api/classes/{class_id}/queries",
        headers=tenant_headers(tenant_id),
        json={
            "name": name,
            "description": description,
            "query_spec": valid_query_spec(stream_id),
        },
    )


def post_query_spec(
    app: TestClient, tenant_id: UUID, class_id: object, query_spec: object
) -> Response:
    return app.post(
        f"/api/classes/{class_id}/queries",
        headers=tenant_headers(tenant_id),
        json={
            "name": "Validation Query",
            "description": "PRIVATE_QUERY_VALIDATION_SECRET_2049",
            "query_spec": query_spec,
        },
    )


def invalid_query_spec(
    case: str, member_stream_id: UUID, non_member_stream_id: UUID, cross_tenant_stream_id: UUID
) -> object:
    if case == "plain_string":
        return "not a query specification"
    if case == "sql_string":
        return "SELECT * FROM telemetry"
    if case == "flux_string":
        return 'from(bucket: "telemetry") |> range(start: -1h)'
    if case == "python_string":
        return '__import__("os").system("echo unsafe")'
    if case == "javascript_string":
        return 'fetch("https://example.invalid")'
    if case == "null":
        return None
    if case == "empty_object":
        return {}

    spec = valid_query_spec(member_stream_id)
    series = cast(list[dict[str, object]], spec["series"])
    series_item = series[0]
    time_window = cast(dict[str, object], spec["time_window"])
    aggregation = cast(dict[str, object], spec["aggregation"])
    visualization = cast(dict[str, object], spec["visualization"])
    if case == "unknown_top_level":
        spec["unexpected"] = True
    elif case == "unsupported_spec_version":
        spec["spec_version"] = "saved-class-query.v2"
    elif case == "empty_series":
        spec["series"] = []
    elif case == "too_many_series":
        spec["series"] = [
            {"stream_id": str(member_stream_id), "field_path": '$["temperature"]'}
            for _ in range(101)
        ]
    elif case == "unknown_series_field":
        series_item["unexpected"] = True
    elif case == "malformed_stream_id":
        series_item["stream_id"] = "not-a-uuid"
    elif case == "unknown_stream_id":
        series_item["stream_id"] = str(uuid4())
    elif case == "non_member_stream":
        series_item["stream_id"] = str(non_member_stream_id)
    elif case == "cross_tenant_stream":
        series_item["stream_id"] = str(cross_tenant_stream_id)
    elif case == "empty_field_path":
        series_item["field_path"] = ""
    elif case == "malformed_field_path":
        series_item["field_path"] = "temperature"
    elif case == "overlong_field_path":
        series_item["field_path"] = '$["' + "x" * 1020 + '"]'
    elif case == "overlong_alias":
        series_item["alias"] = "x" * 121
    elif case == "unknown_time_window_field":
        time_window["unexpected"] = True
    elif case == "unsupported_time_window_mode":
        time_window["mode"] = "absolute"
    elif case == "zero_lookback":
        time_window["lookback_seconds"] = 0
    elif case == "negative_lookback":
        time_window["lookback_seconds"] = -1
    elif case == "overlong_lookback":
        time_window["lookback_seconds"] = 31536001
    elif case == "unknown_aggregation_field":
        aggregation["unexpected"] = True
    elif case == "unsupported_aggregation":
        aggregation["function"] = "median"
    elif case == "raw_aggregation_bucket":
        aggregation["bucket_seconds"] = 1
    elif case == "non_raw_missing_bucket":
        aggregation["function"] = "mean"
        aggregation["bucket_seconds"] = None
    elif case == "non_raw_zero_bucket":
        aggregation["function"] = "mean"
        aggregation["bucket_seconds"] = 0
    elif case == "non_raw_negative_bucket":
        aggregation["function"] = "mean"
        aggregation["bucket_seconds"] = -1
    elif case == "overlong_bucket":
        aggregation["function"] = "mean"
        aggregation["bucket_seconds"] = 86401
    elif case == "unknown_visualization_field":
        visualization["unexpected"] = True
    elif case == "unsupported_visualization":
        visualization["kind"] = "scatter"
    elif case == "invalid_live_append":
        spec["live_append"] = []
    else:
        raise AssertionError(f"unknown invalid query specification case: {case}")
    return spec


def saved_query_record(app: TestClient, query_id: UUID) -> SavedClassQuery | None:
    async def record() -> SavedClassQuery | None:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            return cast(SavedClassQuery | None, await session.get(SavedClassQuery, query_id))

    return asyncio.run(record())


def task_and_outbox_counts(app: TestClient) -> tuple[int, int]:
    async def counts() -> tuple[int, int]:
        application = app.app
        assert isinstance(application, FastAPI)
        async with application.state.database.session() as session:
            task_count = cast(
                int,
                await session.scalar(select(func.count()).select_from(ObservationProcessingTask)),
            )
            outbox_count = cast(
                int,
                await session.scalar(select(func.count()).select_from(ObservationOutbox)),
            )
            return task_count, outbox_count

    return asyncio.run(counts())


def assert_bounded_error(response_text: str) -> None:
    for forbidden in (
        "SQLAlchemy",
        "IntegrityError",
        "UNIQUE constraint",
        "FOREIGN KEY constraint",
        "sqlite://",
        "postgresql://",
        "database connection",
        "<app.",
    ):
        assert forbidden not in response_text


def assert_safe_membership_error(response_text: str) -> None:
    assert_bounded_error(response_text)
    for forbidden in ("PRIVATE_MEMBERSHIP_SECRET_5184", "tenant-b", "Tenant B"):
        assert forbidden not in response_text


def assert_safe_saved_query_error(response_text: str) -> None:
    assert_bounded_error(response_text)
    for forbidden in (
        "PRIVATE_SAVED_QUERY_SECRET_9362",
        "Tenant A Query",
        "tenant-a",
        '$["temperature"]',
    ):
        assert forbidden not in response_text


def assert_safe_validation_error(response: Response) -> None:
    assert len(response.content) < 4096
    assert_safe_saved_query_error(response.text)
    for forbidden in (
        "PRIVATE_QUERY_VALIDATION_SECRET_2049",
        "Traceback",
        "stack trace",
        "SELECT * FROM telemetry",
        'from(bucket: "telemetry")',
        "__import__",
        "fetch(",
    ):
        assert forbidden not in response.text


def assert_invalid_query_request_has_no_persistence(
    app: TestClient, tenant_id: UUID, class_id: object, member_stream: StreamFixture
) -> None:
    assert class_detail(app, tenant_id, class_id)["query_count"] == 0
    listed = app.get(f"/api/classes/{class_id}/queries", headers=tenant_headers(tenant_id))
    assert listed.status_code == 200
    assert listed.json()["items"] == []
    assert member_ids(app, tenant_id, class_id) == [str(member_stream.stream_id)]
    assert stream_source_site_exist(app, member_stream)
    assert task_and_outbox_counts(app) == (0, 0)


def test_create_and_get_class_persists_to_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(
            app, tenant_id, "  Building Temperature  ", "Indoor temperature streams"
        )
        assert created["name"] == "Building Temperature"
        assert created["description"] == "Indoor temperature streams"
        assert created["member_count"] == created["query_count"] == 0
        class_id = UUID(str(created["id"]))
        stored = app.get(f"/api/classes/{class_id}", headers=tenant_headers(tenant_id))
        assert stored.status_code == 200
        assert stored.json()["name"] == "Building Temperature"
        assert stored.json()["description"] == "Indoor temperature streams"
    finally:
        app.__exit__(None, None, None)


def test_update_class_persists_to_database(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Original", "Before")
        updated = app.patch(
            f"/api/classes/{created['id']}",
            headers=tenant_headers(tenant_id),
            json={"name": "  Updated Name  ", "description": "After"},
        )
        assert updated.status_code == 200
        assert updated.json()["name"] == "Updated Name"
        assert updated.json()["description"] == "After"
        assert updated.json()["updated_at"] >= updated.json()["created_at"]
        stored = app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_id))
        assert stored.json()["name"] == "Updated Name" and stored.json()["description"] == "After"
    finally:
        app.__exit__(None, None, None)


def test_delete_class_removes_class(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Delete Me", "Temporary")
        deleted = app.delete(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_id))
        assert deleted.status_code == 204
        missing = app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_id))
        assert missing.status_code == 404
        assert_bounded_error(missing.text)
    finally:
        app.__exit__(None, None, None)


def test_class_name_is_trimmed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "  Trimmed  ", "Description")
        stored = app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_id))
        assert stored.json()["name"] == "Trimmed"
    finally:
        app.__exit__(None, None, None)


def test_normalized_duplicate_class_name_returns_conflict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        create_class(app, tenant_id, "Building Temperature", "Description")
        for name in (" building temperature", "BUILDING TEMPERATURE"):
            response = app.post(
                "/api/classes", headers=tenant_headers(tenant_id), json={"name": name}
            )
            assert response.status_code == 409
            assert_bounded_error(response.text)
        listed = app.get("/api/classes", headers=tenant_headers(tenant_id))
        assert listed.status_code == 200 and len(listed.json()) == 1
    finally:
        app.__exit__(None, None, None)


def test_class_list_is_deterministically_ordered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        for name in ("Zulu", "alpha", "Building Temperature"):
            create_class(app, tenant_id, name, f"{name} description")

        first = app.get("/api/classes", headers=tenant_headers(tenant_id))
        second = app.get("/api/classes", headers=tenant_headers(tenant_id))
        assert first.status_code == second.status_code == 200
        assert [item["name"] for item in first.json()] == [
            "alpha",
            "Building Temperature",
            "Zulu",
        ]
        assert first.json() == second.json()
    finally:
        app.__exit__(None, None, None)


def test_class_list_pagination_is_bounded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        for name in ("Zulu", "alpha", "Building Temperature"):
            create_class(app, tenant_id, name, f"{name} description")

        first = app.get("/api/classes?limit=2&offset=0", headers=tenant_headers(tenant_id))
        second = app.get("/api/classes?limit=2&offset=2", headers=tenant_headers(tenant_id))
        assert first.status_code == second.status_code == 200
        first_items = first.json()
        second_items = second.json()
        assert [item["name"] for item in first_items + second_items] == [
            "alpha",
            "Building Temperature",
            "Zulu",
        ]
        assert {item["id"] for item in first_items}.isdisjoint(
            {item["id"] for item in second_items}
        )

        for query in ("limit=2&offset=-1", "limit=0", "limit=101"):
            invalid = app.get(f"/api/classes?{query}", headers=tenant_headers(tenant_id))
            assert invalid.status_code == 422
            assert_bounded_error(invalid.text)
    finally:
        app.__exit__(None, None, None)


def test_class_list_is_tenant_scoped(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        class_a = create_class(app, tenant_a, "Tenant A Class", "Tenant A private description")
        class_b = create_class(app, tenant_b, "Tenant B Class", "Tenant B private description")

        listed_a = app.get("/api/classes", headers=tenant_headers(tenant_a))
        listed_b = app.get("/api/classes", headers=tenant_headers(tenant_b))
        assert listed_a.status_code == listed_b.status_code == 200
        assert listed_a.json() == [
            {key: class_a[key] for key in ("id", "name", "description", "created_at", "updated_at")}
        ]
        assert listed_b.json() == [
            {key: class_b[key] for key in ("id", "name", "description", "created_at", "updated_at")}
        ]
    finally:
        app.__exit__(None, None, None)


def test_same_normalized_class_name_is_allowed_across_tenants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        class_a = create_class(app, tenant_a, "Building Temperature", "Tenant A")
        class_b = create_class(app, tenant_b, " building temperature ", "Tenant B")
        assert str(class_a["name"]).casefold() == str(class_b["name"]).casefold()

        detail_a = app.get(f"/api/classes/{class_a['id']}", headers=tenant_headers(tenant_a))
        detail_b = app.get(f"/api/classes/{class_b['id']}", headers=tenant_headers(tenant_b))
        listed_a = app.get("/api/classes", headers=tenant_headers(tenant_a))
        listed_b = app.get("/api/classes", headers=tenant_headers(tenant_b))
        assert detail_a.status_code == detail_b.status_code == 200
        assert detail_a.json()["description"] == "Tenant A"
        assert detail_b.json()["description"] == "Tenant B"
        assert [item["id"] for item in listed_a.json()] == [class_a["id"]]
        assert [item["id"] for item in listed_b.json()] == [class_b["id"]]
    finally:
        app.__exit__(None, None, None)


def test_cross_tenant_class_read_returns_not_found(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Tenant A Class", "PRIVATE_TENANT_CLASS_SECRET_6238")

        response = app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_b))
        assert response.status_code == 404
        assert_bounded_error(response.text)
        for forbidden in ("PRIVATE_TENANT_CLASS_SECRET_6238", "Tenant A Class", str(tenant_a)):
            assert forbidden not in response.text
    finally:
        app.__exit__(None, None, None)


def test_cross_tenant_class_update_returns_not_found(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Tenant A Class", "Original description")
        response = app.patch(
            f"/api/classes/{created['id']}",
            headers=tenant_headers(tenant_b),
            json={"name": "Tenant B Mutation", "description": "Mutated"},
        )
        assert response.status_code == 404
        assert_bounded_error(response.text)

        stored = app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_a))
        assert stored.status_code == 200
        assert stored.json()["name"] == "Tenant A Class"
        assert stored.json()["description"] == "Original description"
        assert stored.json()["updated_at"] == created["updated_at"]
    finally:
        app.__exit__(None, None, None)


def test_cross_tenant_class_delete_returns_not_found(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Tenant A Class", "Original description")
        response = app.delete(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_b))
        assert response.status_code == 404
        assert_bounded_error(response.text)

        stored = app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_a))
        assert stored.status_code == 200
        assert stored.json()["name"] == "Tenant A Class"
        assert stored.json()["description"] == "Original description"
    finally:
        app.__exit__(None, None, None)


def test_class_list_and_tenant_errors_are_bounded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Tenant A Class", "PRIVATE_TENANT_CLASS_SECRET_6238")
        responses = (
            app.get("/api/classes?offset=-1", headers=tenant_headers(tenant_a)),
            app.get(f"/api/classes/{created['id']}", headers=tenant_headers(tenant_b)),
        )
        for response in responses:
            assert response.status_code in {404, 422}
            assert_bounded_error(response.text)
            assert "PRIVATE_TENANT_CLASS_SECRET_6238" not in response.text
    finally:
        app.__exit__(None, None, None)


def test_add_single_class_member_persists(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "single-member")

        response = add_members(app, tenant_id, created["id"], [stream.stream_id])
        assert response.status_code == 201
        returned = response.json()
        assert len(returned) == 1
        assert returned[0]["stream_id"] == str(stream.stream_id)
        assert returned[0]["membership_source"] == "manual"
        persisted = membership_record(app, UUID(str(created["id"])), stream.stream_id)
        assert persisted is not None
        assert persisted.telemetry_class_id == UUID(str(created["id"]))
        assert persisted.stream_id == stream.stream_id
        assert persisted.membership_source == "manual"
        assert member_ids(app, tenant_id, created["id"]) == [str(stream.stream_id)]
        detail = class_detail(app, tenant_id, created["id"])
        assert detail["member_count"] == 1
        assert detail["query_count"] == 0
    finally:
        app.__exit__(None, None, None)


def test_add_multiple_class_members_persists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        first = create_stream(app, tenant_id, "tenant-a", "multiple-zulu")
        second = create_stream(app, tenant_id, "tenant-a", "multiple-alpha")

        response = add_members(app, tenant_id, created["id"], [first.stream_id, second.stream_id])
        assert response.status_code == 201
        assert {member["stream_id"] for member in response.json()} == {
            str(first.stream_id),
            str(second.stream_id),
        }
        listed = member_ids(app, tenant_id, created["id"])
        assert set(listed) == {str(first.stream_id), str(second.stream_id)}
        assert len(listed) == len(set(listed)) == 2
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 2
    finally:
        app.__exit__(None, None, None)


def test_class_members_are_deterministically_ordered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        zulu = create_stream(app, tenant_id, "tenant-a", "ordering-zulu")
        alpha = create_stream(app, tenant_id, "tenant-a", "ordering-alpha")
        middle = create_stream(app, tenant_id, "tenant-a", "ordering-middle")
        response = add_members(
            app, tenant_id, created["id"], [zulu.stream_id, alpha.stream_id, middle.stream_id]
        )
        assert response.status_code == 201

        expected = [str(alpha.stream_id), str(middle.stream_id), str(zulu.stream_id)]
        assert member_ids(app, tenant_id, created["id"]) == expected
        assert member_ids(app, tenant_id, created["id"]) == expected
    finally:
        app.__exit__(None, None, None)


def test_duplicate_class_membership_returns_conflict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "duplicate-member")
        assert add_members(app, tenant_id, created["id"], [stream.stream_id]).status_code == 201

        duplicate = add_members(app, tenant_id, created["id"], [stream.stream_id])
        assert duplicate.status_code == 409
        assert_safe_membership_error(duplicate.text)
        persisted = membership_record(app, UUID(str(created["id"])), stream.stream_id)
        assert persisted is not None
        assert member_ids(app, tenant_id, created["id"]) == [str(stream.stream_id)]
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 1
    finally:
        app.__exit__(None, None, None)


def test_remove_class_member_preserves_stream_and_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "remove-member")
        assert add_members(app, tenant_id, created["id"], [stream.stream_id]).status_code == 201

        deleted = app.delete(
            f"/api/classes/{created['id']}/members/{stream.stream_id}",
            headers=tenant_headers(tenant_id),
        )
        assert deleted.status_code == 204
        assert membership_record(app, UUID(str(created["id"])), stream.stream_id) is None
        assert member_ids(app, tenant_id, created["id"]) == []
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 0
        assert stream_source_site_exist(app, stream)

        repeated = app.delete(
            f"/api/classes/{created['id']}/members/{stream.stream_id}",
            headers=tenant_headers(tenant_id),
        )
        assert repeated.status_code == 404
        assert_safe_membership_error(repeated.text)
    finally:
        app.__exit__(None, None, None)


def test_unknown_stream_membership_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        response = add_members(app, tenant_id, created["id"], [uuid4()])
        assert response.status_code == 404
        assert_safe_membership_error(response.text)
        assert member_ids(app, tenant_id, created["id"]) == []
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 0
    finally:
        app.__exit__(None, None, None)


def test_cross_tenant_stream_membership_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Class A", "Description")
        stream_b = create_stream(
            app,
            tenant_b,
            "tenant-b",
            "cross-tenant-member",
            "PRIVATE_MEMBERSHIP_SECRET_5184",
        )
        response = add_members(app, tenant_a, created["id"], [stream_b.stream_id])
        assert response.status_code == 404
        assert_safe_membership_error(response.text)
        assert member_ids(app, tenant_a, created["id"]) == []
        assert class_detail(app, tenant_a, created["id"])["member_count"] == 0
        assert stream_source_site_exist(app, stream_b)
    finally:
        app.__exit__(None, None, None)


def test_multi_member_add_rolls_back_atomically(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        valid = create_stream(app, tenant_id, "tenant-a", "atomic-valid")
        response = add_members(app, tenant_id, created["id"], [valid.stream_id, uuid4()])
        assert response.status_code == 404
        assert_safe_membership_error(response.text)
        assert membership_record(app, UUID(str(created["id"])), valid.stream_id) is None
        assert member_ids(app, tenant_id, created["id"]) == []
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 0
    finally:
        app.__exit__(None, None, None)


def test_membership_endpoints_are_tenant_isolated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Tenant A Class", "Description")
        stream = create_stream(app, tenant_a, "tenant-a", "isolated-member")
        assert add_members(app, tenant_a, created["id"], [stream.stream_id]).status_code == 201

        responses = (
            app.get(f"/api/classes/{created['id']}/members", headers=tenant_headers(tenant_b)),
            add_members(app, tenant_b, created["id"], [stream.stream_id]),
            app.delete(
                f"/api/classes/{created['id']}/members/{stream.stream_id}",
                headers=tenant_headers(tenant_b),
            ),
        )
        for response in responses:
            assert response.status_code == 404
            assert_safe_membership_error(response.text)

        assert member_ids(app, tenant_a, created["id"]) == [str(stream.stream_id)]
        assert class_detail(app, tenant_a, created["id"])["member_count"] == 1
    finally:
        app.__exit__(None, None, None)


def test_class_detail_member_count_tracks_persisted_members(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        created = create_class(app, tenant_id, "Class A", "Description")
        first = create_stream(app, tenant_id, "tenant-a", "count-first")
        second = create_stream(app, tenant_id, "tenant-a", "count-second")
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 0
        assert class_detail(app, tenant_id, created["id"])["query_count"] == 0

        assert (
            add_members(
                app, tenant_id, created["id"], [first.stream_id, second.stream_id]
            ).status_code
            == 201
        )
        detail = class_detail(app, tenant_id, created["id"])
        assert detail["member_count"] == 2 and detail["query_count"] == 0
        for stream in (first, second):
            deleted = app.delete(
                f"/api/classes/{created['id']}/members/{stream.stream_id}",
                headers=tenant_headers(tenant_id),
            )
            assert deleted.status_code == 204
        assert class_detail(app, tenant_id, created["id"])["member_count"] == 0
        assert class_detail(app, tenant_id, created["id"])["query_count"] == 0
    finally:
        app.__exit__(None, None, None)


def test_membership_errors_are_bounded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        created = create_class(app, tenant_a, "Class A", "Description")
        valid = create_stream(app, tenant_a, "tenant-a", "safe-valid")
        private = create_stream(
            app, tenant_b, "tenant-b", "safe-private", "PRIVATE_MEMBERSHIP_SECRET_5184"
        )
        assert add_members(app, tenant_a, created["id"], [valid.stream_id]).status_code == 201

        responses = (
            add_members(app, tenant_a, created["id"], [valid.stream_id]),
            add_members(app, tenant_a, created["id"], [uuid4()]),
            add_members(app, tenant_a, created["id"], [private.stream_id]),
            add_members(app, tenant_a, created["id"], [valid.stream_id, private.stream_id]),
            app.get(f"/api/classes/{uuid4()}/members", headers=tenant_headers(tenant_a)),
        )
        for response in responses:
            assert response.status_code in {404, 409}
            assert_safe_membership_error(response.text)
    finally:
        app.__exit__(None, None, None)


def test_create_and_read_saved_class_query_persists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-create")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )

        created = create_saved_query(
            app,
            tenant_id,
            telemetry_class["id"],
            "Temperature Overview",
            "Saved temperature query",
            stream.stream_id,
        )
        assert created.status_code == 201
        body = created.json()
        query_id = UUID(str(body["id"]))
        assert body["name"] == "Temperature Overview"
        assert body["description"] == "Saved temperature query"
        assert body["spec_version"] == "saved-class-query.v1"
        assert body["query_spec"] == valid_query_spec(stream.stream_id)
        assert body["created_at"] and body["updated_at"]
        persisted = saved_query_record(app, query_id)
        assert persisted is not None
        assert persisted.tenant_id == tenant_id
        assert persisted.telemetry_class_id == UUID(str(telemetry_class["id"]))

        reopened = app.get(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert reopened.status_code == 200
        assert reopened.json() == body
        assert class_detail(app, tenant_id, telemetry_class["id"])["query_count"] == 1
    finally:
        app.__exit__(None, None, None)


def test_saved_class_queries_are_deterministically_ordered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-ordering")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        for name in ("Zulu Query", "alpha query", "Temperature Overview"):
            assert (
                create_saved_query(
                    app, tenant_id, telemetry_class["id"], name, name, stream.stream_id
                ).status_code
                == 201
            )
        other_class = create_class(app, tenant_id, "Class B", "Other class")
        assert add_members(app, tenant_id, other_class["id"], [stream.stream_id]).status_code == 201
        assert (
            create_saved_query(
                app, tenant_id, other_class["id"], "Other Query", "Other", stream.stream_id
            ).status_code
            == 201
        )

        first = app.get(
            f"/api/classes/{telemetry_class['id']}/queries", headers=tenant_headers(tenant_id)
        )
        second = app.get(
            f"/api/classes/{telemetry_class['id']}/queries", headers=tenant_headers(tenant_id)
        )
        assert first.status_code == second.status_code == 200
        assert [item["name"] for item in first.json()["items"]] == [
            "alpha query",
            "Temperature Overview",
            "Zulu Query",
        ]
        assert first.json() == second.json()
    finally:
        app.__exit__(None, None, None)


def test_update_saved_class_query_persists(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        first = create_stream(app, tenant_id, "tenant-a", "query-update-first")
        second = create_stream(app, tenant_id, "tenant-a", "query-update-second")
        assert (
            add_members(
                app, tenant_id, telemetry_class["id"], [first.stream_id, second.stream_id]
            ).status_code
            == 201
        )
        created = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Original Query", "Before", first.stream_id
        )
        assert created.status_code == 201
        query_id = created.json()["id"]

        updated = app.patch(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
            json={
                "name": "  Updated Query  ",
                "description": "After",
                "query_spec": valid_query_spec(second.stream_id),
            },
        )
        assert updated.status_code == 200
        assert updated.json()["name"] == "Updated Query"
        assert updated.json()["description"] == "After"
        assert updated.json()["query_spec"] == valid_query_spec(second.stream_id)
        assert updated.json()["updated_at"] >= updated.json()["created_at"]
        reopened = app.get(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert reopened.status_code == 200
        assert reopened.json() == updated.json()
    finally:
        app.__exit__(None, None, None)


def test_delete_saved_class_query_removes_only_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-delete")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        created = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Delete Query", "Temporary", stream.stream_id
        )
        assert created.status_code == 201
        query_id = UUID(str(created.json()["id"]))

        deleted = app.delete(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert deleted.status_code == 204
        missing = app.get(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert missing.status_code == 404
        assert_safe_saved_query_error(missing.text)
        assert saved_query_record(app, query_id) is None
        assert class_detail(app, tenant_id, telemetry_class["id"])["query_count"] == 0
        assert member_ids(app, tenant_id, telemetry_class["id"]) == [str(stream.stream_id)]
        assert stream_source_site_exist(app, stream)
        repeated = app.delete(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert repeated.status_code == 404
        assert_safe_saved_query_error(repeated.text)
    finally:
        app.__exit__(None, None, None)


def test_normalized_saved_query_name_returns_conflict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-conflict")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        first = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Temperature Overview", "First", stream.stream_id
        )
        assert first.status_code == 201
        for name in (" temperature overview", "TEMPERATURE OVERVIEW"):
            conflict = create_saved_query(
                app, tenant_id, telemetry_class["id"], name, "Conflict", stream.stream_id
            )
            assert conflict.status_code == 409
            assert_safe_saved_query_error(conflict.text)
        other = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Other Query", "Other", stream.stream_id
        )
        assert other.status_code == 201
        rename_conflict = app.patch(
            f"/api/classes/{telemetry_class['id']}/queries/{other.json()['id']}",
            headers=tenant_headers(tenant_id),
            json={"name": " TEMPERATURE OVERVIEW "},
        )
        assert rename_conflict.status_code == 409
        assert_safe_saved_query_error(rename_conflict.text)
        listed = app.get(
            f"/api/classes/{telemetry_class['id']}/queries", headers=tenant_headers(tenant_id)
        )
        assert listed.status_code == 200
        assert len(listed.json()["items"]) == 2
        assert (
            len(
                [
                    item
                    for item in listed.json()["items"]
                    if item["name"].casefold() == "temperature overview"
                ]
            )
            == 1
        )
    finally:
        app.__exit__(None, None, None)


def test_same_saved_query_name_is_allowed_in_different_classes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        first_class = create_class(app, tenant_id, "Class A", "Description")
        second_class = create_class(app, tenant_id, "Class B", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-cross-class")
        for telemetry_class in (first_class, second_class):
            assert (
                add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
                == 201
            )
            assert (
                create_saved_query(
                    app,
                    tenant_id,
                    telemetry_class["id"],
                    "Temperature Overview",
                    "Allowed per class",
                    stream.stream_id,
                ).status_code
                == 201
            )
        assert class_detail(app, tenant_id, first_class["id"])["query_count"] == 1
        assert class_detail(app, tenant_id, second_class["id"])["query_count"] == 1
    finally:
        app.__exit__(None, None, None)


def test_saved_query_requires_current_class_members(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        member = create_stream(app, tenant_id, "tenant-a", "query-member")
        non_member = create_stream(
            app,
            tenant_id,
            "tenant-a",
            "query-non-member",
            "PRIVATE_SAVED_QUERY_SECRET_9362",
        )
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [member.stream_id]).status_code
            == 201
        )
        rejected = create_saved_query(
            app,
            tenant_id,
            telemetry_class["id"],
            "Non-member Query",
            "Rejected",
            non_member.stream_id,
        )
        assert rejected.status_code in {400, 404, 422}
        assert_safe_saved_query_error(rejected.text)
        assert class_detail(app, tenant_id, telemetry_class["id"])["query_count"] == 0
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [non_member.stream_id]).status_code
            == 201
        )
        accepted = create_saved_query(
            app,
            tenant_id,
            telemetry_class["id"],
            "Non-member Query",
            "Accepted after membership",
            non_member.stream_id,
        )
        assert accepted.status_code == 201
    finally:
        app.__exit__(None, None, None)


def test_saved_query_is_scoped_to_requested_class(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        first_class = create_class(app, tenant_id, "Class A", "Description")
        second_class = create_class(app, tenant_id, "Class B", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-ownership")
        assert add_members(app, tenant_id, first_class["id"], [stream.stream_id]).status_code == 201
        created = create_saved_query(
            app, tenant_id, first_class["id"], "Tenant A Query", "Original", stream.stream_id
        )
        assert created.status_code == 201
        query_id = created.json()["id"]
        responses = (
            app.get(
                f"/api/classes/{second_class['id']}/queries/{query_id}",
                headers=tenant_headers(tenant_id),
            ),
            app.patch(
                f"/api/classes/{second_class['id']}/queries/{query_id}",
                headers=tenant_headers(tenant_id),
                json={"description": "Mutated"},
            ),
            app.delete(
                f"/api/classes/{second_class['id']}/queries/{query_id}",
                headers=tenant_headers(tenant_id),
            ),
        )
        for response in responses:
            assert response.status_code == 404
            assert_safe_saved_query_error(response.text)
        stored = app.get(
            f"/api/classes/{first_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert stored.status_code == 200
        assert stored.json()["description"] == "Original"
    finally:
        app.__exit__(None, None, None)


def test_saved_query_endpoints_are_tenant_isolated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        telemetry_class = create_class(app, tenant_a, "Class A", "Description")
        stream = create_stream(
            app,
            tenant_a,
            "tenant-a",
            "query-tenant-isolation",
            "PRIVATE_SAVED_QUERY_SECRET_9362",
        )
        assert (
            add_members(app, tenant_a, telemetry_class["id"], [stream.stream_id]).status_code == 201
        )
        created = create_saved_query(
            app, tenant_a, telemetry_class["id"], "Tenant A Query", "Private", stream.stream_id
        )
        assert created.status_code == 201
        query_id = created.json()["id"]
        responses = (
            app.get(
                f"/api/classes/{telemetry_class['id']}/queries", headers=tenant_headers(tenant_b)
            ),
            create_saved_query(
                app,
                tenant_b,
                telemetry_class["id"],
                "Tenant B Query",
                "Attempt",
                stream.stream_id,
            ),
            app.get(
                f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
                headers=tenant_headers(tenant_b),
            ),
            app.patch(
                f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
                headers=tenant_headers(tenant_b),
                json={"description": "Mutated"},
            ),
            app.delete(
                f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
                headers=tenant_headers(tenant_b),
            ),
        )
        for response in responses:
            assert response.status_code == 404
            assert_safe_saved_query_error(response.text)
        stored = app.get(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_a),
        )
        assert stored.status_code == 200
        assert stored.json()["name"] == "Tenant A Query"
        assert stored.json()["description"] == "Private"
    finally:
        app.__exit__(None, None, None)


def test_saved_query_storage_has_no_task_or_outbox_side_effects(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "query-side-effects")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        assert task_and_outbox_counts(app) == (0, 0)
        created = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Side Effect Query", "Before", stream.stream_id
        )
        assert created.status_code == 201
        query_id = created.json()["id"]
        assert task_and_outbox_counts(app) == (0, 0)
        updated = app.patch(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
            json={"description": "After"},
        )
        assert updated.status_code == 200
        assert task_and_outbox_counts(app) == (0, 0)
        deleted = app.delete(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert deleted.status_code == 204
        assert task_and_outbox_counts(app) == (0, 0)
    finally:
        app.__exit__(None, None, None)


def test_saved_query_errors_are_bounded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        telemetry_class = create_class(app, tenant_a, "Class A", "Description")
        stream = create_stream(
            app,
            tenant_a,
            "tenant-a",
            "query-errors-member",
            "PRIVATE_SAVED_QUERY_SECRET_9362",
        )
        non_member = create_stream(app, tenant_a, "tenant-a", "query-errors-non-member")
        assert (
            add_members(app, tenant_a, telemetry_class["id"], [stream.stream_id]).status_code == 201
        )
        created = create_saved_query(
            app, tenant_a, telemetry_class["id"], "Tenant A Query", "Private", stream.stream_id
        )
        assert created.status_code == 201
        responses = (
            create_saved_query(
                app, tenant_a, telemetry_class["id"], "tenant a query", "Conflict", stream.stream_id
            ),
            create_saved_query(
                app, tenant_a, telemetry_class["id"], "Non-member", "Rejected", non_member.stream_id
            ),
            app.get(
                f"/api/classes/{telemetry_class['id']}/queries/{created.json()['id']}",
                headers=tenant_headers(tenant_b),
            ),
            app.get(
                f"/api/classes/{telemetry_class['id']}/queries/{uuid4()}",
                headers=tenant_headers(tenant_a),
            ),
        )
        for response in responses:
            assert response.status_code in {400, 404, 409, 422}
            assert_safe_saved_query_error(response.text)
    finally:
        app.__exit__(None, None, None)


@pytest.mark.parametrize(
    "case",
    (
        "plain_string",
        "sql_string",
        "flux_string",
        "null",
        "empty_object",
        "unknown_top_level",
        "unsupported_spec_version",
        "empty_series",
        "too_many_series",
        "unknown_series_field",
        "malformed_stream_id",
        "unknown_stream_id",
        "non_member_stream",
        "cross_tenant_stream",
        "empty_field_path",
        "malformed_field_path",
        "overlong_field_path",
        "overlong_alias",
        "unknown_time_window_field",
        "unsupported_time_window_mode",
        "zero_lookback",
        "negative_lookback",
        "overlong_lookback",
        "unknown_aggregation_field",
        "unsupported_aggregation",
        "raw_aggregation_bucket",
        "non_raw_missing_bucket",
        "non_raw_zero_bucket",
        "non_raw_negative_bucket",
        "overlong_bucket",
        "unknown_visualization_field",
        "unsupported_visualization",
        "invalid_live_append",
    ),
)
def test_invalid_saved_query_specs_are_rejected_without_persistence(
    case: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        telemetry_class = create_class(app, tenant_a, "Class A", "Description")
        member = create_stream(app, tenant_a, "tenant-a", "validation-member")
        non_member = create_stream(app, tenant_a, "tenant-a", "validation-non-member")
        cross_tenant = create_stream(app, tenant_b, "tenant-b", "validation-cross-tenant")
        assert (
            add_members(app, tenant_a, telemetry_class["id"], [member.stream_id]).status_code == 201
        )

        response = post_query_spec(
            app,
            tenant_a,
            telemetry_class["id"],
            invalid_query_spec(
                case, member.stream_id, non_member.stream_id, cross_tenant.stream_id
            ),
        )
        expected_statuses = (
            {404}
            if case
            in {
                "unknown_stream_id",
                "non_member_stream",
                "cross_tenant_stream",
            }
            else {422}
        )
        assert response.status_code in expected_statuses
        if response.status_code == 422:
            assert "query_spec" in response.text
        assert_safe_validation_error(response)
        assert_invalid_query_request_has_no_persistence(
            app, tenant_a, telemetry_class["id"], member
        )
        assert stream_source_site_exist(app, non_member)
        assert stream_source_site_exist(app, cross_tenant)
    finally:
        app.__exit__(None, None, None)


@pytest.mark.parametrize(
    "case",
    (
        "unsupported_spec_version",
        "empty_series",
        "non_member_stream",
        "unsupported_aggregation",
        "raw_aggregation_bucket",
        "malformed_field_path",
        "unsupported_visualization",
        "unknown_series_field",
    ),
)
def test_invalid_saved_query_updates_preserve_existing_query(
    case: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        member = create_stream(app, tenant_id, "tenant-a", "validation-update-member")
        non_member = create_stream(app, tenant_id, "tenant-a", "validation-update-non-member")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [member.stream_id]).status_code
            == 201
        )
        created = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Original Query", "Original", member.stream_id
        )
        assert created.status_code == 201
        query_id = created.json()["id"]
        original = app.get(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert original.status_code == 200

        response = app.patch(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
            json={
                "query_spec": invalid_query_spec(
                    case, member.stream_id, non_member.stream_id, uuid4()
                )
            },
        )
        assert response.status_code in ({404} if case == "non_member_stream" else {422})
        if response.status_code == 422:
            assert "query_spec" in response.text
        assert_safe_validation_error(response)
        stored = app.get(
            f"/api/classes/{telemetry_class['id']}/queries/{query_id}",
            headers=tenant_headers(tenant_id),
        )
        assert stored.status_code == 200
        assert stored.json() == original.json()
        assert class_detail(app, tenant_id, telemetry_class["id"])["query_count"] == 1
        assert member_ids(app, tenant_id, telemetry_class["id"]) == [str(member.stream_id)]
        assert task_and_outbox_counts(app) == (0, 0)
    finally:
        app.__exit__(None, None, None)


@pytest.mark.parametrize(
    "query_spec",
    (
        "SELECT * FROM telemetry",
        'from(bucket: "telemetry") |> range(start: -1h)',
        '__import__("os").system("echo unsafe")',
        'fetch("https://example.invalid")',
    ),
)
def test_executable_query_language_strings_are_rejected(
    query_spec: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        member = create_stream(app, tenant_id, "tenant-a", "executable-string-member")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [member.stream_id]).status_code
            == 201
        )
        response = post_query_spec(app, tenant_id, telemetry_class["id"], query_spec)
        assert response.status_code == 422
        assert "query_spec" in response.text
        assert_safe_validation_error(response)
        assert_invalid_query_request_has_no_persistence(
            app, tenant_id, telemetry_class["id"], member
        )
    finally:
        app.__exit__(None, None, None)


def test_saved_query_validation_errors_are_bounded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        member = create_stream(app, tenant_id, "tenant-a", "validation-error-member")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [member.stream_id]).status_code
            == 201
        )
        response = post_query_spec(
            app,
            tenant_id,
            telemetry_class["id"],
            invalid_query_spec("unknown_series_field", member.stream_id, uuid4(), uuid4()),
        )
        assert response.status_code == 422
        assert "query_spec" in response.text
        assert_safe_validation_error(response)
        assert_invalid_query_request_has_no_persistence(
            app, tenant_id, telemetry_class["id"], member
        )
    finally:
        app.__exit__(None, None, None)


def test_saved_query_validation_has_no_task_or_outbox_side_effects(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Class A", "Description")
        member = create_stream(app, tenant_id, "tenant-a", "validation-effects-member")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [member.stream_id]).status_code
            == 201
        )
        assert task_and_outbox_counts(app) == (0, 0)
        rejected_create = post_query_spec(
            app, tenant_id, telemetry_class["id"], "not a query specification"
        )
        assert rejected_create.status_code == 422
        assert_safe_validation_error(rejected_create)
        assert task_and_outbox_counts(app) == (0, 0)
        created = create_saved_query(
            app, tenant_id, telemetry_class["id"], "Original Query", "Original", member.stream_id
        )
        assert created.status_code == 201
        rejected_update = app.patch(
            f"/api/classes/{telemetry_class['id']}/queries/{created.json()['id']}",
            headers=tenant_headers(tenant_id),
            json={"query_spec": "not a query specification"},
        )
        assert rejected_update.status_code == 422
        assert_safe_validation_error(rejected_update)
        assert task_and_outbox_counts(app) == (0, 0)
    finally:
        app.__exit__(None, None, None)


def test_delete_class_cascades_memberships_and_saved_queries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Delete Class", "Description")
        first = create_stream(app, tenant_id, "tenant-a", "delete-cascade-first")
        second = create_stream(app, tenant_id, "tenant-a", "delete-cascade-second")
        assert (
            add_members(
                app, tenant_id, telemetry_class["id"], [first.stream_id, second.stream_id]
            ).status_code
            == 201
        )
        for name, stream in (("First Query", first), ("Second Query", second)):
            assert (
                create_saved_query(
                    app, tenant_id, telemetry_class["id"], name, name, stream.stream_id
                ).status_code
                == 201
            )
        class_id = UUID(str(telemetry_class["id"]))
        assert class_owned_record_counts(app, class_id) == (1, 2, 2)
        detail = class_detail(app, tenant_id, telemetry_class["id"])
        assert detail["member_count"] == 2 and detail["query_count"] == 2

        deleted = app.delete(f"/api/classes/{class_id}", headers=tenant_headers(tenant_id))
        assert deleted.status_code == 204
        assert class_owned_record_counts(app, class_id) == (0, 0, 0)
        missing = app.get(f"/api/classes/{class_id}", headers=tenant_headers(tenant_id))
        assert missing.status_code == 404
        assert_bounded_error(missing.text)
    finally:
        app.__exit__(None, None, None)


def test_delete_class_preserves_stream_source_and_raw_observation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Delete Class", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "delete-raw")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        payload = b"PRIVATE_CLASS_DELETE_RAW_SECRET_6148"
        evidence = create_operational_evidence(app, stream, payload)

        deleted = app.delete(
            f"/api/classes/{telemetry_class['id']}", headers=tenant_headers(tenant_id)
        )
        assert deleted.status_code == 204
        raw_observation, _, _ = operational_evidence_records(app, evidence)
        assert raw_observation is not None
        assert raw_observation.stream_id == stream.stream_id
        assert raw_observation.payload == payload
        assert stream_source_site_exist(app, stream)
    finally:
        app.__exit__(None, None, None)


def test_delete_class_preserves_processing_and_outbox_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Delete Class", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "delete-operational")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        evidence = create_operational_evidence(app, stream)

        deleted = app.delete(
            f"/api/classes/{telemetry_class['id']}", headers=tenant_headers(tenant_id)
        )
        assert deleted.status_code == 204
        raw_observation, processing_task, outbox = operational_evidence_records(app, evidence)
        assert raw_observation is not None
        assert processing_task is not None and processing_task.state == "pending"
        assert outbox is not None and outbox.state == "pending"
    finally:
        app.__exit__(None, None, None)


def test_delete_class_does_not_affect_other_classes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        first_class = create_class(app, tenant_id, "Class A", "Description")
        second_class = create_class(app, tenant_id, "Class B", "Description")
        first_stream = create_stream(app, tenant_id, "tenant-a", "delete-class-a")
        second_stream = create_stream(app, tenant_id, "tenant-a", "delete-class-b")
        assert (
            add_members(app, tenant_id, first_class["id"], [first_stream.stream_id]).status_code
            == 201
        )
        assert (
            add_members(app, tenant_id, second_class["id"], [second_stream.stream_id]).status_code
            == 201
        )
        assert (
            create_saved_query(
                app, tenant_id, first_class["id"], "Class A Query", "A", first_stream.stream_id
            ).status_code
            == 201
        )
        assert (
            create_saved_query(
                app, tenant_id, second_class["id"], "Class B Query", "B", second_stream.stream_id
            ).status_code
            == 201
        )

        deleted = app.delete(f"/api/classes/{first_class['id']}", headers=tenant_headers(tenant_id))
        assert deleted.status_code == 204
        assert class_detail(app, tenant_id, second_class["id"])["member_count"] == 1
        assert class_detail(app, tenant_id, second_class["id"])["query_count"] == 1
        assert member_ids(app, tenant_id, second_class["id"]) == [str(second_stream.stream_id)]
        queries = app.get(
            f"/api/classes/{second_class['id']}/queries", headers=tenant_headers(tenant_id)
        )
        assert queries.status_code == 200
        assert [item["name"] for item in queries.json()["items"]] == ["Class B Query"]
    finally:
        app.__exit__(None, None, None)


def test_delete_class_does_not_affect_other_tenants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_a = create_tenant(app, "tenant-a")
        tenant_b = create_tenant(app, "tenant-b")
        class_a = create_class(app, tenant_a, "Class A", "Description")
        class_b = create_class(app, tenant_b, "Class B", "Description")
        stream_a = create_stream(app, tenant_a, "tenant-a", "delete-tenant-a")
        stream_b = create_stream(app, tenant_b, "tenant-b", "delete-tenant-b")
        assert add_members(app, tenant_a, class_a["id"], [stream_a.stream_id]).status_code == 201
        assert add_members(app, tenant_b, class_b["id"], [stream_b.stream_id]).status_code == 201
        assert (
            create_saved_query(
                app, tenant_b, class_b["id"], "Tenant B Query", "B", stream_b.stream_id
            ).status_code
            == 201
        )
        evidence_b = create_operational_evidence(app, stream_b)

        deleted = app.delete(f"/api/classes/{class_a['id']}", headers=tenant_headers(tenant_a))
        assert deleted.status_code == 204
        assert class_detail(app, tenant_b, class_b["id"])["member_count"] == 1
        assert class_detail(app, tenant_b, class_b["id"])["query_count"] == 1
        assert stream_source_site_exist(app, stream_b)
        raw_observation, _, _ = operational_evidence_records(app, evidence_b)
        assert raw_observation is not None
        assert raw_observation.stream_id == stream_b.stream_id
    finally:
        app.__exit__(None, None, None)


def test_class_delete_responses_do_not_leak_raw_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = sqlite_client(monkeypatch, tmp_path)
    try:
        tenant_id = create_tenant(app, "tenant-a")
        telemetry_class = create_class(app, tenant_id, "Delete Class", "Description")
        stream = create_stream(app, tenant_id, "tenant-a", "delete-error-safety")
        assert (
            add_members(app, tenant_id, telemetry_class["id"], [stream.stream_id]).status_code
            == 201
        )
        payload = b"PRIVATE_CLASS_DELETE_RAW_SECRET_6148"
        create_operational_evidence(app, stream, payload)

        deleted = app.delete(
            f"/api/classes/{telemetry_class['id']}", headers=tenant_headers(tenant_id)
        )
        assert deleted.status_code == 204
        missing = app.get(
            f"/api/classes/{telemetry_class['id']}", headers=tenant_headers(tenant_id)
        )
        assert missing.status_code == 404
        assert_bounded_error(missing.text)
        assert "PRIVATE_CLASS_DELETE_RAW_SECRET_6148" not in missing.text
        assert payload.decode() not in missing.text
    finally:
        app.__exit__(None, None, None)


def test_manual_class_routes_are_registered_and_require_tenant_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class_id, stream_id, query_id = uuid4(), uuid4(), uuid4()
    requests = (
        ("get", "/api/classes"),
        ("get", f"/api/classes/{class_id}"),
        ("get", f"/api/classes/{class_id}/members"),
        ("delete", f"/api/classes/{class_id}/members/{stream_id}"),
        ("get", f"/api/classes/{class_id}/queries"),
        ("get", f"/api/classes/{class_id}/queries/{query_id}"),
    )
    with client(monkeypatch) as app:
        for method, path in requests:
            response = getattr(app, method)(path)
            assert response.status_code == 422


def test_manual_class_request_contracts_reject_unknown_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with client(monkeypatch) as app:
        response = app.post(
            "/api/classes",
            headers={"X-Tenant-ID": str(uuid4())},
            json={"name": "Class", "tenant_id": str(uuid4())},
        )
    assert response.status_code == 422
    assert "tenant_id" in response.text
