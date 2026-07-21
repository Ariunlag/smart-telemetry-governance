from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings
from app.core.contracts import (
    MAX_TRANSPORT_METADATA_ENCODED_BYTES,
    MAX_TRANSPORT_METADATA_ENTRIES,
    MAX_TRANSPORT_METADATA_KEY_LENGTH,
    MAX_TRANSPORT_METADATA_STRING_LENGTH,
    RawObservation,
)
from app.domain.streams.models import (
    ObservationEvidence,
    ObservationOutbox,
    ObservationProcessingTask,
    RawObservationRecord,
    Stream,
)
from app.services.stream_catalog import ObservationCommand, StreamCatalogService


def observation(**overrides: object) -> RawObservation:
    return RawObservation(
        source_id=cast(str, overrides.get("source_id", "source-a")),
        source_type=cast(str, overrides.get("source_type", "mqtt")),
        external_stream_id=cast(str, overrides.get("external_stream_id", "site/one/telemetry")),
        payload=cast(bytes, overrides.get("payload", b'{"metric":"temperature","value":21}')),
        received_at=cast(datetime, overrides.get("received_at", datetime.now(UTC))),
        content_type=cast(str | None, overrides.get("content_type", "application/json")),
        transport_metadata=cast(
            Mapping[str, str | int | float | bool | None],
            overrides.get("transport_metadata", {"qos": 1, "retain": False}),
        ),
    )


def test_raw_observation_is_immutable_and_preserves_bytes() -> None:
    payload = b"\x00source-native\xff"
    metadata = {"mqtt_topic": "site/one", "qos": 1, "retain": False}
    raw = observation(payload=payload, transport_metadata=metadata)
    metadata["qos"] = 2

    assert raw.payload == payload
    assert raw.transport_metadata == {"mqtt_topic": "site/one", "qos": 1, "retain": False}
    assert not any(hasattr(raw, name) for name in ("topic", "qos", "retain", "broker"))
    with pytest.raises(AttributeError):
        setattr(raw, "source_id", "other")
    with pytest.raises(TypeError):
        cast(dict[str, object], raw.transport_metadata)["qos"] = 2


def test_raw_observation_accepts_bounded_transport_scalars() -> None:
    raw = observation(
        transport_metadata={
            "mqtt_topic": "site/one",
            "qos": 1,
            "retain": False,
            "content_encoding": "utf-8",
            "content_length": 24,
            "message_id": None,
            "ratio": 1.5,
        }
    )

    assert raw.transport_metadata["mqtt_topic"] == "site/one"
    assert raw.transport_metadata["qos"] == 1
    assert raw.transport_metadata["retain"] is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_id": ""},
        {"external_stream_id": ""},
        {"received_at": datetime(2026, 1, 1)},
        {"transport_metadata": {"nested": {"mqtt": "only"}}},
        {"transport_metadata": {"item": "x" * 1025}},
    ],
)
def test_raw_observation_rejects_unbounded_or_nonportable_values(
    overrides: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        observation(**overrides)


@pytest.mark.parametrize(
    "key",
    ["authorization", "clientSecret", "API_KEY", "raw_payload", "message-body"],
)
def test_raw_observation_rejects_sensitive_or_raw_content_metadata_keys(key: str) -> None:
    with pytest.raises(ValueError):
        observation(transport_metadata={key: "placeholder"})


@pytest.mark.parametrize(
    "value",
    [{"nested": "value"}, ["value"], b"value", object(), float("nan"), float("inf")],
)
def test_raw_observation_rejects_non_scalar_metadata_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        observation(transport_metadata={"transport_value": value})


def test_raw_observation_rejects_metadata_bounds() -> None:
    with pytest.raises(ValueError):
        observation(
            transport_metadata={
                str(index): index for index in range(MAX_TRANSPORT_METADATA_ENTRIES + 1)
            }
        )
    with pytest.raises(ValueError):
        observation(transport_metadata={"x" * (MAX_TRANSPORT_METADATA_KEY_LENGTH + 1): "value"})
    with pytest.raises(ValueError):
        observation(transport_metadata={"item": "x" * (MAX_TRANSPORT_METADATA_STRING_LENGTH + 1)})
    with pytest.raises(ValueError):
        observation(
            transport_metadata={
                f"item-{index}": "x" * (MAX_TRANSPORT_METADATA_ENCODED_BYTES // 4)
                for index in range(4)
            }
        )


@pytest.mark.asyncio
async def test_raw_observation_preserves_existing_catalog_evidence_and_outbox_flow() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(cast(Table, Stream.__table__).create)
            await connection.run_sync(cast(Table, ObservationEvidence.__table__).create)
            await connection.run_sync(cast(Table, ObservationOutbox.__table__).create)
            await connection.run_sync(cast(Table, RawObservationRecord.__table__).create)
            await connection.run_sync(cast(Table, ObservationProcessingTask.__table__).create)
        async with session_factory.begin() as session:
            await service.record_raw(session, observation())

        async with session_factory() as session:
            stream = await session.scalar(select(Stream))
            evidence = await session.scalar(select(ObservationEvidence))
            outbox = await session.scalar(select(ObservationOutbox))
            raw = await session.scalar(select(RawObservationRecord))
            task = await session.scalar(select(ObservationProcessingTask))

        assert stream is not None and stream.topic == "site/one/telemetry"
        assert evidence is not None and evidence.broker_metadata == {"qos": 1, "retain": False}
        assert outbox is not None and outbox.point_payload["topic"] == "site/one/telemetry"
        assert raw is not None and raw.payload == b'{"metric":"temperature","value":21}'
        assert raw.payload_size == len(raw.payload) and raw.evidence_id == evidence.id
        assert task is not None and task.raw_observation_id == raw.id
    finally:
        await engine.dispose()


async def create_catalog_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        for model in (
            Stream,
            ObservationEvidence,
            ObservationOutbox,
            RawObservationRecord,
            ObservationProcessingTask,
        ):
            await connection.run_sync(cast(Table, model.__table__).create)


@pytest.mark.asyncio
async def test_exact_replay_reuses_raw_record_and_schema_task() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    raw = observation(received_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
    service = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    try:
        await create_catalog_tables(engine)
        for _ in range(2):
            async with sessions.begin() as session:
                await service.record_raw(session, raw)
        async with sessions() as session:
            assert len((await session.scalars(select(RawObservationRecord))).all()) == 1
            assert len((await session.scalars(select(ObservationProcessingTask))).all()) == 1
            assert len((await session.scalars(select(ObservationOutbox))).all()) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mqtt_redelivery_within_receive_window_reuses_raw_task() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(
        Settings(mqtt_topic_allowlist=["site/#"], observation_fallback_window_seconds=60)
    )
    try:
        await create_catalog_tables(engine)
        for second in (5, 40):
            async with sessions.begin() as session:
                await service.record_raw(
                    session, observation(received_at=datetime(2026, 1, 2, 3, 4, second, tzinfo=UTC))
                )
        async with sessions() as session:
            assert len((await session.scalars(select(RawObservationRecord))).all()) == 1
            assert len((await session.scalars(select(ObservationProcessingTask))).all()) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_valid_broker_timestamps_within_window_remain_distinct() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(
        Settings(mqtt_topic_allowlist=["site/#"], observation_fallback_window_seconds=60)
    )
    try:
        await create_catalog_tables(engine)
        for second in (5, 40):
            received_at = datetime(2026, 1, 2, 3, 4, second, tzinfo=UTC)
            async with sessions.begin() as session:
                await service.record_raw(
                    session,
                    observation(
                        received_at=received_at,
                        transport_metadata={"timestamp": received_at.isoformat()},
                    ),
                )
        async with sessions() as session:
            assert len((await session.scalars(select(RawObservationRecord))).all()) == 2
            assert len((await session.scalars(select(ObservationProcessingTask))).all()) == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_exact_broker_timestamp_replay_reuses_raw_record_and_task() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(Settings(mqtt_topic_allowlist=["site/#"]))
    received_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    raw = observation(
        received_at=received_at,
        transport_metadata={"timestamp": received_at.isoformat()},
    )
    try:
        await create_catalog_tables(engine)
        for _ in range(2):
            async with sessions.begin() as session:
                await service.record_raw(session, raw)
        async with sessions() as session:
            assert len((await session.scalars(select(RawObservationRecord))).all()) == 1
            assert len((await session.scalars(select(ObservationProcessingTask))).all()) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "content_type", "allowlist", "outcome"),
    [
        (b"{}", "application/json", ["other/#"], "rejected"),
        (b"{", "application/json", ["site/#"], "malformed"),
        (b"\xff", "text/plain", ["site/#"], "unsupported_encoding"),
        (b"x" * 5, "text/plain", ["site/#"], "oversized"),
    ],
)
async def test_negative_outcomes_remain_evidence_only(
    payload: bytes, content_type: str, allowlist: list[str], outcome: str
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(
        Settings(mqtt_topic_allowlist=allowlist, mqtt_max_payload_bytes=4)
    )
    try:
        await create_catalog_tables(engine)
        async with sessions.begin() as session:
            await service.record_raw(
                session, observation(payload=payload, content_type=content_type)
            )
        async with sessions() as session:
            evidence = await session.scalar(select(ObservationEvidence))
            assert evidence is not None and evidence.outcome == outcome
            assert await session.scalar(select(RawObservationRecord)) is None
            assert await session.scalar(select(ObservationProcessingTask)) is None
            assert await session.scalar(select(ObservationOutbox)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_raw_task_failure_rolls_back_all_catalog_state() -> None:
    class FailingTaskService(StreamCatalogService):
        async def _enqueue_schema_observation(
            self,
            session: AsyncSession,
            raw: RawObservationRecord,
            available_at: datetime,
        ) -> None:
            del session, raw, available_at
            raise RuntimeError("task insert failure")

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = FailingTaskService(Settings(mqtt_topic_allowlist=["site/#"]))
    try:
        await create_catalog_tables(engine)
        with pytest.raises(RuntimeError, match="task insert failure"):
            async with sessions.begin() as session:
                await service.record_raw(session, observation())
        async with sessions() as session:
            for model in (
                Stream,
                ObservationEvidence,
                RawObservationRecord,
                ObservationProcessingTask,
                ObservationOutbox,
            ):
                assert await session.scalar(select(model)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_raw_insert_failure_rolls_back_all_catalog_state() -> None:
    class FailingRawService(StreamCatalogService):
        async def _raw_observation(
            self,
            session: AsyncSession,
            stream: Stream,
            evidence: ObservationEvidence,
            command: ObservationCommand,
            fingerprint: str,
            received_at: datetime,
        ) -> RawObservationRecord:
            del session, stream, evidence, command, fingerprint, received_at
            raise RuntimeError("raw insert failure")

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = FailingRawService(Settings(mqtt_topic_allowlist=["site/#"]))
    try:
        await create_catalog_tables(engine)
        with pytest.raises(RuntimeError, match="raw insert failure"):
            async with sessions.begin() as session:
                await service.record_raw(session, observation())
        async with sessions() as session:
            for model in (
                Stream,
                ObservationEvidence,
                RawObservationRecord,
                ObservationProcessingTask,
                ObservationOutbox,
            ):
                assert await session.scalar(select(model)) is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_raw_retention_boundary_uses_received_time() -> None:
    received_at = datetime(2026, 1, 2, tzinfo=UTC)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = StreamCatalogService(
        Settings(mqtt_topic_allowlist=["site/#"], raw_observation_retention_days=7)
    )
    try:
        await create_catalog_tables(engine)
        async with sessions.begin() as session:
            await service.record_raw(session, observation(received_at=received_at))
        async with sessions() as session:
            raw = await session.scalar(select(RawObservationRecord))
        assert raw is not None
        # SQLite does not round-trip timezone offsets; PostgreSQL preserves them.
        assert raw.received_at.replace(tzinfo=UTC) == received_at
        assert raw.retention_until.replace(tzinfo=UTC) == received_at + timedelta(days=7)
        assert Settings().raw_observation_retention_days == 30
    finally:
        await engine.dispose()
