from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.session import Database
from app.domain.streams.models import ObservationEvidence, ObservationOutbox, Stream
from app.services.influx_observation_writer import DeliveryFailure, DeliveryItem, ObservationWriter
from app.services.observation_delivery_worker import ObservationDeliveryWorker
from app.services.observation_outbox_repository import ObservationOutboxRepository

pytestmark = pytest.mark.postgresql


def point_payload(number: int) -> dict[str, object]:
    return {"content_schema_version": "r1.normalized-point.v1", "metric": f"metric-{number}"}


async def seed_outbox(
    sessions: async_sessionmaker[AsyncSession],
    *,
    state: str = "pending",
    available_at: datetime | None = None,
    processing_started_at: datetime | None = None,
    attempt_count: int = 0,
    number: int = 1,
) -> ObservationOutbox:
    now = datetime.now(UTC)
    stream_id, evidence_id, outbox_id = uuid4(), uuid4(), uuid4()
    row = ObservationOutbox(
        id=outbox_id,
        delivery_key=f"delivery-{uuid4()}",
        stream_id=stream_id,
        evidence_id=evidence_id,
        state=state,
        point_payload=point_payload(number),
        attempt_count=attempt_count,
        available_at=available_at or now,
        processing_started_at=processing_started_at,
    )
    async with sessions() as session:
        async with session.begin():
            session.add(
                Stream(
                    id=stream_id,
                    stream_key=f"stream-{uuid4()}",
                    source_id="source",
                    topic="test/topic",
                    first_observed_at=now,
                    last_observed_at=now,
                )
            )
            await session.flush()
            session.add(
                ObservationEvidence(
                    id=evidence_id,
                    stream_id=stream_id,
                    received_at=now,
                    outcome="accepted",
                    payload_size=1,
                    payload_fingerprint="0" * 64,
                )
            )
            await session.flush()
            session.add(row)
    return row


async def read_row(sessions: async_sessionmaker[AsyncSession], row_id: UUID) -> ObservationOutbox:
    async with sessions() as session:
        row = await session.get(ObservationOutbox, row_id)
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_claim_eligibility_stale_recovery_and_batch_limit(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    claimable = [
        await seed_outbox(
            postgresql_sessions, state="pending", available_at=now - timedelta(minutes=4), number=1
        ),
        await seed_outbox(
            postgresql_sessions,
            state="retryable",
            available_at=now - timedelta(minutes=3),
            number=2,
        ),
        await seed_outbox(
            postgresql_sessions,
            state="processing",
            available_at=now - timedelta(minutes=2),
            processing_started_at=now - timedelta(minutes=5),
            number=3,
        ),
    ]
    excluded = [
        await seed_outbox(
            postgresql_sessions, state="pending", available_at=now + timedelta(hours=1)
        ),
        await seed_outbox(
            postgresql_sessions, state="retryable", available_at=now + timedelta(hours=1)
        ),
        await seed_outbox(postgresql_sessions, state="delivered"),
        await seed_outbox(postgresql_sessions, state="dead_letter"),
        await seed_outbox(
            postgresql_sessions,
            state="processing",
            processing_started_at=now - timedelta(seconds=10),
        ),
    ]
    repository = ObservationOutboxRepository()
    async with postgresql_sessions() as session:
        async with session.begin():
            first = await repository.claim(session, 2, 60)
    async with postgresql_sessions() as session:
        async with session.begin():
            second = await repository.claim(session, 2, 60)

    claimed = first + second
    assert [delivery.id for delivery in claimed] == [str(row.id) for row in claimable]
    assert all(delivery.attempt_count == 1 for delivery in claimed)
    for row in claimable:
        stored = await read_row(postgresql_sessions, row.id)
        assert stored.state == "processing"
        assert stored.processing_started_at is not None
        assert stored.attempt_count == 1
        assert stored.delivery_key == row.delivery_key
        assert stored.point_payload == row.point_payload
    for row in excluded:
        stored = await read_row(postgresql_sessions, row.id)
        assert stored.state == row.state
        assert stored.attempt_count == 0


@pytest.mark.asyncio
async def test_concurrent_claimers_skip_locked_rows(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    first, second = await seed_outbox(postgresql_sessions), await seed_outbox(postgresql_sessions)
    repository = ObservationOutboxRepository()
    async with postgresql_sessions() as session_a, postgresql_sessions() as session_b:
        async with session_a.begin():
            claimed_a = await repository.claim(session_a, 1, 60)
            async with session_b.begin():
                claimed_b = await repository.claim(session_b, 1, 60)

    assert {claimed_a[0].id, claimed_b[0].id} == {str(first.id), str(second.id)}
    assert claimed_a[0].id != claimed_b[0].id


@pytest.mark.asyncio
async def test_delivered_retryable_and_dead_letter_finalization(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    repository = ObservationOutboxRepository()
    delivered, retryable, dead_letter = (
        await seed_outbox(postgresql_sessions, number=1),
        await seed_outbox(postgresql_sessions, number=2),
        await seed_outbox(postgresql_sessions, number=3),
    )
    async with postgresql_sessions() as session:
        async with session.begin():
            claims = await repository.claim(session, 3, 60)
    by_id = {delivery.id: delivery for delivery in claims}
    future = datetime.now(UTC) + timedelta(hours=1)
    async with postgresql_sessions() as session:
        async with session.begin():
            assert await repository.finalize(session, by_id[str(delivered.id)], "delivered")
            assert await repository.finalize(
                session, by_id[str(retryable.id)], "retryable", future, "http_429"
            )
            assert await repository.finalize(
                session, by_id[str(dead_letter.id)], "dead_letter", None, "invalid_point"
            )

    stored_delivered = await read_row(postgresql_sessions, delivered.id)
    stored_retryable = await read_row(postgresql_sessions, retryable.id)
    stored_dead_letter = await read_row(postgresql_sessions, dead_letter.id)
    assert stored_delivered.state == "delivered" and stored_delivered.delivered_at is not None
    assert stored_delivered.processing_started_at is None
    assert stored_delivered.last_error_code is None and stored_delivered.last_error_detail is None
    assert stored_retryable.state == "retryable" and stored_retryable.available_at == future
    assert stored_retryable.processing_started_at is None and stored_retryable.delivered_at is None
    assert stored_retryable.last_error_code == stored_retryable.last_error_detail == "http_429"
    assert (
        stored_dead_letter.state == "dead_letter"
        and stored_dead_letter.processing_started_at is None
    )
    assert (
        stored_dead_letter.last_error_code
        == stored_dead_letter.last_error_detail
        == "invalid_point"
    )


@pytest.mark.asyncio
async def test_stale_worker_cannot_overwrite_current_claim(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    old_time = datetime.now(UTC) - timedelta(minutes=5)
    row = await seed_outbox(
        postgresql_sessions,
        state="processing",
        processing_started_at=old_time,
        attempt_count=1,
    )
    stale_claim = DeliveryItem(str(row.id), row.delivery_key, row.point_payload, 1, old_time)
    repository = ObservationOutboxRepository()
    async with postgresql_sessions() as session:
        async with session.begin():
            current_claim = (await repository.claim(session, 1, 60))[0]
    async with postgresql_sessions() as session:
        async with session.begin():
            assert not await repository.finalize(session, stale_claim, "delivered")
            assert not await repository.finalize(
                session, stale_claim, "retryable", datetime.now(UTC), "timeout"
            )
            assert not await repository.finalize(
                session, stale_claim, "dead_letter", None, "invalid_point"
            )
            assert await repository.finalize(session, current_claim, "delivered")
    stored = await read_row(postgresql_sessions, row.id)
    assert stored.state == "delivered" and stored.attempt_count == 2


@pytest.mark.asyncio
async def test_selected_dead_letter_replay_and_status_query(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    selected = await seed_outbox(postgresql_sessions, state="dead_letter", attempt_count=3)
    unselected = await seed_outbox(postgresql_sessions, state="dead_letter", attempt_count=2)
    pending = await seed_outbox(postgresql_sessions, available_at=now - timedelta(minutes=1))
    await seed_outbox(postgresql_sessions, state="delivered")
    stale = await seed_outbox(
        postgresql_sessions,
        state="processing",
        processing_started_at=now - timedelta(minutes=5),
    )
    repository = ObservationOutboxRepository()
    async with postgresql_sessions() as session:
        async with session.begin():
            count = await repository.replay_dead_letters(
                session, [selected.id, uuid4(), pending.id]
            )
    assert count == 1
    replayed = await read_row(postgresql_sessions, selected.id)
    untouched = await read_row(postgresql_sessions, unselected.id)
    unchanged_pending = await read_row(postgresql_sessions, pending.id)
    assert replayed.state == "pending" and replayed.attempt_count == 3
    assert replayed.processing_started_at is None
    assert replayed.last_error_code is None and replayed.last_error_detail is None
    assert (
        replayed.delivery_key == selected.delivery_key
        and replayed.point_payload == selected.point_payload
    )
    assert untouched.state == "dead_letter"
    assert unchanged_pending.state == "pending"
    async with postgresql_sessions() as session:
        counts, stale_count, oldest = await repository.status(session, 60)
    assert counts == {"pending": 2, "processing": 1, "delivered": 1, "dead_letter": 1}
    assert stale_count == 1 and oldest is not None and oldest <= pending.available_at
    assert stale.id != selected.id


@pytest.mark.asyncio
async def test_rollback_restores_claim_and_releases_lock(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    row = await seed_outbox(postgresql_sessions)
    repository = ObservationOutboxRepository()
    with pytest.raises(RuntimeError, match="abort"):
        async with postgresql_sessions() as session:
            async with session.begin():
                claimed = await repository.claim(session, 1, 60)
                assert claimed[0].id == str(row.id)
                raise RuntimeError("abort")
    stored = await read_row(postgresql_sessions, row.id)
    assert stored.state == "pending" and stored.attempt_count == 0
    assert stored.processing_started_at is None
    async with postgresql_sessions() as session:
        async with session.begin():
            claim_after_rollback = await repository.claim(session, 1, 60)
    assert claim_after_rollback[0].id == str(row.id)


class FakeWriter:
    def __init__(self, failures: dict[str, DeliveryFailure] | None = None) -> None:
        self.failures = failures or {}
        self.writes: list[str] = []

    async def initialize(self) -> None:
        return None

    async def write(self, item: DeliveryItem) -> None:
        self.writes.append(item.id)
        if failure := self.failures.get(item.id):
            raise failure

    async def close(self) -> None:
        return None


async def run_worker_cycle(
    sessions: async_sessionmaker[AsyncSession],
    writer: FakeWriter,
    *,
    maximum_attempts: int = 3,
    batch_size: int = 5,
) -> None:
    worker_settings = Settings(
        app_env="test",
        database_url=os.environ["TEST_DATABASE_URL"],
        influxdb_enabled=True,
        influxdb_token="worker-test-token",
        outbox_worker_batch_size=batch_size,
        outbox_max_attempts=maximum_attempts,
        outbox_backoff_base_seconds=60,
    )
    database = Database(worker_settings)
    await database.initialize()
    try:
        await ObservationDeliveryWorker(
            worker_settings, database, cast(ObservationWriter, writer)
        ).run_cycle()
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_worker_cycle_transitions_with_real_postgresql(
    postgresql_sessions: async_sessionmaker[AsyncSession],
) -> None:
    success, retryable, permanent, at_limit = (
        await seed_outbox(postgresql_sessions),
        await seed_outbox(postgresql_sessions),
        await seed_outbox(postgresql_sessions),
        await seed_outbox(postgresql_sessions, attempt_count=2),
    )
    failures = {
        str(retryable.id): DeliveryFailure("http_429", True),
        str(permanent.id): DeliveryFailure("invalid_point", False),
        str(at_limit.id): DeliveryFailure("timeout", True),
    }
    writer = FakeWriter(failures)
    await run_worker_cycle(postgresql_sessions, writer, maximum_attempts=3)

    assert (await read_row(postgresql_sessions, success.id)).state == "delivered"
    retried = await read_row(postgresql_sessions, retryable.id)
    assert retried.state == "retryable" and retried.available_at > datetime.now(UTC)
    assert (await read_row(postgresql_sessions, permanent.id)).state == "dead_letter"
    assert (await read_row(postgresql_sessions, at_limit.id)).state == "dead_letter"
    assert writer.writes == [
        str(success.id),
        str(retryable.id),
        str(permanent.id),
        str(at_limit.id),
    ]
