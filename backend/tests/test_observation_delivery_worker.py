from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast

import pytest

from app.core.config import Settings
from app.db.session import Database
from app.services.influx_observation_writer import DeliveryFailure, DeliveryItem, ObservationWriter
from app.services.observation_delivery_worker import ObservationDeliveryWorker, backoff
from app.services.observation_outbox_repository import ObservationOutboxRepository


def settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "test",
        "database_url": "sqlite+aiosqlite:///worker.db",
        "influxdb_enabled": True,
        "influxdb_token": "worker-test-token",
        "outbox_worker_poll_interval_ms": 1000,
        "outbox_worker_batch_size": 2,
        "outbox_max_attempts": 3,
        "outbox_backoff_base_seconds": 5,
        "outbox_backoff_max_seconds": 20,
    }
    values.update(overrides)
    return Settings.model_validate(values)


def item(number: int, attempt_count: int = 1) -> DeliveryItem:
    return DeliveryItem(
        id=f"outbox-{number}",
        delivery_key=f"delivery-{number}",
        point_payload={"topic": "must-not-leak", "tenant": "must-not-leak"},
        attempt_count=attempt_count,
        processing_started_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )


class FakeSession:
    pass


class FakeDatabase:
    def __init__(self) -> None:
        self.in_transaction = False
        self.transactions: list[str] = []

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[FakeSession]:
        assert not self.in_transaction
        self.in_transaction = True
        self.transactions.append("open")
        try:
            yield FakeSession()
        finally:
            self.in_transaction = False
            self.transactions.append("closed")


class FakeWriter:
    def __init__(
        self, database: FakeDatabase, failures: dict[str, Exception] | None = None
    ) -> None:
        self.database = database
        self.failures = failures or {}
        self.writes: list[DeliveryItem] = []

    async def initialize(self) -> None:
        return None

    async def write(self, delivery: DeliveryItem) -> None:
        assert self.database.in_transaction is False
        self.writes.append(delivery)
        failure = self.failures.get(delivery.id)
        if failure:
            raise failure

    async def close(self) -> None:
        return None


class FakeRepository:
    def __init__(
        self,
        database: FakeDatabase,
        batches: list[list[DeliveryItem]] | None = None,
        *,
        claim_error: Exception | None = None,
        finalize_failures: dict[str, Exception] | None = None,
        lost_ids: set[str] | None = None,
    ) -> None:
        self.database = database
        self.batches: deque[list[DeliveryItem]] = deque(batches or [[]])
        self.claim_error = claim_error
        self.finalize_failures = finalize_failures or {}
        self.lost_ids = lost_ids or set()
        self.claim_limits: list[int] = []
        self.finalizations: list[tuple[DeliveryItem, str, datetime | None, str | None]] = []
        self.claimed = asyncio.Event()
        self.finalized = asyncio.Event()

    async def claim(
        self, session: FakeSession, limit: int, lease_seconds: int
    ) -> list[DeliveryItem]:
        del session, lease_seconds
        assert self.database.in_transaction
        self.claim_limits.append(limit)
        self.claimed.set()
        if self.claim_error:
            raise self.claim_error
        return self.batches.popleft() if self.batches else []

    async def finalize(
        self,
        session: FakeSession,
        delivery: DeliveryItem,
        state: str,
        available_at: datetime | None,
        code: str | None,
    ) -> bool:
        del session
        assert self.database.in_transaction
        failure = self.finalize_failures.get(delivery.id)
        if failure:
            raise failure
        self.finalizations.append((delivery, state, available_at, code))
        self.finalized.set()
        return delivery.id not in self.lost_ids


def worker(
    database: FakeDatabase,
    repository: FakeRepository,
    writer: FakeWriter,
    worker_settings: Settings | None = None,
) -> ObservationDeliveryWorker:
    return ObservationDeliveryWorker(
        worker_settings or settings(),
        cast(Database, database),
        cast(ObservationWriter, writer),
        cast(ObservationOutboxRepository, repository),
    )


async def wait_for(event: asyncio.Event) -> None:
    await asyncio.wait_for(event.wait(), timeout=1)


@pytest.mark.asyncio
async def test_disabled_start_and_stop_are_safe() -> None:
    database = FakeDatabase()
    repository = FakeRepository(database)
    fake_writer = FakeWriter(database)
    delivery_worker = worker(database, repository, fake_writer, Settings())

    await delivery_worker.start()
    await delivery_worker.stop()
    await delivery_worker.stop()

    assert delivery_worker.running is False
    assert delivery_worker._task is None
    assert repository.claim_limits == []


@pytest.mark.asyncio
async def test_start_is_idempotent_and_stop_waits_for_interruptible_poll() -> None:
    database = FakeDatabase()
    repository = FakeRepository(database)
    fake_writer = FakeWriter(database)
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    first_task = delivery_worker._task
    await delivery_worker.start()
    await wait_for(repository.claimed)
    await delivery_worker.stop()

    assert first_task is not None and first_task.done()
    assert delivery_worker._task is None
    assert delivery_worker.running is False
    assert repository.claim_limits == [2]


@pytest.mark.asyncio
async def test_successful_delivery_uses_separate_transactions_and_preserves_order() -> None:
    database = FakeDatabase()
    deliveries = [item(1), item(2)]
    repository = FakeRepository(database, [deliveries])
    fake_writer = FakeWriter(database)
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.finalized)
    await delivery_worker.stop()

    assert fake_writer.writes == deliveries
    assert repository.claim_limits == [2]
    assert [record[1] for record in repository.finalizations] == ["delivered", "delivered"]
    assert [record[0].attempt_count for record in repository.finalizations] == [1, 1]
    assert all(record[2] is None and record[3] is None for record in repository.finalizations)
    assert database.transactions == ["open", "closed", "open", "closed", "open", "closed"]
    assert delivery_worker.last_successful_cycle is not None


@pytest.mark.asyncio
async def test_empty_claim_batch_does_not_mark_a_successful_cycle() -> None:
    database = FakeDatabase()
    repository = FakeRepository(database, [[]])
    fake_writer = FakeWriter(database)
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.claimed)
    await delivery_worker.stop()

    assert fake_writer.writes == []
    assert repository.finalizations == []
    assert delivery_worker.last_successful_cycle is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        DeliveryFailure("timeout", True),
        DeliveryFailure("network_error", True),
        DeliveryFailure("http_429", True),
        DeliveryFailure("http_503", True),
    ],
)
async def test_retryable_failures_schedule_bounded_backoff(failure: DeliveryFailure) -> None:
    database = FakeDatabase()
    delivery = item(1, attempt_count=2)
    repository = FakeRepository(database, [[delivery]])
    fake_writer = FakeWriter(database, {delivery.id: failure})
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.finalized)
    await delivery_worker.stop()

    finalized = repository.finalizations[0]
    assert finalized[0] == delivery
    assert finalized[1] == "retryable"
    assert finalized[2] is not None
    assert finalized[2] >= datetime.now(UTC).replace(microsecond=0)
    assert finalized[3] == failure.code
    assert "worker-test-token" not in (finalized[3] or "")


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt_count", [3, 4])
async def test_maximum_attempts_dead_letter_without_retry(attempt_count: int) -> None:
    database = FakeDatabase()
    delivery = item(1, attempt_count=attempt_count)
    repository = FakeRepository(database, [[delivery]])
    fake_writer = FakeWriter(database, {delivery.id: DeliveryFailure("timeout", True)})
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.finalized)
    await delivery_worker.stop()

    assert repository.finalizations[0][1:] == ("dead_letter", None, "timeout")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        DeliveryFailure("invalid_point", False),
        DeliveryFailure("http_401", False),
        DeliveryFailure("http_403", False),
        DeliveryFailure("configuration_error", False),
    ],
)
async def test_permanent_failures_dead_letter_and_continue(failure: DeliveryFailure) -> None:
    database = FakeDatabase()
    failed, succeeded = item(1), item(2)
    repository = FakeRepository(database, [[failed, succeeded]])
    fake_writer = FakeWriter(database, {failed.id: failure})
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.finalized)
    await delivery_worker.stop()

    assert fake_writer.writes == [failed, succeeded]
    assert [(record[1], record[2], record[3]) for record in repository.finalizations] == [
        ("dead_letter", None, failure.code),
        ("delivered", None, None),
    ]


@pytest.mark.asyncio
async def test_per_item_failures_are_isolated() -> None:
    database = FakeDatabase()
    retryable, permanent, succeeded = item(1), item(2), item(3)
    repository = FakeRepository(database, [[retryable, permanent, succeeded]])
    fake_writer = FakeWriter(
        database,
        {
            retryable.id: DeliveryFailure("network_error", True),
            permanent.id: DeliveryFailure("invalid_point", False),
        },
    )
    delivery_worker = worker(
        database, repository, fake_writer, settings(outbox_worker_batch_size=3)
    )

    await delivery_worker.start()
    await wait_for(repository.finalized)
    await delivery_worker.stop()

    assert fake_writer.writes == [retryable, permanent, succeeded]
    assert [record[1] for record in repository.finalizations] == [
        "retryable",
        "dead_letter",
        "delivered",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["delivered", "retryable", "dead_letter"])
async def test_lost_finalization_is_nonfatal(state: str) -> None:
    database = FakeDatabase()
    first, second = item(1), item(2)
    failure = DeliveryFailure("network_error", True) if state == "retryable" else None
    if state == "dead_letter":
        failure = DeliveryFailure("invalid_point", False)
    repository = FakeRepository(database, [[first, second]], lost_ids={first.id})
    fake_writer = FakeWriter(database, {first.id: failure} if failure else None)
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.finalized)
    await delivery_worker.stop()

    assert [record[0] for record in repository.finalizations] == [first, second]
    assert repository.finalizations[0][1] == state
    assert repository.finalizations[1][1] == "delivered"
    assert delivery_worker.last_error_code is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "claim_error,finalize_error",
    [
        (RuntimeError("database url=postgres://secret"), None),
        (None, RuntimeError("database token=DO_NOT_EXPOSE")),
    ],
)
async def test_repository_failures_are_sanitized_and_shutdown_is_interruptible(
    claim_error: Exception | None, finalize_error: Exception | None
) -> None:
    database = FakeDatabase()
    delivery = item(1)
    repository = FakeRepository(
        database,
        [[delivery]],
        claim_error=claim_error,
        finalize_failures={delivery.id: finalize_error} if finalize_error else None,
    )
    fake_writer = FakeWriter(database)
    delivery_worker = worker(database, repository, fake_writer)

    await delivery_worker.start()
    await wait_for(repository.claimed)
    if finalize_error:
        await asyncio.sleep(0)
    await delivery_worker.stop()

    assert delivery_worker.last_error_code == "unexpected_error"
    assert "secret" not in delivery_worker.last_error_code
    assert "DO_NOT_EXPOSE" not in delivery_worker.last_error_code


@pytest.mark.parametrize(
    ("attempt", "expected"), [(0, 5), (1, 5), (2, 10), (3, 20), (4, 20), (20, 20)]
)
def test_backoff_is_bounded_and_exponential(attempt: int, expected: int) -> None:
    assert backoff(attempt, 5, 20) == expected
