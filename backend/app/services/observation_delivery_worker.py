from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from app.core.config import Settings
from app.db.session import Database
from app.services.influx_observation_writer import DeliveryFailure, ObservationWriter
from app.services.observation_outbox_repository import ObservationOutboxRepository


def backoff(attempt: int, base: int, maximum: int) -> int:
    return int(min(base * 2 ** max(attempt - 1, 0), maximum))


class ObservationDeliveryWorker:
    def __init__(
        self,
        settings: Settings,
        database: Database,
        writer: ObservationWriter,
        repository: ObservationOutboxRepository | None = None,
    ) -> None:
        self._settings, self._database, self._writer = settings, database, writer
        self._repository = repository or ObservationOutboxRepository()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.running = False
        self.last_successful_cycle: datetime | None = None
        self.last_error_code: str | None = None

    async def start(self) -> None:
        if self._settings.influxdb_enabled and (self._task is None or self._task.done()):
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
        self._task = None
        self.running = False

    async def _run(self) -> None:
        self.running = True
        while not self._stop.is_set():
            try:
                await self.run_cycle()
            except Exception:
                self.last_error_code = "unexpected_error"
            try:
                await asyncio.wait_for(
                    self._stop.wait(), self._settings.outbox_worker_poll_interval_ms / 1000
                )
            except TimeoutError:
                pass
        self.running = False

    async def run_cycle(self) -> None:
        """Claim and finalize one bounded batch without holding database locks during writes."""
        async with self._database.transaction() as session:
            items = await self._repository.claim(
                session,
                self._settings.outbox_worker_batch_size,
                self._settings.outbox_processing_lease_seconds,
            )
        for item in items:
            try:
                await self._writer.write(item)
                state, when, code = "delivered", None, None
            except DeliveryFailure as error:
                code = error.code
                state = (
                    "dead_letter"
                    if not error.retryable
                    or item.attempt_count >= self._settings.outbox_max_attempts
                    else "retryable"
                )
                when = (
                    None
                    if state == "dead_letter"
                    else datetime.now(UTC)
                    + timedelta(
                        seconds=backoff(
                            item.attempt_count,
                            self._settings.outbox_backoff_base_seconds,
                            self._settings.outbox_backoff_max_seconds,
                        )
                    )
                )
            async with self._database.transaction() as session:
                await self._repository.finalize(session, item, state, when, code)
        if items:
            self.last_successful_cycle = datetime.now(UTC)
