from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.exc import SQLAlchemyError

from app.core.config import Settings
from app.db.session import Database
from app.services.field_projection_contract import (
    FIELD_PROJECTION_PROCESSOR_TYPE,
    FIELD_PROJECTION_PROCESSOR_VERSION,
)
from app.services.field_projection_service import (
    FieldProjectionFailure,
    FieldProjectionService,
)
from app.services.processing_task_repository import ProcessingTaskItem, ProcessingTaskRepository
from app.services.schema_observation_service import StaleProcessingClaim
from app.services.schema_observation_worker import backoff

TERMINAL_FAILURE_CODES = frozenset(
    {
        "raw_observation_missing",
        "invalid_json",
        "root_not_object",
        "unsupported_persisted_input",
        "stream_missing",
        "field_depth_exceeded",
        "field_count_exceeded",
        "field_node_limit_exceeded",
        "field_path_too_long",
        "document_too_large",
        "non_finite_number",
    }
)

FieldProjectionServiceFactory = Callable[[], FieldProjectionService]


class FieldProjectionWorker:
    """Lifecycle-managed durable processor for R2 scalar field projection tasks."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        repository: ProcessingTaskRepository | None = None,
        service_factory: FieldProjectionServiceFactory | None = None,
    ) -> None:
        self._settings = settings
        self._database = database
        self._repository = repository or ProcessingTaskRepository()
        self._service_factory = service_factory or (
            lambda: FieldProjectionService(settings=settings)
        )
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.running = False
        self.last_successful_cycle_at: datetime | None = None
        self.last_error_code: str | None = None
        self.processed_count = 0
        self.failed_count = 0

    async def start(self) -> None:
        if not self._settings.field_projection_worker_enabled:
            return
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None
        self.running = False

    async def _run(self) -> None:
        self.running = True
        try:
            while not self._stop.is_set():
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.last_error_code = "unexpected_error"
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        self._settings.field_projection_worker_poll_interval_ms / 1000,
                    )
                except TimeoutError:
                    pass
        finally:
            self.running = False

    async def run_once(self) -> int:
        async with self._database.transaction() as session:
            items = await self._repository.claim(
                session,
                FIELD_PROJECTION_PROCESSOR_TYPE,
                FIELD_PROJECTION_PROCESSOR_VERSION,
                self._settings.field_projection_worker_batch_size,
                self._settings.field_projection_worker_lease_seconds,
            )
        completed = 0
        for item in items:
            if await self._process_item(item):
                completed += 1
        self.last_successful_cycle_at = datetime.now(UTC)
        return completed

    async def _process_item(self, item: ProcessingTaskItem) -> bool:
        try:
            async with self._database.transaction() as session:
                await self._service_factory().process_claim(session, item)
        except asyncio.CancelledError:
            raise
        except StaleProcessingClaim:
            return False
        except FieldProjectionFailure as error:
            await self._finalize_failure(item, error.code, error.code in TERMINAL_FAILURE_CODES)
            return False
        except SQLAlchemyError:
            await self._finalize_failure(item, "database_error", False)
            return False
        except Exception:
            await self._finalize_failure(item, "unexpected_error", False)
            return False
        self.processed_count += 1
        return True

    async def _finalize_failure(self, item: ProcessingTaskItem, code: str, terminal: bool) -> None:
        state = (
            "dead_letter"
            if terminal or item.attempt_count >= self._settings.field_projection_worker_max_attempts
            else "retryable"
        )
        available_at = (
            None
            if state == "dead_letter"
            else datetime.now(UTC)
            + timedelta(
                seconds=backoff(
                    item.attempt_count,
                    self._settings.schema_observation_worker_backoff_base_seconds,
                    self._settings.schema_observation_worker_backoff_max_seconds,
                )
            )
        )
        try:
            async with self._database.transaction() as session:
                finalized = await self._repository.finalize(
                    session, item, state, available_at, code
                )
        except asyncio.CancelledError:
            raise
        except SQLAlchemyError:
            self.last_error_code = "database_error"
            return
        if finalized:
            self.failed_count += 1
            self.last_error_code = code
