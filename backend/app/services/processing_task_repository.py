from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.streams.models import ObservationProcessingTask


@dataclass(frozen=True)
class ProcessingTaskItem:
    id: str
    raw_observation_id: str
    processor_type: str
    processor_version: str
    attempt_count: int
    processing_started_at: datetime


class ProcessingTaskRepository:
    """Durable generic work-state operations; no worker is started by this slice."""

    async def claim(
        self,
        session: AsyncSession,
        processor_type: str,
        processor_version: str,
        limit: int,
        lease_seconds: int,
    ) -> list[ProcessingTaskItem]:
        now = datetime.now(UTC)
        eligible = or_(
            and_(
                ObservationProcessingTask.state.in_(("pending", "retryable")),
                ObservationProcessingTask.available_at <= now,
            ),
            and_(
                ObservationProcessingTask.state == "processing",
                ObservationProcessingTask.processing_started_at
                < now - timedelta(seconds=lease_seconds),
            ),
        )
        rows = list(
            (
                await session.scalars(
                    select(ObservationProcessingTask)
                    .where(
                        eligible,
                        ObservationProcessingTask.processor_type == processor_type,
                        ObservationProcessingTask.processor_version == processor_version,
                    )
                    .order_by(ObservationProcessingTask.available_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        for row in rows:
            row.state = "processing"
            row.processing_started_at = now
            row.attempt_count += 1
        await session.flush()
        return [
            ProcessingTaskItem(
                str(row.id),
                str(row.raw_observation_id),
                row.processor_type,
                row.processor_version,
                row.attempt_count,
                now,
            )
            for row in rows
        ]

    async def finalize(
        self,
        session: AsyncSession,
        item: ProcessingTaskItem,
        state: str,
        available_at: datetime | None = None,
        code: str | None = None,
    ) -> bool:
        values: dict[str, object] = {
            "state": state,
            "processing_started_at": None,
            "last_error_code": code,
            "last_error_detail": code,
        }
        if state == "completed":
            values["completed_at"] = datetime.now(UTC)
            values["last_error_code"] = None
            values["last_error_detail"] = None
        if available_at is not None:
            values["available_at"] = available_at
        result = await session.execute(
            update(ObservationProcessingTask)
            .where(
                ObservationProcessingTask.id == UUID(item.id),
                ObservationProcessingTask.state == "processing",
                ObservationProcessingTask.attempt_count == item.attempt_count,
                ObservationProcessingTask.processing_started_at == item.processing_started_at,
            )
            .values(**values)
        )
        return result.rowcount == 1

    async def counts(
        self, session: AsyncSession, processor_type: str, processor_version: str
    ) -> dict[str, int]:
        rows = (
            await session.execute(
                select(ObservationProcessingTask.state, func.count())
                .where(
                    ObservationProcessingTask.processor_type == processor_type,
                    ObservationProcessingTask.processor_version == processor_version,
                )
                .group_by(ObservationProcessingTask.state)
            )
        ).all()
        return {state: count for state, count in rows}
