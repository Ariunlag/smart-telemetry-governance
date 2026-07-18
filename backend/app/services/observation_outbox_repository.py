from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.streams.models import ObservationOutbox
from app.services.influx_observation_writer import DeliveryItem


class ObservationOutboxRepository:
    async def claim(
        self, session: AsyncSession, limit: int, lease_seconds: int
    ) -> list[DeliveryItem]:
        now = datetime.now(UTC)
        eligible = or_(
            and_(
                ObservationOutbox.state.in_(("pending", "retryable")),
                ObservationOutbox.available_at <= now,
            ),
            and_(
                ObservationOutbox.state == "processing",
                ObservationOutbox.processing_started_at < now - timedelta(seconds=lease_seconds),
            ),
        )
        rows = list(
            (
                await session.scalars(
                    select(ObservationOutbox)
                    .where(eligible)
                    .order_by(ObservationOutbox.available_at)
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
            DeliveryItem(str(row.id), row.delivery_key, row.point_payload, row.attempt_count, now)
            for row in rows
        ]

    async def finalize(
        self,
        session: AsyncSession,
        item: DeliveryItem,
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
        if state == "delivered":
            values["delivered_at"] = datetime.now(UTC)
            values["last_error_code"] = None
            values["last_error_detail"] = None
        if available_at is not None:
            values["available_at"] = available_at
        result = await session.execute(
            update(ObservationOutbox)
            .where(
                ObservationOutbox.id == UUID(item.id),
                ObservationOutbox.state == "processing",
                ObservationOutbox.attempt_count == item.attempt_count,
                ObservationOutbox.processing_started_at == item.processing_started_at,
            )
            .values(**values)
        )
        return result.rowcount == 1

    async def replay_dead_letters(self, session: AsyncSession, ids: list[UUID]) -> int:
        result = await session.execute(
            update(ObservationOutbox)
            .where(ObservationOutbox.id.in_(ids), ObservationOutbox.state == "dead_letter")
            .values(
                state="pending",
                available_at=datetime.now(UTC),
                processing_started_at=None,
                last_error_code=None,
                last_error_detail=None,
            )
        )
        return result.rowcount or 0

    async def counts(self, session: AsyncSession) -> dict[str, int]:
        rows = (
            await session.execute(
                select(ObservationOutbox.state, func.count()).group_by(ObservationOutbox.state)
            )
        ).all()
        return {state: count for state, count in rows}

    async def status(
        self, session: AsyncSession, lease_seconds: int
    ) -> tuple[dict[str, int], int, datetime | None]:
        now = datetime.now(UTC)
        counts = await self.counts(session)
        stale = await session.scalar(
            select(func.count())
            .select_from(ObservationOutbox)
            .where(
                ObservationOutbox.state == "processing",
                ObservationOutbox.processing_started_at < now - timedelta(seconds=lease_seconds),
            )
        )
        oldest = await session.scalar(
            select(func.min(ObservationOutbox.available_at)).where(
                ObservationOutbox.state.in_(("pending", "retryable")),
                ObservationOutbox.available_at <= now,
            )
        )
        return counts, stale or 0, oldest
