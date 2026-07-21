from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

TimestampSource = Literal["source", "broker", "received"]


def normalize_observation_time(timestamp: datetime) -> datetime:
    """Treat SQLite's offset-less persistence of known UTC values as UTC."""
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp


def select_observation_timestamp(
    source_value: object,
    broker_metadata: dict[str, object] | None,
    received_at: datetime,
    future_skew_seconds: int,
) -> tuple[datetime, TimestampSource]:
    normalized_received_at = normalize_observation_time(received_at)
    candidates: tuple[tuple[object, TimestampSource], ...] = (
        (source_value, "source"),
        ((broker_metadata or {}).get("timestamp"), "broker"),
    )
    for value, source in candidates:
        if isinstance(value, str):
            try:
                candidate = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
            if candidate.tzinfo is not None:
                candidate = candidate.astimezone(UTC)
                if candidate <= normalized_received_at + timedelta(seconds=future_skew_seconds):
                    return candidate, source
    return normalized_received_at, "received"
