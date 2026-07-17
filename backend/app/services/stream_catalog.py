from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.domain.streams.identity import normalize_identifier, normalize_topic, stream_key
from app.domain.streams.models import ObservationEvidence, Stream

OUTCOMES = {"accepted", "malformed", "unsupported_encoding", "oversized", "rejected"}


@dataclass(frozen=True)
class ObservationCommand:
    source_id: str
    topic: str
    payload: bytes
    tenant: str | None = None
    content_type: str | None = None
    broker_metadata: dict[str, object] | None = None


class StreamCatalogService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def authorize(self, topic: str) -> str:
        normalized = normalize_topic(topic)
        if not any(
            self._topic_matches(pattern, normalized)
            for pattern in self._settings.mqtt_topic_allowlist
        ):
            raise PermissionError("MQTT topic is not authorized")
        return normalized

    @staticmethod
    def _topic_matches(pattern: str, topic: str) -> bool:
        pattern_parts, topic_parts = normalize_topic(pattern).split("/"), topic.split("/")
        for index, value in enumerate(pattern_parts):
            if value == "#":
                return index == len(pattern_parts) - 1
            if index >= len(topic_parts) or (value != "+" and value != topic_parts[index]):
                return False
        return len(pattern_parts) == len(topic_parts)

    def classify(self, payload: bytes, content_type: str | None) -> str:
        if len(payload) > self._settings.mqtt_max_payload_bytes:
            return "oversized"
        if content_type and content_type not in {
            "application/json",
            "text/plain",
            "application/octet-stream",
        }:
            return "unsupported_encoding"
        try:
            payload.decode("utf-8")
        except UnicodeDecodeError:
            return "unsupported_encoding"
        return "accepted"

    async def record(self, session: AsyncSession, command: ObservationCommand) -> Stream | None:
        now = datetime.now(UTC)
        fingerprint = hashlib.sha256(command.payload).hexdigest()
        try:
            topic = self.authorize(command.topic)
            source_id = normalize_identifier(command.source_id)
        except (PermissionError, ValueError):
            await self._evidence(session, None, command, "rejected", fingerprint, now)
            return None
        outcome = self.classify(command.payload, command.content_type)
        key = stream_key(source_id, topic, command.tenant)
        stream = await session.scalar(select(Stream).where(Stream.stream_key == key))
        if stream is None:
            stream = Stream(
                stream_key=key,
                source_id=source_id,
                topic=topic,
                tenant=command.tenant,
                first_observed_at=now,
                last_observed_at=now,
                observation_count=1,
                payload_format=command.content_type,
                provenance={"source_id": source_id},
            )
            session.add(stream)
            await session.flush()
        else:
            stream.last_observed_at = now
            stream.observation_count += 1
            stream.payload_format = command.content_type or stream.payload_format
        await self._evidence(session, stream.id, command, outcome, fingerprint, now)
        return stream

    async def _evidence(
        self,
        session: AsyncSession,
        stream_id: object,
        command: ObservationCommand,
        outcome: str,
        fingerprint: str,
        received_at: datetime,
    ) -> None:
        preview = command.payload[: self._settings.evidence_preview_bytes].decode(
            "utf-8", errors="replace"
        )
        session.add(
            ObservationEvidence(
                stream_id=stream_id,
                received_at=received_at,
                outcome=outcome,
                payload_size=len(command.payload),
                content_type=command.content_type,
                payload_preview=preview,
                payload_fingerprint=fingerprint,
                broker_metadata=command.broker_metadata,
            )
        )
