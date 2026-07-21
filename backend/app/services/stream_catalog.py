from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from math import isfinite
from typing import Literal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.contracts import RawObservation
from app.domain.streams.identity import normalize_identifier, normalize_topic, stream_key
from app.domain.streams.models import ObservationEvidence, ObservationOutbox, Stream

OUTCOMES = {"accepted", "malformed", "unsupported_encoding", "oversized", "rejected"}


@dataclass(frozen=True)
class ObservationCommand:
    source_id: str
    external_stream_id: str
    payload: bytes
    tenant: str | None = None
    content_type: str | None = None
    broker_metadata: dict[str, object] | None = None


@dataclass(frozen=True)
class NormalizedObservationPoint:
    stream_id: str
    source_id: str
    tenant: str | None
    topic: str
    observation_timestamp: str
    received_timestamp: str
    timestamp_source: Literal["source", "broker", "received"]
    metric: str
    unit: str | None
    value_type: Literal["integer", "float", "boolean", "string"]
    value: int | float | bool | str
    content_schema_version: str
    quality_status: str
    provenance_reference: str

    def payload(self) -> dict[str, object]:
        return asdict(self)


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
        if content_type == "application/json":
            try:
                json.loads(payload)
            except (TypeError, ValueError):
                return "malformed"
        return "accepted"

    async def record_raw(self, session: AsyncSession, observation: RawObservation) -> Stream | None:
        """Translate the protocol-neutral boundary into the existing catalog contract."""
        return await self.record(
            session,
            ObservationCommand(
                source_id=observation.source_id,
                external_stream_id=observation.external_stream_id,
                payload=observation.payload,
                content_type=observation.content_type,
                broker_metadata=dict(observation.transport_metadata),
            ),
        )

    async def record(self, session: AsyncSession, command: ObservationCommand) -> Stream | None:
        now = datetime.now(UTC)
        fingerprint = hashlib.sha256(
            command.payload[: self._settings.mqtt_max_payload_bytes]
        ).hexdigest()
        try:
            topic = self.authorize(command.external_stream_id)
            source_id = normalize_identifier(command.source_id)
        except (PermissionError, ValueError):
            await self._evidence(session, None, command, "rejected", fingerprint, now)
            return None
        outcome = self.classify(command.payload, command.content_type)
        key = stream_key(source_id, topic, command.tenant)
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            insert_statement = pg_insert(Stream).values(
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
            statement = insert_statement.on_conflict_do_update(
                constraint="uq_streams_stream_key",
                set_={
                    "last_observed_at": now,
                    "observation_count": Stream.observation_count + 1,
                    "payload_format": insert_statement.excluded.payload_format,
                    "updated_at": now,
                },
            ).returning(Stream.id)
            stream_id = (await session.execute(statement)).scalar_one()
            stream = await session.get(Stream, stream_id)
        else:
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
        if stream is None:
            raise RuntimeError("stream upsert did not return a stream")
        evidence = await self._evidence(session, stream.id, command, outcome, fingerprint, now)
        if outcome == "accepted":
            point = self._normalized_point(stream, evidence.id, command, now)
            if point is not None:
                await self._outbox(session, stream, evidence, point, fingerprint, now)
        return stream

    async def _evidence(
        self,
        session: AsyncSession,
        stream_id: object,
        command: ObservationCommand,
        outcome: str,
        fingerprint: str,
        received_at: datetime,
    ) -> ObservationEvidence:
        preview = command.payload[: self._settings.evidence_preview_bytes].decode(
            "utf-8", errors="replace"
        )
        evidence = ObservationEvidence(
            stream_id=stream_id,
            received_at=received_at,
            outcome=outcome,
            payload_size=len(command.payload),
            content_type=command.content_type,
            payload_preview=preview,
            payload_fingerprint=fingerprint,
            broker_metadata=command.broker_metadata,
        )
        session.add(evidence)
        await session.flush()
        return evidence

    def _normalized_point(
        self,
        stream: Stream,
        evidence_id: object,
        command: ObservationCommand,
        received_at: datetime,
    ) -> NormalizedObservationPoint | None:
        if command.content_type != "application/json":
            return None
        try:
            envelope = json.loads(command.payload)
        except (TypeError, ValueError):
            return None
        if not isinstance(envelope, dict):
            return None
        metric = envelope.get("metric")
        value = envelope.get("value")
        unit = envelope.get("unit")
        if not isinstance(metric, str) or not metric.strip() or len(metric.strip()) > 255:
            return None
        if unit is not None and (not isinstance(unit, str) or len(unit.strip()) > 64):
            return None
        if isinstance(value, bool):
            value_type: Literal["integer", "float", "boolean", "string"] = "boolean"
        elif isinstance(value, int):
            value_type = "integer"
        elif isinstance(value, float) and isfinite(value):
            value_type = "float"
        elif isinstance(value, str):
            value_type = "string"
        else:
            return None
        timestamp, source = self._timestamp(envelope.get("timestamp"), command, received_at)
        return NormalizedObservationPoint(
            stream_id=str(stream.id),
            source_id=stream.source_id,
            tenant=stream.tenant,
            topic=stream.topic,
            observation_timestamp=timestamp.isoformat(),
            received_timestamp=received_at.isoformat(),
            timestamp_source=source,
            metric=metric.strip().lower(),
            unit=unit.strip() if isinstance(unit, str) else None,
            value_type=value_type,
            value=value,
            content_schema_version="r1.normalized-point.v1",
            quality_status="unassessed",
            provenance_reference=str(evidence_id),
        )

    def _timestamp(
        self, source_value: object, command: ObservationCommand, received_at: datetime
    ) -> tuple[datetime, Literal["source", "broker", "received"]]:
        candidates: tuple[tuple[object, Literal["source", "broker"]], ...] = (
            (source_value, "source"),
            ((command.broker_metadata or {}).get("timestamp"), "broker"),
        )
        for value, source in candidates:
            if isinstance(value, str):
                try:
                    candidate = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if candidate.tzinfo is not None:
                    candidate = candidate.astimezone(UTC)
                    if candidate <= received_at + timedelta(
                        seconds=self._settings.observation_future_skew_seconds
                    ):
                        return candidate, source
        return received_at, "received"

    async def _outbox(
        self,
        session: AsyncSession,
        stream: Stream,
        evidence: ObservationEvidence,
        point: NormalizedObservationPoint,
        fingerprint: str,
        received_at: datetime,
    ) -> None:
        timestamp = datetime.fromisoformat(point.observation_timestamp)
        if point.timestamp_source == "received":
            window = self._settings.observation_fallback_window_seconds
            timestamp = timestamp.replace(
                second=timestamp.second - timestamp.second % window, microsecond=0
            )
        material = "\x1f".join(
            (stream.stream_key, timestamp.isoformat(), fingerprint, point.metric)
        )
        delivery_key = hashlib.sha256(material.encode("utf-8")).hexdigest()
        values = {
            "delivery_key": delivery_key,
            "stream_id": stream.id,
            "evidence_id": evidence.id,
            "state": "pending",
            "point_payload": point.payload(),
            "attempt_count": 0,
            "available_at": received_at,
        }
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            await session.execute(
                pg_insert(ObservationOutbox)
                .values(**values)
                .on_conflict_do_nothing(constraint="uq_observation_outbox_delivery_key")
            )
        elif (
            await session.scalar(
                select(ObservationOutbox.id).where(ObservationOutbox.delivery_key == delivery_key)
            )
            is None
        ):
            session.add(ObservationOutbox(**values))
