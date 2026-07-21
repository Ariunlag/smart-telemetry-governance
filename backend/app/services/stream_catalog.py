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
from app.domain.streams.models import (
    ObservationEvidence,
    ObservationOutbox,
    ObservationProcessingTask,
    RawObservationRecord,
    Stream,
)
from app.services.field_projection_contract import (
    FIELD_PROJECTION_PROCESSOR_TYPE,
    FIELD_PROJECTION_PROCESSOR_VERSION,
)
from app.services.observation_time import select_observation_timestamp

OUTCOMES = {"accepted", "malformed", "unsupported_encoding", "oversized", "rejected"}
SCHEMA_OBSERVATION_PROCESSOR_TYPE = "schema_observation"
SCHEMA_OBSERVATION_PROCESSOR_VERSION = "r2.schema.v1"


@dataclass(frozen=True)
class ObservationCommand:
    source_id: str
    external_stream_id: str
    payload: bytes
    tenant: str | None = None
    content_type: str | None = None
    broker_metadata: dict[str, object] | None = None
    source_type: str = "mqtt"
    received_at: datetime | None = None


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
                source_type=observation.source_type,
                received_at=observation.received_at,
            ),
        )

    async def record(self, session: AsyncSession, command: ObservationCommand) -> Stream | None:
        now = command.received_at or datetime.now(UTC)
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
            raw = await self._raw_observation(session, stream, evidence, command, fingerprint, now)
            if command.content_type == "application/json":
                await self._enqueue_schema_observation(session, raw, now)
                await self._enqueue_field_projection(session, raw, now)
            point = self._normalized_point(stream, evidence.id, command, now)
            if point is not None:
                await self._outbox(session, stream, evidence, point, fingerprint, now)
        return stream

    async def _raw_observation(
        self,
        session: AsyncSession,
        stream: Stream,
        evidence: ObservationEvidence,
        command: ObservationCommand,
        fingerprint: str,
        received_at: datetime,
    ) -> RawObservationRecord:
        """Persist accepted bytes once per stream, payload digest, and receive timestamp.

        The receive timestamp is part of the identity so legitimately separate observations
        remain distinguishable while retries of the same captured observation converge.
        """
        # Source/broker timestamps distinguish legitimate observations. When unavailable,
        # R1's bounded receive-time window collapses immediate broker redelivery.
        identity_timestamp, timestamp_source = select_observation_timestamp(
            self._source_timestamp(command),
            command.broker_metadata,
            received_at,
            self._settings.observation_future_skew_seconds,
        )
        if timestamp_source == "received":
            window = self._settings.observation_fallback_window_seconds
            identity_timestamp = identity_timestamp.replace(
                second=identity_timestamp.second - identity_timestamp.second % window,
                microsecond=0,
            )
        material = "\x1f".join(
            (
                stream.stream_key,
                command.external_stream_id,
                identity_timestamp.isoformat(),
                fingerprint,
            )
        )
        observation_key = hashlib.sha256(material.encode("utf-8")).hexdigest()
        values = {
            "observation_key": observation_key,
            "stream_id": stream.id,
            "evidence_id": evidence.id,
            "source_id": command.source_id,
            "source_type": command.source_type,
            "external_stream_id": command.external_stream_id,
            "received_at": received_at,
            "content_type": command.content_type,
            "payload": command.payload,
            "payload_size": len(command.payload),
            "payload_fingerprint": fingerprint,
            "transport_metadata": command.broker_metadata,
            "retention_until": received_at
            + timedelta(days=self._settings.raw_observation_retention_days),
        }
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            statement = (
                pg_insert(RawObservationRecord)
                .values(**values)
                .on_conflict_do_nothing(constraint="uq_raw_observations_observation_key")
                .returning(RawObservationRecord.id)
            )
            raw_id = (await session.execute(statement)).scalar_one_or_none()
            if raw_id is not None:
                raw = await session.get(RawObservationRecord, raw_id)
            else:
                raw = await session.scalar(
                    select(RawObservationRecord).where(
                        RawObservationRecord.observation_key == observation_key
                    )
                )
        else:
            raw = await session.scalar(
                select(RawObservationRecord).where(
                    RawObservationRecord.observation_key == observation_key
                )
            )
            if raw is None:
                raw = RawObservationRecord(**values)
                session.add(raw)
                await session.flush()
        if raw is None:
            raise RuntimeError("raw observation insert did not return a record")
        return raw

    @staticmethod
    def _source_timestamp(command: ObservationCommand) -> object:
        if command.content_type != "application/json":
            return None
        try:
            envelope = json.loads(command.payload)
        except (TypeError, ValueError):
            return None
        return envelope.get("timestamp") if isinstance(envelope, dict) else None

    async def _enqueue_schema_observation(
        self, session: AsyncSession, raw: RawObservationRecord, available_at: datetime
    ) -> None:
        values = {
            "raw_observation_id": raw.id,
            "processor_type": SCHEMA_OBSERVATION_PROCESSOR_TYPE,
            "processor_version": SCHEMA_OBSERVATION_PROCESSOR_VERSION,
            "state": "pending",
            "attempt_count": 0,
            "available_at": available_at,
        }
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            await session.execute(
                pg_insert(ObservationProcessingTask)
                .values(**values)
                .on_conflict_do_nothing(constraint="uq_processing_tasks_processor_identity")
            )
        elif (
            await session.scalar(
                select(ObservationProcessingTask.id).where(
                    ObservationProcessingTask.raw_observation_id == raw.id,
                    ObservationProcessingTask.processor_type == SCHEMA_OBSERVATION_PROCESSOR_TYPE,
                    ObservationProcessingTask.processor_version
                    == SCHEMA_OBSERVATION_PROCESSOR_VERSION,
                )
            )
            is None
        ):
            session.add(ObservationProcessingTask(**values))

    async def _enqueue_field_projection(
        self, session: AsyncSession, raw: RawObservationRecord, available_at: datetime
    ) -> None:
        values = {
            "raw_observation_id": raw.id,
            "processor_type": FIELD_PROJECTION_PROCESSOR_TYPE,
            "processor_version": FIELD_PROJECTION_PROCESSOR_VERSION,
            "state": "pending",
            "attempt_count": 0,
            "available_at": available_at,
        }
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            await session.execute(
                pg_insert(ObservationProcessingTask)
                .values(**values)
                .on_conflict_do_nothing(constraint="uq_processing_tasks_processor_identity")
            )
        elif (
            await session.scalar(
                select(ObservationProcessingTask.id).where(
                    ObservationProcessingTask.raw_observation_id == raw.id,
                    ObservationProcessingTask.processor_type == FIELD_PROJECTION_PROCESSOR_TYPE,
                    ObservationProcessingTask.processor_version
                    == FIELD_PROJECTION_PROCESSOR_VERSION,
                )
            )
            is None
        ):
            session.add(ObservationProcessingTask(**values))

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
        timestamp, source = select_observation_timestamp(
            envelope.get("timestamp"),
            command.broker_metadata,
            received_at,
            self._settings.observation_future_skew_seconds,
        )
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
