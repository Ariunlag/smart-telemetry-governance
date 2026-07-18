# Current State

## Implemented

- FastAPI application with `/health`, `/modules`, and `/tools` endpoints.
- In-memory `EventBus`, `ModuleRegistry`, and `ToolRegistry` primitives.
- Shared `SourceConfig`, `RawMessage`, `TelemetryPoint`, and `Event` data contracts.
- One `system_status` module, one ping tool, and unit tests for the core primitives.
- React console that calls the three endpoints and displays registered modules/tools.
- R0B foundation, complete and merged through PR #2: typed environment settings, correlation IDs, lifecycle-managed resources, liveness/readiness endpoints, optional async SQLAlchemy connection/session infrastructure, and Alembic scaffolding.
- Initial R1 vertical slice, complete and merged through PR #5: lifecycle-managed MQTT adapter with an explicit topic allowlist, configurable TLS verified/unverified/off modes, bounded reconnect, and safe shutdown.
- PostgreSQL `streams` and `observation_evidence` tables, deterministic stream keys, and conflict-safe idempotent stream upserts. Ingestion outcomes cover accepted, malformed, unsupported, oversized, and rejected observations with bounded evidence.
- `/streams` list/detail APIs, including database-unavailable 503 responses, and a React Streams UI with retry behavior.
- PostgreSQL observation-outbox records for accepted explicit normalized JSON envelopes. The stream update, bounded evidence, and outbox insert share one transaction. A lifecycle-managed worker claims outbox rows with `FOR UPDATE SKIP LOCKED`, commits the claim before network I/O, and finalizes each result in a separate optimistic transaction. Stale processing leases are reclaimable; a stale worker cannot overwrite a newer claim.
- The delivery path is MQTT ingestion → normalized observation → PostgreSQL observation outbox → delivery worker → InfluxDB 2.x. PostgreSQL is the durable system of record for delivery state; InfluxDB is the normalized time-series query destination. Processing is sequential in the current worker. Retryable failures use bounded exponential backoff without jitter; maximum attempts and permanent failures transition to `dead_letter`. Selected dead-letter rows can be replayed through repository functionality; no public replay endpoint exists.
- `telemetry_observation` is the stable InfluxDB measurement. Tags are stream/source identifiers, optional tenant, metric, optional unit, timestamp source, quality status, and schema version. Fields are exactly one typed value (`integer`, `float`, `boolean`, or `string`), MQTT topic, delivery key, received timestamp, and provenance reference. Observation time is normalized to UTC and is the point timestamp. Topics, delivery keys, provenance/evidence references, payload fingerprints, raw payloads, and broker metadata are not tags.
- Writer and worker failure state uses bounded sanitized codes. Timeouts, connection/DNS/network failures, HTTP 408/429, and HTTP 5xx are retryable; invalid points, HTTP 400/401/403, and configuration failures are permanent. Credentials, database URLs, full response bodies, raw telemetry, and stack traces are not retained.
- `GET /api/delivery/status` is read-only and exposes only enabled/running state, bounded runtime error code, last successful cycle timestamp, outbox state counts, stale-processing count, and oldest eligible availability timestamp.

## Placeholders and provisioned-only components

The UI has disabled navigation for Sources, Topics, Classes, Duplicates, Dashboards, and Query; these are placeholders, not features. Docker Compose starts PostgreSQL, InfluxDB, Mosquitto, and ChromaDB. PostgreSQL, Mosquitto, and the InfluxDB delivery path are used by the current R1 work; ChromaDB remains provisioned-only.

## Not implemented

There is no schema or unit registry, semantic classifier, quality assessment, broader provenance/audit store, review workflow, benchmark harness, authentication, production deployment, production hardening, or pilot integration. No Neo4j configuration or code exists. The R1 slice remains deliberately narrow and does not complete broader governance capabilities.
