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
- GitHub Actions CI run #22 passed. Coverage includes 46 non-PostgreSQL tests, 7 PostgreSQL tests, and 11 MQTT adapter tests.

## Placeholders and provisioned-only components

The UI has disabled navigation for Sources, Topics, Classes, Duplicates, Dashboards, and Query; these are placeholders, not features. Docker Compose starts PostgreSQL, InfluxDB, Mosquitto, and ChromaDB. PostgreSQL and Mosquitto are used by the initial R1 slice; InfluxDB and ChromaDB remain provisioned-only and must not be described as active dependencies.

## Not implemented

There is no schema or unit registry, semantic classifier, quality assessment, broader provenance/audit store, review workflow, benchmark harness, authentication, production deployment, production hardening, or pilot integration. InfluxDB has no observation sink. No Neo4j configuration or code exists. The initial R1 slice is deliberately narrow and does not complete broader governance capabilities.
