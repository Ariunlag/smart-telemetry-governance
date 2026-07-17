# Current State

## Implemented

- FastAPI application with `/health`, `/modules`, and `/tools` endpoints.
- In-memory `EventBus`, `ModuleRegistry`, and `ToolRegistry` primitives.
- Shared `SourceConfig`, `RawMessage`, `TelemetryPoint`, and `Event` data contracts.
- One `system_status` module, one ping tool, and unit tests for the core primitives.
- React console that calls the three endpoints and displays registered modules/tools.
- R0B foundation: typed environment settings, correlation IDs, lifecycle-managed resources, liveness/readiness endpoints, optional async SQLAlchemy connection/session infrastructure, and Alembic scaffolding with no domain tables.

## Placeholders and provisioned-only components

The UI has disabled navigation for Sources, Topics, Classes, Duplicates, Dashboards, and Query; these are placeholders, not features. Docker Compose starts PostgreSQL, InfluxDB, Mosquitto, and ChromaDB, but no application code connects to them. ChromaDB is provisioned-only and must not be described as an active dependency.

## Not implemented

There is no ingestion adapter, durable governance storage, schema or unit registry, semantic classifier, quality assessment, provenance/audit store, review workflow, benchmark harness, authentication, production deployment, production hardening, or pilot integration. The R0B database layer is an optional async connection/session and migration foundation only; it has no governance tables or selected R1 store. No Neo4j configuration or code exists. The repository's earlier diagrams and roadmap presented several of these proposed components as active; the realigned diagrams correct that distinction.
