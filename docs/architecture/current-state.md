# Current State

## Implemented

- FastAPI application with `/health`, `/modules`, and `/tools` endpoints.
- In-memory `EventBus`, `ModuleRegistry`, and `ToolRegistry` primitives.
- Shared `SourceConfig`, `RawMessage`, `TelemetryPoint`, and `Event` data contracts.
- One `system_status` module, one ping tool, and unit tests for the core primitives.
- React console that calls the three endpoints and displays registered modules/tools.

## Placeholders and provisioned-only components

The UI has disabled navigation for Sources, Topics, Classes, Duplicates, Dashboards, and Query; these are placeholders, not features. Docker Compose starts PostgreSQL, InfluxDB, Mosquitto, and ChromaDB, but no application code connects to them. ChromaDB is provisioned-only and must not be described as an active dependency.

## Not implemented

There is no ingestion adapter, durable storage, schema or unit registry, semantic classifier, quality assessment, provenance/audit store, review workflow, benchmark harness, authentication, production hardening, or pilot integration. No Neo4j configuration or code exists. The repository's earlier diagrams and roadmap presented several of these proposed components as active; the realigned diagrams correct that distinction.
