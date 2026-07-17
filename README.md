# Smart Telemetry Governance

An early-stage, production-oriented framework for governing heterogeneous IoT and OT telemetry in critical and public infrastructure. Its long-term objective is to develop and validate an AI-assisted system that inventories, classifies, validates, traces, and operationalizes telemetry streams.

## Repository truth

This repository is **not production ready**. The implemented foundation is limited to a FastAPI service with health, module, and tool endpoints; in-memory event, module, and tool registries; shared telemetry-shaped contracts; a system-status module; a ping tool; optional async database connection/session infrastructure and Alembic migration tooling without domain tables; tests for those primitives; and a React status console. The console contains disabled navigation for future features.

No MQTT ingestion, durable telemetry catalog, domain schema, schema governance, classification, quality scoring, provenance store, human review workflow, benchmark, production deployment, or pilot has been implemented. Docker Compose provisions PostgreSQL, InfluxDB, Mosquitto, and ChromaDB for local development, but the application does not connect to InfluxDB, Mosquitto, or ChromaDB. ChromaDB is therefore not an active runtime dependency.

## Immediate focus

1. Telemetry stream inventory and discovery.
2. Schema, metric, and unit governance.
3. Semantic classification with confidence and abstention.
4. Data-quality assessment, provenance, and auditability.
5. Human review, reproducible evaluation, production hardening, and independent pilots.

Duplicate detection, clustering, RAG chat, autonomous agents, causal inference, federation, generic anomaly detection, and knowledge-graph infrastructure are deferred. A knowledge-graph projection is optional only after operational graph queries have been validated.

## Architecture and evidence

- [Current state](docs/architecture/current-state.md)
- [Target state](docs/architecture/target-state.md)
- [Data ownership](docs/architecture/data-ownership.md)
- [Decision log](docs/architecture/decision-log.md)
- [Evidence plan](docs/evaluation/evidence-plan.md)
- [Production readiness checklist](docs/operations/production-readiness-checklist.md)
- [Roadmap](docs/roadmap/milestones.md)
- [Progress](docs/PROGRESS.md)

## Development status

R0A documentation realignment and R0B engineering foundation are complete and merged through PR #2. R1 is in progress with a narrow authorized MQTT-to-PostgreSQL stream-catalog slice: deterministic stream identity, bounded observation evidence, and discovered-stream APIs. PostgreSQL is authoritative; this is not a production deployment or pilot. AI classification, quality scoring, human review, duplicate detection, clustering, RAG, agents, graph, and causal capabilities remain unimplemented.

PR #5 contains the first R1 MQTT-to-PostgreSQL vertical slice under [ADR-003](docs/decisions/ADR-003-r1-stream-catalog-entry-decisions.md). R1 remains incomplete until review and merge; PostgreSQL is authoritative and evidence previews are bounded. This is not production readiness or pilot validation.

## Public-repository boundary

This is a technical public repository. It documents public-impact hypotheses and validation plans; it does not contain private partner information or claims that national importance has already been proven.
