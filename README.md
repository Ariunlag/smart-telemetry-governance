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

R0A documentation realignment and R0B engineering foundation are complete and merged through PR #2. GitHub Actions CI passed for the merged R0B implementation, including async database lifecycle, migration, backend test, frontend build, and Compose validation checks. R0B is an engineering-foundation milestone, not a production deployment. No telemetry ingestion, domain schema, durable catalog, or governance functionality has been completed. Future runtime changes are sequenced in the roadmap and must be accompanied by durable data ownership, audit evidence, security controls, and reproducible tests.

PR #2 added a typed configuration, lifecycle, readiness, optional async database connection/session, non-destructive migration, CI, and logging baseline only. The next milestone is R1, the durable MQTT-to-stream-catalog vertical slice. R1 implementation must not begin until the required durable-store, retention, MQTT authorization, stream-identity/idempotency, and malformed-payload/redelivery decisions are recorded.

## Public-repository boundary

This is a technical public repository. It documents public-impact hypotheses and validation plans; it does not contain private partner information or claims that national importance has already been proven.
