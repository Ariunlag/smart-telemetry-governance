# Smart Telemetry Governance

An early-stage, production-oriented framework for governing heterogeneous IoT and OT telemetry in critical and public infrastructure. Its long-term objective is to develop and validate an AI-assisted system that inventories, classifies, validates, traces, and operationalizes telemetry streams.

## Repository truth

This repository is **not production ready**. R0 engineering foundation is complete, and the initial R1 MQTT-to-PostgreSQL stream-catalog vertical slice is merged. PostgreSQL is the authoritative stream catalog. The application includes an allowlisted MQTT adapter with configurable TLS and bounded reconnect, deterministic stream identity, bounded observation evidence, Streams APIs, and a React Streams view.

Schema, metric, and unit governance; AI classification and abstention; quality scoring; broader provenance and audit workflows; human review; benchmark evaluation; production deployment; and independent pilot validation remain unimplemented. The in-progress R1 delivery boundary writes accepted normalized observations through PostgreSQL outbox state to InfluxDB 2.x; InfluxDB is a time-series query destination, never the stream catalog. ChromaDB remains provisioned-only and is not an active runtime dependency.

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

R0A documentation realignment and R0B engineering foundation are complete and merged through PR #2. The initial R1 MQTT-to-PostgreSQL stream-catalog slice is merged through PR #5: PostgreSQL is authoritative, MQTT subscriptions are allowlisted, evidence previews are bounded, and streams can be listed in the API and React UI. This is not a production deployment or pilot. AI classification, quality scoring, human review, duplicate detection, clustering, RAG, agents, graph, and causal capabilities remain unimplemented.

PR #5 merged the first R1 MQTT-to-PostgreSQL vertical slice under [ADR-003](docs/decisions/ADR-003-r1-stream-catalog-entry-decisions.md). PR #9 merged the PostgreSQL-outbox-to-InfluxDB delivery boundary (merge commit `f7b8c419d4b1851dee453011259dc381ea6f08c4`) and GitHub Actions CI passed. Broader R1 now continues with governed tenant/site-aware source and subscription control; this remains neither production readiness nor pilot validation.

## Local delivery development

Start the local PostgreSQL and InfluxDB 2.x services with:

```powershell
docker compose --env-file .env.example up -d postgres influxdb
docker compose --env-file .env.example ps
```

`.env.example` contains development-only placeholders. Production credentials must come from secret management. The delivery configuration uses `INFLUXDB_ENABLED`, `INFLUXDB_URL`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`, `INFLUXDB_TOKEN`, and `INFLUXDB_VERIFY_SSL`; Compose initializes local InfluxDB deterministically through `DOCKER_INFLUXDB_INIT_*` variables. Do not place tokens in repository files or documentation.

## Public-repository boundary

This is a technical public repository. It documents public-impact hypotheses and validation plans; it does not contain private partner information or claims that national importance has already been proven.
