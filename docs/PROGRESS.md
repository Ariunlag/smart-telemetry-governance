# Progress

## Implemented foundation

| Area | Status | Evidence |
|---|---|---|
| FastAPI health/module/tool endpoints | Implemented | `backend/app/main.py`, route modules |
| In-memory event/module/tool registries | Implemented | `backend/app/core/`, `backend/tests/test_m1_core.py` |
| Shared telemetry-shaped contracts | Implemented | `backend/app/core/contracts.py` |
| System status module and ping tool | Implemented | `backend/app/modules/`, `backend/app/tools/` |
| React status console | Implemented/placeholder | `frontend/src/App.tsx`; future navigation is disabled |
| R0B engineering foundation | Complete and merged | PR #2; CI passed async database lifecycle, migrations, backend tests, frontend build, and Compose validation |
| R1 initial MQTT-to-PostgreSQL slice | Complete and merged | PR #5, merge commit `4a5b218628c5ffe3fc0a671d6208c915654036c8`; GitHub Actions CI run #22 passed |

## Milestone status

- **R0A -- Repository truth and documentation realignment:** complete after the review corrections in PR #1.
- **R0B -- Engineering foundation:** complete and merged through PR #2. It provides typed settings, lifecycle/readiness, correlation IDs, optional async SQLAlchemy connection/session infrastructure, Alembic scaffolding without domain tables, pinned and locked Python tooling, and CI configuration. CI passed async database lifecycle, migration, backend test, frontend build, and Compose validation checks. R0B does not establish production readiness and does not add telemetry ingestion, a domain schema, durable catalog, or governance functionality.
- **R1:** in progress beyond its merged initial slice; **R2:** in progress with its protocol-neutral boundary; **R3-R8:** planned; **R9-R10:** optional and evidence-gated.

**R1 -- Durable MQTT-to-stream-catalog vertical slice:** the initial slice is complete and merged through PR #5 under [ADR-003](decisions/ADR-003-r1-stream-catalog-entry-decisions.md). Broader R1 remains in progress. [ADR-004](decisions/ADR-004-influx-observation-delivery.md) is implemented as PostgreSQL outbox → InfluxDB 2.x delivery with optimistic claims, retry/dead-letter state, and read-only delivery status; this is not production-ready and adds no AI classification, unit governance, quality assessment, or human review workflow.

**R1 observation delivery:** PR #9 merged the PostgreSQL-outbox-to-InfluxDB delivery boundary at merge commit `f7b8c419d4b1851dee453011259dc381ea6f08c4`; GitHub Actions CI passed. PostgreSQL remains authoritative for outbox and delivery state, while InfluxDB is the normalized time-series projection. The next R1 implementation gate is the proposed [source/subscription threshold](evaluation/thresholds/r1.yaml): tenant/site-aware source registration, external credential references, controlled subscription lifecycle, ingestion-run evidence, retained-message policy, broker-backed validation, and retention enforcement. Broader R1 remains in progress and the repository is not production-ready.

**R1 source/subscription persistence foundation:** tenant, site, telemetry-source, MQTT-subscription, and ingestion-run records now have migration-backed PostgreSQL persistence with tenant/site ownership constraints, opaque external credential references, bounded configuration and run fields, and tenant-aware repository access. This slice adds no source/subscription HTTP APIs, React UI, credential retrieval, or MQTT runtime orchestration; those control-plane capabilities remain unfinished.

**R2 -- Schema-flexible interoperability foundation:** its protocol-neutral boundary now persists accepted source-native bytes in PostgreSQL with a bounded retention timestamp and durable generic processing tasks. MQTT remains the only active adapter and maps its topic to the generic external stream identifier before the existing catalog/evidence/outbox flow. Accepted JSON observations queue `schema_observation` work records, but no schema worker, observed schema/field, fingerprint, mapping, second adapter, or AI is implemented. Physical retention cleanup remains pending; R2 is not production readiness.

PR #1 remains documentation-only. PostgreSQL, the allowlisted MQTT adapter, and the InfluxDB delivery projection are current R1 integrations; ChromaDB remains provisioned-only. No production readiness, benchmark result, or pilot validation has been completed.

## Next recommended branch

Complete source/subscription APIs and runtime orchestration under the approved [R1 source/subscription threshold](evaluation/thresholds/r1.yaml), then implement the proposed R2 schema-observation and protocol-neutral boundary. The persistence foundation and R2 architecture documentation must not be presented as a complete control plane, implemented semantic AI, or production readiness.

## Deferred experiments

Duplicate detection, generic clustering, RAG, autonomous agents, causal inference, federation, generic anomaly detection, and knowledge-graph infrastructure are deferred. They may only proceed under R9/R10 with an approved need and evaluation plan.
