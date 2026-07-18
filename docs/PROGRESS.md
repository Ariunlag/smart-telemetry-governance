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
- **R1:** in progress beyond its merged initial slice; **R2-R8:** planned; **R9-R10:** optional and evidence-gated.

**R1 -- Durable MQTT-to-stream-catalog vertical slice:** the initial slice is complete and merged through PR #5 under [ADR-003](decisions/ADR-003-r1-stream-catalog-entry-decisions.md). Broader R1 remains in progress. [ADR-004](decisions/ADR-004-influx-observation-delivery.md) is implemented as PostgreSQL outbox → InfluxDB 2.x delivery with optimistic claims, retry/dead-letter state, and read-only delivery status; this is not production-ready and adds no AI classification, unit governance, quality assessment, or human review workflow.

**R1 observation delivery:** PostgreSQL remains authoritative for outbox and delivery state; InfluxDB is the normalized time-series projection. Verified coverage includes writer/point-mapping tests, worker and status-route tests, PostgreSQL repository/worker integration, real InfluxDB integration, PostgreSQL-to-Influx flow, outage/recovery, invalid-token permanent failure, and data-minimization assertions. Focused tests: 63 passed; PostgreSQL marker tests: 14 passed; InfluxDB marker tests: 9 passed; confirmed across these groups: 86 passed. This is not a final full-suite count.

PR #1 remains documentation-only. PostgreSQL, the allowlisted MQTT adapter, and the InfluxDB delivery projection are current R1 integrations; ChromaDB remains provisioned-only. No production readiness, benchmark result, or pilot validation has been completed.

## Next recommended branch

Complete final validation, migration cycle, frontend lint/build, security/diff review, commit and draft PR, then GitHub Actions verification after push. Remaining R1 work also includes governed source/subscription registration, real broker-backed validation, retained-message behavior, retention enforcement, expanded schema observation, and ingestion-run recovery evidence.

## Deferred experiments

Duplicate detection, generic clustering, RAG, autonomous agents, causal inference, federation, generic anomaly detection, and knowledge-graph infrastructure are deferred. They may only proceed under R9/R10 with an approved need and evaluation plan.
