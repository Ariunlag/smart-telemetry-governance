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

**R1 -- Durable MQTT-to-stream-catalog vertical slice:** the initial slice is complete and merged through PR #5 under [ADR-003](decisions/ADR-003-r1-stream-catalog-entry-decisions.md). It provides authorized MQTT observation handling, PostgreSQL stream discovery, bounded evidence, and stream listing. Broader R1 remains in progress; it is not production-ready and adds no AI classification, unit governance, quality assessment, human review workflow, or InfluxDB observation sink.

PR #1 remains documentation-only. PostgreSQL and the allowlisted MQTT adapter are initial R1 integrations; InfluxDB and ChromaDB remain provisioned-only. No production readiness, benchmark result, or pilot validation has been completed.

## Next recommended branch

Plan the remaining R1 work under [ADR-003](decisions/ADR-003-r1-stream-catalog-entry-decisions.md), including operational validation and recovery evidence for the merged initial slice.

## Deferred experiments

Duplicate detection, generic clustering, RAG, autonomous agents, causal inference, federation, generic anomaly detection, and knowledge-graph infrastructure are deferred. They may only proceed under R9/R10 with an approved need and evaluation plan.
