# Progress

## Implemented foundation

| Area | Status | Evidence |
|---|---|---|
| FastAPI health/module/tool endpoints | Implemented | `backend/app/main.py`, route modules |
| In-memory event/module/tool registries | Implemented | `backend/app/core/`, `backend/tests/test_m1_core.py` |
| Shared telemetry-shaped contracts | Implemented | `backend/app/core/contracts.py` |
| System status module and ping tool | Implemented | `backend/app/modules/`, `backend/app/tools/` |
| React status console | Implemented/placeholder | `frontend/src/App.tsx`; future navigation is disabled |

## Milestone status

- **R0A -- Repository truth and documentation realignment:** complete after the review corrections in PR #1.
- **R0B -- Engineering foundation:** in progress and incomplete pending final review and merge. This branch adds typed settings, lifecycle/readiness, correlation IDs, optional async SQLAlchemy connection/session infrastructure, Alembic scaffolding without domain tables, pinned and locked Python tooling, and CI configuration. GitHub Actions CI passed for corrected head `197bca65933ffedb6b39468a25710a55fff494cb`, including async database, lifecycle, migration, backend test, frontend build, and Compose validation checks. No production deployment, telemetry ingestion, domain schema, durable catalog, or governance functionality has been completed.
- **R1-R8:** planned; **R9-R10:** optional and evidence-gated.

**R1 -- Durable MQTT-to-stream-catalog vertical slice:** decision gates are defined in [ADR-003](decisions/ADR-003-r1-stream-catalog-entry-decisions.md); implementation has not started and the next implementation branch is planned. No MQTT ingestion or durable stream catalog exists yet.

PR #1 remains documentation-only. Docker-provisioned services are not application integrations. No production readiness, benchmark result, pilot validation, or runtime capability has been completed.

## Next recommended branch

Create the planned R1 implementation branch only after reviewing ADR-003 and approving the R1 threshold artifact. R1 implementation has not begun.

## Deferred experiments

Duplicate detection, generic clustering, RAG, autonomous agents, causal inference, federation, generic anomaly detection, and knowledge-graph infrastructure are deferred. They may only proceed under R9/R10 with an approved need and evaluation plan.
