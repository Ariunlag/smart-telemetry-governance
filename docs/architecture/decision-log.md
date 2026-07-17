# Architecture Decision Log

| ID | Decision | Status | Rationale |
|---|---|---|---|
| D-001 | Treat current service as a foundation only | Accepted | Code implements registries and status endpoints, not governance workflows. |
| D-002 | Make stream catalog the first vertical slice | Accepted | It creates the durable evidence base required by all later governance decisions. |
| D-003 | Prefer confidence plus abstention to forced labels | Accepted | Ambiguous telemetry must be reviewable rather than silently misclassified. |
| D-004 | Preserve raw evidence, derived outputs, and human decisions separately | Accepted | Enables audit, rollback, and reproducibility. |
| D-005 | Defer broad AI and graph capabilities | Accepted | They do not yet have a validated operational need or evidence base. |
| D-006 | Add graph projection only after query validation | Proposed | A projection must be derived/rebuildable and show measurable benefit. |

New material decisions should record context, alternatives, evidence, owner, reversal conditions, and affected data contracts.
