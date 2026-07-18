# Data Ownership and Lifecycle

| Data class | Authoritative owner | Change rule | Retention/recovery |
|---|---|---|---|
| Source registration and access scope | Governance service | Versioned administrative change | Retain per agreement; revoke credentials independently |
| Raw observation/sample | Source system plus captured evidence reference | Immutable capture metadata | Bounded by agreement; preserve hash/manifest for replay |
| Stream catalog and governance metadata | PostgreSQL | Conflict-safe, versioned or append-only as applicable | Durable; PostgreSQL is authoritative |
| Normalized time-series observation | InfluxDB delivery projection | Idempotent outbox delivery from PostgreSQL | Bucket retention policy; not a stream catalog |
| Observation-delivery outbox | PostgreSQL | Transactional state transition and replay | Retain for delivery and audit needs |
| Observed schema | Catalog | Append new version; never mutate history | Rebuild mappings from observations |
| Metric/unit mapping | Governance decision record | Supersede with rationale and reviewer | Restore prior effective mapping if needed |
| Semantic candidate | Classification run | Derived and rebuildable | Withdraw model/rule version and rerun |
| Quality assessment | Assessment record | Derived with rule/version evidence | Invalidate and recompute |
| Human review | Audit ledger | Append/supersede only | Exportable and retained per policy |
| Evaluation artifact | Benchmark manifest | Immutable versioned run | Reproduce from dataset/config manifest |

Raw data ownership remains with the contributing organization. The platform must minimize collection, enforce purpose and retention boundaries, and avoid placing partner-identifying or sensitive operational data in this public repository.

The InfluxDB projection is governed by [ADR-004](../decisions/ADR-004-influx-observation-delivery.md): only accepted normalized observations may be delivered through a PostgreSQL transactional outbox. InfluxDB does not own stream identity, governance metadata, provenance, evidence metadata, or delivery state.
