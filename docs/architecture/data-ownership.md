# Data Ownership and Lifecycle

| Data class | Authoritative owner | Change rule | Retention/recovery |
|---|---|---|---|
| Source registration and access scope | Governance service | Versioned administrative change | Retain per agreement; revoke credentials independently |
| Raw observation/sample | Source system plus captured evidence reference | Immutable capture metadata | Bounded by agreement; preserve hash/manifest for replay |
| Observed schema | Catalog | Append new version; never mutate history | Rebuild mappings from observations |
| Metric/unit mapping | Governance decision record | Supersede with rationale and reviewer | Restore prior effective mapping if needed |
| Semantic candidate | Classification run | Derived and rebuildable | Withdraw model/rule version and rerun |
| Quality assessment | Assessment record | Derived with rule/version evidence | Invalidate and recompute |
| Human review | Audit ledger | Append/supersede only | Exportable and retained per policy |
| Evaluation artifact | Benchmark manifest | Immutable versioned run | Reproduce from dataset/config manifest |

Raw data ownership remains with the contributing organization. The platform must minimize collection, enforce purpose and retention boundaries, and avoid placing partner-identifying or sensitive operational data in this public repository.
