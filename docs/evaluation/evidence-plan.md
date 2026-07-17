# Evidence Plan

## Claims and measures

| Claim to test | Evidence | Primary measures |
|---|---|---|
| Discovery finds governed streams | Versioned annotated source samples | precision, recall, catalog coverage, ingestion reliability |
| Schema/unit governance improves metadata validity | Expert-reviewed mappings | mapping accuracy, invalid-unit detection, reviewer agreement |
| Classification is safe under uncertainty | Held-out labeled streams | macro-F1, calibration error, selective risk, abstention coverage |
| Quality assessments are actionable | Fault scenarios and operational traces | detection precision/recall, time-to-detection, evidence completeness |
| Review improves outcomes | Timestamped review events | correction rate, latency, agreement, reopened decisions |

## Reproducibility requirements

Every evaluation run must identify dataset version and license, split manifest, taxonomy, code revision, configuration/rule/model version, seed, environment, metric implementation, and failures/exclusions. Keep site-specific data out of the repository unless it is expressly shareable; publish aggregate or synthetic artifacts where appropriate.

## Decision gates

Advance a capability only when a predefined benchmark target, safety analysis, and recovery procedure are met. Pilot evaluation must be independently reported against a documented baseline and include negative findings.
