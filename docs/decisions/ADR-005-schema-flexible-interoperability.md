# ADR-005: Schema-Flexible Telemetry Interoperability

## Status

Accepted architectural boundary; implementation is planned, not complete.

## Context

Heterogeneous telemetry sources use inconsistent protocols, payload structures, field names, units, identifiers, and semantic meanings. MQTT is the first validated source adapter, but it is not the final system boundary. A single global telemetry payload schema would reject useful evidence or silently discard source meaning before it can be governed.

## Decision

### Preserve source-native evidence

Raw source evidence remains authoritative. The target flow is:

```text
Raw source evidence
    → Observed schema and field profile
    → Versioned deterministic or AI-assisted mapping
    → Derived canonical observation
```

Canonical observations are derived interpretations, never replacements for original payloads, field paths, source units, source timestamps, transport metadata, or mapping/model versions. Reprocessing must be possible whenever a mapping or model changes.

### Keep the control plane strict and the data plane flexible

Tenants, sites, sources, collection rules/subscriptions, ingestion runs, schema versions, mapping definitions, review decisions, provenance records, delivery state, retries, and dead letters remain strongly typed and database-constrained.

Source payloads are accepted without a universal schema. A target protocol-neutral envelope contains an observation identifier; tenant, site, and source identifiers; source type; external stream identifier; received time; content type; payload reference or hash; and transport metadata. MQTT topics and broker metadata are adapter-owned transport details, not required fields of the generic contract.

### Observe and version structures

The target interpretation plane includes `ObservedSchema`, `ObservedField`, `FieldProfile`, `SchemaFingerprint`, `SchemaVersion`, `SchemaDriftEvent`, `MetricConcept`, `UnitConcept`, `MappingCandidate`, `MappingRule`, `MappingVersion`, `SemanticRecommendation`, and `ReviewDecision`. Fingerprints, field paths, and inferred types must be deterministic and bounded. Schema metadata must not copy sensitive or unbounded source payload content.

### Govern semantic recommendations

Semantic correspondence considers field name/path, type, unit hints, sample-value profile, neighboring fields, source/device metadata, descriptions, and historical behavior. It is not a generic same/not-same decision: schema-field correspondence, metric classification, unit interpretation, stream duplication, and class membership are separate tasks.

AI is not authoritative and is not implemented. Where deterministic mappings are unavailable, a future semantic layer may retrieve and rank candidates, return calibrated confidence and bounded evidence, abstain when evidence is insufficient, and route uncertain cases to human review. A conceptual recommendation records field profile, candidate concept, rank, confidence, model-or-rule version, evidence summary, recommendation/abstention decision, review status, and creation time. Approved decisions should become versioned deterministic mapping rules where appropriate.

## Consequences

R2 must introduce a source-adapter contract and lifecycle manager, migrate MQTT to that generic boundary, implement observed schema/version/drift records, and validate the boundary with a second source adapter. R3 may add metric/unit vocabularies, deterministic mappings, semantic candidate retrieval/ranking, confidence, abstention, review, and mapping-version provenance only after evaluation criteria are approved.

PostgreSQL remains authoritative for governance and delivery state. InfluxDB remains a derived normalized time-series projection. This decision does not add models, migrations, adapters, APIs, AI models, embeddings, or frontend behavior.

## Alternatives considered

- Impose one global schema at ingestion: rejected because it loses heterogeneous source evidence and makes drift handling brittle.
- Let AI rewrite evidence directly: rejected because interpretations must remain traceable, reversible, and reviewable.
- Treat MQTT as the universal ingestion contract: rejected because transport-specific metadata does not define multi-source interoperability.
