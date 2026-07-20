# Target State

The target architecture is a governed telemetry pipeline with four accountable layers:

1. **Discovery:** ingest bounded observations from authorized sources and build a durable stream catalog.
2. **Governance:** version observed schemas and approved metric/unit interpretations.
3. **Decision:** produce confidence-aware semantic candidates, abstain when evidence is insufficient, and route material decisions to human review.
4. **Evidence:** retain provenance, quality assessments, reviewer decisions, version identifiers, and reproducible evaluation artifacts.

The authoritative record is a relational governance store plus immutable/raw evidence references. PostgreSQL owns the catalog, governance metadata, provenance, evidence metadata, and observation-delivery outbox. The implemented ADR-004 InfluxDB projection stores normalized accepted observations for time-series use, but is not a catalog and is delivered through the transactional-outbox boundary. Derived views and model outputs must be rebuildable and must never overwrite raw observation or human decision history.

The initial R1 slice implements the [ADR-003](../decisions/ADR-003-r1-stream-catalog-entry-decisions.md) constraints for PostgreSQL catalog authority, bounded evidence, MQTT authorization, stream identity/idempotency, and malformed/redelivery handling. [ADR-004](../decisions/ADR-004-influx-observation-delivery.md) implements the current R1 InfluxDB observation-delivery boundary. InfluxDB is not the catalog, and ChromaDB, Neo4j, and `pgvector` are not used in R1.

## Schema-flexible interoperability target

MQTT is the first validated source adapter, not the final system boundary. Transport interoperability supports source-specific adapters such as MQTT, REST, files, Kafka, and OPC-UA. Structural interoperability handles JSON, CSV, nested fields, scalar messages, schema drift, and changing types through deterministic schema observation and versioning. Semantic interoperability evaluates whether names such as `temp`, `temperature`, `temp_c`, `airTemp`, and `T_AMB` correspond in their source context. Governance interoperability records evidence, observed schema version, mapping or model version, candidate concept, confidence, decision status, and human review where required.

The control plane remains strict and database-constrained: tenants, sites, sources, collection rules or subscriptions, ingestion runs, schema versions, mapping definitions, review decisions, provenance records, delivery state, retry state, and dead-letter state. The telemetry data plane remains flexible: source payloads need not match a global schema before acceptance. A protocol-neutral raw observation envelope conceptually contains `observation_id`, tenant/site/source identifiers, `source_type`, `external_stream_id`, `received_at`, `content_type`, a payload reference or hash, and transport metadata. These are target-state concepts, not final columns or implemented contracts.

The interpretation plane will version `ObservedSchema`, `ObservedField`, `FieldProfile`, `SchemaFingerprint`, `SchemaVersion`, `SchemaDriftEvent`, `MetricConcept`, `UnitConcept`, `MappingCandidate`, `MappingRule`, `MappingVersion`, `SemanticRecommendation`, and `ReviewDecision`. The authoritative flow is: raw source evidence → observed schema and field profile → versioned deterministic or AI-assisted mapping → derived canonical observation. Raw evidence remains authoritative; canonical observations must retain source field path, source unit representation, source timestamp, transport metadata, and mapping/model version so mappings and models can be reprocessed without replacing evidence.

## Semantic recommendation boundary

AI is not an authoritative decision-maker and is not implemented. Where deterministic mappings are unavailable, a future semantic layer may retrieve and rank candidate metric concepts using field name/path, type, unit hints, sample-value profile, neighboring fields, source/device metadata, descriptions, and historical behavior. Its conceptual result records `field_profile_id`, `candidate_concept_id`, rank, calibrated confidence, model-or-rule version, bounded evidence summary, decision (`recommended`, `review_required`, or `abstained`), review status, and creation time. It must abstain when evidence is insufficient and route uncertain results to human review.

The decision hierarchy is approved exact mapping; approved source-specific rule; unit and structural constraints; semantic candidate retrieval; context-aware ranking; confidence policy; then human review or abstention. Approved recommendations should become versioned deterministic mapping rules where appropriate. Schema-field correspondence, metric classification, unit interpretation, stream duplication, and class membership remain separate tasks rather than one generic similarity decision. The architecture must remain vendor-neutral and may support open-source local embedding or classification models later.

Optional graph projection comes after operational graph queries are measured. RAG, agents, causal analysis, federation, clustering, duplicate detection, and anomaly detection remain isolated research candidates, not target runtime dependencies.
