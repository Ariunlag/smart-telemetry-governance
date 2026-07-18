# Target State

The target architecture is a governed telemetry pipeline with four accountable layers:

1. **Discovery:** ingest bounded observations from authorized sources and build a durable stream catalog.
2. **Governance:** version observed schemas and approved metric/unit interpretations.
3. **Decision:** produce confidence-aware semantic candidates, abstain when evidence is insufficient, and route material decisions to human review.
4. **Evidence:** retain provenance, quality assessments, reviewer decisions, version identifiers, and reproducible evaluation artifacts.

The authoritative record is a relational governance store plus immutable/raw evidence references. PostgreSQL owns the catalog, governance metadata, provenance, evidence metadata, and observation-delivery outbox. The implemented ADR-004 InfluxDB projection stores normalized accepted observations for time-series use, but is not a catalog and is delivered through the transactional-outbox boundary. Derived views and model outputs must be rebuildable and must never overwrite raw observation or human decision history.

The initial R1 slice implements the [ADR-003](../decisions/ADR-003-r1-stream-catalog-entry-decisions.md) constraints for PostgreSQL catalog authority, bounded evidence, MQTT authorization, stream identity/idempotency, and malformed/redelivery handling. [ADR-004](../decisions/ADR-004-influx-observation-delivery.md) implements the current R1 InfluxDB observation-delivery boundary. InfluxDB is not the catalog, and ChromaDB, Neo4j, and `pgvector` are not used in R1.

Optional graph projection comes after operational graph queries are measured. RAG, agents, causal analysis, federation, clustering, duplicate detection, and anomaly detection remain isolated research candidates, not target runtime dependencies.
