# Target State

The target architecture is a governed telemetry pipeline with four accountable layers:

1. **Discovery:** ingest bounded observations from authorized sources and build a durable stream catalog.
2. **Governance:** version observed schemas and approved metric/unit interpretations.
3. **Decision:** produce confidence-aware semantic candidates, abstain when evidence is insufficient, and route material decisions to human review.
4. **Evidence:** retain provenance, quality assessments, reviewer decisions, version identifiers, and reproducible evaluation artifacts.

The authoritative record is a relational governance store plus immutable/raw evidence references; time-series storage is selected only when needed by a validated slice. Derived views and model outputs must be rebuildable and must never overwrite raw observation or human decision history.

Before R1 implementation, the entry decisions are recorded in [ADR-003](../decisions/ADR-003-r1-stream-catalog-entry-decisions.md): PostgreSQL is the authoritative stream catalog; durable catalog metadata and bounded observation evidence are separated from raw-payload retention; and MQTT authorization, stream identity/idempotency, and malformed/redelivery handling are constrained. Concrete implementation limits remain for the R1 threshold artifact. InfluxDB is not the catalog, and ChromaDB, Neo4j, and `pgvector` are not used in R1. None of these decisions is implemented by the current repository.

Optional graph projection comes after operational graph queries are measured. RAG, agents, causal analysis, federation, clustering, duplicate detection, and anomaly detection remain isolated research candidates, not target runtime dependencies.
