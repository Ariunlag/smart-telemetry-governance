# ADR-004: Influx Observation Delivery Boundary

## Status

Implemented for the current R1 delivery boundary. This decision governs the PostgreSQL outbox to InfluxDB 2.x implementation; it does not establish production readiness.

## Context

The merged initial R1 slice uses PostgreSQL as the authoritative stream catalog and keeps bounded observation evidence. A later R1 capability may support normalized time-series queries, visualizations, quality windows, and reproducible analysis. It must not turn InfluxDB into a stream catalog or weaken the PostgreSQL transaction boundary.

## Decision

### Authoritative ownership and eligibility

PostgreSQL remains authoritative for stream identity, catalog metadata, schema and governance metadata, provenance, evidence metadata, and delivery/outbox state. InfluxDB may store normalized timestamped observations only for time-series queries, visualization, quality-assessment windows, and later benchmark or research analysis. InfluxDB is not the stream catalog.

Only observations classified as `accepted` are candidates for delivery. Malformed, unsupported, oversized, and rejected payloads remain bounded PostgreSQL evidence and must not be written as normal telemetry points.

### Normalized observation contract

Each delivery candidate must contain `stream_id`, `source_id`, optional `tenant`, normalized topic, event timestamp, received timestamp, metric name, optional unit, typed value, content/schema version, quality status, and a provenance reference. The InfluxDB measurement name must be stable and low-cardinality; raw MQTT topics must not be measurement names.

Low-cardinality tags may include stream ID, source ID, tenant, metric, and unit. Typed values are fields. High-cardinality values, raw payloads, credentials, partner-identifying values, and sensitive operational values are prohibited as tags.

### Timestamp and provenance policy

The selected observation timestamp uses this precedence: validated source event timestamp, then a reliable broker timestamp, then the platform received timestamp. Provenance must record the selected source. Invalid, missing, or future-skewed timestamps fall through to the next valid source; if no supplied timestamp is valid, delivery uses the received timestamp and records that fallback. The skew limits and validation rules require configuration and policy review before implementation.

### Transactional outbox and delivery states

MQTT ingestion must not directly dual-write to PostgreSQL and InfluxDB. The PostgreSQL catalog transaction creates an observation-delivery outbox record, and a separate worker delivers pending records to InfluxDB. Outbox states are `pending`, `processing`, `delivered`, `retryable`, and `dead_letter`.

An InfluxDB outage must not stop MQTT discovery, PostgreSQL catalog updates, or bounded evidence recording. The worker uses bounded exponential backoff, records retry and dead-letter transitions, exposes operator-visible delivery status, and supports replay after recovery.

### Idempotency and retention

The delivery key combines stream identity, selected timestamp, payload or normalized-point fingerprint, and metric identity. Repeated MQTT delivery and worker retry must not create unintended duplicate logical observations. This is at-least-once delivery with idempotent writes; it makes no global exactly-once claim.

PostgreSQL catalog metadata is durable. PostgreSQL bounded evidence is configuration-driven. PostgreSQL outbox records are retained according to delivery and audit needs. InfluxDB observations use a bucket retention policy, and raw source-system data remains source-owned. Production retention periods must not be hardcoded without evidence and policy review.

### Security and evidence boundary

InfluxDB URL, token, organization, and bucket must come from environment configuration or secret management. Tokens and complete payloads must not be logged. Non-local environments require TLS verification.

The future sink should produce measurable technical evidence: delivery success rate, retry and recovery time, duplicate-write rate, timestamp validity rate, schema coverage, observation throughput, and quality-rule evaluation coverage. Public documentation remains technical and does not make non-technical adjudicative claims.

## Consequences

The implemented boundary has a normalized point contract, migration-backed outbox state, lifecycle-managed worker recovery behavior, PostgreSQL and real InfluxDB integration coverage, and a read-only delivery-status endpoint. Remaining R1 work includes governed source/subscription registration, retention enforcement, real broker-backed integration tests, retained-message behavior, expanded schema observation, and ingestion-run recovery evidence. AI classification, embeddings, duplicate stream detection, clustering, RAG, agents, causal inference, and full raw-payload archival are explicitly deferred and are not part of this decision.
