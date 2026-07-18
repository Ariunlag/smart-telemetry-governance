# ADR-003: R1 Stream-Catalog Entry Decisions

## Status

Accepted R1 entry gate. The initial constraints were implemented through PR #5; broader R1 work remains in progress.

## Context

R1 is the first durable telemetry vertical slice. It must establish a bounded, authorized MQTT discovery path and an authoritative stream catalog without treating provisioned local services as application integrations or adding AI processing to ingestion.

## Decision

### 1. Authoritative stream catalog

PostgreSQL is the authoritative durable store for stream identity, metadata, lifecycle status, schema observations, provenance, review state, and delivery state. InfluxDB is not the stream catalog; the implemented ADR-004 boundary stores accepted normalized observations there only for time-series queries. ChromaDB and Neo4j are not used in R1.

### 2. Observation retention

R1 will keep durable catalog metadata indefinitely unless a future governed deletion workflow explicitly removes it. It will separately retain bounded observation samples for discovery, schema inference, debugging, and audit evidence; it will not retain every raw telemetry message in PostgreSQL. Retention limits must be configuration-driven, not hardcoded. Production values require validation and policy review before deployment.

### 3. MQTT authorization and subscription scope

Subscriptions must use an explicit configured allowlist and must not default to unrestricted `#`. Wildcards are permitted only when explicitly configured and documented. Broker credentials may come only from environment configuration or secret management; anonymous production connections are prohibited. TLS verification must be configurable and enabled in production profiles. Authorization failures must fail clearly without exposing credentials.

### 4. Stream identity and idempotency

A stream has a deterministic identity derived from normalized source context and normalized MQTT topic, with an optional configured tenant or namespace. Payload values are not primary stream identity. R1 must specify canonical normalization rules, generate a stable deterministic stream key, enforce it with a unique database constraint, and use idempotent upserts. Reconnects and repeated observations update or attach evidence to the same stream rather than create a new record. Stream identity is distinct from individual message identity.

### 5. Malformed payload and redelivery handling

Malformed payloads must not crash the ingestion worker. R1 must preserve bounded failure evidence without logging secrets or excessive payload content, and classify outcomes as accepted, malformed, unsupported encoding, oversized, or rejected. Redelivered messages must not create duplicate stream records. Message-level deduplication should use reliable broker metadata and a bounded fallback fingerprint when needed. R1 targets at-least-once processing with idempotent durable writes, makes no exactly-once claim, retries only transient failures, and routes permanent failures to structured rejection evidence.

## Consequences

R1 implementation must add migration-backed PostgreSQL schema and tests before it can claim a stream catalog. It must provide authorized subscriptions, restart/reconnect recovery, deterministic stream identity, idempotent upserts, redelivery safety, malformed-payload isolation, bounded observation evidence, provenance timestamps and source identifiers, and an API to list discovered streams. Tests must cover reconnect, redelivery, malformed payloads, and concurrent discovery. No LLM or embedding model may be in the ingestion hot path.

## Unresolved implementation details

R1 must select and document concrete configuration defaults and limits, topic-normalization syntax, broker metadata reliability rules, fallback-fingerprint window, retry/backoff bounds, and the API contract. Those details must satisfy this ADR and the approved R1 threshold artifact; they are not implemented by this decision.
