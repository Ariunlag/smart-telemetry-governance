# Vision

Develop and validate an AI-assisted telemetry governance framework that inventories, classifies, validates, traces, and operationalizes heterogeneous IoT and OT telemetry streams for critical and public infrastructure environments.

The near-term product is a governed stream catalog, not a general AI platform. It must make a stream's source, observed schema, metric and unit interpretation, quality evidence, semantic decision, confidence, reviewer action, and lineage inspectable and reproducible.

## Design principles

- Treat unobserved or ambiguous meaning as an abstention, not a confident label.
- Keep raw evidence distinct from inferred metadata and human decisions.
- Make every material decision traceable to inputs, rules or model version, and reviewer action.
- Measure performance on versioned, representative data before claiming operational value.
- Design for safe failure, review queues, and recovery from incorrect metadata.

## Scope boundaries

Immediate work covers discovery, schema/metric/unit governance, semantic classification, confidence and abstention, quality, provenance, human review, evaluation, hardening, and pilot validation. Duplicate detection, generic clustering, RAG, agents, causal analysis, federation, generic anomaly detection, and graph infrastructure are research options, not current dependencies. Knowledge graphs are a possible later projection after concrete operational graph queries are proven.

Success means independently evaluated governance decisions that users can inspect and correct—not a collection of speculative AI features.
