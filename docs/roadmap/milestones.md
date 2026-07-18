# Implementation Roadmap

All milestones are planned unless their status says otherwise. Before coding a future milestone, approve its versioned threshold artifact in `docs/evaluation/thresholds/<milestone>.yaml` (or an equivalent ADR) and add the listed commands to its implementation PR.

## R0A — Repository truth and documentation realignment
### Status
Complete after the review corrections in this PR.
### Objective
Make the public repository truthful, scoped, and implementation-oriented.
### Dependencies
None.
### In scope
Architecture, roadmap, evidence, security, operations, progress, and repository instructions.
### Out of scope
Runtime, infrastructure, dependency, migration, test, and CI changes.
### Likely implementation areas
`README.md`, `AGENTS.md`, `docs/**`.
### Database impact
None.
### API impact
None.
### Required tests
Markdown-link and Mermaid rendering checks when a renderer is selected.
### Edge cases
Provisioned services or placeholders presented as implemented features.
### Acceptance criteria
All changed documentation labels implemented, planned, and optional components; diff contains no runtime files.
### Validation gate
`git diff --check` and changed-path review against `origin/main` complete with no prohibited paths.
### Recovery or rollback
Revert the documentation commit if a verified repository fact is incorrect.
### Evidence produced
Repository inventory and reviewable decision records.

## R0B — Engineering foundation
### Status
Complete and merged through PR #2. CI passed async database lifecycle, migration, backend test, frontend build, and Compose validation checks. R0B establishes an engineering foundation only; it does not establish production readiness, telemetry ingestion, a domain schema, a durable catalog, or governance functionality.
### Objective
Establish safe, repeatable engineering delivery before durable telemetry work.
### Dependencies
R0A.
### In scope
Typed settings, dependency/lock management, lint/type checks, CI, lifecycle, health/readiness, database sessions/migrations, dev consistency, container health checks, structured logging/correlation IDs, and secret foundations.
### Out of scope
MQTT catalog ingestion or governance features.
### Likely implementation areas
Backend settings/lifecycle, dependency manifests, CI, compose health checks, migrations, test configuration, operations docs.
### Database impact
Migration tooling and session foundation only; no governance tables.
### API impact
Defined health/readiness contracts.
### Required tests
Settings validation, lifecycle, readiness, migration smoke, lint/type/build, and CI execution.
### Edge cases
Missing secrets, unavailable database, startup ordering, shutdown during work, stale migrations.
### Acceptance criteria
Approved commands demonstrate a clean checkout can install locked dependencies, run checks, start readiness endpoints, and upgrade/verify a clean database without manual fixes.
### Validation gate
Approve `docs/evaluation/thresholds/r0b.yaml` with commands, supported environments, and recovery evidence before coding.
### Recovery or rollback
Forward-fix failed migrations; retain backups and documented rollback steps.
### Evidence produced
Reproducible engineering-environment and CI reports.

## R1 — Durable MQTT-to-stream-catalog vertical slice
### Status
Initial MQTT-to-PostgreSQL stream-catalog slice merged through PR #5. Broader R1 remains in progress and is not production readiness.
### Objective
Capture authorized MQTT observations into a durable, tenant/site-owned stream catalog.
### Dependencies
R0B and [ADR-003](../decisions/ADR-003-r1-stream-catalog-entry-decisions.md): PostgreSQL catalog authority, retention, MQTT authorization/scope, stream identity/idempotency, and malformed/redelivery handling.
### In scope
Source/subscription registration, credential references, bounded sampling, raw evidence, schema observation, stream identity, and catalog query.
### Out of scope
Multi-protocol ingestion, semantic inference, long-term raw telemetry analytics, and LLM processing.
### Likely implementation areas
MQTT adapter, catalog repository/service, migrations, source/stream routes, integration tests, R1 ADRs.
### Database impact
Tenant/site/source/subscription/run/evidence/stream/schema/provenance records with idempotency boundaries.
### API impact
Authorized source registration, subscription control, stream list/detail, and ingestion-run status contracts.
### Required tests
Broker-backed integration, authorization, idempotent replay, reconnect, retained-message, payload-limit, transaction/outbox, and shutdown tests.
### Edge cases
QoS redelivery, replay, wildcard/high-cardinality topics, malformed payloads, clock skew, DB outage, broker loss.
### Acceptance criteria
Authorized MQTT subscriptions only; a broker restart reconnects within the approved bound; deterministic stream identity and idempotent upserts prevent duplicate stream records from redelivery; malformed payloads are isolated with bounded evidence; catalog records retain provenance timestamps and source identifiers; a migration-backed PostgreSQL schema and API list discovered streams; tests cover reconnect, redelivery, malformed payloads, and concurrent discovery; no LLM or embedding model runs in the ingestion hot path.
### Validation gate
Approve `docs/evaluation/thresholds/r1.yaml` with broker test commands, concrete normalization and identity rules, recovery bounds, retention tests, and the implementation limits deferred by ADR-003 before coding.
### Recovery or rollback
Disable subscription, retain run/audit evidence, quarantine failures, and reprocess bounded samples after recovery.
### Evidence produced
Discovery coverage, ingestion reliability, duplicate/replay, and recovery reports.

### Remaining R1 work
Governed source/subscription registration; real broker-backed integration testing; retained-message behavior; observation delivery/outbox under [ADR-004](../decisions/ADR-004-influx-observation-delivery.md); optional InfluxDB time-series sink; retention enforcement; expanded schema observation; and ingestion-run status and recovery evidence.

## R2 — Schema, metric, and unit governance
### Status
Planned.
### Objective
Version observed schemas and govern metric/unit mappings.
### Dependencies
R1.
### In scope
Schema observations, field mappings, metric vocabulary, unit validation/conversion proposals, and human overrides.
### Out of scope
Universal ontology and destructive automatic conversion.
### Likely implementation areas
Governance contracts, migrations, rule services, review routes, fixtures, and ADRs.
### Database impact
Append-only schema versions, mappings, metric/unit definitions, and superseding decisions.
### API impact
Schema/metric/unit review and effective-mapping endpoints.
### Required tests
Versioning, incompatible changes, units, null/array parsing, locale formats, authorization, and decision rollback.
### Edge cases
Mixed units, unknown metrics, schema drift, conflicting mappings.
### Acceptance criteria
Every effective mapping references observed schema/version and reviewer or rule evidence; ambiguous/invalid units produce `needs_review` and never overwrite historical mappings.
### Validation gate
Approve `docs/evaluation/thresholds/r2.yaml` with mapping test fixtures and accepted unit-validation rules before coding.
### Recovery or rollback
Supersede a mapping, preserve prior decision, and recompute derived views.
### Evidence produced
Versioned mapping corpus, accuracy report, and reviewer agreement.

## R3 — Confidence-aware semantic classification
### Status
Planned.
### Objective
Produce safe semantic candidates with confidence and abstention.
### Dependencies
R1 and approved taxonomy/data contracts from R2.
### In scope
Taxonomy, deterministic/model candidates, calibration, `unknown`/`needs_review`, explanations, and review queue integration.
### Out of scope
Forced labels, autonomous operational action, RAG, and LLM ingestion-path calls.
### Likely implementation areas
Classifier/rules, typed output policy, review APIs, model cards, evaluation fixtures.
### Database impact
Versioned candidates, taxonomy/model/rule versions, confidence, evidence, status, and audit events.
### API impact
Candidate, queue, approve/reject, and audit endpoints.
### Required tests
Schema validation, calibration, threshold/abstention, deterministic replay, adversarial text, authorization, and service-degradation tests.
### Edge cases
Unseen labels, conflicting evidence, distribution shift, low coverage, unavailable model service.
### Acceptance criteria
Every record includes taxonomy, model/rule version, confidence, evidence, timestamp, and status; below-approved-threshold results are `needs_review`; unavailable model services degrade without blocking ingestion.
### Validation gate
Approve `docs/evaluation/thresholds/r3.yaml` with held-out split, calibration method, selective-risk measures, and review policy before coding.
### Recovery or rollback
Withdraw a model/rule version, restore last approved decisions, and replay derived candidates.
### Evidence produced
Calibration, coverage, abstention, correction, and reproducibility reports.

## R4 — Data-quality and provenance assessment
### Status
Planned.
### Objective
Expose quality risk and replayable lineage.
### Dependencies
R1 and relevant R2 contracts.
### In scope
Completeness, timeliness, consistency checks, append-only provenance, evidence links, and remediation status.
### Out of scope
Generic anomaly detection and causal claims.
### Likely implementation areas
Quality rules, provenance store/service, reports, migrations, fixtures.
### Database impact
Append-only provenance, versioned assessments/rules, and remediation records.
### API impact
Quality summary/detail and lineage export contracts.
### Required tests
Late data, missing windows, backfill, rule replay, tamper detection, retention, and failure recovery.
### Edge cases
Sparse streams, unavailable clocks, deleted credentials, partial evidence.
### Acceptance criteria
Every assessment links authoritative evidence, rule version, timestamp, and status; rerunning with the same evidence/rule produces the recorded result or a documented deterministic exception.
### Validation gate
Approve `docs/evaluation/thresholds/r4.yaml` with quality definitions, test data, and replay commands before coding.
### Recovery or rollback
Invalidate derived assessments and rerun from preserved evidence.
### Evidence produced
Quality issue-detection, evidence-completeness, and recovery reports.

## R5 — Human-review workflow
### Status
Planned.
### Objective
Make governance decisions accountable, attributable, and reversible.
### Dependencies
R2–R4 decision contracts.
### In scope
Roles, queues, assignments, rationale, conflicts, escalation, reopening, and audit export.
### Out of scope
Autonomous approval and external partner workflow automation.
### Likely implementation areas
Review UI/routes/services, identity integration, audit migrations, authorization tests.
### Database impact
Review tasks, assignments, comments, append-only decisions, and audit events.
### API impact
Queue, claim, decide, reopen, and export endpoints.
### Required tests
Authorization, tenant isolation, concurrent claim, supersession, audit immutability, and sensitive-comment handling.
### Edge cases
Reviewer absence, conflicts, expired claims, PII in comments.
### Acceptance criteria
Every material effective decision has an attributable review/rule event; reopening supersedes rather than overwrites it; unauthorized users cannot read or change another tenant/site’s tasks.
### Validation gate
Approve `docs/evaluation/thresholds/r5.yaml` with role matrix, audit invariants, and review-time measures before coding.
### Recovery or rollback
Reopen and supersede the decision while preserving complete audit history.
### Evidence produced
Agreement, latency, correction, and audit-reconstruction reports.

## R6 — Reproducible benchmark and evaluation
### Status
Planned; benchmark design may begin after R1.
### Objective
Make claims repeatable with versioned data, baselines, and reports.
### Dependencies
R1 for benchmark design; publication-grade runs depend on the relevant tested capability.
### In scope
Dataset cards, licenses, annotation protocols, splits, baselines, harness, manifests, and negative-result reporting.
### Out of scope
Undisclosed proprietary-data claims.
### Likely implementation areas
`benchmarks/`, `datasets/`, evaluation code, containers, documentation.
### Database impact
Optional immutable experiment metadata only.
### API impact
None required.
### Required tests
Fixed-seed reproduction, schema validation, leakage checks, metric regression, and artifact integrity.
### Edge cases
Licensing restrictions, imbalance, unavailable artifacts, annotation disagreement.
### Acceptance criteria
A clean documented environment reproduces a named run from manifest and reports deviations; every result identifies data, split, code, configuration, and exclusions.
### Validation gate
Approve `docs/evaluation/thresholds/r6.yaml` with reproduction commands and tolerance policy before implementation.
### Recovery or rollback
Retract invalid run, preserve its manifest, and publish corrected run identity.
### Evidence produced
Benchmark, baseline, reproducibility, negative-result, and limitation artifacts.

## R7 — Production hardening
### Status
Planned.
### Objective
Prepare the validated R1–R5 core for controlled deployment.
### Dependencies
Validated R1–R5 core and R6 methods relevant to reliability claims.
### In scope
Identity, secrets, TLS, limits, observability, backup/restore, security assurance, SLOs, and operational exercises.
### Out of scope
Any production-readiness claim before evidenceable controls pass.
### Likely implementation areas
Runtime/config/deployment/security changes, migrations, CI, and operations runbooks.
### Database impact
Retention, backup/restore, migration, and access policies.
### API impact
Authenticated administrative, health/readiness, and audit-access contracts.
### Required tests
Security, authorization, load, failure injection, restore, migration, shutdown, and rollback exercises.
### Edge cases
Broker outage, credential rotation, connection exhaustion, partial write, clock failure, dependency loss.
### Acceptance criteria
Named release-blocking checklist artifacts demonstrate backup restoration, rollback, dependency degradation, authorization, and load/failure exercises with approved outcomes.
### Validation gate
Approve `docs/evaluation/thresholds/r7.yaml` and accountable-owner release checklist before coding.
### Recovery or rollback
Documented deployment rollback, backup restore, credential revocation, and forward recovery.
### Evidence produced
Hardening, security, SLO, incident, and recovery exercise reports.

## R8 — Independent pilot validation
### Status
Planned.
### Objective
Evaluate the framework with independent operational users.
### Dependencies
R6 evaluation readiness and applicable R7 deployment controls.
### In scope
Protocol, agreements, baseline, independent evaluator, site deployment, acceptance/stopping criteria, and limitations.
### Out of scope
Claims of broad impact, federation, or generalization beyond evidence.
### Likely implementation areas
Pilot protocol, scoped deployment configuration, aggregate reports, data-governance records.
### Database impact
Site-local governed records subject to approved retention/deletion.
### API impact
Site-scoped access only if necessary.
### Required tests
Protocol dry run, access review, pilot acceptance, restore, and withdrawal tests.
### Edge cases
Withdrawal, data-sharing restrictions, outage, taxonomy mismatch, evaluator conflict.
### Acceptance criteria
An independent evaluator signs or authors a preregistered result report that includes baseline comparison, deviations, limitations, and data disposition.
### Validation gate
Approve `docs/evaluation/thresholds/r8.yaml`, data agreements, evaluator independence, and stopping rules before deployment.
### Recovery or rollback
Revoke access, return/delete data per agreement, recover deployment, and archive permitted evidence.
### Evidence produced
Independent pilot report and permissioned aggregate artifacts.

## R9 — Optional knowledge-graph projection
### Status
Optional; not on the critical path.
### Objective
Evaluate a rebuildable projection for validated operational graph queries.
### Dependencies
Named user queries and an approved need after R8 evidence.
### In scope
Query inventory, derived projection, consistency checks, baseline comparison, and cost/benefit study.
### Out of scope
Neo4j or graph infrastructure as a default dependency.
### Likely implementation areas
ADR, projection worker, read-only queries, query tests.
### Database impact
Derived/rebuildable projection only.
### API impact
Read-only validated graph-query endpoints.
### Required tests
Projection replay, consistency, isolation, query usefulness, and disable/rebuild tests.
### Edge cases
Stale projection, growth, misleading inferred relations, retention deletion.
### Acceptance criteria
Named users, a catalog baseline, and measured approved improvement are documented before a projection becomes a supported feature.
### Validation gate
Approved ADR and `docs/evaluation/thresholds/r9.yaml` define queries, baseline, measures, and exit criteria before coding.
### Recovery or rollback
Disable the projection and rebuild from authoritative records.
### Evidence produced
Query utility, cost, consistency, and limitation report.

## R10 — Optional research modules
### Status
Optional; not on the critical path.
### Objective
Evaluate deferred capabilities only after evidence identifies an unmet need.
### Dependencies
Approved ADR, R6 reproducibility controls, and evidence of need; operational experiments also require applicable R7/R8 controls.
### In scope
Isolated experiments for duplicates, clustering, RAG, agents, causal inference, federation, or anomaly detection.
### Out of scope
Production endpoints/dependencies without a separate approved milestone.
### Likely implementation areas
Isolated experiment directories, ADRs, datasets, and reports.
### Database impact
Experiment-owned, disposable derived stores.
### API impact
No production API by default.
### Required tests
Reproducibility, safety, cost, privacy, and comparison to core baseline.
### Edge cases
Hallucination, automation overreach, privacy leakage, distribution shift, uncontrolled cost.
### Acceptance criteria
An ADR records user need, baseline, measurable gain, risks, costs, exit criteria, and disposition before promotion is considered.
### Validation gate
Approve a module-specific threshold artifact and isolation plan before coding.
### Recovery or rollback
Retire experiment and delete derived data under retention policy.
### Evidence produced
Positive or negative experiment report.
