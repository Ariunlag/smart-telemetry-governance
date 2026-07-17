# Paper Roadmap

All papers are proposed. They require appropriately governed data, an approved annotation protocol, reproducibility artifacts, and explicit limitations. Publication order follows evidence availability, not aspiration.

## 1. Benchmark and Reference Architecture for Governing Heterogeneous IoT Telemetry Streams

- **Prerequisite milestones:** R0B, R1, and R6 benchmark-design controls.
- **Research question / contribution:** Can a reference architecture and benchmark make stream inventory, normalization, and governance outcomes comparable across heterogeneous sources?
- **Datasets / baselines / metrics:** De-identified public or permissioned traces; rule-only normalization, curated catalog, and field-name matching; discovery precision/recall, mapping accuracy, unit validity, coverage, latency, and annotation agreement.
- **Ground-truth or annotation protocol:** Domain-qualified annotators define labels using versioned guidelines; independent double annotation, disagreement adjudication, inter-rater agreement, leakage checks, split isolation, and explicit `unknown`/ambiguous outcomes are required.
- **Reproducibility artifact / limitations:** Dataset cards, licenses, split manifests, containers, baseline code, reports; limitations include heterogeneity, incomplete truth, and restricted sharing.
- **Operational grounding:** Permissioned data and named data-governance basis are required; operational claims wait for later pilots.

## 2. Confidence-Aware Semantic Classification of IoT Telemetry with Human Review

- **Prerequisite milestones:** R2, R3, R5, and R6.
- **Research question / contribution:** Does calibrated classification with abstention and review improve safe semantic labeling over forced prediction?
- **Datasets / baselines / metrics:** Benchmark corpus plus held-out source families; keyword rules, forced classifier, nearest-neighbor, and manual-only review; macro-F1, calibration error, selective risk/coverage, abstention precision, review rate, correction time.
- **Ground-truth or annotation protocol:** Taxonomy stewards and qualified domain reviewers use versioned guidance; double-label a defined sample, adjudicate disagreement, report agreement, prevent train/test leakage, and preserve `unknown`/ambiguous cases.
- **Reproducibility artifact / limitations:** Labels, model cards, seeds, configuration, review-event schema, and evaluation harness; limits include taxonomy drift, reviewer variability, and domain transfer.
- **Operational grounding:** Operator review workflow is required; no deployment or cross-site claim without R8 evidence.

## 3. Automated Data Quality and Provenance Assessment for Operational Telemetry

- **Prerequisite milestones:** R1, R2, R4, R5, and R6.
- **Research question / contribution:** Can versioned quality and lineage evidence identify governance risk without masking missing or unreliable observations?
- **Datasets / baselines / metrics:** Windowed operational traces with permissioned natural and documented injected faults; threshold, freshness-only, and unlinked-quality baselines; issue precision/recall, detection time, evidence completeness, false-review rate, recovery verification.
- **Ground-truth or annotation protocol:** Qualified operators/rule owners classify issue windows using versioned fault definitions; record disagreement, adjudication, leakage prevention, and unknown cases.
- **Reproducibility artifact / limitations:** Rule packs, incident scenarios, manifests, report generator; accuracy may lack an external reference and synthetic faults may differ from field failures.
- **Operational grounding:** Requires permissioned operational data and operator validation; does not make causal or production-wide claims.

## 4. Cross-Site Evaluation of Telemetry Governance in Smart Infrastructure Environments

- **Prerequisite milestones:** A first independently evaluated R8 pilot; preregistered cross-site protocol; approved data agreements; at least one defined site-transfer question; R6 and applicable R7 controls.
- **Research question / contribution:** Which governance outcomes transfer across sites and which need local configuration or review?
- **Datasets / baselines / metrics:** Separately governed pilot data; site-specific rules, shared rules, no-governance baseline; per-site/pooled task measures, calibration, review agreement, recovery time, operator acceptance, configuration burden.
- **Ground-truth or annotation protocol:** Site-qualified annotators follow a common versioned protocol; local deviations, agreement, adjudication, split isolation, and ambiguous outcomes are reported per site.
- **Reproducibility artifact / limitations:** Protocol, analysis plan, environment manifests, permitted anonymized aggregates; limited sites, non-random participation, confidentiality, and no generalization beyond observed transfer questions.
- **Operational grounding:** Requires real pilot data, operators, independent evaluators, permissions, and explicit site-specific limitations.

## 5. Optional domain-specific extension selected after pilot evidence

- **Prerequisite milestones:** R8 evidence, R6 reproducibility, and an approved R10 ADR.
- **Research question / contribution:** Selected from an observed pilot limitation, with the selection rationale published before experimentation.
- **Datasets / baselines / metrics:** Pilot-approved data and defined holdout; validated core workflow plus best relevant domain baseline; pre-approved task, safety, review-burden, and cost measures.
- **Ground-truth or annotation protocol:** Defined before data use with qualified annotators, guidelines, disagreement resolution, agreement reporting, versioning, leakage prevention, and unknown handling.
- **Reproducibility artifact / limitations:** Selection record, protocol, code/configuration, permitted artifacts; domain specificity and pilot dependence are explicit.
- **Operational grounding:** Requires the appropriate operators, independent evaluators, permissioned data, and limits on operational conclusions.
