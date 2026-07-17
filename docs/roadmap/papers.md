# Paper Roadmap

All papers are proposed. They require ethically shareable or appropriately governed data, preregistered evaluation choices where practical, and released code/configuration sufficient to reproduce reported results.

## 1. Benchmark and Reference Architecture for Governing Heterogeneous IoT Telemetry Streams

- **Research question:** Can a reference architecture and benchmark make stream inventory, normalization, and governance outcomes comparable across heterogeneous telemetry sources?
- **Technical contribution:** Versioned reference architecture, catalog task definitions, annotations, and baseline implementations.
- **Datasets:** De-identified public or permissioned IoT/OT traces with source, schema, unit, and lineage annotations.
- **Baselines:** Rule-only normalization, manually curated catalog, and simple field-name matching.
- **Evaluation metrics:** Discovery precision/recall, schema mapping accuracy, unit validity, catalog coverage, latency, and annotation agreement.
- **Reproducibility artifact:** Dataset cards, split manifests, containers, baseline code, and evaluation reports.
- **Limitations:** Site heterogeneity, incomplete ground truth, and restricted sharing of operational data.

## 2. Confidence-Aware Semantic Classification of IoT Telemetry with Human Review

- **Research question:** Does calibrated classification with abstention and review improve semantic labeling safety over forced predictions?
- **Technical contribution:** Confidence calibration, abstention policy, and reviewer-feedback protocol for telemetry classes.
- **Datasets:** The benchmark corpus plus held-out site or source families.
- **Baselines:** Keyword rules, forced classifier, nearest-neighbor classifier, and manual-only review.
- **Evaluation metrics:** Macro-F1, calibration error, selective risk/coverage, abstention precision, review rate, and correction time.
- **Reproducibility artifact:** Versioned labels, model cards, seeds, prompts/configuration if applicable, and review-event schema.
- **Limitations:** Label taxonomy drift, reviewer variability, and domain transfer uncertainty.

## 3. Automated Data Quality and Provenance Assessment for Operational Telemetry

- **Research question:** Can explicit quality and lineage evidence identify governance risk without masking missing or unreliable observations?
- **Technical contribution:** Evidence-based quality dimensions linked to immutable provenance events and recovery actions.
- **Datasets:** Time-windowed operational traces with injected and naturally occurring completeness, timeliness, and consistency issues.
- **Baselines:** Threshold checks, freshness-only checks, and unlinked quality summaries.
- **Evaluation metrics:** Issue detection precision/recall, time-to-detection, evidence completeness, false-review rate, and recovery verification rate.
- **Reproducibility artifact:** Rule packs, incident scenarios, trace manifests, and report generator.
- **Limitations:** Accuracy often lacks an external reference; synthetic faults may differ from field failures.

## 4. Cross-Site Evaluation of Telemetry Governance in Smart Infrastructure Environments

- **Research question:** Which governance outcomes transfer across sites and which require local configuration or review?
- **Technical contribution:** Independent pilot protocol and cross-site analysis of coverage, calibration, review burden, and operational reliability.
- **Datasets:** Separately governed pilot datasets, with site-specific data agreements and no private data committed here.
- **Baselines:** Site-specific rules, shared rules, and no-governance workflow.
- **Evaluation metrics:** Per-site and pooled task metrics, calibration, reviewer agreement, failure recovery time, and operator acceptance.
- **Reproducibility artifact:** Pilot template, anonymized aggregates where permitted, analysis plan, and environment manifests.
- **Limitations:** Small number of sites, non-random participation, and confidentiality constraints.

## 5. Optional domain-specific extension selected after pilot evidence

- **Research question:** To be selected from an observed pilot limitation rather than assumed in advance.
- **Technical contribution:** A narrowly scoped extension with a documented decision rationale.
- **Datasets:** Pilot-approved domain data and a defined holdout set.
- **Baselines:** The validated core workflow and the best relevant domain baseline.
- **Evaluation metrics:** Predefined by the selected problem, including safety and review burden where relevant.
- **Reproducibility artifact:** Selection record, protocol, code/configuration, and limits on data release.
- **Limitations:** Domain specificity and dependence on pilot evidence.
