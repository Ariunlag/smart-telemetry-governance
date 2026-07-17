# Evidence Plan

Technical performance alone does not establish broad public impact, adoption, or cross-sector applicability. All entries below are planned evidence, not claims that artifacts currently exist.

| Claim area | Claim | Evidence required | Metric or observable output | Evidence owner | Current status | Decision gate |
|---|---|---|---|---|---|---|
| Technical effectiveness | Discovery, schema/metric/unit governance, classification, calibration, quality detection, and review correction improve over baselines | Versioned datasets, annotation protocol, held-out splits, baseline manifests | Coverage; mapping/classification accuracy; calibration/selective risk; detection and correction measures | Evaluation lead | Planned | R6 threshold artifact approved |
| Production reliability | The governed path behaves predictably under load and failure | Load, p95/p99 latency, reconnect, replay/duplicate, dependency-outage, backup-restore, data-loss reports | Throughput, latency, recovery, duplicate rate, loss accounting, restore integrity | Operations lead | Planned | R7 release evidence approved |
| Cross-site/cross-sector applicability | Performance and configuration burden transfer beyond one source/site | Held-out source families, separately configured sites, portability log, per-site reports | Per-site measures, taxonomy transfer, configuration effort, limitations | Independent evaluation lead | Planned | R8 protocol and agreements approved |
| Independent interest and adoption | Independent parties evaluate, use, or critique the framework | Independently run evaluations, pilot participation, documented technical interest, issues/contributions, repeat-use and operator-acceptance records | Signed/authored evaluation, participation, accepted use, external reports | Project owner | Planned | Evidence reviewed without treating interest as impact proof |
| Public-infrastructure need alignment | Capabilities address documented public-infrastructure needs | Authoritative public sources, traceable need-to-capability mapping, capability evidence, limitations | Versioned source register and mapping review | Research lead | Planned | Sources and limitations approved before public claim |

## Reproducibility and governance

Each run must identify dataset version/license, annotation and ground-truth protocol, split manifest, taxonomy, code revision, configuration/rule/model version, seed, environment, metric implementation, failures/exclusions, data-governance basis, and evaluator independence. Keep non-shareable site data outside this repository; publish approved aggregate or synthetic artifacts where possible.

## Decision gates

No capability advances on metric results alone. Its approved threshold artifact must define baseline, held-out evaluation, failure/negative-result handling, recovery procedure, and accountable evidence owner. Pilot and cross-site claims require independent evaluation, permissions, and explicit generalization limits.
