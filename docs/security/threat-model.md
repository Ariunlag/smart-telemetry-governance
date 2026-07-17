# Threat Model

## Assets

Source credentials, broker connectivity, telemetry observations, schema/semantic decisions, review identities, audit evidence, model/rule artifacts, and evaluation datasets require protection.

## Primary threats and controls

| Threat | Risk | Required controls before deployment |
|---|---|---|
| Unauthorized broker/source access | Confidentiality and integrity loss | least-privilege credentials, TLS, rotation, source allowlists |
| Poisoned or malformed telemetry | Incorrect catalog/decisions or resource exhaustion | size/rate limits, schema quarantine, provenance, bounded sampling |
| Misclassification treated as fact | Unsafe downstream use | confidence calibration, abstention, review gates, reversible decisions |
| Audit tampering | Loss of accountability | append-only events, access logging, integrity checks, backups |
| Excessive data retention | Privacy/contract breach | minimization, retention schedules, deletion verification |
| UI/API authorization failure | Unauthorized changes | authenticated roles, server-side authorization, CSRF/CORS review |
| Dependency or deployment compromise | System compromise | pinned releases, scanning, secret isolation, patch process |

Current code has no production authentication, secret management, durable audit trail, or hardened deployment. This document is a target control baseline, not a claim that controls exist.
