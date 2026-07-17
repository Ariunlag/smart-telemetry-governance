# Repository Engineering Instructions

## Repository inspection

Before modifying implementation, read applicable repository instructions and inspect the current code, tests, configuration, migrations, APIs, and documentation. Verify files, libraries, commands, and behavior rather than assuming they exist. Clearly distinguish implemented, proposed, and experimental components.

## Milestone discipline

- Complete one milestone per task and keep one independently reviewable scope per branch or pull request.
- Do not implement future milestones automatically.
- Do not perform unrelated refactoring, renaming, formatting, or dependency upgrades.
- Keep diffs small enough to review and record unexpected scope expansion before implementing it.

## Contracts and compatibility

- Use typed request, response, domain, event, and persistence contracts.
- Validate untrusted input at system boundaries.
- Preserve backward compatibility unless a documented migration explicitly changes a contract.
- Version schemas and inferred decisions; never silently mutate historical evidence.

## Database and migration safety

- Use migration tooling for every schema change; make migrations forward-safe and backward-compatible where practical.
- Test migrations against representative existing data and define rollback or forward-recovery behavior.
- Never delete or recreate user data merely to simplify development.
- Add indexes only for documented query patterns.

## Reliability

Every external dependency and background operation must define applicable timeouts, retry, exponential backoff with jitter, connection limits, idempotency, duplicate-delivery behavior, failure visibility, dead-letter or recovery behavior, and graceful startup/shutdown.

## MQTT behavior

MQTT work must consider QoS redelivery, retained messages, reconnects, out-of-order messages, missing or invalid timestamps, device clock skew, malformed and oversized payloads, schema drift, high-cardinality topics, replay, backpressure, and broker outages. Do not imply exact-once MQTT delivery.

## AI and classification safety

- Do not put an LLM in the MQTT ingestion hot path; prefer explicit metadata and deterministic rules before model calls.
- Model output is derived evidence, not authoritative truth. Return `unknown` or `needs_review` when evidence is insufficient.
- Record model, rule, and taxonomy versions, confidence, evidence, timestamp, and status; validate all model output against typed schemas.
- Protect against prompt injection or adversarial topic names, metadata, and payloads; define degraded behavior for unavailable model services and prevent unbounded model calls or costs.

## Security and privacy

Never commit credentials, tokens, certificates, private endpoints, or production data. Apply least privilege, authorization, and tenant/site boundaries where relevant. Avoid sensitive logs; validate payload size and content; review for injection, unsafe deserialization, SSRF, and secret exposure; respect retention and deletion requirements.

## Testing and completion claims

For every implementation milestone, add normal, negative, boundary, repeated-operation, and dependency-failure tests. Run formatter, lint, type check, unit tests, integration tests, migrations, builds, and focused smoke tests as applicable. Never claim a command passed unless it ran successfully; document commands that could not run and why. Review the final diff for dead code, debug output, leaked secrets, missing authorization, and unbounded queries.

## Documentation and progress

Every milestone updates relevant documentation and `docs/PROGRESS.md`. Completion reports state what changed and did not change, files changed, commands/tests and actual results, migration/deployment impact, limitations, deferred work, and the recommended next milestone. Before committing, inspect the complete diff and perform a secret review.
