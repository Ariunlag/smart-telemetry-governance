# Production Readiness Checklist

No item is currently asserted complete. Complete and evidence each item before declaring a deployment ready.

- [ ] Threat model reviewed; risks accepted by accountable owner.
- [ ] Authenticated, authorized source and reviewer access implemented and tested.
- [ ] Secrets are managed, rotated, and absent from images/logs.
- [ ] Data classification, retention, deletion, and access agreements are approved.
- [ ] Stream catalog, decisions, and audit events are durable and recoverable.
- [ ] Backup restoration and rollback exercises meet recovery objectives.
- [ ] Ingestion is bounded, idempotent where needed, and handles broker outage/replay.
- [ ] Classification calibration, abstention, and human-review controls pass defined tests.
- [ ] Quality/provenance outputs are reproducible from versioned evidence.
- [ ] Observability, alerting, capacity limits, and incident runbooks are tested.
- [ ] Dependency, container, and API security reviews are complete.
- [ ] Load, failure-injection, and access-control tests meet acceptance criteria.
- [ ] Independent pilot protocol and exit criteria are approved.
