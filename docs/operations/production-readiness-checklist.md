# Production Readiness Checklist

No control is currently complete. Every row is release-blocking unless explicitly reclassified by the accountable release owner with recorded risk acceptance.

| Area and control | Status | Evidence artifact | Test or exercise | Owner role | Blocking |
|---|---|---|---|---|---|
| Authentication, role authorization, tenant/site isolation, and administrative-operation protection | Not started | Access-control matrix and test report | Authorization/negative-access tests | Security owner | Yes |
| External secret storage, rotation, and prohibition of insecure production defaults | Not started | Secret inventory/rotation record | Rotation and secret-scan exercise | Platform owner | Yes |
| TLS verified for HTTP, MQTT, databases, and dependencies | Not started | Transport configuration review | Certificate/hostname failure tests | Platform owner | Yes |
| MQTT QoS/redelivery, retained messages, replay, idempotency, ordering, and reconnect | Not started | Ingestion reliability report | Broker restart/redelivery tests | Ingestion owner | Yes |
| Backpressure, payload limits, malformed-message isolation, and broker-outage recovery | Not started | Capacity/quarantine report | Load, malformed input, outage tests | Ingestion owner | Yes |
| Migration upgrade on clean and representative prior data | Not started | Migration report and backup record | Upgrade/representative-data test | Data owner | Yes |
| Database transaction boundaries, connection exhaustion, backup, restore, and recovery | Not started | Restore integrity report | Connection/load and restore exercise | Data owner | Yes |
| Graceful startup/shutdown, liveness, readiness, dependency degradation, worker recovery | Not started | Lifecycle runbook | Shutdown/dependency-failure test | Service owner | Yes |
| Structured logs, correlation IDs, metrics, justified traces, and alert ownership | Not started | Dashboard/alert inventory | Alert-routing and telemetry test | Operations owner | Yes |
| Ingestion lag, queue depth, API latency/error rate, classification abstention, dependency failures | Not started | SLO dashboard and report | Load/failure measurement | Operations owner | Yes |
| Dependency/container/static-security/secret scanning and SBOM generation | Not started | Scan reports and SBOM | CI security gate | Security owner | Yes |
| Threat-model review and vulnerability remediation policy | Not started | Approved threat-model review | Remediation drill | Security owner | Yes |
| Incident response, disaster recovery, backup/restore, and deployment rollback runbooks | Not started | Exercised runbooks | Incident/DR/rollback exercise | Operations owner | Yes |
| Retention/deletion, capacity/load testing, SLOs, change approval, accountable release owner | Not started | Policy, load report, approval record | Deletion and capacity exercise | Release owner | Yes |

Production readiness may not be claimed until every applicable blocking row has an approved evidence artifact and a successfully executed test or exercise.
