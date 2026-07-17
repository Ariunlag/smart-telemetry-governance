# ADR-002: R0B Engineering Foundation

## Status

Accepted for the R0B branch.

## Context

The repository has an in-memory FastAPI foundation but lacked typed settings, a database session/migration baseline, readiness behavior, CI checks, and correlation-aware logging. R1 remains blocked on its separate durable-store and retention ADR.

## Decision

R0B introduces typed environment settings, optional SQLAlchemy connectivity, Alembic migration scaffolding with no domain tables, lifecycle-managed resources, liveness/readiness contracts, correlation IDs, pinned development tooling, and CI checks. The R0B threshold artifact defines the commands that must pass in a supported environment.

## Consequences

No MQTT, stream catalog, governance, or AI behavior is added. The database is optional in development but readiness fails when it is marked required and cannot be reached. Future schema changes must use Alembic revisions and preserve existing data. The R0B baseline contains no schema operations; data-writing work must use explicit transaction scopes with rollback tests before it is introduced.
