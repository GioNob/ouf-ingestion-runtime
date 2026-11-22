# Operational Awareness — Ingestion Runtime v1.3 traceability

Normative baseline: Ingestion Runtime PET v1.3, MCP PET v1.3, Gateway PET v1.4, Authorization PET v1.5, Cross-Module Alignment Matrix v1.6.

## Implemented evidence

- Existing persisted `runtime_issue`, `source_health` and `ing_run` state is projected through a new bounded internal API.
- `POST /api/internal/v1/ingestion/operations/{status,history,incidents,summary}` requires server-established Authorization context.
- `ingestion.operations.read`, `operations.incident.read` and `operations.status.read` are separate capability checks.
- Tenant filtering is performed server-side from trusted authorization context.
- Results expose semantic state/error/correlation/evidence references; raw logs, stack traces and credentials are not returned.
- Query limits are bounded (100 incidents/status, 200 history).

## Evidence boundary

This increment reuses persisted runtime state already written by Ingestion. It does not yet prove scheduler misfire production, incident dedup/recovery correlation for every failure class, or 30-day deployed retention. Those remain `EVIDENCE PENDING` for the next producer-hardening tranche.

No central incident service is introduced.
