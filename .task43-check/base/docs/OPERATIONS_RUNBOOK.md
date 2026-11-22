# Ingestion Runtime operational runbook

Every procedure requires an authenticated operator, correlation ID, current bundle reference and a captured before/after metrics snapshot. Never edit checkpoints or watermarks directly.

## Pause, resume and disable a source

Precondition: identify tenant, source and active run. Pause through `POST /runs/{id}/pause`; success means the run is `PAUSED`, its checkpoint and committed watermark are unchanged, and no lease remains active. Resume only after the pinned bundle and semantic references pass preflight; use `POST /runs/{id}/resume` and verify forward progress without duplicate idempotency keys. To disable, disable the binding in Onboarding/Gateway and pause the schedule; rollback is re-enabling the same governed binding.

## Schema drift and replay

Confirm the runtime issue and safe evidence hash, leaving raw data in the Data Lake. Only the affected source/type is paused. Correct configuration through Onboarding, then choose `REPRODUCE`, `REPROCESS_CURRENT` or `REPROCESS_TARGET`; success requires a new immutable attempt linked to the original. Rollback is reopening the quarantine item without deleting history.

## Downstream outage

Confirm outbox growth and a stationary watermark. Allow pressure control to suppress bootstrap/replay and preserve READY handoffs. After UDP recovery, verify ordered ACKs and backlog reduction. Do not re-fetch the source merely to retry an already durable handoff.

## PostgreSQL restore

Stop admission and take a platform-approved backup. Restore PostgreSQL, run `flyway validate`, check configuration snapshots, open runs, watermarks, quarantine and non-ACKED handoffs, then wait for expired leases before enabling workers. Success means readiness is `UP`, no watermark was synthesized and expired delivery leases are reclaimable. Rollback is returning to the untouched backup and compatible application release; migrations are forward-only.

## Credential rotation and adapter rollback

Rotate secrets in the platform secret manager while retaining the stable binding reference, then verify token audience and route. For an adapter rollback, deploy a version inside the pinned compatibility window and replay contract fixtures. Roll back the release if preflight or fixture validation fails.

## SLO triage

Initial objectives: local status/read p95 ≤300 ms, commands p95 ≤500 ms, 95% scheduler pickup within 60 seconds, delta freshness within five minutes, zero silent loss, zero duplicate logical effects and 100% watermark safety. Use Prometheus run, backlog, retry, circuit, quarantine and watermark gauges; source/profile labels must remain bounded.

## External projections

MCP Server exposes only the tool-eligible issue/quarantine operations declared in OpenAPI. Common THS performs authentication and policy decisions and forwards trusted context. Those deployables are external consumers; this module owns tenant-scoped APIs and audit evidence, not their deployment or availability.
