# PET v1.2 traceability checklist

`IMPLEMENTED` requires executable tests; unchecked items are release blockers unless explicitly delegated.

## Runtime and persistence

- [x] Consume and pin exactly one ACTIVE PublishedConfigurationBundle per run.
- [x] Preflight contract/semantic/binding compatibility before every scheduled run and governed resume.
- [x] FULL_SNAPSHOT → CATCH_UP → DELTA state machine with invalid-transition rejection.
- [x] Separate restartable checkpoint from committed watermark.
- [x] Advance watermark only after durable downstream ACK for all required handoffs.
- [x] Transactional outbox with short claim/completion transactions and leased DELIVERING state.
- [x] Idempotency/deduplication ledger, conflict detection, strict partition delivery ordering and expired-lease crash reclaim.
- [x] PostgreSQL job scheduler, tenant fairness, global/per-source admission, workload priority, pressure hysteresis and multi-worker `SKIP LOCKED` leases.
- [x] Persisted finite-window retry budgets, exponential backoff, `Retry-After`, selective circuit breaker and source HEALTHY/DEGRADED/PAUSED/recovery probe.
- [x] Quarantine and replay execution with immutable attempt history, parent evidence, idempotency and explicit REPRODUCE/REPROCESS_CURRENT/REPROCESS_TARGET modes.
- [x] Cross-module quarantine handoff: Onboarding intake quarantine remains distinct from runtime record/batch quarantine.
- [x] Schema observation/drift isolation; incompatible shapes pause only their run and never guess mappings.
- [x] Historical bundle/adapter/semantic references, dependency-based deletion guard and append-only technical legal-hold decisions.

## Adapter framework

- [x] Common connector and adapter SPI with capability declaration and compatibility window.
- [x] Gateway-only REST/JSON adapter with governed paging and fault classification.
- [x] Gateway-only OGC WFS adapter with paging and CRS/axis-order evidence.
- [x] INTERNAL_MANAGED CSV adapter: one row = one source object.
- [x] INTERNAL_MANAGED XLSX adapter: explicit sheet policy; formulas never executed.
- [x] Bounded response/file, page and record sizes; remote bootstrap is consumed page-by-page without whole-bootstrap accumulation.
- [x] Adapter errors classified as transient, authorization/route, configuration, data or programming.

## Processing and handoff

- [x] Canonical Data Envelope rc3 validation.
- [x] Technical normalization, configured field mapping and frozen-contract runtime validation; unsupported transforms fail closed.
- [x] SourceObjectIdentityPolicy behavior with duplicate/reorder tests for managed files.
- [x] Candidate canonical object and unresolved-relationship handoff only; UDP owns resolution.
- [x] HandoffPayload rc3 and LineageRecord rc3 validation.
- [x] Durable ACK idempotency and downstream receipt reference.
- [x] Data Lake raw/normalized/curated ports with content addressing.
- [x] ChangeRepresentationProfile/delta-aware handoff, including historical FULL_SNAPSHOT default, delta base/event evidence and no UDP current-state dependency.

## Security and operations

- [x] Trusted Authorization principal/capability/tenant/decision context for quarantine operations; no actor headers.
- [x] Gateway binding/workload/secret references only; raw URLs and credential-like configuration are rejected before I/O.
- [x] Technical replay/status APIs remain non-MCP; PET-listed safe issue/quarantine search, explain and inspect capabilities are explicitly MCP-tool-eligible and tenant-scoped.
- [ ] Governed MCP-readable status/issue capability projection through MCP Server (external consumer; tool-eligible contract is implemented and tested here).
- [x] Governed quarantine lifecycle `OPEN -> RETRY_READY -> REPROCESSING -> RESOLVED|OPEN`, plus human dismissal/supersession; transitions use optimistic locking, durable audit and operational logs.
- [x] Governed PAUSE/RESUME/ABORT preserving pinned baseline, progress and outbox; terminal abort and `COMPLETED_WITH_WARNINGS` are enforced in PostgreSQL.
- [x] JSON structured logs configured; run, handoff, quarantine, replay and schema-surveillance paths emit safe identifiers/codes and persist protected operational events.
- [x] Append-only audit for governed run control, quarantine remediation/reprocessing and protected-log access; denied authorization decisions remain owned and logged by Authorization.
- [x] Implemented paths never log row payloads, tokens, secrets, receipts or source-object identifiers.
- [x] Metrics: handoff/quarantine/replay counters and active-run, due-schedule, outbox, quarantine, replay, circuit and watermark-age gauges.
- [ ] Common THS backend integration for protected log/issue inspection and correlation (external consumer; trusted-context API and audit are implemented here).
- [x] DB-backed health/readiness, graceful shutdown, initial SLO runbook and automated PostgreSQL restore/recovery evidence.

## Evidence gates

- [x] Frozen contract checksums and JSON Schema positive/negative fixtures, including classpath resolution of the common DataAccessLabel contract.
- [x] PostgreSQL 17 empty-schema/upgrade validation, concurrency and crash/restart recovery tests; release rollback is forward-schema compatible and documented.
- [x] Adapter contract suite and Gateway/UDP ACK/fault fixtures.
- [x] Managed CSV/XLSX adapter slices and REST/WFS end-to-end slices through durable UDP ACK and watermark.
- [x] OpenAPI 3.1 structural, security, unique-operation and implementation-parity release gate.
- [x] Non-root immutable-image deployment, CycloneDX SBOM, vulnerability gate and checksummed evidence package.
