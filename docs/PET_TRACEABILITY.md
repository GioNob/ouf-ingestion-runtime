# PET v1.2 traceability checklist

`IMPLEMENTED` requires executable tests; unchecked items are release blockers unless explicitly delegated.

## Runtime and persistence

- [x] Consume and pin exactly one ACTIVE PublishedConfigurationBundle per run.
- [x] Preflight contract/semantic/binding compatibility before every scheduled run and governed resume.
- [ ] FULL_SNAPSHOT → CATCH_UP → DELTA state machine.
- [x] Separate restartable checkpoint from committed watermark.
- [x] Advance watermark only after durable downstream ACK for all required handoffs.
- [x] Transactional outbox with short claim/completion transactions and leased DELIVERING state.
- [ ] Idempotency/deduplication ledger and ordering per source object/partition.
- [ ] PostgreSQL job scheduler, fairness, admission control and multi-worker `SKIP LOCKED` leases.
- [ ] Retry budgets, exponential backoff, circuit breaker and source HEALTHY/DEGRADED/PAUSED.
- [x] Quarantine and replay execution with immutable attempt history, parent evidence, idempotency and explicit REPRODUCE/REPROCESS_CURRENT/REPROCESS_TARGET modes.
- [x] Cross-module quarantine handoff: Onboarding intake quarantine remains distinct from runtime record/batch quarantine.
- [ ] Schema observation/drift isolation; never guess mappings.
- [x] Historical bundle/adapter/semantic references, dependency-based deletion guard and append-only technical legal-hold decisions.

## Adapter framework

- [x] Common connector and adapter SPI with capability declaration and compatibility window.
- [ ] Gateway-only REST/JSON adapter.
- [ ] Gateway-only OGC WFS adapter with paging and CRS/axis-order evidence.
- [x] INTERNAL_MANAGED CSV adapter: one row = one source object.
- [x] INTERNAL_MANAGED XLSX adapter: explicit sheet policy; formulas never executed.
- [ ] Bounded payload/page/record sizes and streaming memory safety.
- [x] Adapter errors classified as transient, authorization/route, configuration, data or programming.

## Processing and handoff

- [x] Canonical Data Envelope rc3 validation.
- [ ] Technical normalization, configured field/vocabulary mapping and runtime validation.
- [x] SourceObjectIdentityPolicy behavior with duplicate/reorder tests for managed files.
- [x] Candidate canonical object and unresolved-relationship handoff only; UDP owns resolution.
- [x] HandoffPayload rc3 and LineageRecord rc3 validation.
- [x] Durable ACK idempotency and downstream receipt reference.
- [x] Data Lake raw/normalized/curated ports with content addressing.
- [x] ChangeRepresentationProfile/delta-aware handoff, including historical FULL_SNAPSHOT default, delta base/event evidence and no UDP current-state dependency.

## Security and operations

- [x] Trusted Authorization principal/capability/tenant/decision context for quarantine operations; no actor headers.
- [ ] Gateway binding/workload/secret references only; no raw URLs or credentials.
- [x] Technical replay/status APIs remain non-MCP; PET-listed safe issue/quarantine search, explain and inspect capabilities are explicitly MCP-tool-eligible and tenant-scoped.
- [ ] Governed MCP-readable status/issue capability projection through MCP Server.
- [x] Governed quarantine lifecycle `OPEN -> RETRY_READY -> REPROCESSING -> RESOLVED|OPEN`, plus human dismissal/supersession; transitions use optimistic locking, durable audit and operational logs.
- [x] Governed PAUSE/RESUME/ABORT preserving pinned baseline, progress and outbox; terminal abort and `COMPLETED_WITH_WARNINGS` are enforced in PostgreSQL.
- [x] JSON structured logs configured; run, handoff, quarantine, replay and schema-surveillance paths emit safe identifiers/codes and persist protected operational events.
- [x] Append-only audit for governed run control, quarantine remediation/reprocessing and protected-log access; denied authorization decisions remain owned and logged by Authorization.
- [x] Implemented paths never log row payloads, tokens, secrets, receipts or source-object identifiers.
- [x] Metrics: handoff/quarantine/replay counters and active-run, due-schedule, outbox, quarantine, replay, circuit and watermark-age gauges.
- [ ] Common THS backend integration for protected log/issue inspection and correlation.
- [ ] Health/readiness, SLO alerts, runbook and backup/restore. Historical-contract retention/deletion guard and technical legal hold are implemented.

## Evidence gates

- [x] Frozen contract checksums and JSON Schema positive/negative fixtures, including classpath resolution of the common DataAccessLabel contract.
- [ ] PostgreSQL 17 migrations, upgrade, concurrency, crash/restart and rollback tests.
- [ ] Adapter contract suite and Gateway/UDP fault fixtures.
- [ ] Managed CSV/XLSX and REST/WFS vertical slices.
- [ ] OpenAPI 3.1 structural and implementation-parity tests.
- [ ] Container non-root, SBOM/dependency scan and evidence package.
