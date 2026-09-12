# PET v1.2 traceability checklist

`IMPLEMENTED` requires executable tests; unchecked items are release blockers unless explicitly delegated.

## Runtime and persistence

- [ ] Consume and pin exactly one ACTIVE PublishedConfigurationBundle per run.
- [ ] Preflight contract/semantic/binding compatibility before every scheduled run.
- [ ] FULL_SNAPSHOT → CATCH_UP → DELTA state machine.
- [x] Separate restartable checkpoint from committed watermark.
- [x] Advance watermark only after durable downstream ACK for all required handoffs.
- [x] Transactional outbox with short claim/completion transactions and leased DELIVERING state.
- [ ] Idempotency/deduplication ledger and ordering per source object/partition.
- [ ] PostgreSQL job scheduler, fairness, admission control and multi-worker `SKIP LOCKED` leases.
- [ ] Retry budgets, exponential backoff, circuit breaker and source HEALTHY/DEGRADED/PAUSED.
- [x] Quarantine, replay request and immutable processing-attempt history. Replay execution worker remains open.
- [x] Cross-module quarantine handoff: Onboarding intake quarantine remains distinct from runtime record/batch quarantine.
- [ ] Schema observation/drift isolation; never guess mappings.
- [ ] Historical bundle/adapter/semantic contract retention.

## Adapter framework

- [x] Common connector and adapter SPI with capability declaration and compatibility window.
- [ ] Gateway-only REST/JSON adapter.
- [ ] Gateway-only OGC WFS adapter with paging and CRS/axis-order evidence.
- [x] INTERNAL_MANAGED CSV adapter: one row = one source object.
- [x] INTERNAL_MANAGED XLSX adapter: explicit sheet policy; formulas never executed.
- [ ] Bounded payload/page/record sizes and streaming memory safety.
- [x] Adapter errors classified as transient, authorization/route, configuration, data or programming.

## Processing and handoff

- [ ] Canonical Data Envelope rc3 validation.
- [ ] Technical normalization, configured field/vocabulary mapping and runtime validation.
- [x] SourceObjectIdentityPolicy behavior with duplicate/reorder tests for managed files.
- [ ] Candidate canonical object and unresolved-relationship handoff only; UDP owns resolution.
- [ ] HandoffPayload rc3 and LineageRecord rc3 validation.
- [x] Durable ACK idempotency and downstream receipt reference.
- [ ] Data Lake raw/normalized/curated ports with content addressing.
- [ ] ChangeRepresentationProfile/delta-aware handoff.

## Security and operations

- [x] Trusted Authorization principal/capability/tenant/decision context for quarantine operations; no actor headers.
- [ ] Gateway binding/workload/secret references only; no raw URLs or credentials.
- [x] Implemented quarantine APIs are internal/THS and explicitly not MCP tool-eligible.
- [ ] Governed MCP-readable status/issue capability projection through MCP Server.
- [x] Human-only protected quarantine replay request.
- [ ] JSON structured logs configured; implemented handoff/quarantine paths use safe identifiers/codes, but wider run coverage remains open.
- [ ] Append-only audit for run/pause/replay/quarantine/remediation and denied operations.
- [x] Implemented paths never log row payloads, tokens, secrets, receipts or source-object identifiers.
- [ ] Metrics: handoff, quarantine and replay counters implemented; run/record/lag/circuit/queue gauges remain open.
- [ ] Common THS backend integration for protected log/issue inspection and correlation.
- [ ] Health/readiness, SLO alerts, runbook, backup/restore and retention/legal hold.

## Evidence gates

- [ ] Frozen contract checksums and JSON Schema positive/negative fixtures.
- [ ] PostgreSQL 17 migrations, upgrade, concurrency, crash/restart and rollback tests.
- [ ] Adapter contract suite and Gateway/UDP fault fixtures.
- [ ] Managed CSV/XLSX and REST/WFS vertical slices.
- [ ] OpenAPI 3.1 structural and implementation-parity tests.
- [ ] Container non-root, SBOM/dependency scan and evidence package.
