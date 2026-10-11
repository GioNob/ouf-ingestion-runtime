# Persistent owner incident pages — implementation and qualification

Follow [platform resilience policy v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md), [stable cursor design](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md), Ingestion PET1.3 §46 and MCP PET1.4 §§33–34. Native qualification is pending.

## Transition truth and publication

V17 captures an immutable safe transition snapshot in the same transaction as every real incident mutation. Existing rows receive an honest BASELINE. Operational version and incident/run/source identity cannot be forged through ordinary updates. Protected evidence/custody values are excluded from this projection.

The private bigserial is never a public cursor. Publication first acquires a tenant transaction advisory lock, then allocates revisions and marks pending entries atomically. Pending scans do not discard lower sequence values. A late writer, publisher crash/rollback or competing publisher cannot skip or duplicate a published revision. Writers never acquire the publication counter lock; no remote I/O occurs in these transactions.

## Private owner API

Reuse POST /api/internal/v1/ingestion/operations/incidents through the existing Gateway producer capability. Add snapshot:true for a fresh stable page; optional sourceId/state/since/limit retain typed semantics. Continue with cursor alone. Other operational paths reject cursor/snapshot rather than silently ignoring them. Legacy non-snapshot clients retain their current response contract. Global MCP cursor publication is still being integrated separately.

Each session is an immutable captured set at a committed owner publication watermark, with priority before pagination. Later incident changes do not reorder or remove captured rows. Every page rechecks current collection and row authorization, live tenant custody and principal context. Tokens are random server-stored UUIDs bound to principal/tenant/actor/service principal/claims fingerprint/policy version/filters. They survive service recreation; they never grant access. Invalid, expired, changed-policy and altered-filter use fail explicitly.

A source collection grant is required for snapshot creation. Explicit row denies still redact safely. An authorized empty source stays empty/AUTHORIZED, while hidden rows, publication lag or unavailable historical coverage are partial. No protected count or raw publication revision is returned.

## Finite profile ouf.incident-owner-cursor.v1

Limits:100 items/page,200 pages,20,000 captured rows/session,30-minute session expiry,16 sessions/principal,128/tenant,4,096 global;64,000 captured rows/tenant and200,000 global. At most four256-entry local publication batches run per fresh page. A two-second DB transaction budget covers publication/capture/first page, with try-lock admission and no wait/retry loop. One expired session is cleaned per fresh admission. Producer response remains within512KiB. Capacity/selection pressure rejects new sessions and preserves independent incident persistence; it does not drop incidents or renew execution budgets.

The API exposes publication lag, baseline history gap, stable snapshot identity/expiry, continuation and truncation. A bounded captured publication snapshot is not proof that all live or in-flight writes have published. Reading sets consultationRecorded:false. Cursor continuation never acknowledges or resolves an incident.

## Required evidence and ongoing integration

Native tests cover late lower private sequence commit, atomic capture/publication rollback, competing publishers, immutable revisions, stable ordering/retry across later updates and service recreation, principal/tenant/policy/filter/expiry replay, row redaction/live tenant transfer, publication lag/history gap, finite capacity and governed HTTP/empty-source behavior. Existing owner/HUMAN/priority and complete module suites remain mandatory.

Still under development: owner revision-based catch-up bookmarks and confirmation, global MCP vector/merge/coverage, persistent consultation receipts, current health separate from a history window, bounded historical transition compaction/retention/legal hold and ledger admission pressure. The cursor quotas above do not prove bounded incident-history storage or protected DB connection capacity. Live grants/network/deployment and populated target migration are not qualified by native fixtures. No VPS apply/restart, provider call, email or consumed R9/R10 replay is performed.


## Qualification 2026-10-11

Candidate `248b06256c2ea28236ae1bc7bdd55e3f8233d231` passed all 18 push/PR GitHub Actions workflows. PR #37 merged as `25de72ce1b01307378874ae8aef73e9f87dd0969`. PostgreSQL 17 focused run [38120034345](https://github.com/GioNob/ouf-ingestion-runtime/actions/runs/38120034345) and full module run [38120034381](https://github.com/GioNob/ouf-ingestion-runtime/actions/runs/38120034381) qualify the implementation, including authorization, concurrency, rollback and retained source/serving regressions.

Each page includes `itemsDigest` over the visible item projection. A cross-owner consumer must refetch and reauthorize a saved owner page before using its offset, and reject continuation if the digest changes. A cached offset is not authorization to disclose saved incident payloads.

This is owner snapshot paging, not a persistent operator consultation receipt, a production deployment, or proof of complete timestamp catch-up. Late commits are captured by pending publication, but a future persistent resume bookmark must use owner publication coverage rather than a caller timestamp.
