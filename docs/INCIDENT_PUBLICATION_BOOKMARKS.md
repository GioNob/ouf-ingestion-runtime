# Owner publication bookmarks — PostgreSQL implementation

Follow [mandatory platform resilience policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md), [cursor/bookmark/consultation design](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md), Ingestion PET1.3 operational awareness and MCP PET1.4. This is an owner-local Java primitive; no public input schema/route is widened and runtime migration remains explicit.

Profile `ouf.owner-publication-bookmark.v1` stores opaque principal/tenant/actor/workload/issuer/audience-bound metadata, policy reference, source/state scope, committed publication boundary and parent bookmark. New login/authentication context does not erase coverage; all calls use current owner-local SDK collection/row decisions. Bookmarks are neither grants nor cached incident payloads.

Resume selects latest incident versions with committed publication revisions after the saved boundary and at/below the new captured watermark. The pending ledger/tenant publication clock from V17 catches late lower private sequence commits, including old occurrence times. No caller timestamp or raw revision chooses the boundary. Saved source/state scopes are immutable. Current urgent OPEN state remains a separate read.

A complete non-redacted captured page chain may issue one stable successor metadata token; partial/lag/hidden/missing pages do not. A bounded transaction and try-lock admission recheck coverage before issuance; no remote I/O or sleeps occur while those metadata locks are held. V18 keeps bookmark metadata after temporary page/session expiry and prevents updates/live deletion. Monotone owner history floor and expiry expose explicit410 gaps; actual incident history compaction remains OPEN.

Versioned finite profile:90-day bookmark expiry,512 per principal,4,096 per tenant,32,768 globally; one expired bookmark cleanup per fresh issue. Existing session/page/row/response budgets remain enforced. Capacity rejects new metadata while keeping existing replay and independent incident persistence available. These are OUF-reviewed reference limits, not AWS-mandated values.

All results retain consultationRecorded=false. A publicationBookmark is only a read coverage position, not proof of client delivery or human viewing. Global MCP vector/lineage, a separate explicit confirmation capability, client receipt and monotone expected-version checkpoint remain separate gates; no incident acknowledgement/resolution or recovery budget renewal is performed.

Native PostgreSQL qualification covers late lower sequence publication resume, page expiry/service recreation/new login, complete versus redacted chains, replay, actor/tenant/policy fencing/missing SDK, retention floor/expired token, immutable metadata/no payload, finite persistent capacity with independent writes and concurrent issuance. Full module/source/serving/authorization/HUMAN regressions remain required.

OPEN: public producer/global schema/transport, actual host/SDK/grants/HA and populated target migration, owner history compactor, MCP client delivery/checkpoint and actual alerts. No VPS operation, provider call, email or consumed R9/R10 replay is performed.

Captured frame metadata has an immutable digest binding, so source/state/watermark/expiry/configuration drift invalidates progress even if row permissions still allow a projection. Repeated initial reads at the same logical scope/boundary reuse an anchor; unchanged resume returns its parent token with noPublicationProgress=true. These fresh authorized reads do not create duplicate bookmark records or infer consultation. Page/session and control budgets remain finite.


## Owner HTTP coverage profile v1

The existing hidden incidents READ endpoint adds three disjoint modes: `{"snapshot":true,"coverage":true,"sourceId":"source","state":"OPEN","limit":50}` (optional filters/limit); `{"cursor":"canonical-nonzero-uuid","coverage":true}`; `{"resumeFrom":"canonical-nonzero-uuid","limit":50}` (optional limit). Resume inherits persisted source/state; no timestamp cutoff or revision argument. Missing, duplicated, unknown/null, mixed or noncanonical fields are rejected before owner I/O. Summary/status/history reject these modes.

Current owner SDK collection/source and row authorization remain mandatory, including new login, revocation and policy drift. Spring wires the qualified bookmark core; the production adapter does not synthesize authorization. Legacy snapshots stay separate, without bookmark issuance or operator receipt. Global MCP bookmark vector and concrete client confirmation/CAS are still OPEN; consultationRecorded=false.

Core PR38 qualified47 focused PostgreSQL tests and163 complete-module tests. Additional HTTP tests qualify fresh login/resume, saved source inheritance, redacted/current policy page chain and malformed requests before writes. No target migration/deployment or R9/R10 operation is implied.
