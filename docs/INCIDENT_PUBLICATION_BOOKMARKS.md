# Owner publication bookmarks — PostgreSQL implementation

Follow [mandatory platform resilience policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md), [cursor/bookmark/consultation design](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md), Ingestion PET1.3 operational awareness and MCP PET1.4. This is an owner-local Java primitive; no public input schema/route is widened and runtime migration remains explicit.

Profile `ouf.owner-publication-bookmark.v1` stores opaque principal/tenant/actor/workload/issuer/audience-bound metadata, policy reference, source/state scope, committed publication boundary and parent bookmark. New login/authentication context does not erase coverage; all calls use current owner-local SDK collection/row decisions. Bookmarks are neither grants nor cached incident payloads.

Resume selects latest incident versions with committed publication revisions after the saved boundary and at/below the new captured watermark. The pending ledger/tenant publication clock from V17 catches late lower private sequence commits, including old occurrence times. No caller timestamp or raw revision chooses the boundary. Saved source/state scopes are immutable. Current urgent OPEN state remains a separate read.

A complete non-redacted captured page chain may issue one stable successor metadata token; partial/lag/hidden/missing pages do not. A bounded transaction and try-lock admission recheck coverage before issuance; no remote I/O or sleeps occur while those metadata locks are held. V18 keeps bookmark metadata after temporary page/session expiry and prevents updates/live deletion. Monotone owner history floor and expiry expose explicit410 gaps; actual incident history compaction remains OPEN.

Versioned finite profile:90-day bookmark expiry,512 per principal,4,096 per tenant,32,768 globally; one expired bookmark cleanup per fresh issue. Existing session/page/row/response budgets remain enforced. Capacity rejects new metadata while keeping existing replay and independent incident persistence available. These are OUF-reviewed reference limits, not AWS-mandated values.

All results retain consultationRecorded=false. A publicationBookmark is only a read coverage position, not proof of client delivery or human viewing. Global MCP vector/lineage, a separate explicit confirmation capability, client receipt and monotone expected-version checkpoint remain separate gates; no incident acknowledgement/resolution or recovery budget renewal is performed.

Native PostgreSQL qualification covers late lower sequence publication resume, page expiry/service recreation/new login, complete versus redacted chains, replay, actor/tenant/policy fencing/missing SDK, retention floor/expired token, immutable metadata/no payload, finite persistent capacity with independent writes and concurrent issuance. Full module/source/serving/authorization/HUMAN regressions remain required.

OPEN: public producer/global schema/transport, actual host/SDK/grants/HA and populated target migration, owner history compactor, MCP client delivery/checkpoint and actual alerts. No VPS operation, provider call, email or consumed R9/R10 replay is performed.
