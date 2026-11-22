# Ingestion operational priority and recovery catch-up v1

[Platform resilience policy v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md); Ingestion PET1.3 §46 and MCP PET1.4 §33.

Select OPEN before resolved/dismissed projections, ERROR before WARNING, then last-seen and stable issue identity before SQL LIMIT. A resolution of an old issue is included by since using coalesce(resolved_at,created_at), matching the existing last_seen_at projection. Tenant join, source/state filters and post-selection owner visibility remain enforced. This does not introduce CRITICAL into the existing two-severity runtime_issue schema or manufacture updated-at timestamps for metadata-only changes.

Native PostgreSQL tests cover important older errors amid recent warnings/resolved history; resolution catch-up of old incidents; tenant isolation, source/state and invalid-state behavior. Existing owner authorization and HUMAN recovery tests also run.

No cursor/checkpoint, complete last-transition ledger, cross-run dependency correlation, impact counters, alerts or deploy acceptance is introduced. A full bounded candidate page may omit additional visible rows after owner redaction; global ranking/continuation proof remains open. This tranche makes priority of returned owner candidates consistent with the aggregator; complete platform coverage remains pending.
