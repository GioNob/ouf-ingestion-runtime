> **Policy di piattaforma vigente:** [Resilienza, quarantena, incidenti e alert — v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md). Obbligatoria per sviluppo e revisione; [adozione e prove](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_ADOPTION.md).

# HUMAN recovery budgets v1 — first implementation tranche

Platform policy: [OUF resilience/quarantine v2](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md). PET Ingestion1.3 §7.3/§14/§15/§20.1/§21/§141/§46; IAM/HUMAN enforcement preserved.

A HUMAN with ingestion.run.resume can resume the same PAUSED run with a NEW finite durable budget even after exhaustion. Default128 owned worker batches and86400 seconds, reason OPERATOR_REQUESTED_RECOVERY. Optional maxAttempts/maxElapsedSeconds/reasonCode in the existing resume command. Invalid limits, missing HUMAN/capability/decisionRef, wrong tenant, stale control_version and failed pinned baseline preflight deny.

Versioned operation profile **ouf.run-recovery-batch.v1** counts one existing execution-worker claim/batch before adapter I/O; each batch is bounded by100 records or45 seconds in the current worker (individual transport deadlines remain a separate qualification). A replayed/crashed claim consumes its original budget; reclaim consumes another batch. Recovery runs admit at most one owned batch at a time, through existing pressure control, so this is a conservative recovery profile rather than unconstrained bulk restart. This profile is not a count of all downstream HTTP retries.

V15 is additive. Session IDs/epochs, HUMAN decision/actor/reason, limits/deadline and old counters remain in an immutable-history ledger. New grant and PAUSED→RUNNING are one tenant-scoped expected-version transaction after remote semantic preflight outside TX. Existing partition fencing, snapshot, operation idempotency keys, checkpoint, watermark, lineage and ACK semantics remain. Duplicate operator commands cannot create two sessions.

Admission counts batches atomically. Exhaustion pauses the affected run/partitions, releases leases and opens one persistent runtime issue across repeated budget renewals. Fresh budget does not mark that issue resolved. Resolution requires actual run completion/drain/ACK. Before adapter I/O, each lake effect and durable stage, current lease/generation and session deadline are checked. A late durable lake response cannot trigger later effects, handoff or checkpoint; that already-durable immutable lake object is retained for reconciliation/dedup, not called nonexistent. Stage rechecks after waiting on idempotency locks and before commit. No remote call occurs inside the budget TX.

## Qualification

[PR34](https://github.com/GioNob/ouf-ingestion-runtime/pull/34) merged as6f583ca6b226a63c4a9b55df19f28728c4fd9348. Exact qualified candidate80bcb2f8c1f478c0b6d18c46b9eeff5cede5f68f: [native PostgreSQL38104209271](https://github.com/GioNob/ouf-ingestion-runtime/actions/runs/38104209271), job114366142916,35 tests PASS including16 new policy tests. [Whole module38104209251](https://github.com/GioNob/ouf-ingestion-runtime/actions/runs/38104209251):134 tests PASS, supply-chain/nonroot runtime/DR jobs SUCCESS. Authorization, Onboarding, serving, historical publication, GeoPackage and retained continuity checks on the same candidate are also SUCCESS. [Structured evidence](../evidence/human-recovery-budget-qualified-20261011.json).

The first run38104025072 failed on the new test's use of the JSON ? operator within JdbcClient named parameters. Replaced with jsonb_exists, without changing production behavior; corrected38104163969 and the final candidate pass. The legacy control test fixture now uses SDK actor type HUMAN and ingestion.run.resume capability, matching actual service enforcement. Added16 native PostgreSQL tests cover human renewal after exhaustion, concurrent grants, stale-worker fencing, cumulative budgets/one issue, restart/elapsed exhaustion, late remote response without checkpoint/ACK, AI/tenant/capability denial, revoked preflight, immutable history, pressure admission and invalid policy.

## Explicit remaining scope

- This tranche implements the **existing HUMAN original-run resume path**. Initial runs, publication/scheduler activation budgets, source circuit windows, downstream dispatcher retries and separate historical replay execution still need adoption of platform v2. Existing source-health rolling counters are circuit metrics, not proof of a finite incident recovery budget; their automatic renewal must not be treated as a new authorized session.
- Quarantine retry readiness remains a separate HUMAN action; existing blocking quarantines must be RETRY_READY before resume. Budget/session grant alone does not approve changed bundles, new grants or domain decisions.
- Source circuit isolation and gradual multi-run recovery quotas, quarantine capacity/retention/backpressure/gestor surfaces and all-module adoption remain open.
- Remote transports must enforce their own timeouts; these guards prevent further/staged publication after an expired result, not force-cancel arbitrary callbacks.
- No target migration, deploy, VPS restart or R9/R10 replay. R10 and the prior root preflight remain suspended. This is not platform acceptance.


## 11 ottobre 2026 — resilienza v3 e prima tranche Operational Awareness

[Policy canonica v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) obbligatoria, [registro con prove](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_ADOPTION.md) e [piano di allineamento](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_RESILIENCE_PLAN.md). Comprende retry finiti, quarantena/budget umano, un incidente per dipendenza con lavori coinvolti separati, catch-up persistente, crisi su segnali attuali e alert deduplicati.

Integrate MCP PR51, Ingestion PR35 e Gateway PR59: priorità prima dei limiti, recuperi vecchi visibili nel catch-up supportato, conteggi Gateway dell'intera finestra e troncamento esplicito. Prove: MCP full38105960598/race38105960600 e gate sicurezza con0 vulnerabilità raggiungibili dopo aggiornamento Go/dipendenze; Ingestion native21/module137 test; Gateway229 test + APISIX reale. [Evidenza](https://github.com/GioNob/ouf-deploy/blob/main/evidence/operational-priority-qualified-20261011.json).

Restano OPEN cursor/checkpoint operatore, grouping multirun per dipendenza/contatori impatto, detector/quote/capacità, outbox alert e integrazione target multi-owner/autorizzazioni. Reference/fixture e summary di finestra non provano conformità globale o salute corrente. Nessun deploy VPS o email; R9/R10 consumati e precedente preflight sospeso. Le accettazioni precedenti restano preservate.
