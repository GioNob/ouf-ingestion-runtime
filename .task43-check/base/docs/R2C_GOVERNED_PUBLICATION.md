# R2c — governed publication and historical serving

Baseline: Reality Baseline 1.7, Semantic PET 1.3, Onboarding PET 1.6, Ingestion/UDP PET 1.3. PETs remain normative; this scenario is bounded implementation evidence.

Four owner JVM processes use PostgreSQL/PostGIS and MinIO. Semantic discovers a deterministic external candidate, adopts it as DRAFT, explicitly sets its version, validates it, rejects SERVICE approval, records a test HUMAN approval and publishes idempotently. The resulting real revision/publication identifiers are pinned in approved Onboarding configurations. The Gateway proxies exact reference reads to the real Registry.

The run creates file/PULL data and holds delivery acknowledgements. An approved v2 Onboarding publication becomes ACTIVE while the old executions remain unfinished. The runtime restarts, recovers the original outbox and uses v2 for the next admission. Old snapshots and historical publication bytes remain unchanged. Source-scoped watermarks advance only after durable receipts. Human-authorized serving omits restricted properties and exposes lineage.

The UDP human governance API plans and executes REPRODUCE for the old PULL handoff while v2 remains ACTIVE. It verifies the retained handoff bytes against the Lake hash and stored payload, resolves v1 references, and materializes with that baseline. Re-execution is idempotent. The source raw retention reference survives reproduction. This is distinct from merely redelivering an outbox item.

The runtime historical bundle port validates the source, bundle ID/version/checksum and exact published Semantic membership. Previously published DEPRECATED/RETIRED revisions are allowed for historical resolution; DRAFT is denied. New-run preflight remains ACTIVE-only. No fallback to current configuration is permitted. Historical API reads expose references inside the R2 published snapshot as well as legacy snapshots.

## Human acceptance and remaining gates

Persona: authorized municipal operator/reader. Goal: change a source configuration and still understand and reproduce a previous result. Verified surface: owner APIs with test-only identity. Observable result: original bundle references, reproducible object values, provenance and explicit errors on invalid references.

Not certified here: production APISIX/IAM/THS, live upstream discovery (covered separately by the existing live Semantic workflow), browser/MCP workflow, complete Ingestion raw quarantine REPRODUCE/REPROCESS execution, capacity or HA. The fixture's human decision is not a production authentication mechanism. Full owner historical artifact retrieval beyond approved bundle comparisons remains an existing UDP acceptance gap.

Run `.github/workflows/r2c-publication-live.yml`; evidence includes `summary.json`, `semantic-publication.json`, `historical-replay.json` and the human serving scenario. Consumer repository revisions are pinned in that workflow. Do not substitute the R2b run for R2c evidence.
