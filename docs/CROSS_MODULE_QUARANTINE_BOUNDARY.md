# Cross-module quarantine boundary

Quarantine is mandatory and split by ownership; it is never a generic error bucket.

## Source Onboarding

Owns quarantine of control-plane intake that cannot safely become an ACTIVE configuration:

- invalid or unsupported managed files retained by governed staging policy;
- ambiguous CRS/layer/sheet/header selection;
- incompatible managed-file schema drift;
- profiling evidence that needs human remediation;
- invalid configuration/mapping proposals.

It stores opaque `stagingRef`/evidence references, reason code, safe diagnostics, state, actor/correlation and remediation history. It does not store row-level runtime failures or execute ingestion. Release from quarantine is a governed Onboarding/THS operation and creates new immutable evidence; history is not overwritten.

## Ingestion Runtime

Owns quarantine of execution-time records, batches, relationships and handoffs:

- parse/validation/mapping/vocabulary failures;
- ambiguous source-object identity or ordering;
- schema observations detected during a run;
- relationship inputs requiring review;
- permanently failed downstream handoff where policy permits isolation.

Every item carries `quarantineId`, run/attempt/source identity, bundle and adapter versions, safe reason code, payload/evidence reference, lineage/correlation, state and retention/legal-hold metadata. Replay creates a new ProcessingAttempt and lineage; it never mutates historical evidence. A quarantined item never advances the committed watermark unless the pinned policy explicitly declares that terminal outcome admissible and records it durably.

## Shared operational surface

Search, detail, correlation and permitted remediation are rendered by the common THS. Authorization owns access decisions. MCP may expose governed read/status or proposal capabilities only through the MCP Server; it cannot silently release, rewrite or approve quarantined material.
