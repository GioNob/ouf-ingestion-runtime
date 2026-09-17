# R1b consumer integration

Canonical owner: GioNob/ouf-source-onboarding at eeffed582a8792b6116e15f01dc5e57f8b318298, SDK 1.1.0. Vendored sources and test vectors are byte-identical and checked in pairwise CI. Normative requirements: PET Authorization 1.5 §§109.2, 109.6, 36.10 and 34.2.

Grant constraints use authoritative resource type/id, module/source/job scope, DAL, permitted operational detail and trusted external role/ACR/AMR/authentication time. A missing required claim denies. Explicit DENY overrides ALLOW. Generic grants do not authorize sensitive labels or nonpublic detail. Existing domain-specific guards remain mandatory after coarse local authorization.

Configure `ouf.authorization.registry-url` (HTTPS), `ouf.authorization.registry-token-file`, `ouf.authorization.refresh-seconds` (30 default) and `ouf.authorization.max-staleness-seconds` (300 default). Refresh interval must be below maximum staleness. The startup file/hash configuration remains available. Request evaluation performs no remote policy lookup. Refresh failure retains the valid snapshot only until expiry, then all protected resolution fails closed. Bundle rollback must be expressed as a higher immutable version.

A deployment must bind trusted identity claims and authoritative resource context before `ServletAuthorization.resolve`; no raw request/header label or coarse boolean is an authority. IAM/THS acceptance and owner-specific OA output projection remain separate roadmap gates. These lab tests certify policy semantics and existing consumer boundaries, not production IAM.
