# R1b owner enforcement — PET mapping

Normative sources: Authorization 1.5 §§109.2–109.3, 36.10 (owner-side DAL), 34.2 (operational detail and denial semantics); Semantic 1.3 §65/§160.7. This follow-up closes the code boundaries identified in the previous R1b endpoint audit. Real IAM/workload/THS browser acceptance remains AUT-04/R6.

Shared SDK 1.2 adds `OwnerAuthorization`, a request-bound evaluator using the exact same immutable snapshot as servlet authentication. `candidates()` is admission only: it checks declared actor/scope and MUST NOT authorize release. `decide`/`require` is called with owner-resolved resource metadata before each output. No directory or remote PDP lookup occurs in this path. Raw capability/label headers and coarse attributes remain non-authoritative.

Operational status/history/incidents/summary/explain use owner-derived source and job identifiers under resource type `operational`, `module=INGESTION`, `detailLevel=TENANT_OPERATIONAL`. Results need the requested detail explicitly granted; generic coarse grants cannot release tenant operational detail. Private evidence/quarantine references are omitted from these safe operational projections. Rejected records produce `partial=true`/`authorization=REDACTED`; a denied summary is UNKNOWN, never HEALTHY. Incident explain returns 403 on a denied resource. Empty collections also require a valid decision; absence of data is not proof of authority.

Tests: OperationalOwnerAuthorizationTest uses actual HTTP controller, policy engine and output projection with controlled persisted-result fixtures, checking source mismatch, missing detail, private evidence removal and denial-to-UNKNOWN semantics. Existing PostgreSQL/module gates remain enabled.

Protected log search and aggregate also require a canonical HUMAN and an explicit
SECURITY_SENSITIVE grant on `protected-log` with module=INGESTION. The deployment
must set `ouf.protected-log.tenant-id` to the platform audit-owner namespace:
missing configuration denies. This store is platform-wide; the configuration is
not a row-level tenant filter and its policies must be restricted to operators
entitled to the whole store. Never infer this namespace from the request.
The exact bundle/version/capability decision reference enters the access audit.
`ProtectedLogOwnerBoundaryTest` covers both HTTP routes: missing explicit detail,
AI actor, missing/wrong namespace deny before the data service; matching HUMAN
policy succeeds. Existing bounded windows and payload-free event storage remain.
