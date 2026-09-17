# R2a — automatic publication admission

Authority: Reality Baseline Package v1.7; Onboarding 1.6 §37/§109.17;
Ingestion 1.3 §5, §21 and §§32–37; roadmap R2a.

The production Ingestion application now discovers current Onboarding publications
through Gateway and automatically creates the first MANAGED_ONCE or FULL_SNAPSHOT
run. A run becomes RUNNING only after checksum/identity verification and exact
Semantic preflight. No operator invokes a Java worker method. Data acquisition,
Data Lake/UDP handoff, ACK and committed watermark execution are R2b, not an R2a
completion claim.

Onboarding publishes; Ingestion schedules. The runtime never reads Onboarding DB
tables. Its own persisted schedule records the publication UUID, monotonic owner
sequence, checksum, syncProfile and a locally bounded discovery freshness marker.
Each dispatch rechecks the remote ACTIVE pointer before creating a run. Failed
or missing mandatory references deny admission; an active run keeps its immutable
full published bundle and exact Semantic bindings.

Managed files have interval=0 and one successful admission per publication. A
successful preflight and schedule consumption commit atomically. Lease generations
fence stale workers; an expired preflight resumes the same schedule slot/run.
A replacement publication cannot rewrite an old run snapshot. Source DISABLED
prevents new admission; it does not silently cancel an in-flight run.

PULL supports the existing ISO-8601 `syncProfile.pollInterval` contract, 60 seconds
to 31 days, with explicit IANA timezone. Cron expressions are not introduced.
`operationalPolicy` requires the existing bounded misfire, timeout, retry maxima,
dedup, visibility and retention fields. Additive `syncProfile.retryBackoffSeconds`
provides explicit bounded retry backoff (1–86400 seconds); no frozen schema bytes
change. Retry on transient activation failures is bounded by attempts and elapsed
time. Other failures pause; repolling the same publication cannot reset the budget.
A corrected new publication can unblock its failed admission. Missing managed-file
retry policy means no automatic retry, not an invented source-specific policy.
Misfire incident projection and retention execution remain R3; sourceTimeout applies
to source acquisition in R2b, while control-plane HTTP uses bounded technical timeouts.

`semanticReferenceBindings` is copied from approved configuration into the checksum
covered publication. Every `semanticRefs` item needs one exact binding:

```json
{"semanticId":"core","semanticVersion":"1","revisionId":"00000000-0000-0000-0000-000000000001","publicationSetId":"00000000-0000-0000-0000-000000000002"}
```

The client calls the existing `/api/semantic/v1/references:resolve` endpoint via
Gateway and verifies all four identity/version fields plus ACTIVE status. No
latest-version fallback or Semantic search is used. Legacy publications lacking
required cadence/policy/bindings fail closed and need a governed new version.

Deployment configuration (technical values, not source policy):

```properties
ouf.ingestion.activation.enabled=true
ouf.ingestion.activation.gateway-url=https://gateway.example.invalid
ouf.ingestion.activation.token-file=/var/run/ouf/workload/token
ouf.ingestion.activation.tenant-id=platform-control-plane
ouf.ingestion.activation.poll-ms=10000
```

The token file is supplied by the approved workload identity mechanism and reread
for each request; it is never logged or copied into bundle/snapshot/evidence.
Gateway must route the protected publication feed/ACTIVE lookup and exact Semantic
resolve. Redirects are disabled, each response is limited to 2 MiB, discovery pages
to 20 publications, Semantic bindings to 20, and each complete HTTP exchange to
five seconds. One bounded page and one dispatch are attempted per loop. Discovery
freshness is limited to five minutes; tune loop capacity for catalog size without
changing source cadence. The configured namespace is the platform control-plane
catalog namespace, not a data-row tenant filter. Multi-tenant data ownership and
real IAM acceptance remain governed integration work.

Validation: `PublishedActivationTest`, `PublicationGatewayClientTest`,
`ActivationLeaseRuntimeTest`, `AutomaticActivationRuntimeTest`; plus the
`R2a Onboarding automatic activation` workflow. That workflow starts two real JVM
processes: Onboarding performs CSV profiling and the governed publication lifecycle;
the production Ingestion JAR starts file/PULL runs and is restarted to verify no
duplicate runs. Gateway, Semantic responses, compatibility attestation and the
publisher identity adapter are explicit laboratory fixtures. They do not certify
APISIX, real Semantic publication acceptance, source acquisition or production IAM.
The publisher fixture is test-only and excluded from the production JAR.
