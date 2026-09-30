# R4a: frozen configuration consumer compatibility

The 30 September operator inventory is PASS: live Flyway 14 and staged max 14;
staged `e3f04f1d8ed47a62cde8b9c7831882c9cea17169` differs from live. Policy
`ouf-lab-authorization:31` contains one SERVICE descriptor and one SERVICE grant
for `ouf.ingestion.configuration.attest`, with no other grants. This is operator
evidence, not an assistant observation of the VPS or a positive attestation.

## Normative basis and scope

Reality Baseline Package v1.7, Cross-Module Matrix v1.7 and Blueprint v0.3 apply.
Ingestion PET v1.3 §§104–106 requires consumer compatibility tests, exact
resolvable references and adapter support before execution. Onboarding PET v1.6
§38.2 binds source identity, change profile, authority and access policy to the
immutable configuration; §109.6 preserves historical bundles and N/N+1 support.
THS remains the HUMAN authority for approval/activation. This addition neither
approves a source nor changes its configuration, schema, identity policy or grants.

## Production consumer proof

`FrozenConfigurationProbeMain` is a separate entry point in the deployable jar.
It starts no Spring application context, Flyway, scheduler, worker or server.
The probe checks the frozen state and the same sorted-Jackson configuration hash
used by Onboarding. It projects the candidate with `PublishedExecutionMapper`,
resolves exact Semantic bindings via `PublicationGatewayClient`, reads the
immutable object via `ExecutionGatewayClient`, and executes the real managed
CSV/XLSX/GeoPackage adapter. Every record passes the production pipeline's
mapping and CDE/lineage/handoff validators, with no lake or database ports.
CSV delimiter and BOM handling are those of the shipped adapter. Adapter id and
runtime version, source identity strategy, expected fields and file integrity
are fail-closed gates. Unsupported source modes and empty assets are explicitly
BLOCKED; a sampled row never establishes whole-file compatibility.

The proof concerns consumer execution contracts. It does not prove a durable
handoff, UDP object resolution/materialization, ACK/watermark, search, recovery,
or R-SMOKE/R-INSTALL acceptance. Synthetic probe IDs are local validation inputs
only; no run or domain object is created. No source record or token is printed.

## Operator preparation

`scripts/r4a_prepare_frozen_compatibility_probe.py` takes an exact candidate commit,
source, version, expected configuration hash and explicit `--tenant-id`. It reads the frozen configuration
with a read-only transaction and retains it only in memory/stdin. It verifies the
old staged worktree, ancestry, remote HEAD, exact migration-file checksums and
live Flyway 14. It creates an isolated worktree and builds a revision-labelled
image, leaving the old stage manifest, checked-out branches and live container
intact. A build failure stops the gate; its log is private under `/opt/ouf/r4a-stage`.

The candidate JVM runs non-root with read-only auth/properties mounts, a bounded
128 MiB disposable `/tmp` for parser/native-library scratch space, one backend
network, no published ports, dropped Linux capabilities and bounded memory/CPU.
It receives no database credentials or receipt-signing key. Its only remote
operations are authenticated Gateway GETs for the asset and Semantic references.
The disposable probe container is removed, including after timeout; worktree,
image and evidence remain available. The live image and frozen hash are reread
after validation. A PASS is saved as `ingestion-compatibility-probe.json` with
`candidateDeployed=false` and `attestationSubmitted=false`.

The first VPS attempt stopped with `ING_COMPAT_TRANSPORT_CONFIG_REQUIRED`:
all three activation properties were absent from the live summary file. Operator
diagnostics confirmed an explicit registry URL on the lab Gateway and an existing
SERVICE token for `ouf-ingestion`, tenant `ouf-lab`, correct issuer/audience,
TTL 277 seconds at inspection, and scopes `authorization.bundle.read`,
`ouf.ingestion.configuration.attest`, `ouf.internal.object-storage.read` plus
OIDC profile/email. This is diagnostic evidence, not a positive consumer proof.

The preparer now checks existing `ouf.authorization.registry-url` and
`ouf.authorization.registry-token-file` before building. It derives the Gateway
origin from that configured HTTPS registry URL and uses the supplied tenant.
Missing/duplicate/nonliteral properties, missing or escaped token files, wrong
service/client/tenant, short token lifetime or missing object-read scope block
preparation. JWT claims are decoded only for diagnostics; actual Gateway and
owner checks authenticate and authorize every consumer GET.

A temporary root-owned, group-10002, mode-0440 file contains only the three
activation transport properties. It replaces the properties mount **only in the
disposable probe container**, while the existing auth directory stays read-only.
No token is copied. The live summary file is never changed, activation/execution
are never enabled, and no IAM scope/grant/route is created. The temporary file is
removed after the probe, including errors. This also avoids `docker exec cat`,
which is unavailable in the distroless runtime. Secrets remain in existing
mounts; never paste their values or file contents into chat.

The observed token has no explicit Semantic scope. Exact-reference access,
required route and owner authorization remain unproved; the actual consumer
GET must succeed. A denial must be corrected through the governed IAM/policy/
route workflow, with no fallback to direct Semantic or object-storage access.

## Remaining deployment and attestation gates

### Semantic SERVICE access inventory after the consumer 403

The operator rerun on `dae05e6…` passed transport/build/identity and stopped at
`ING_ACTIVATION_GATEWAY_403` during the exact-reference Semantic preflight,
before asset reading. Gateway's `r2b-semantic-reference` binding points to
`ouf.semantic.read`, whose required scope is absent from the observed Ingestion
token. Current live route enforcement and effective owner grant remain distinct
unproved dependencies; a missing scope must not be treated as the only cause.

`scripts/r4a_semantic_access_inventory.py` with its pinned sibling preparer
reads the existing token reference, ACTIVE policy (read-only SQL) and existing
kcadm session. It reports scope presence, descriptor actor/scope, grant selector
counts and exact Keycloak scope/default/optional assignment, without token,
principal IDs, policy payloads or credentials. Selector counts never assert ALLOW;
constraints, freshness and owner enforcement still apply. An expired kcadm
session is reported safely and does not prompt for or renew credentials.
The inventory performs no HTTP data GET, mutation, build, live switch or POST.
Three Python tests cover safe policy summarization, expired-session suppression
and read-only exact-name Keycloak inspection. Scope/binding remediation may be
prepared after this evidence; policy publication remains a HUMAN THS action.

### Source identity contract correction after the VPS probe

The operator attempt on `1eb70c4…` passed transport and build but stopped with
`ING_COMPAT_IDENTITY_POLICY_UNSUPPORTED`, before Semantic and asset GETs.
The original probe mistook the runtime `rowIdentityBasis=ASSET_AND_ROW_ORDINAL`
for the policy strategy. The owner contract admits `NATIVE_KEY`,
`COMPOSITE_NATIVE_KEY` and `MANAGED_DETERMINISTIC`; Onboarding's
`ManagedFileService` emits the last strategy with `sourceFields=["$managedRowOrdinal"]`,
`normalizationRuleRef=normalization://managed-file/asset-row-ordinal-v1` and the
runtime row-ordinal basis. The probe now checks this exact combination for the
tabular adapter. It also supports native and composite native strategies with
valid, distinct nonsynthetic fields and their respective key cardinalities.
It rejects the noncontract strategy `ASSET_AND_ROW_ORDINAL`, unknown normalization
and inconsistent runtime basis. No frozen configuration, hash or owner policy is
modified. Source provenance identity remains distinct from UrbanObjectId.

Java regression fixtures now use the exact owner representation and cover
composite native identity plus negative strategy, normalization, basis and
cardinality cases. These run in Java 21 CI with the production adapter/pipeline.
Until that CI and the operator rerun pass, no consumer proof or attestation is
claimed. The untested Gateway/Semantic access gate remains open.

After the candidate proof, prepare a verified DB backup, retained old container,
controlled image switch and readiness smoke. No schema migration is added here.
Repeat the consumer proof against the actual deployed immutable image before
submitting any compatibility result. Before submission, verify the Gateway route,
SERVICE token capability and exact frozen hash/state; never write the attestation
table directly or use a HUMAN token. The current Onboarding endpoint accepts a
boolean/detail, so the submitter must bind detail to the checked runtime image,
version/hash and evidence; it must not post `compatible:true` from inventory alone.
Automatic submission is deliberately absent from this preparation gate.

Rollback for preparation requires no live restore: live was never switched.
Retain existing backups, rollback containers and route snapshots. The later
switch procedure must restore the retained old application container on failure,
without an automatic database restore. Only after a positive live compatibility
attestation and current UDP activation gate may the HUMAN review proceed in THS.

## Validation

`FrozenConfigurationProbeTest` exercises real CSV parsing, every-row validation,
native/row-ordinal identity, mapping/transform failures, hash/state/adapter-version
gates, asset integrity failure and Semantic denial. Python tests verify read-only
SQL, source-ID injection rejection, mount restrictions and evidence binding.
Twelve Python tests now also cover transport derivation without live mutation/token
copy, wrong principal/tenant/lifetime/scope and missing/escaped token paths.
The first transport correction (`160e339…`) passed its CI but the operator
attempt exposed an orchestration bug: `current` was used before its live snapshot
assignment. It stopped before build, transport-file creation or consumer GETs.
Transport creation now follows the post-build live-image and frozen-version
checks. Two full-preparer regression tests replace only external VPS operations:
they cover the build/snapshot/transport/probe order, successful proof persistence,
and consumer denial with temporary-file/container cleanup and no positive proof.
The dedicated Java 21 workflow runs these plus existing mapper/adapter/frozen
contract tests and the packaged JVM entry point. Module CI and VPS execution
remain separate evidence. The first full module run passed functional/DB and
restore checks but Trivy blocked the inherited Jackson Databind 2.21.4 for
CVE-2026-68497 (HIGH, fixed in 2.21.6 on this line). The Jackson BOM is advanced
to 2.21.6; the existing vulnerability threshold remains unchanged. A green scan
and all regressions on the updated commit are required before rollout.
