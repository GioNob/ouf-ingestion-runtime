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
source, version and expected configuration hash. It reads the frozen configuration
with a read-only transaction and retains it only in memory/stdin. It verifies the
old staged worktree, ancestry, remote HEAD, exact migration-file checksums and
live Flyway 14. It creates an isolated worktree and builds a revision-labelled
image, leaving the old stage manifest, checked-out branches and live container
intact. A build failure stops the gate; its log is private under `/opt/ouf/r4a-stage`.

The candidate JVM runs non-root with read-only auth/properties mounts, one backend
network, no published ports, dropped Linux capabilities and bounded memory/CPU.
It receives no database credentials or receipt-signing key. Its only remote
operations are authenticated Gateway GETs for the asset and Semantic references.
The disposable probe container is removed, including after timeout; worktree,
image and evidence remain available. The live image and frozen hash are reread
after validation. A PASS is saved as `ingestion-compatibility-probe.json` with
`candidateDeployed=false` and `attestationSubmitted=false`.

The mounted `/run/secrets/ingestion-summary.properties` must contain explicit
`ouf.ingestion.activation.gateway-url`, `token-file` and `tenant-id`. Unresolved
placeholders or unavailable credentials fail closed. Secrets remain in existing
mounts; never paste their values or file contents into chat. Gateway/Authorization
enforce the existing service access to content and exact Semantic references.

## Remaining deployment and attestation gates

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
The dedicated Java 21 workflow runs these plus existing mapper/adapter/frozen
contract tests. Module CI and VPS execution remain separate evidence.
