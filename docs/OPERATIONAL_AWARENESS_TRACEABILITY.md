# Operational Awareness — Ingestion Runtime v1.3 traceability

Normative baseline: Ingestion Runtime PET v1.3, MCP PET v1.3, Gateway PET v1.4, Authorization PET v1.5, Cross-Module Alignment Matrix v1.7.

## Implemented evidence

- Existing persisted `runtime_issue`, `source_health` and `ing_run` state is projected through a new bounded internal API.
- `POST /api/internal/v1/ingestion/operations/{status,history,incidents,summary}` requires server-established Authorization context.
- `ingestion.operations.read`, `operations.incident.read` and `operations.status.read` are separate capability checks.
- Tenant filtering is performed server-side from trusted authorization context.
- Results expose semantic state/error/correlation/evidence references; raw logs, stack traces and credentials are not returned.
- Query limits are bounded (100 incidents/status, 200 history).

## Evidence boundary

This increment reuses persisted runtime state already written by Ingestion. It does not yet prove scheduler misfire production, incident dedup/recovery correlation for every failure class, or 30-day deployed retention. Those remain `EVIDENCE PENDING` for the next producer-hardening tranche.

No central incident service is introduced.


## Live R3 acceptance evidence — 21 September 2026

The deployed Ingestion producer completed
`ouf.ingestion.operations.summary` with HTTP 200 as part of the first complete
live `ouf.operations.summary` positive path. The aggregate result was HEALTHY
and non-partial.

### Critical distinction: producer capability versus owner resource capability

The live acceptance exposed an important two-stage Authorization contract.

The producer receipt/filter first authorizes:

`ouf.ingestion.operations.summary`

for a capability resource with TENANT_OPERATIONAL detail.

After that admission, the Ingestion owner performs fine-grained checks using:

`operations.status.read`

against `ResourceContext` values such as:

- `resourceType=operational`;
- `module=INGESTION`;
- optional `sourceRef` / `jobRef`;
- `detailLevel=TENANT_OPERATIONAL`.

The OIDC scope named `operations.status.read` is not a substitute for a
declared OUF capability of the same name, and the producer capability does not
implicitly authorize owner-side operational resources.

In the observed v13 policy, the producer descriptor/grant existed while
`operations.status.read` was absent as both capability and grant. The owner
therefore failed closed with 403. This was correct behavior.

The governed fix introduced an immutable capability registration plus a v14
PolicyBundle descriptor and a temporary nominal grant constrained to:

- `resourceType=operational`;
- `resourceAttributes.module=INGESTION`;
- `allowedDetailLevels=[TENANT_OPERATIONAL]`.

After v14 refresh, the same live producer returned HTTP 200.

### Diagnostic order for Ingestion summary 403

Before changing code or weakening Authorization:

1. verify the parent/producer status in APISIX;
2. verify grant validity time;
3. verify the receipt key mount/path and numeric UID/GID readability;
4. compare the APISIX and owner receipt-key fingerprint without disclosing it;
5. verify `SPRING_CONFIG_ADDITIONAL_LOCATION` and Authorization refresh;
6. verify image/commit provenance;
7. inspect ACTIVE policy for both the producer capability and
   `operations.status.read`;
8. inspect the exact owner ResourceContext and grant constraints.

A completely valid receipt can still produce 403 at owner re-evaluation. That
is a security property, not a transport defect.

### Policy refresh evidence

During the v13 -> v14 transition, the MCP Go evaluator and both Java owner
evaluators continued polling the ACTIVE bundle successfully. HTTP 200 alone was
not treated as enough evidence; the ACTIVE version/hash and changed payload
size were also checked before rerunning the client E2E.

No restart was needed to adopt v14.

## Remaining evidence boundary

The positive summary path is live-evidenced. Negative authorization,
revocation, source-specific partial/redaction, stale policy behavior, real
Ingestion fault/recovery, restart/reboot and long-retention evidence remain
separate acceptance gates.
