# OUF Ingestion Runtime

Implementation baseline for PET `OUF_PET_EF_Ingestion_Runtime_v1_2`.

Normative priority: L0 invariants > PET v1.2 > frozen rc3 contracts > implementation/tests.

The deployable owns adapter execution, PULL scheduling, managed-file ingestion, transformation, checkpointing, durable handoff, lineage, quarantine and operational state. It does not own onboarding, semantic authoring, authoritative Urban Object resolution, final UDP persistence, IAM policy or Gateway routing.

For managed CSV, the frozen Onboarding runtime carries `csvDelimiter` (`;` or `,`).
The adapter removes an optional leading UTF-8 BOM only while parsing; the
approved byte count and content hash still cover the original file. Historical
bundles without the field retain comma parsing.

No module is accepted without adapter contract tests, restart/idempotency evidence, durable-ACK/watermark tests, structured logs, audit, metrics and governed operational/THS integration.


## Governed Gateway client

External REST/WFS adapters call only a configured OUF Gateway endpoint through `HttpGatewaySourceClient`. Enable it with `OUF_INGESTION_GATEWAY_BASE_URL` (Spring property `ouf.ingestion.gateway.base-url`). The request contains the governed `gateway://` binding reference, parameters and correlation ID; raw source URLs and credentials are rejected at the adapter boundary.

Optional settings are `OUF_INGESTION_GATEWAY_CONNECT_TIMEOUT_MS`, `OUF_INGESTION_GATEWAY_REQUEST_TIMEOUT_MS` and `OUF_INGESTION_GATEWAY_MAX_RESPONSE_BYTES`. HTTP 429, 408, 5xx and transport failures are transient; 401/403 are authorization/route failures; other non-success statuses are configuration failures. A valid HTTP response with invalid source content remains a deterministic data error in the selected adapter.


## Consolidated live lineage — 2026-10-10

Main reconciles the advanced governed-record-retry / IAM / frozen-configuration source baseline 163c167d09b8371ff7a62ce7068e9d485b6969b7 with supported-runtime corrections. All fourteen retained SQL migrations are unchanged. Candidate 8b908554314617298242eb2ae8312f734f55e817 passes all eight workflows, 118 tests (zero failures/errors/skips), full HIGH/CRITICAL image scan including unfixed entries (zero findings), PostgreSQL restore, R2 serving/GeoPackage and frozen consumer probes. SQLite native plus XLSX round-trip also pass inside the actual non-root runtime image with network disabled, root filesystem read-only and isolated executable /tmp for native-library loading. [Qualification](docs/LIVE_BASELINE_QUALIFICATION_20261010.md), [current evidence](evidence/live-baseline-qualified-20261010.json).

The original distroless scan failed on nine OS HIGH; its evidence is retained. No exceptions were added. Older file-loop-ci-qualified evidence remains historical and does not describe this candidate. Source reconciliation does not import an image, mutate the VPS, replay Cinema or grant release acceptance. Current PET v1.3 / Matrix v1.7 / Blueprint v0.3 and terminology notice v1.1 remain applicable; file consolidation and authorized search precede native vertical API PULL v2.3. Pin actual Docker base digests during release preparation.
