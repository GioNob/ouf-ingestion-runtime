# OUF Ingestion Runtime

Implementation lineage from PET v1.2; the applicable current baseline is `OUF_PET_EF_Ingestion_Runtime_v1_3_Operational_Awareness`, with Matrix v1.7 / Blueprint v0.3 and terminology notice v1.1. Historical evidence retains its original version.

Normative priority: L0 invariants > applicable PET v1.3 > frozen contracts > implementation/tests. The terminology notice governs terminology only; frozen machine-readable field semantics are preserved.

The deployable owns adapter execution, PULL scheduling, managed-file ingestion, transformation, checkpointing, durable handoff, lineage, quarantine and operational state. It does not own onboarding, semantic authoring, authoritative Urban Object resolution, final UDP persistence, IAM policy or Gateway routing.

No module is accepted without adapter contract tests, restart/idempotency evidence, durable-ACK/watermark tests, structured logs, audit, metrics and governed operational/THS integration.


## Governed Gateway client

External REST/WFS adapters call only a configured OUF Gateway endpoint through `HttpGatewaySourceClient`. Enable it with `OUF_INGESTION_GATEWAY_BASE_URL` (Spring property `ouf.ingestion.gateway.base-url`). The request contains the governed `gateway://` binding reference, parameters and correlation ID; raw source URLs and credentials are rejected at the adapter boundary.

Optional settings are `OUF_INGESTION_GATEWAY_CONNECT_TIMEOUT_MS`, `OUF_INGESTION_GATEWAY_REQUEST_TIMEOUT_MS` and `OUF_INGESTION_GATEWAY_MAX_RESPONSE_BYTES`. HTTP 429, 408, 5xx and transport failures are transient; 401/403 are authorization/route failures; other non-success statuses are configuration failures. A valid HTTP response with invalid source content remains a deterministic data error in the selected adapter.


## File-loop consolidation, 2026-10-10

The owner prioritizes complete and consolidated file onboarding → semantic enrichment → Ingestion → UDP → authorized search, followed by clean-host repeatable installation and then native vertical API PULL according to v2.3. [Executed checkpoint](https://github.com/GioNob/ouf-semantic-registry/blob/052bab768fffccebeefdf69c32225fbbd8bf7bcc/docs/FILE_TO_SEARCH_STATUS_20261010.md), [component CI evidence](evidence/file-loop-ci-qualified-20261010.json) and [supported runtime decision](docs/SUPPORTED_RUNTIME_QUALIFICATION_20261010.md).

Current source qualifications repair the MinIO fixture and security gates; they do not grant target readiness or release acceptance. The technical Spring version evolution is explicit in the deployment checkpoint, preserves Java 21 and interface/DB contract bytes, and requires the same acceptance/recovery checks. No consumed target claim is replayed.
