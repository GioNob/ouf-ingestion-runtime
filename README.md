# OUF Ingestion Runtime

Implementation baseline for PET `OUF_PET_EF_Ingestion_Runtime_v1_2`.

Normative priority: L0 invariants > PET v1.2 > frozen rc3 contracts > implementation/tests.

The deployable owns adapter execution, PULL scheduling, managed-file ingestion, transformation, checkpointing, durable handoff, lineage, quarantine and operational state. It does not own onboarding, semantic authoring, authoritative Urban Object resolution, final UDP persistence, IAM policy or Gateway routing.

No module is accepted without adapter contract tests, restart/idempotency evidence, durable-ACK/watermark tests, structured logs, audit, metrics and governed operational/THS integration.


## Governed Gateway client

External REST/WFS adapters call only a configured OUF Gateway endpoint through `HttpGatewaySourceClient`. Enable it with `OUF_INGESTION_GATEWAY_BASE_URL` (Spring property `ouf.ingestion.gateway.base-url`). The request contains the governed `gateway://` binding reference, parameters and correlation ID; raw source URLs and credentials are rejected at the adapter boundary.

Optional settings are `OUF_INGESTION_GATEWAY_CONNECT_TIMEOUT_MS`, `OUF_INGESTION_GATEWAY_REQUEST_TIMEOUT_MS` and `OUF_INGESTION_GATEWAY_MAX_RESPONSE_BYTES`. HTTP 429, 408, 5xx and transport failures are transient; 401/403 are authorization/route failures; other non-success statuses are configuration failures. A valid HTTP response with invalid source content remains a deterministic data error in the selected adapter.
