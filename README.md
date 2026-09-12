# OUF Ingestion Runtime

Implementation baseline for PET `OUF_PET_EF_Ingestion_Runtime_v1_2`.

Normative priority: L0 invariants > PET v1.2 > frozen rc3 contracts > implementation/tests.

The deployable owns adapter execution, PULL scheduling, managed-file ingestion, transformation, checkpointing, durable handoff, lineage, quarantine and operational state. It does not own onboarding, semantic authoring, authoritative Urban Object resolution, final UDP persistence, IAM policy or Gateway routing.

No module is accepted without adapter contract tests, restart/idempotency evidence, durable-ACK/watermark tests, structured logs, audit, metrics and governed operational/THS integration.
