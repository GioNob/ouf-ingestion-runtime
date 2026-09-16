# Ingestion Operational Awareness — incident explain

Normative baseline: Ingestion Runtime PET v1.3 Operational Awareness, Authorization PET v1.5, MCP PET v1.3, Cross-Module Alignment Matrix v1.6.

The internal Operational Awareness API now exposes `/operations/incidents/explain`. It requires `operations.incident.explain`, validates a concrete incident UUID, reuses the existing tenant-scoped `RuntimeIssueService.explain` projection and returns classification, severity, lifecycle state, evidence reference and recommended action without raw logs or protected evidence payloads.

CI evidence proves compile/test/package behavior. Real Authorization/IAM deployment and cross-module runtime invocation remain EVIDENCE PENDING until pairwise/full-path execution.
