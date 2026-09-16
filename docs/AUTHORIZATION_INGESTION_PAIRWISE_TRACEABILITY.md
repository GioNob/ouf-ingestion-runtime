# Authorization ↔ Ingestion pairwise traceability

Normative baseline: OUF Reality Baseline v1.7, Authorization PET v1.5, Ingestion Runtime PET v1.3, Cross-Module Alignment Matrix v1.7.

## Scope

This increment aligns Ingestion's trusted authorization context with the scenario-neutral Authorization SDK already published by Source Onboarding & Configuration.

- Canonical actor vocabulary is `HUMAN`, `SERVICE`, `AI_AGENT`.
- Ingestion continues to consume only server-established request attributes; it does not trust actor-supplied authorization headers.
- Tenant, capability set and `authorizationDecisionRef` remain mandatory and fail closed when absent.
- Human-only operations now require canonical actor type `HUMAN`; the legacy `HUMAN_USER` value is rejected.
- The exact `authorizationDecisionRef` is preserved into the Ingestion context for downstream audit/evidence correlation.
- CI pins Source Onboarding main at `fb2dd51dfc204577a47c1e702f17531dd0709b3c` and reads `contracts/authorization/authorization-sdk-v1.json` directly in the pairwise test.

## Evidence classification

CI evidence proves contract compatibility and local trusted-context enforcement. It does not prove deployed IAM/JWKS/issuer configuration, Gateway-to-Ingestion identity propagation, credential rotation/revocation, or production NetworkPolicy behavior; those remain **EVIDENCE PENDING** until deployment/full-path acceptance.
