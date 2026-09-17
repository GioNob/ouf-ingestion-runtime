# R2b — source data to an authorized, verifiable result

## Human acceptance scenario

An operator approves a file or a PULL configuration with explicit field mappings, identity, classifications and execution/resolution policies. The runtime acquires records automatically, preserves raw evidence in the Lake, sends a durable handoff and advances its watermark only after a valid durable receipt. UDP resolves and materializes the objects using the exact historical publication. An authorized reader can find the result and inspect its lineage. Restricted properties must be absent from that reader's response.

The process regression uses a file containing `Alpha` and a PULL response containing `Beta`. Both contain a restricted `secret` field. A successful test must demonstrate the two public values through the real UDP HTTP serving API and omit the restricted field; SQL counts alone are insufficient.

`SUCCEEDED` in Ingestion means its delivery responsibility is complete. It does not mean UDP materialization is complete. The acceptance test waits separately for authorized serving. A browser journey, MCP tool projection and integrated human progress cards remain R4/R3 deliverables; this increment does not claim that those interfaces exist.

## Normative basis

Reality Baseline Package 1.7 and applicable PETs remain authoritative. Ingestion §§21, 45.2, 80–89, 128, 139; UDP §§95–97, 109.6–109.7. Frozen RC3 contracts and prior Flyway migrations remain unchanged.

## Execution composition

Enable `ouf.ingestion.execution.enabled=true` alongside R2a activation. The execution client uses the configured Gateway root and workload token file (`ouf.ingestion.activation.gateway-url`, `token-file`). Tokens are reread for rotation and excluded from snapshots and evidence. Calls have bounded response sizes and complete-exchange deadlines; redirects are disabled. HTTP 202 never counts as a durable receipt.

The new loop invokes the existing acquisition, pipeline and outbox components. Acquisition uses a bounded batch and a fenced partition claim. Record/outbox/checkpoint persistence verifies the current lease owner, generation, deadline and run state in the same short transaction. Remote calls run outside database transactions.

`PublishedExecutionMapper` deterministically projects the run's immutable `publishedBundle`; it never reads current ACTIVE while processing a run. Under the approved `extractionProfile.runtime.execution`, configurations explicitly supply:

- `acquisitionMode`, `adapterId`, `adapterRuntimeVersion`;
- `sourceSchemaRef`, `sourceSchemaId`, `sourceSchemaVersion`, `semanticPublicationSetRef`, `adapterProfileRef`, `observationPolicy`;
- adapter-specific size/parser/paging limits, and optional exact mapping/authority/relationship references already supported by the pipeline.

Field mappings, expected fields, source identity and access classifications come from the approved mapping, projection, identity policy and data-access policies. Managed asset reference/hash come from the approved runtime asset. There is no automatic invention of property IRIs or identity fields. Missing execution data fails closed. Older R2a admission-only publications require a governed new version before this execution profile can process them.

The bounded adapter modes in this increment are existing CSV/XLSX, REST JSON and WFS implementations; the cross-process scenario exercises CSV and REST JSON. It does not certify other GIS profiles, transformations, incremental strategies or replay modes.

## Downstream boundaries

- Managed content: Gateway `GET /internal/object-storage/v1/content?ref=...` with size and SHA-256 verification.
- PULL: Gateway `POST /internal/sources/v1/fetch`, carrying a logical binding and parameters, never a source URL or source credential.
- Lake: `POST /api/internal/v1/lake/objects`, returning a checksum-verified `lake://` reference.
- Handoff: `POST /api/internal/v1/handoffs`, requiring HTTP 201 plus `durable=true` and the exact handoff receipt.

The owner-side Lake/handoff services require the canonical SERVICE actor and `datalake.write` / `udp.candidate.write`, with configured tenant and source/run scopes. JSON requests have a 10 MiB encoded ceiling. Existing adapter maximum file sizes do not override Gateway/deployment body limits.

## Evidence and explicit limits

Workflow `r2b-serving-live.yml` pins the Onboarding and UDP commits. It runs three real JVM processes, PostgreSQL/PostGIS 17 and MinIO. The Onboarding fixture invokes the real governed lifecycle; the UDP fixture only supplies laboratory identity and creates the test bucket. Both fixtures are excluded from production JARs. The Ingestion process runs its packaged production JAR and scheduled workers.

Gateway HTTP forwarding, managed input bytes, PULL source, Semantic responses, IAM and approval identity are explicitly controlled laboratory seams. APISIX/etcd deployment, real southbound binding invocation, managed object service, IAM/THS and real Semantic integration remain named environment/integration gates. No direct cross-module database adapter is introduced; SQL in the regression is observation/assertion only.

The test returns HTTP 202 after UDP has actually persisted a handoff, verifies the watermark is still absent, kills and restarts Ingestion, then allows the real durable receipt through. It checks idempotent duplicate delivery, both completed runs, committed watermarks, materialization, authorized values, restricted-field omission, lineage, real Lake zone catalog entries and serving after UDP restart.

Final accepted commit/run IDs belong in the cross-module evidence register after CI completes. This document is not itself a PASS assertion.
