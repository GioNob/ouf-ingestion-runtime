# Bounded acquisition redaction diagnostics

The acquisition read includes additive `redactionReasons`: a sorted, deduplicated list of fixed categories for the already disclosed REDACTED state. No denied labels, row/property/object/tenant identifiers, hidden counts, values, receipt references or evaluator internals are included.

| Category | Meaning |
| --- | --- |
| DATA_LABEL_METADATA_INVALID | Retained classification metadata is absent, empty, structurally invalid or outside the recognized label domain. |
| DATA_LABEL_NOT_AUTHORIZED | A recognized retained row label fails the existing owner capability/detail checks. |
| RETAINED_TENANT_NOT_VERIFIED | UDP cannot establish the existing retained RAW/object tenant ownership invariant. |
| OBJECT_NOT_AUTHORIZED | UDP row passes tenant and label checks but the existing object permission check denies. |
| PROPERTY_NOT_AUTHORIZED | UDP omits a property under the existing property permission check. |

Ingestion emits only the first two categories. No category implies a particular hidden label, tenant, object or property. The list has no per-record association or counts. A tenant failure prevents evaluating/disclosing finer row reasons. The first failed row guard stops evaluation for that row. Authorized pages, including pagination-only partial pages, have an empty list.

Existing base admission, source/run/tenant, scopes, per-label/object/property checks, final freshness, read-only transaction, bounded paging and hidden-cursor suppression remain in force. No new permission, SQL projection, migration, worker, route or acquisition. This diagnostic does not authorize data release or E2E acceptance.

Validation uses synthetic identities/data in PostgreSQL17 and the packaged isolated read image. It checks missing/empty/malformed/unknown labels, valid denied labels, tenant precedence, distinct object/property denials, hidden data suppression and existing no-write behavior. Live cause remains unknown until separately reviewed installation and governed read. CI is not VPS evidence.

Normative references consulted from the attached baseline: Authorization PET1.5 §§23.2,36.10 and Operational Awareness (server-side minimization, fail-closed missing labels, auditable redaction without removed secrets); UDP PET1.3 §§36,40,50 (owner filtering before serialization and anti-enumeration); miniPET CSV-R20.
