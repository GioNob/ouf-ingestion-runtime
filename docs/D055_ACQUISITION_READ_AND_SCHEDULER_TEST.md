# Acquisition evidence and cross-tenant scheduling on the D-055 baseline

[PR #47](https://github.com/GioNob/ouf-ingestion-runtime/pull/47) adds an
authorized read of the contract, ACK and watermark for an existing source/run.
See the [projection and permissions](ACQUISITION_EVIDENCE_READ_20261025.md)
and [redaction diagnostics](ACQUISITION_REDACTION_DIAGNOSTICS_20261026.md).

Ported from [PR #44](https://github.com/GioNob/ouf-ingestion-runtime/pull/44)
(`8a15d8c`), `RunControlRuntimeTest.globalClaimSkipsBackedOffSourceAndServesAnotherTenant`
records a transient 429 with a five-minute retry delay for one tenant's source.
The global scheduler must claim the ready source of the other tenant. While that
claim is held, a second worker must find no eligible source. The test uses the
existing transactional PostgreSQL fixture on `main`.
