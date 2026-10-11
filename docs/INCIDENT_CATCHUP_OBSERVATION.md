# Incident catch-up observation — policy v3

Follow the [platform resilience policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and Ingestion PET1.3 Operational Awareness contract.

V16 adds an owner-maintained last_updated_at to runtime_issue. Existing rows start from the last provable created/resolved timestamp. Inserts retain owner first-seen time; every committed change, including severity, investigation metadata and reopening, advances the observation time monotonically. A no-op does not manufacture activity. Callers cannot directly change the field. The timestamp and incident update roll back atomically.

Incident projection, since filtering and deterministic priority ties use this field, with current run/tenant visibility unchanged. Before V16, non-resolution changes to old incidents could be absent from catch-up. V16 does not invent or recover metadata history that was never recorded.

Native regression qualification is pending: old metadata updates, tenant isolation, no-op, direct timestamp tampering, rollback and reopening, plus the earlier resolution/priority tests and owner authorization/HUMAN budget tests. Full module CI applies the additive migration in PostgreSQL.

This is an observation timestamp, not a commit-ordered revision or snapshot cursor. Concurrent transactions can commit after a reader's time boundary. Timestamp-only queries cannot prove complete exactly-once catch-up; use overlapping bounded windows and incident identity while the commit-ordered transition ledger, stable snapshot cursor and persistent operator delivery receipts remain OPEN. Reading is never human acknowledgement or resolution. Full history retention/capacity and current-health summary remain separate open work.

No target migration, deployment/restart, runtime authority change or consumed R9/R10 replay is included.
