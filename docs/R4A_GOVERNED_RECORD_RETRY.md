# Governed record retry after a blocking ingestion failure

A run paused on a failed source record must keep the failed attempt and its frozen execution snapshot. The original execution cursor resumes after a HUMAN authorizes the blocking quarantine through retry (expected lifecycleVersion) and resumes the run (expected controlVersion). This is the original run, not a new source activation or a raw replay request.

## Behavior

- `RunExecutionRepository.nextAttemptNumber` reads the next record attempt number under a currently owned, unexpired partition lease; the worker no longer hardcodes attempt 1. Existing stage transaction fencing and the append-only attempt uniqueness constraint remain in place.
- Run resume locks the blocking quarantine rows and permits only RETRY_READY rows. OPEN or REPROCESSING records require a governed transition first.
- Durable delivery ACK resolves only RETRY_READY quarantines from earlier FAILED attempts of the same run and source object with an authorization decision reference. Staging an outbox entry or retrying delivery is insufficient.
- Quarantine lifecycle, linked runtime issue and the QUARANTINE_RETRY_RESOLVED audit event update in the ACK transaction. Failed attempts remain immutable. Duplicate ACK creates no duplicate resolution audit.
- OPEN quarantine, another source object, another run and replay-in-progress are not implicitly resolved. No schema migration, frozen configuration rewrite or business-source condition is added.

## Regression evidence

`RunExecutionWorkerRuntimeTest` adds PostgreSQL regression cases for failure -> blocked resume -> HUMAN retry -> resumed attempt 2 -> retryable delivery failure -> durable ACK -> resolved quarantine/issue -> successful original run; stale partition lease rejection; and ACK isolation from OPEN quarantine or another record. These tests require an isolated test database, never the live database. Module CI runs the full Maven verify suite against PostgreSQL 17.

At preparation time local Maven/PostgreSQL execution was unavailable. CI outcome must be recorded for the exact final commit before deployment; no test success is inferred from inspection.

## Deployment and operations gates

The live R4a release remains `0dfab1e7b2253fd939088259ea61754d6e56706c` until a separately verified candidate switch. Preserve both enabled activation/execution loops, existing transport/token mounts, database history and the PAUSED run. Use database backup/restore drill and retained rollback runtime.

Shared HUMAN run read/resume and quarantine read/retry routes were absent in the latest live inventory; catalogue capabilities, HUMAN grants/scopes and owner authorization must be reconciled before applying recovery. Policy grants/routes are tenant-wide service boundaries, not cinema-only exceptions. Do not use SQL lifecycle writes, delete failed attempts, dismiss an unresolved failure, or reactivate the source as a workaround.

The replay execution SPI and per-source/per-zone lake policies remain separate open requirements. This change implements governed cursor retry; it does not claim a complete raw REPRODUCE/REPROCESS executor or universal replay conformance.
