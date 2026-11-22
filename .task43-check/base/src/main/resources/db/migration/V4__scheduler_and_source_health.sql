ALTER TABLE ouf_ingestion.ing_run
  ADD COLUMN tenant_id text NOT NULL DEFAULT 'system',
  ADD COLUMN phase text NOT NULL DEFAULT 'MANAGED_ONCE'
    CHECK(phase IN('FULL_SNAPSHOT','CATCH_UP','DELTA','MANAGED_ONCE','REPLAY')),
  ADD COLUMN failure_code text;

CREATE TABLE ouf_ingestion.ing_schedule(
  schedule_id uuid PRIMARY KEY,
  tenant_id text NOT NULL,
  source_id text NOT NULL UNIQUE,
  interval_seconds integer NOT NULL CHECK(interval_seconds BETWEEN 60 AND 2592000),
  next_run_at timestamptz NOT NULL,
  state text NOT NULL DEFAULT 'ACTIVE' CHECK(state IN('ACTIVE','PAUSED','DISABLED')),
  lease_owner text,
  lease_until timestamptz,
  last_dispatched_at timestamptz,
  lock_version bigint NOT NULL DEFAULT 0,
  CHECK((lease_owner IS NULL)=(lease_until IS NULL))
);
CREATE INDEX ing_schedule_claim_idx ON ouf_ingestion.ing_schedule(state,next_run_at,last_dispatched_at);

CREATE TABLE ouf_ingestion.source_health(
  source_id text PRIMARY KEY,
  status text NOT NULL DEFAULT 'HEALTHY' CHECK(status IN('HEALTHY','DEGRADED','PAUSED')),
  consecutive_failures integer NOT NULL DEFAULT 0,
  circuit_open_until timestamptz,
  last_error_code text,
  updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
