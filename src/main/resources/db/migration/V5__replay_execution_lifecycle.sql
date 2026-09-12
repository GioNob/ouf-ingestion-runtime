CREATE TABLE ouf_ingestion.replay_execution(
  replay_id uuid PRIMARY KEY REFERENCES ouf_ingestion.replay_request(replay_id) ON DELETE RESTRICT,
  state text NOT NULL DEFAULT 'QUEUED' CHECK(state IN('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')),
  attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 10),
  max_attempts integer NOT NULL DEFAULT 5 CHECK(max_attempts BETWEEN 1 AND 10),
  claimed_by text,
  lease_until timestamptz,
  next_attempt_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  result_attempt_id uuid REFERENCES ouf_ingestion.processing_attempt(attempt_id) ON DELETE RESTRICT,
  downstream_receipt_ref text,
  last_error_code text,
  updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  completed_at timestamptz,
  CHECK((state='RUNNING')=(claimed_by IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX replay_execution_claim_idx ON ouf_ingestion.replay_execution(state,next_attempt_at,lease_until);

CREATE TABLE ouf_ingestion.replay_attempt(
  replay_attempt_id uuid PRIMARY KEY,
  replay_id uuid NOT NULL REFERENCES ouf_ingestion.replay_request(replay_id) ON DELETE RESTRICT,
  attempt_no integer NOT NULL,
  worker_id text NOT NULL,
  outcome text NOT NULL CHECK(outcome IN('STARTED','SUCCEEDED','FAILED_RETRYABLE','FAILED_TERMINAL')),
  safe_error_code text,
  processing_attempt_id uuid REFERENCES ouf_ingestion.processing_attempt(attempt_id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(replay_id,attempt_no,outcome)
);
CREATE TRIGGER replay_attempt_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.replay_attempt
  FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();
