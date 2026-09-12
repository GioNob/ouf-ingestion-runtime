ALTER TABLE ouf_ingestion.handoff_outbox
  ADD COLUMN partition_key text NOT NULL DEFAULT 'default',
  ADD COLUMN sequence_no bigint NOT NULL DEFAULT 0,
  ADD COLUMN candidate_watermark_json jsonb NOT NULL DEFAULT '{}',
  ADD COLUMN last_error_code text;
CREATE UNIQUE INDEX handoff_partition_sequence_uq
  ON ouf_ingestion.handoff_outbox(run_id,partition_key,sequence_no);

CREATE TABLE ouf_ingestion.ing_watermark(
  source_id text NOT NULL,
  partition_key text NOT NULL,
  watermark_json jsonb NOT NULL,
  committed_handoff_id uuid NOT NULL REFERENCES ouf_ingestion.handoff_outbox ON DELETE RESTRICT,
  committed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  lock_version bigint NOT NULL DEFAULT 0,
  PRIMARY KEY(source_id,partition_key)
);
CREATE TABLE ouf_ingestion.checkpoint_history(
  checkpoint_id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ouf_ingestion.ing_run ON DELETE RESTRICT,
  partition_key text NOT NULL,
  sequence_no bigint NOT NULL,
  checkpoint_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE(run_id,partition_key,sequence_no)
);
CREATE TABLE ouf_ingestion.replay_request(
  replay_id uuid PRIMARY KEY,
  quarantine_id uuid NOT NULL REFERENCES ouf_ingestion.ing_quarantine ON DELETE RESTRICT,
  requested_by text NOT NULL,
  capability text NOT NULL,
  target_bundle_ref text NOT NULL,
  state text NOT NULL DEFAULT 'QUEUED' CHECK(state IN('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
  correlation_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE TRIGGER checkpoint_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.checkpoint_history
  FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();
CREATE TRIGGER replay_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.replay_request
  FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();
