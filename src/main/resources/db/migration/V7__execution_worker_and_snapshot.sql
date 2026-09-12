CREATE TABLE ouf_ingestion.runtime_configuration_snapshot(
  snapshot_id uuid PRIMARY KEY,
  run_id uuid NOT NULL UNIQUE REFERENCES ouf_ingestion.ing_run ON DELETE RESTRICT,
  bundle_id text NOT NULL,
  bundle_version text NOT NULL,
  bundle_checksum text NOT NULL,
  snapshot_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE TRIGGER runtime_snapshot_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.runtime_configuration_snapshot
  FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();

ALTER TABLE ouf_ingestion.ing_partition
  ADD COLUMN lease_owner text,
  ADD COLUMN lease_until timestamptz,
  ADD COLUMN lease_generation bigint NOT NULL DEFAULT 0,
  ADD CONSTRAINT ing_partition_lease_pair CHECK((lease_owner IS NULL)=(lease_until IS NULL));

CREATE INDEX ing_partition_worker_claim_idx ON ouf_ingestion.ing_partition(state,lease_until);
