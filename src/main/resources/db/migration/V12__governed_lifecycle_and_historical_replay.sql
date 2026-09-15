ALTER TABLE ouf_ingestion.ing_run DROP CONSTRAINT ing_run_state_check;
ALTER TABLE ouf_ingestion.ing_run
  ADD CONSTRAINT ing_run_state_check CHECK(state IN('READY','PREFLIGHT','RUNNING','DRAINING','SUCCEEDED','COMPLETED_WITH_WARNINGS','FAILED','PAUSED','ABORTED')),
  ADD COLUMN control_version bigint NOT NULL DEFAULT 0;

ALTER TABLE ouf_ingestion.ing_quarantine
  ADD COLUMN blocking boolean NOT NULL DEFAULT true;

ALTER TABLE ouf_ingestion.replay_request
  ADD COLUMN replay_mode text NOT NULL DEFAULT 'REPROCESS_TARGET' CHECK(replay_mode IN('REPRODUCE','REPROCESS_CURRENT','REPROCESS_TARGET')),
  ADD COLUMN original_run_id uuid REFERENCES ouf_ingestion.ing_run(run_id) ON DELETE RESTRICT,
  ADD COLUMN original_bundle_ref text,
  ADD COLUMN raw_object_ref text,
  ADD COLUMN parent_attempt_id uuid REFERENCES ouf_ingestion.processing_attempt(attempt_id) ON DELETE RESTRICT,
  ADD COLUMN idempotency_key text;
CREATE UNIQUE INDEX replay_request_idempotency_uq ON ouf_ingestion.replay_request(tenant_id,idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE ouf_ingestion.historical_contract_legal_hold_event(
  event_id uuid PRIMARY KEY,
  contract_ref text NOT NULL,
  action text NOT NULL CHECK(action IN('APPLY','RELEASE')),
  reason text NOT NULL,
  created_by text NOT NULL,
  tenant_id text NOT NULL,
  authorization_decision_ref text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE INDEX historical_contract_legal_hold_ref_idx ON ouf_ingestion.historical_contract_legal_hold_event(contract_ref,created_at DESC);
CREATE TRIGGER historical_contract_legal_hold_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.historical_contract_legal_hold_event FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();
