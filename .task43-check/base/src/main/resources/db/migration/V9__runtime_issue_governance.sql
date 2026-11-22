ALTER TABLE ouf_ingestion.runtime_issue
  ADD COLUMN lock_version bigint NOT NULL DEFAULT 0,
  ADD COLUMN resolution_reason text,
  ADD COLUMN resolution_actor text,
  ADD COLUMN authorization_decision_ref text,
  ADD COLUMN onboarding_review_ref text,
  ADD COLUMN quarantine_id uuid REFERENCES ouf_ingestion.ing_quarantine ON DELETE RESTRICT;
CREATE UNIQUE INDEX runtime_issue_quarantine_uq ON ouf_ingestion.runtime_issue(quarantine_id) WHERE quarantine_id IS NOT NULL;

CREATE TABLE ouf_ingestion.onboarding_review_outbox(
  request_id uuid PRIMARY KEY,
  issue_id uuid NOT NULL REFERENCES ouf_ingestion.runtime_issue ON DELETE RESTRICT,
  idempotency_key text NOT NULL UNIQUE,
  correlation_id text NOT NULL,
  state text NOT NULL DEFAULT 'READY' CHECK(state IN('READY','DELIVERING','ACKED','FAILED_RETRYABLE','FAILED_TERMINAL')),
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  lease_owner text,
  lease_until timestamptz,
  onboarding_review_ref text,
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE INDEX onboarding_review_claim_idx ON ouf_ingestion.onboarding_review_outbox(state,next_attempt_at,lease_until);

CREATE OR REPLACE FUNCTION ouf_ingestion.capture_runtime_issue_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ouf_ingestion.operational_event(severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code,detail_json)
  VALUES(CASE WHEN NEW.severity='ERROR' THEN 'ERROR' ELSE 'WARN' END,CASE WHEN NEW.issue_code LIKE 'ING_SCHEMA_%' THEN 'SCHEMA_DRIFT_' ELSE 'RUNTIME_ISSUE_' END||NEW.status,NEW.correlation_id,NEW.run_id,NEW.source_id,'RUNTIME_ISSUE',NEW.issue_id::text,NEW.issue_code,jsonb_build_object('typeCode',NEW.type_code,'evidenceRef',NEW.evidence_ref));RETURN NEW;
END$$;
