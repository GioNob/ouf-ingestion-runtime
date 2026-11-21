CREATE TABLE ouf_ingestion.pull_schema_incident (
  issue_id uuid PRIMARY KEY REFERENCES ouf_ingestion.runtime_issue,
  tenant_id text NOT NULL,
  source_id text NOT NULL,
  binding_ref text NOT NULL,
  expected_schema jsonb NOT NULL,
  observed_schema jsonb NOT NULL,
  differences jsonb NOT NULL,
  frozen_checkpoint jsonb NOT NULL,
  observation_polls bigint NOT NULL DEFAULT 1,
  last_poll uuid NOT NULL REFERENCES ouf_ingestion.ing_run,
  resume_run uuid REFERENCES ouf_ingestion.ing_run,
  resumed_at timestamptz,
  last_observed_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE UNIQUE INDEX pull_schema_open_scope ON ouf_ingestion.pull_schema_incident(tenant_id,source_id)
  WHERE resumed_at IS NULL;

CREATE VIEW ouf_ingestion.pull_schema_notice AS
 SELECT g.tenant_id,g.source_id,g.issue_id,i.created_at AS since,g.last_observed_at,
        g.expected_schema,g.observed_schema,g.differences,g.observation_polls,
        'SCHEMA_CHANGED'::text AS state,'REVISE_ONBOARDING'::text AS next_action
 FROM ouf_ingestion.pull_schema_incident g JOIN ouf_ingestion.runtime_issue i USING(issue_id)
 WHERE g.resumed_at IS NULL;

-- The first staged record or successful empty poll proves consumption resumed.
-- A crash between page approval and processing leaves the notice open.
CREATE FUNCTION ouf_ingestion.resolve_pull_schema_gate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  WITH resumed AS (
    UPDATE ouf_ingestion.pull_schema_incident SET resumed_at=clock_timestamp()
    WHERE resume_run=NEW.run_id AND resumed_at IS NULL RETURNING issue_id
  )
  UPDATE ouf_ingestion.runtime_issue i SET status='RESOLVED',resolution_reason='SCHEMA_MATCH_CONSUMPTION_RESUMED',
    resolved_at=clock_timestamp(),lock_version=lock_version+1 FROM resumed WHERE i.issue_id=resumed.issue_id;
  RETURN NEW;
END $$;
CREATE INDEX pull_schema_resuming ON ouf_ingestion.pull_schema_incident(resume_run) WHERE resumed_at IS NULL;
CREATE TRIGGER pull_schema_resumed AFTER UPDATE ON ouf_ingestion.ing_partition
 FOR EACH ROW WHEN (NEW.checkpoint_json IS DISTINCT FROM OLD.checkpoint_json OR NEW.state='DRAINING' AND OLD.state='RUNNING')
 EXECUTE FUNCTION ouf_ingestion.resolve_pull_schema_gate();

ALTER TABLE ouf_ingestion.onboarding_review_outbox
 ADD COLUMN post_reserved boolean NOT NULL DEFAULT false,
 ADD COLUMN first_attempt_at timestamptz,
 ADD COLUMN report_json jsonb;

-- Historical attempts may already have reached their owner.
UPDATE ouf_ingestion.onboarding_review_outbox SET post_reserved=true,first_attempt_at=created_at WHERE attempts>0;

