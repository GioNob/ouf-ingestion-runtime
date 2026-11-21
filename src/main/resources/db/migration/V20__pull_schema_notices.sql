-- One verified descriptor set per source for publications which pin only its fingerprint.
CREATE TABLE ouf_ingestion.pull_verified_fields (
  tenant_id text NOT NULL,
  source_id text NOT NULL,
  fields_sha256 text NOT NULL,
  fields jsonb NOT NULL,
  PRIMARY KEY (tenant_id, source_id)
);

ALTER TABLE ouf_ingestion.onboarding_review_outbox ADD COLUMN notice_sequence bigint;
CREATE UNIQUE INDEX onboarding_notice_order ON ouf_ingestion.onboarding_review_outbox(issue_id, notice_sequence)
  WHERE notice_sequence IS NOT NULL;

ALTER TABLE ouf_ingestion.pull_schema_incident ADD COLUMN opening_approval jsonb,
  ADD COLUMN resume_reason text
  CHECK (resume_reason IN ('SCHEMA_RETURNED','REVISION_ACTIVATED'));

ALTER TABLE ouf_ingestion.pull_schema_incident ADD COLUMN notice_interval_seconds integer NOT NULL DEFAULT 0
  CHECK (notice_interval_seconds >= 0);

-- V19 already marks resumed_at atomically with the first durable checkpoint or empty drain.
CREATE FUNCTION ouf_ingestion.enqueue_schema_notice_progress() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  transition text;
  observed timestamptz;
  seq bigint;
  event_body jsonb;
  opening ouf_ingestion.onboarding_review_outbox%ROWTYPE;
BEGIN
  IF NEW.resumed_at IS NOT NULL AND OLD.resumed_at IS NULL THEN
    transition := 'RESOLVED';
    observed := NEW.resumed_at;
    seq := NEW.observation_polls + 1;
  ELSIF NEW.observation_polls > OLD.observation_polls THEN
    transition := 'UPDATE';
    observed := NEW.last_observed_at;
    seq := NEW.observation_polls;
    IF NEW.notice_interval_seconds > 0 AND observed < (
      SELECT (q.report_json->>'observedAt')::timestamptz + make_interval(secs => NEW.notice_interval_seconds)
      FROM ouf_ingestion.onboarding_review_outbox q
      WHERE q.issue_id=NEW.issue_id AND q.notice_sequence IS NOT NULL
      ORDER BY q.notice_sequence DESC LIMIT 1
    ) THEN RETURN NEW; END IF;
  ELSE
    RETURN NEW;
  END IF;
  -- Keep immutable evidence once. Historical incidents continue on their original protocol.
  SELECT * INTO opening FROM ouf_ingestion.onboarding_review_outbox q
  WHERE q.issue_id=NEW.issue_id AND q.notice_sequence=1;
  IF NOT FOUND OR opening.last_error_code='ING_SCHEMA_NOTICE_EVIDENCE_INCOMPLETE' THEN RETURN NEW; END IF;
  event_body := jsonb_strip_nulls(jsonb_build_object(
    'incidentKey',opening.report_json->>'incidentKey','state',transition,
    'observedAt',date_trunc('milliseconds',observed),'observationPolls',NEW.observation_polls,
    'reason',CASE WHEN transition='RESOLVED' THEN NEW.resume_reason END));

  -- Keep the next UPDATE and the newest cumulative UPDATE while dispatch is behind.
  -- The incident row serializes producers. A reservation freezes the body and transport key.
  IF transition='UPDATE' THEN
    UPDATE ouf_ingestion.onboarding_review_outbox q SET notice_sequence=seq,report_json=event_body,
      idempotency_key='schema-notice:' || NEW.issue_id || ':' || seq
    WHERE q.issue_id=NEW.issue_id AND q.notice_sequence>1 AND q.state='READY' AND NOT q.post_reserved
      AND EXISTS (SELECT 1 FROM ouf_ingestion.onboarding_review_outbox prior WHERE prior.issue_id=q.issue_id
        AND prior.notice_sequence>1 AND prior.notice_sequence<q.notice_sequence AND prior.state<>'ACKED');
    IF FOUND THEN RETURN NEW; END IF;
  END IF;
  INSERT INTO ouf_ingestion.onboarding_review_outbox
    (request_id,issue_id,idempotency_key,correlation_id,notice_sequence,report_json)
  VALUES (gen_random_uuid(),NEW.issue_id,'schema-notice:' || NEW.issue_id || ':' || seq,
    opening.correlation_id,seq,event_body);
  RETURN NEW;
END $$;

CREATE TRIGGER pull_schema_notice_progress AFTER UPDATE OF observation_polls,resumed_at
  ON ouf_ingestion.pull_schema_incident FOR EACH ROW
  EXECUTE FUNCTION ouf_ingestion.enqueue_schema_notice_progress();
