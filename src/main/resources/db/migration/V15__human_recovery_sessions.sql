-- Additive recovery ledger. A HUMAN grant is a new session, never a reset.
CREATE TABLE ouf_ingestion.run_recovery_session (
  session_id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ouf_ingestion.ing_run(run_id) ON DELETE RESTRICT,
  epoch bigint NOT NULL CHECK(epoch > 0),
  expected_control_version bigint NOT NULL CHECK(expected_control_version >= 0),
  profile_ref text NOT NULL CHECK(profile_ref = 'ouf.run-recovery-batch.v1'),
  max_attempts integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 10000),
  max_elapsed_seconds integer NOT NULL CHECK(max_elapsed_seconds BETWEEN 1 AND 2592000),
  attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND max_attempts),
  state text NOT NULL DEFAULT 'ACTIVE' CHECK(state IN ('ACTIVE','EXHAUSTED','SUPERSEDED','COMPLETED','CANCELLED')),
  actor_subject text NOT NULL CHECK(length(actor_subject) BETWEEN 1 AND 500),
  authorization_decision_ref text NOT NULL CHECK(length(authorization_decision_ref) BETWEEN 1 AND 1000),
  reason_code text NOT NULL CHECK(reason_code ~ '^[A-Z][A-Z0-9_]{0,79}$'),
  correlation_id text NOT NULL CHECK(length(correlation_id) BETWEEN 1 AND 500),
  granted_at timestamptz NOT NULL,
  deadline_at timestamptz NOT NULL CHECK(deadline_at > granted_at),
  UNIQUE(run_id,epoch), UNIQUE(run_id,expected_control_version)
);
CREATE UNIQUE INDEX run_recovery_one_active ON ouf_ingestion.run_recovery_session(run_id) WHERE state='ACTIVE';
CREATE FUNCTION ouf_ingestion.protect_recovery_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'ING_RECOVERY_HISTORY_IMMUTABLE'; END IF;
  IF (to_jsonb(NEW)-'attempts'-'state') IS DISTINCT FROM (to_jsonb(OLD)-'attempts'-'state')
     OR NEW.attempts<OLD.attempts
     OR (OLD.state<>'ACTIVE' AND NEW IS DISTINCT FROM OLD) THEN
    RAISE EXCEPTION 'ING_RECOVERY_HISTORY_IMMUTABLE';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER run_recovery_history_protected BEFORE UPDATE OR DELETE ON ouf_ingestion.run_recovery_session
  FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.protect_recovery_history();
CREATE UNIQUE INDEX runtime_issue_recovery_budget_uq ON ouf_ingestion.runtime_issue(run_id)
  WHERE issue_code='ING_RECOVERY_BUDGET_EXHAUSTED';
