CREATE TABLE ouf_ingestion.operational_event(
  event_id bigserial PRIMARY KEY,
  occurred_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  severity text NOT NULL CHECK(severity IN('INFO','WARN','ERROR')),
  event_type text NOT NULL,
  correlation_id text,
  run_id uuid,
  source_id text,
  resource_type text NOT NULL,
  resource_id text NOT NULL,
  error_code text,
  detail_json jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX operational_event_search_idx ON ouf_ingestion.operational_event(occurred_at DESC,event_id DESC);
CREATE INDEX operational_event_correlation_idx ON ouf_ingestion.operational_event(correlation_id,occurred_at DESC);
CREATE TRIGGER operational_event_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.operational_event FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();

CREATE TABLE ouf_ingestion.protected_log_access_audit(
  access_id uuid PRIMARY KEY,
  operation text NOT NULL,
  outcome text NOT NULL CHECK(outcome IN('ALLOWED','DENIED')),
  actor_subject text NOT NULL,
  actor_type text NOT NULL,
  tenant_id text NOT NULL,
  authorization_decision_ref text NOT NULL,
  purpose text,
  request_correlation_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE TRIGGER protected_log_access_append_only BEFORE UPDATE OR DELETE ON ouf_ingestion.protected_log_access_audit FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();

CREATE FUNCTION ouf_ingestion.capture_run_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ouf_ingestion.operational_event(severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code,detail_json)
  VALUES(CASE WHEN NEW.state='FAILED' THEN 'ERROR' ELSE 'INFO' END,'RUN_'||NEW.state,NEW.correlation_id,NEW.run_id,NEW.source_id,'RUN',NEW.run_id::text,NEW.failure_code,jsonb_build_object('phase',NEW.phase,'bundleVersion',NEW.bundle_version));RETURN NEW;
END$$;
CREATE TRIGGER ing_run_operational_event AFTER INSERT OR UPDATE OF state,phase ON ouf_ingestion.ing_run FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_run_event();

CREATE FUNCTION ouf_ingestion.capture_outbox_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r ouf_ingestion.ing_run%ROWTYPE;
BEGIN
  SELECT * INTO r FROM ouf_ingestion.ing_run WHERE run_id=NEW.run_id;
  INSERT INTO ouf_ingestion.operational_event(severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code,detail_json)
  VALUES(CASE WHEN NEW.state LIKE 'FAILED%' THEN 'WARN' ELSE 'INFO' END,'HANDOFF_'||NEW.state,r.correlation_id,NEW.run_id,r.source_id,'HANDOFF',NEW.handoff_id::text,NEW.last_error_code,jsonb_build_object('partitionKey',NEW.partition_key,'sequenceNo',NEW.sequence_no,'attempts',NEW.attempts));RETURN NEW;
END$$;
CREATE TRIGGER handoff_operational_event AFTER INSERT OR UPDATE OF state ON ouf_ingestion.handoff_outbox FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_outbox_event();

CREATE FUNCTION ouf_ingestion.capture_quarantine_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r ouf_ingestion.ing_run%ROWTYPE;
BEGIN
  SELECT * INTO r FROM ouf_ingestion.ing_run WHERE run_id=NEW.run_id;
  INSERT INTO ouf_ingestion.operational_event(severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code)
  VALUES(CASE WHEN NEW.state='OPEN' THEN 'WARN' ELSE 'INFO' END,'QUARANTINE_'||NEW.state,NEW.correlation_id,NEW.run_id,r.source_id,'QUARANTINE',NEW.quarantine_id::text,NEW.reason_code);RETURN NEW;
END$$;
CREATE TRIGGER quarantine_operational_event AFTER INSERT OR UPDATE OF state ON ouf_ingestion.ing_quarantine FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_quarantine_event();

CREATE FUNCTION ouf_ingestion.capture_replay_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE q uuid; c text; rid uuid; src text;
BEGIN
  SELECT r.quarantine_id,r.correlation_id,i.run_id,n.source_id INTO q,c,rid,src FROM ouf_ingestion.replay_request r JOIN ouf_ingestion.ing_quarantine i ON i.quarantine_id=r.quarantine_id JOIN ouf_ingestion.ing_run n ON n.run_id=i.run_id WHERE r.replay_id=NEW.replay_id;
  INSERT INTO ouf_ingestion.operational_event(severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code,detail_json)
  VALUES(CASE WHEN NEW.state='FAILED' THEN 'ERROR' WHEN NEW.state='RETRY_WAIT' THEN 'WARN' ELSE 'INFO' END,'REPLAY_'||NEW.state,c,rid,src,'REPLAY',NEW.replay_id::text,NEW.last_error_code,jsonb_build_object('attempts',NEW.attempts));RETURN NEW;
END$$;
CREATE TRIGGER replay_operational_event AFTER INSERT OR UPDATE OF state ON ouf_ingestion.replay_execution FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_replay_event();
