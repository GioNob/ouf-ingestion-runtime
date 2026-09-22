-- Owner-local durable incident state. Writes share the transaction of the run/issue.
ALTER TABLE ouf_ingestion.ing_run DROP CONSTRAINT ing_run_state_check;
ALTER TABLE ouf_ingestion.ing_run ADD CONSTRAINT ing_run_state_check CHECK(state IN
 ('READY','PREFLIGHT','RUNNING','RETRY_WAIT','DRAINING','SUCCEEDED','COMPLETED_WITH_WARNINGS','FAILED','PAUSED','ABORTED'));
ALTER TABLE ouf_ingestion.ing_run ADD COLUMN retry_stage text CHECK(retry_stage IN('PREFLIGHT','EXECUTION'));
ALTER TABLE ouf_ingestion.ing_schedule ADD COLUMN activation_run_id uuid REFERENCES ouf_ingestion.ing_run ON DELETE RESTRICT,
 ADD COLUMN activation_due_at timestamptz;
ALTER TABLE ouf_ingestion.ing_partition
 ADD COLUMN retry_count integer NOT NULL DEFAULT 0 CHECK(retry_count>=0),
 ADD COLUMN retry_first_failure_at timestamptz,
 ADD COLUMN retry_not_before timestamptz,
 ADD COLUMN retry_deadline timestamptz;

CREATE TABLE ouf_ingestion.operational_incident (
 incident_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 origin_key text NOT NULL UNIQUE,
 tenant_id text NOT NULL,
 run_id uuid REFERENCES ouf_ingestion.ing_run ON DELETE RESTRICT,
 schedule_id uuid REFERENCES ouf_ingestion.ing_schedule ON DELETE RESTRICT,
 source_id text NOT NULL,
 error_code text NOT NULL CHECK(error_code ~ '^[A-Z0-9_]{1,80}$'),
 lifecycle_state text NOT NULL CHECK(lifecycle_state IN('OPEN','RECOVERING','RESOLVED')),
 event_type text NOT NULL,
 severity text NOT NULL CHECK(severity IN('WARNING','ERROR')),
 first_seen_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 last_seen_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 resolved_at timestamptz,
 next_retry_at timestamptz,
 attempt_count integer NOT NULL DEFAULT 0,
 retain_until timestamptz NOT NULL DEFAULT transaction_timestamp()+interval '30 days',
 CHECK((lifecycle_state='RESOLVED')=(resolved_at IS NOT NULL))
);
CREATE INDEX operational_incident_tenant_state ON ouf_ingestion.operational_incident(tenant_id,lifecycle_state,last_seen_at DESC);
CREATE TABLE ouf_ingestion.operational_incident_transition (
 sequence_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 event_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
 incident_id uuid NOT NULL REFERENCES ouf_ingestion.operational_incident ON DELETE RESTRICT,
 tenant_id text NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 projection jsonb NOT NULL
);
CREATE INDEX operational_incident_transition_snapshot ON ouf_ingestion.operational_incident_transition(tenant_id,incident_id,sequence_id DESC);
-- References are conservative retention blockers, independent from incident lifecycle.
CREATE TABLE ouf_ingestion.operational_incident_hold (
 incident_id uuid NOT NULL REFERENCES ouf_ingestion.operational_incident ON DELETE RESTRICT,
 reference_type text NOT NULL CHECK(reference_type IN('AUDIT','LINEAGE','LEGAL_HOLD','INVESTIGATION')),
 reference_id text NOT NULL,
 PRIMARY KEY(incident_id,reference_type,reference_id)
);

CREATE FUNCTION ouf_ingestion.record_incident(p_origin text,p_run uuid,p_code text,p_state text,
 p_event text,p_severity text,p_next timestamptz,p_attempts integer,p_schedule uuid DEFAULT NULL) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE r ouf_ingestion.ing_run%ROWTYPE; i ouf_ingestion.operational_incident%ROWTYPE; days integer; ev uuid:=gen_random_uuid();
BEGIN
 -- Serialize sequence allocation through commit, so a snapshot watermark cannot skip an uncommitted earlier event.
 PERFORM pg_advisory_xact_lock(hashtextextended('ouf-ingestion-incident-timeline',0));
 IF p_run IS NOT NULL THEN
  SELECT * INTO STRICT r FROM ouf_ingestion.ing_run WHERE run_id=p_run;
 ELSE
  SELECT tenant_id,source_id INTO STRICT r.tenant_id,r.source_id FROM ouf_ingestion.ing_schedule WHERE schedule_id=p_schedule;
  r.correlation_id:='schedule:'||p_schedule;
 END IF;
 SELECT greatest(30,least(3650,coalesce((snapshot_json#>>'{configuration,syncProfile,operationalPolicy,operationalRetentionDays}')::integer,30)))
 INTO days FROM ouf_ingestion.runtime_configuration_snapshot WHERE run_id=p_run;
 IF p_run IS NULL THEN
  SELECT greatest(30,least(3650,coalesce((sync_profile#>>'{operationalPolicy,operationalRetentionDays}')::integer,30))) INTO days FROM ouf_ingestion.ing_schedule WHERE schedule_id=p_schedule;
 END IF;
 INSERT INTO ouf_ingestion.operational_incident(origin_key,tenant_id,run_id,schedule_id,source_id,error_code,lifecycle_state,event_type,severity,resolved_at,next_retry_at,attempt_count,retain_until)
 VALUES(p_origin,r.tenant_id,p_run,p_schedule,r.source_id,p_code,p_state,p_event,p_severity,
  CASE WHEN p_state='RESOLVED' THEN transaction_timestamp() END,p_next,p_attempts,transaction_timestamp()+coalesce(days,30)*interval '1 day')
 ON CONFLICT(origin_key) DO UPDATE SET lifecycle_state=p_state,event_type=p_event,severity=p_severity,
 last_seen_at=transaction_timestamp(),resolved_at=CASE WHEN p_state='RESOLVED' THEN transaction_timestamp() END,
 next_retry_at=p_next,attempt_count=greatest(operational_incident.attempt_count,p_attempts),
 retain_until=greatest(operational_incident.retain_until,transaction_timestamp()+coalesce(days,30)*interval '1 day')
 RETURNING * INTO i;
 INSERT INTO ouf_ingestion.operational_incident_transition(event_id,incident_id,tenant_id,projection)
 VALUES(ev,i.incident_id,i.tenant_id,jsonb_build_object(
  'event_id',ev,'incident_id',i.incident_id,'module','INGESTION','event_type',i.event_type,
  'lifecycle_state',i.lifecycle_state,'severity',i.severity,'first_seen_at',i.first_seen_at,'last_seen_at',i.last_seen_at,
  'resolved_at',i.resolved_at,'job_ref',i.run_id,'source_ref',i.source_id,'error_code',i.error_code,
  'correlation_id',r.correlation_id,'retry_state',CASE WHEN p_state='RECOVERING' THEN CASE WHEN p_next IS NULL THEN 'RUNNING' ELSE 'RETRY_WAIT' END ELSE 'NONE' END,
  'next_retry_at',i.next_retry_at,'attempt_count',i.attempt_count,
  'duration_ms',CASE WHEN i.resolved_at IS NOT NULL THEN greatest(0,extract(epoch FROM i.resolved_at-i.first_seen_at)*1000)::bigint END,
  'safe_outcome',CASE WHEN p_state='RESOLVED' THEN p_event END,
  'action_required',p_state='OPEN','impact_summary',CASE p_state WHEN 'OPEN' THEN 'Ingestion requires governed remediation.' WHEN 'RECOVERING' THEN 'Ingestion recovery is in progress.' ELSE 'Ingestion incident resolved.' END,
  'dedup_key',i.incident_id,'visibility_class','TENANT_OPERATIONAL'));
 RETURN i.incident_id;
END $$;

CREATE FUNCTION ouf_ingestion.capture_run_incident() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE i record; attempts integer; retry_at timestamptz; code text;
BEGIN
 IF NEW.state IN('RETRY_WAIT','FAILED','PAUSED','ABORTED') AND
    (NEW.state IS DISTINCT FROM OLD.state OR NEW.failure_code IS DISTINCT FROM OLD.failure_code OR NEW.updated_at IS DISTINCT FROM OLD.updated_at) THEN
  code:=CASE WHEN NEW.failure_code ~ '^[A-Z0-9_]{1,80}$' THEN NEW.failure_code ELSE 'ING_RUN_INTERRUPTED' END;
  SELECT coalesce(sum(retry_count),0),min(retry_not_before) INTO attempts,retry_at FROM ouf_ingestion.ing_partition WHERE run_id=NEW.run_id;
  PERFORM ouf_ingestion.record_incident('run:'||NEW.run_id||':'||code,NEW.run_id,code,
    CASE WHEN NEW.state='RETRY_WAIT' THEN 'RECOVERING' ELSE 'OPEN' END,
    CASE WHEN NEW.state='RETRY_WAIT' THEN 'RETRY_SCHEDULED' ELSE 'RUN_FAILED' END,'ERROR',
    CASE WHEN NEW.state='RETRY_WAIT' THEN retry_at END,attempts);
 ELSIF NEW.state IN('SUCCEEDED','COMPLETED_WITH_WARNINGS') AND NEW.state IS DISTINCT FROM OLD.state THEN
  FOR i IN SELECT * FROM ouf_ingestion.operational_incident WHERE (run_id=NEW.run_id AND origin_key LIKE 'run:%' OR schedule_id=NEW.schedule_id AND origin_key LIKE 'schedule:%') AND lifecycle_state<>'RESOLVED' LOOP
   PERFORM ouf_ingestion.record_incident(i.origin_key,i.run_id,i.error_code,'RESOLVED','RUN_RECOVERED',i.severity,NULL,i.attempt_count,i.schedule_id);
  END LOOP;
 ELSIF NEW.state IN('RUNNING','PREFLIGHT') AND OLD.state IN('RETRY_WAIT','PAUSED') THEN
  FOR i IN SELECT * FROM ouf_ingestion.operational_incident WHERE (run_id=NEW.run_id AND origin_key LIKE 'run:%' OR schedule_id=NEW.schedule_id AND origin_key LIKE 'schedule:%') AND lifecycle_state<>'RESOLVED' LOOP
   PERFORM ouf_ingestion.record_incident(i.origin_key,i.run_id,i.error_code,'RECOVERING','RETRY_STARTED',i.severity,NULL,i.attempt_count,i.schedule_id);
  END LOOP;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER run_incident AFTER UPDATE OF state,failure_code,updated_at ON ouf_ingestion.ing_run
 FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_run_incident();

CREATE FUNCTION ouf_ingestion.capture_issue_incident() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM ouf_ingestion.record_incident('issue:'||NEW.issue_id,NEW.run_id,NEW.issue_code,
  CASE WHEN NEW.status='OPEN' THEN 'OPEN' ELSE 'RESOLVED' END,
  CASE WHEN NEW.status='OPEN' THEN 'RUNTIME_ISSUE_OPENED' ELSE 'RUNTIME_ISSUE_RESOLVED' END,
  NEW.severity,NULL,0);
 RETURN NEW;
END $$;
CREATE TRIGGER issue_incident AFTER INSERT OR UPDATE OF status ON ouf_ingestion.runtime_issue
 FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_issue_incident();
-- Preserve already existing governed issues in the new catch-up projection.
DO $$ DECLARE i record; BEGIN
 FOR i IN SELECT * FROM ouf_ingestion.runtime_issue ORDER BY created_at,issue_id LOOP
  PERFORM ouf_ingestion.record_incident('issue:'||i.issue_id,i.run_id,i.issue_code,
   CASE WHEN i.status='OPEN' THEN 'OPEN' ELSE 'RESOLVED' END,'MIGRATED_RUNTIME_ISSUE',i.severity,NULL,0);
 END LOOP;
END $$;

-- Backfill keeps the original incident times; migration time is not an occurrence time.
UPDATE ouf_ingestion.operational_incident o SET first_seen_at=i.created_at,last_seen_at=coalesce(i.resolved_at,i.created_at),resolved_at=CASE WHEN i.status<>'OPEN' THEN coalesce(i.resolved_at,i.created_at) END
 FROM ouf_ingestion.runtime_issue i WHERE o.origin_key='issue:'||i.issue_id;
UPDATE ouf_ingestion.operational_incident_transition t SET projection=t.projection||jsonb_build_object('first_seen_at',i.first_seen_at,'last_seen_at',i.last_seen_at,'resolved_at',i.resolved_at)
 FROM ouf_ingestion.operational_incident i WHERE t.incident_id=i.incident_id;
CREATE TRIGGER incident_transition_immutable BEFORE UPDATE ON ouf_ingestion.operational_incident_transition
 FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();

-- Explicit bounded maintenance, never called during a read. Run-level audit/lineage and any hold block deletion.
CREATE FUNCTION ouf_ingestion.prune_resolved_incidents(p_limit integer DEFAULT 100) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE i record; n integer:=0;
BEGIN
 IF p_limit<1 OR p_limit>100 THEN RAISE EXCEPTION 'ING_RETENTION_LIMIT_INVALID'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('ouf-ingestion-incident-timeline',0));
 FOR i IN SELECT x.incident_id FROM ouf_ingestion.operational_incident x
  WHERE lifecycle_state='RESOLVED' AND retain_until<transaction_timestamp() AND resolved_at<transaction_timestamp()-interval '30 days'
   AND NOT EXISTS(SELECT 1 FROM ouf_ingestion.operational_incident_hold h WHERE h.incident_id=x.incident_id)
   AND NOT EXISTS(SELECT 1 FROM ouf_ingestion.ing_lineage l WHERE l.run_id=x.run_id)
   AND NOT EXISTS(SELECT 1 FROM ouf_ingestion.ing_run r
     JOIN ouf_ingestion.runtime_configuration_snapshot s USING(run_id)
     JOIN (SELECT DISTINCT ON(tenant_id,contract_ref) tenant_id,contract_ref,action
       FROM ouf_ingestion.historical_contract_legal_hold_event ORDER BY tenant_id,contract_ref,created_at DESC,event_id DESC) h
       ON h.tenant_id=r.tenant_id AND h.action='APPLY'
     WHERE r.run_id=x.run_id AND ((r.bundle_id||':'||r.bundle_version||':'||r.bundle_checksum)=h.contract_ref
       OR jsonb_path_exists(s.snapshot_json,'$.** ? (@ == $ref)',jsonb_build_object('ref',h.contract_ref))))
   AND NOT EXISTS(SELECT 1 FROM ouf_ingestion.audit_event a WHERE a.resource_id IN(x.run_id::text,x.incident_id::text,x.schedule_id::text,replace(x.origin_key,'issue:','')))
  ORDER BY retain_until,x.incident_id LIMIT p_limit FOR UPDATE OF x SKIP LOCKED LOOP
  DELETE FROM ouf_ingestion.operational_incident_transition WHERE incident_id=i.incident_id;
  DELETE FROM ouf_ingestion.operational_incident WHERE incident_id=i.incident_id;
  n:=n+1;
 END LOOP;
 RETURN n;
END $$;
