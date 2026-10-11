-- V17: immutable owner transition snapshots, then commit-ordered publication.
-- The private bigserial is NOT a public cursor; late commits are scanned as pending.
ALTER TABLE ouf_ingestion.runtime_issue ADD COLUMN operational_version bigint NOT NULL DEFAULT 0 CHECK(operational_version>=0);

CREATE TABLE ouf_ingestion.incident_history_coverage(
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  baseline_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO ouf_ingestion.incident_history_coverage(singleton) VALUES(true);

CREATE TABLE ouf_ingestion.incident_transition(
  transition_id bigserial PRIMARY KEY,
  tenant_id text NOT NULL,
  issue_id uuid NOT NULL REFERENCES ouf_ingestion.runtime_issue ON DELETE RESTRICT,
  issue_version bigint NOT NULL CHECK(issue_version>=0),
  transition_kind text NOT NULL CHECK(transition_kind IN('BASELINE','CHANGE')),
  source_id text NOT NULL,
  body jsonb NOT NULL CHECK(jsonb_typeof(body)='object'),
  observed_at timestamptz NOT NULL,
  publication_revision bigint,
  published_at timestamptz,
  UNIQUE(issue_id,issue_version),
  CHECK((publication_revision IS NULL AND published_at IS NULL) OR (publication_revision>0 AND published_at IS NOT NULL))
);
CREATE UNIQUE INDEX incident_publication_revision_uq ON ouf_ingestion.incident_transition(tenant_id,publication_revision) WHERE publication_revision IS NOT NULL;
CREATE INDEX incident_pending_publication_idx ON ouf_ingestion.incident_transition(tenant_id,source_id,issue_id,issue_version) WHERE publication_revision IS NULL;
CREATE INDEX incident_published_snapshot_idx ON ouf_ingestion.incident_transition(tenant_id,issue_id,publication_revision DESC) WHERE publication_revision IS NOT NULL;

CREATE TABLE ouf_ingestion.incident_publication_clock(
  tenant_id text PRIMARY KEY,
  revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0)
);

CREATE FUNCTION ouf_ingestion.incident_safe_snapshot(i ouf_ingestion.runtime_issue) RETURNS jsonb LANGUAGE sql STABLE AS $$
 SELECT jsonb_build_object(
  'incident_id',i.issue_id,'module','INGESTION','event_type',i.issue_code,
  'lifecycle_state',CASE WHEN i.status='OPEN' THEN 'OPEN' ELSE 'RESOLVED' END,
  'severity',i.severity,'first_seen_at',i.created_at,'last_seen_at',i.last_updated_at,
  'resolved_at',i.resolved_at,'job_ref',i.run_id,'source_ref',i.source_id,
  'error_code',i.issue_code,'correlation_id',i.correlation_id,
  'action_required',i.status='OPEN',
  'impact_summary','Ingestion '||lower(i.status)||' issue '||i.issue_code,
  'dedup_key',i.source_id||':'||i.issue_code,'visibility_class','TENANT_OPERATIONAL')
$$;

-- Honest baseline: current state only; no fabricated earlier transitions.
INSERT INTO ouf_ingestion.incident_transition(tenant_id,issue_id,issue_version,transition_kind,source_id,body,observed_at)
SELECT r.tenant_id,i.issue_id,0,'BASELINE',i.source_id,ouf_ingestion.incident_safe_snapshot(i),i.last_updated_at
FROM ouf_ingestion.runtime_issue i JOIN ouf_ingestion.ing_run r ON r.run_id=i.run_id;

CREATE FUNCTION ouf_ingestion.maintain_incident_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.operational_version:=1;
 ELSE
  IF NEW.operational_version IS DISTINCT FROM OLD.operational_version THEN
   RAISE EXCEPTION 'ING_INCIDENT_VERSION_OWNER_MAINTAINED';
  END IF;
  IF NEW.issue_id IS DISTINCT FROM OLD.issue_id OR NEW.run_id IS DISTINCT FROM OLD.run_id OR NEW.source_id IS DISTINCT FROM OLD.source_id THEN
   RAISE EXCEPTION 'ING_INCIDENT_IDENTITY_IMMUTABLE';
  END IF;
  IF NEW IS DISTINCT FROM OLD THEN NEW.operational_version:=OLD.operational_version+1; END IF;
 END IF;
 RETURN NEW;
END$$;
-- Alphabetical order places this AFTER V16 observation maintenance.
CREATE TRIGGER runtime_issue_version BEFORE INSERT OR UPDATE ON ouf_ingestion.runtime_issue
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.maintain_incident_version();

CREATE FUNCTION ouf_ingestion.capture_incident_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE tenant text;
BEGIN
 IF TG_OP='UPDATE' AND NEW.operational_version=OLD.operational_version THEN RETURN NEW; END IF;
 SELECT tenant_id INTO STRICT tenant FROM ouf_ingestion.ing_run WHERE run_id=NEW.run_id;
 INSERT INTO ouf_ingestion.incident_transition(tenant_id,issue_id,issue_version,transition_kind,source_id,body,observed_at)
 VALUES(tenant,NEW.issue_id,NEW.operational_version,'CHANGE',NEW.source_id,ouf_ingestion.incident_safe_snapshot(NEW),NEW.last_updated_at);
 RETURN NEW;
END$$;
CREATE TRIGGER runtime_issue_transition AFTER INSERT OR UPDATE ON ouf_ingestion.runtime_issue
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_incident_transition();

CREATE FUNCTION ouf_ingestion.guard_incident_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'ING_INCIDENT_HISTORY_IMMUTABLE'; END IF;
 IF (to_jsonb(NEW)-'publication_revision'-'published_at') IS DISTINCT FROM (to_jsonb(OLD)-'publication_revision'-'published_at')
    OR OLD.publication_revision IS NOT NULL OR NEW.publication_revision IS NULL OR NEW.published_at IS NULL THEN
  RAISE EXCEPTION 'ING_INCIDENT_HISTORY_IMMUTABLE';
 END IF;
 RETURN NEW;
END$$;
CREATE TRIGGER incident_transition_immutable BEFORE UPDATE OR DELETE ON ouf_ingestion.incident_transition
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.guard_incident_transition();

CREATE FUNCTION ouf_ingestion.publish_incident_transitions(p_tenant text,p_source text,p_limit integer) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE item record; next_revision bigint; published integer:=0;
BEGIN
 IF p_tenant IS NULL OR length(p_tenant)=0 OR p_limit IS NULL OR p_limit<1 OR p_limit>256 THEN RAISE EXCEPTION 'ING_INCIDENT_PUBLICATION_INVALID'; END IF;
 -- Lock publication first. Writers never acquire this lock or clock row.
 IF NOT pg_try_advisory_xact_lock(hashtextextended('ouf.incident.publish:'||p_tenant,0)) THEN RETURN 0; END IF;
 INSERT INTO ouf_ingestion.incident_publication_clock(tenant_id) VALUES(p_tenant) ON CONFLICT DO NOTHING;
 SELECT revision INTO next_revision FROM ouf_ingestion.incident_publication_clock WHERE tenant_id=p_tenant FOR UPDATE;
 FOR item IN SELECT transition_id FROM ouf_ingestion.incident_transition
   WHERE tenant_id=p_tenant AND publication_revision IS NULL AND (p_source IS NULL OR source_id=p_source)
   ORDER BY issue_id,issue_version LIMIT p_limit FOR UPDATE
 LOOP
  next_revision:=next_revision+1;
  UPDATE ouf_ingestion.incident_transition SET publication_revision=next_revision,published_at=clock_timestamp() WHERE transition_id=item.transition_id;
  published:=published+1;
 END LOOP;
 UPDATE ouf_ingestion.incident_publication_clock SET revision=next_revision WHERE tenant_id=p_tenant;
 RETURN published;
END$$;

CREATE TABLE ouf_ingestion.incident_read_session(
 session_id uuid PRIMARY KEY,
 principal_id text NOT NULL,tenant_id text NOT NULL,actor_type text NOT NULL,service_principal_id text NOT NULL,
 policy_ref text NOT NULL,principal_fingerprint text NOT NULL,
 source_id text,state text,since_at timestamptz,
 page_limit integer NOT NULL CHECK(page_limit BETWEEN 1 AND 100),
 high_watermark bigint NOT NULL CHECK(high_watermark>=0),
 publication_lag boolean NOT NULL,history_gap boolean NOT NULL,
 item_count integer NOT NULL DEFAULT 0 CHECK(item_count BETWEEN 0 AND 20000),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '30 minutes',
 CHECK(expires_at>created_at)
);
CREATE INDEX incident_read_expiry_idx ON ouf_ingestion.incident_read_session(expires_at);
CREATE INDEX incident_read_scope_idx ON ouf_ingestion.incident_read_session(tenant_id,principal_id,expires_at);
CREATE TABLE ouf_ingestion.incident_read_item(
 session_id uuid NOT NULL REFERENCES ouf_ingestion.incident_read_session ON DELETE CASCADE,
 position integer NOT NULL CHECK(position BETWEEN 0 AND 19999),
 body jsonb NOT NULL,
 PRIMARY KEY(session_id,position)
);
CREATE TABLE ouf_ingestion.incident_read_cursor(
 token uuid PRIMARY KEY,
 session_id uuid NOT NULL REFERENCES ouf_ingestion.incident_read_session ON DELETE CASCADE,
 position integer NOT NULL CHECK(position BETWEEN 0 AND 20000),
 UNIQUE(session_id,position)
);
CREATE TRIGGER incident_read_item_no_update BEFORE UPDATE ON ouf_ingestion.incident_read_item
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();
