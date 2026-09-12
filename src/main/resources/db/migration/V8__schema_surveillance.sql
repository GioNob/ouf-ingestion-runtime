CREATE TABLE ouf_ingestion.schema_observation(
  observation_id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ouf_ingestion.ing_run ON DELETE RESTRICT,
  source_id text NOT NULL,
  type_code text NOT NULL,
  schema_checksum text NOT NULL,
  outcome text NOT NULL CHECK(outcome IN('MATCH','COMPATIBLE_ADDITIVE','UNKNOWN_VERSION','INCOMPATIBLE')),
  expected_schema_version text NOT NULL,
  observed_schema_version text,
  field_names jsonb NOT NULL,
  observation_count bigint NOT NULL DEFAULT 1,
  first_observed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  last_observed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE NULLS NOT DISTINCT(source_id,type_code,schema_checksum,expected_schema_version,observed_schema_version)
);
CREATE INDEX schema_observation_scope_idx ON ouf_ingestion.schema_observation(source_id,type_code,last_observed_at DESC);

CREATE TABLE ouf_ingestion.runtime_issue(
  issue_id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES ouf_ingestion.ing_run ON DELETE RESTRICT,
  source_id text NOT NULL,
  type_code text NOT NULL,
  severity text NOT NULL CHECK(severity IN('WARNING','ERROR')),
  issue_code text NOT NULL,
  evidence_ref text NOT NULL,
  status text NOT NULL DEFAULT 'OPEN' CHECK(status IN('OPEN','RESOLVED','DISMISSED')),
  correlation_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
  resolved_at timestamptz
);
CREATE UNIQUE INDEX runtime_issue_open_uq ON ouf_ingestion.runtime_issue(source_id,type_code,issue_code,evidence_ref) WHERE status='OPEN';
CREATE INDEX runtime_issue_search_idx ON ouf_ingestion.runtime_issue(source_id,status,severity,created_at DESC);

CREATE FUNCTION ouf_ingestion.capture_runtime_issue_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ouf_ingestion.operational_event(severity,event_type,correlation_id,run_id,source_id,resource_type,resource_id,error_code,detail_json)
  VALUES(CASE WHEN NEW.severity='ERROR' THEN 'ERROR' ELSE 'WARN' END,'SCHEMA_DRIFT_'||NEW.status,NEW.correlation_id,NEW.run_id,NEW.source_id,'RUNTIME_ISSUE',NEW.issue_id::text,NEW.issue_code,jsonb_build_object('typeCode',NEW.type_code,'evidenceRef',NEW.evidence_ref));RETURN NEW;
END$$;
CREATE TRIGGER runtime_issue_operational_event AFTER INSERT OR UPDATE OF status ON ouf_ingestion.runtime_issue FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.capture_runtime_issue_event();
