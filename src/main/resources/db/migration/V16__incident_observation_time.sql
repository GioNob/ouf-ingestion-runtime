-- Owner-maintained observation time for all incident changes, not only resolution.
-- Backfill is the last historically provable timestamp; it does not reconstruct
-- metadata transitions that were never persisted.
ALTER TABLE ouf_ingestion.runtime_issue ADD COLUMN last_updated_at timestamptz;
UPDATE ouf_ingestion.runtime_issue
SET last_updated_at = GREATEST(created_at, resolved_at);
ALTER TABLE ouf_ingestion.runtime_issue ALTER COLUMN last_updated_at SET NOT NULL;

CREATE FUNCTION ouf_ingestion.maintain_runtime_issue_observation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.last_updated_at := GREATEST(NEW.created_at, NEW.resolved_at);
  ELSE
    IF NEW.last_updated_at IS DISTINCT FROM OLD.last_updated_at THEN
      RAISE EXCEPTION 'ING_INCIDENT_OBSERVATION_OWNER_MAINTAINED';
    END IF;
    IF NEW IS DISTINCT FROM OLD THEN
      NEW.last_updated_at := GREATEST(OLD.last_updated_at, clock_timestamp(), NEW.resolved_at);
    END IF;
  END IF;
  RETURN NEW;
END$$;

CREATE TRIGGER runtime_issue_observation
BEFORE INSERT OR UPDATE ON ouf_ingestion.runtime_issue
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.maintain_runtime_issue_observation();

CREATE INDEX runtime_issue_observation_idx
ON ouf_ingestion.runtime_issue(source_id, last_updated_at DESC, issue_id);
