ALTER TABLE ouf_ingestion.ing_run ADD COLUMN paused_at timestamptz;
ALTER TABLE ouf_ingestion.ing_schedule ADD COLUMN paused_at timestamptz;

-- Freeze the available historical evidence before later writes can change it.
UPDATE ouf_ingestion.ing_run r
SET paused_at = r.updated_at
WHERE r.state = 'PAUSED';
UPDATE ouf_ingestion.ing_schedule
SET paused_at = least(activation_first_failure_at, discovery_verified_at, last_dispatched_at, next_run_at)
WHERE state = 'PAUSED';

CREATE FUNCTION ouf_ingestion.record_pause_entry() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state = 'PAUSED' THEN
    IF TG_OP = 'INSERT' OR OLD.state IS DISTINCT FROM 'PAUSED' THEN
      NEW.paused_at := clock_timestamp();
    END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER ing_run_pause_entry
BEFORE INSERT OR UPDATE OF state ON ouf_ingestion.ing_run
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.record_pause_entry();

CREATE TRIGGER ing_schedule_pause_entry
BEFORE INSERT OR UPDATE OF state ON ouf_ingestion.ing_schedule
FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.record_pause_entry();
