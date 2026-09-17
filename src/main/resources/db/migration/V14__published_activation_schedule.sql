ALTER TABLE ouf_ingestion.ing_schedule DROP CONSTRAINT ing_schedule_interval_seconds_check;
ALTER TABLE ouf_ingestion.ing_schedule ADD CONSTRAINT ing_schedule_interval_seconds_check CHECK(interval_seconds BETWEEN 0 AND 2678400);
ALTER TABLE ouf_ingestion.ing_schedule
 ADD COLUMN publication_id uuid,
 ADD COLUMN publication_sequence bigint,
 ADD COLUMN publication_checksum text,
 ADD COLUMN trigger_once boolean NOT NULL DEFAULT false,
 ADD COLUMN publication_enabled boolean NOT NULL DEFAULT true,
 ADD COLUMN consumed_publication_id uuid,
 ADD COLUMN sync_profile jsonb,
 ADD COLUMN discovery_verified_at timestamptz;
ALTER TABLE ouf_ingestion.ing_schedule ADD CONSTRAINT ing_schedule_trigger_cadence CHECK((trigger_once AND interval_seconds=0) OR (NOT trigger_once AND interval_seconds>=60));
ALTER TABLE ouf_ingestion.ing_run ADD COLUMN schedule_id uuid REFERENCES ouf_ingestion.ing_schedule ON DELETE RESTRICT, ADD COLUMN schedule_due_at timestamptz;
CREATE UNIQUE INDEX ing_run_schedule_slot ON ouf_ingestion.ing_run(schedule_id,schedule_due_at) WHERE schedule_id IS NOT NULL;
COMMENT ON COLUMN ouf_ingestion.ing_schedule.sync_profile IS 'Exact publisher-owned schedule and operational policy; no implicit source-specific defaults.';
