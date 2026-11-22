ALTER TABLE ouf_ingestion.lake_write_evidence
  ADD COLUMN cause_code text,
  ADD COLUMN cause_at timestamptz,
  ADD COLUMN human_review boolean NOT NULL DEFAULT false,
  ADD COLUMN abort_required boolean NOT NULL DEFAULT false,
  ADD COLUMN intent_at timestamptz;
ALTER TABLE ouf_ingestion.lake_write_evidence ALTER COLUMN intent_at SET DEFAULT clock_timestamp();

CREATE INDEX lake_unresolved_by_partition ON ouf_ingestion.lake_write_evidence(run_id, partition_key, sequence_no)
  WHERE receipt_ref IS NULL OR abort_required OR human_review;
CREATE INDEX lake_cause_by_run ON ouf_ingestion.lake_write_evidence(run_id, cause_at DESC)
  WHERE cause_code IS NOT NULL;

-- The last classified cause remains visible after recovery. Legacy pauses have no recorded cause.
CREATE VIEW ouf_ingestion.run_lake_recovery AS
SELECT r.run_id, coalesce(active.cause_code,
    CASE WHEN r.failure_code='ING_LAKE_RECONCILE_REQUIRED' AND active.run_id IS NULL THEN 'ING_LAKE_EVIDENCE_MISSING'
         WHEN r.failure_code='ING_LAKE_RECONCILE_REQUIRED' THEN 'ING_LAKE_LEGACY_CAUSE_UNKNOWN' END,
    last_failure.cause_code) lake_cause_code,
  CASE WHEN r.state='PAUSED' AND (active.abort_required OR
            (r.failure_code='ING_LAKE_RECONCILE_REQUIRED' AND active.run_id IS NULL)) THEN 'ABORT_REQUIRED'
       WHEN r.state='PAUSED' AND (active.run_id IS NOT NULL OR r.failure_code='ING_LAKE_RECONCILE_REQUIRED')
         THEN 'HUMAN_RESUME_REQUIRED'
       WHEN r.state='RUNNING' AND active.run_id IS NOT NULL AND (active.cause_code IS NOT NULL OR active.retrying) THEN 'AUTO_RECONCILING'
       ELSE 'NONE' END lake_next_action
FROM ouf_ingestion.ing_run r
LEFT JOIN LATERAL (
  SELECT e.run_id,e.cause_code,e.abort_required,p.retry_started_at IS NOT NULL AS retrying
  FROM ouf_ingestion.lake_write_evidence e JOIN ouf_ingestion.ing_partition p USING(run_id,partition_key)
  WHERE e.run_id=r.run_id AND (e.receipt_ref IS NULL OR e.abort_required OR e.human_review)
  ORDER BY e.abort_required DESC,e.cause_at DESC NULLS LAST,e.partition_key,e.sequence_no,e.effect LIMIT 1
) active ON true
LEFT JOIN LATERAL (
  SELECT e.cause_code FROM ouf_ingestion.lake_write_evidence e
  WHERE e.run_id=r.run_id AND e.cause_code IS NOT NULL
  ORDER BY e.cause_at DESC,e.partition_key,e.sequence_no,e.effect LIMIT 1
) last_failure ON true;
