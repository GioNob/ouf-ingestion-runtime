ALTER TABLE ouf_ingestion.source_health
  ADD COLUMN retry_not_before timestamptz,
  ADD COLUMN recovery_probe_owner text,
  ADD COLUMN recovery_probe_until timestamptz,
  ADD CONSTRAINT source_health_probe_pair
    CHECK ((recovery_probe_owner IS NULL) = (recovery_probe_until IS NULL));

CREATE INDEX source_health_retry_idx
  ON ouf_ingestion.source_health(retry_not_before)
  WHERE retry_not_before IS NOT NULL;

CREATE INDEX source_health_recovery_probe_idx
  ON ouf_ingestion.source_health(recovery_probe_until)
  WHERE recovery_probe_until IS NOT NULL;
