ALTER TABLE ouf_ingestion.audit_event
  ADD COLUMN tenant_id text NOT NULL DEFAULT 'system',
  ADD COLUMN authorization_decision_ref text NOT NULL DEFAULT 'bootstrap:migration';
ALTER TABLE ouf_ingestion.replay_request
  ADD COLUMN tenant_id text NOT NULL DEFAULT 'system',
  ADD COLUMN authorization_decision_ref text NOT NULL DEFAULT 'bootstrap:migration';
CREATE INDEX audit_correlation_idx ON ouf_ingestion.audit_event(correlation_id,created_at DESC);
CREATE INDEX quarantine_correlation_idx ON ouf_ingestion.ing_quarantine(correlation_id,created_at DESC);
