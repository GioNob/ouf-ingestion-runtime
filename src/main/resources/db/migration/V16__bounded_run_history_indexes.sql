-- Keep both tenant-wide and source-scoped history searches ordered and bounded.
CREATE INDEX ing_run_history_tenant_time ON ouf_ingestion.ing_run(tenant_id,updated_at DESC,run_id DESC);
CREATE INDEX ing_run_history_tenant_source_time ON ouf_ingestion.ing_run(tenant_id,source_id,updated_at DESC,run_id DESC);
