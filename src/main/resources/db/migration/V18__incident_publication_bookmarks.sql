-- Publication coverage positions are metadata, never operator confirmation.
CREATE TABLE ouf_ingestion.incident_bookmark_scope(
 session_id uuid PRIMARY KEY REFERENCES ouf_ingestion.incident_read_session ON DELETE CASCADE,
 parent_token uuid
);
CREATE TABLE ouf_ingestion.incident_bookmark_page(
 session_id uuid NOT NULL REFERENCES ouf_ingestion.incident_read_session ON DELETE CASCADE,
 position integer NOT NULL CHECK(position BETWEEN 0 AND 20000),
 items_digest text NOT NULL CHECK(items_digest~'^[0-9a-f]{64}$'),
 complete boolean NOT NULL,
 PRIMARY KEY(session_id,position)
);
CREATE TABLE ouf_ingestion.incident_publication_bookmark(
 token uuid PRIMARY KEY,session_id uuid NOT NULL UNIQUE,
 principal_id text NOT NULL,tenant_id text NOT NULL,
 identity_digest text NOT NULL CHECK(identity_digest~'^[0-9a-f]{64}$'),
 policy_ref text NOT NULL CHECK(length(policy_ref) BETWEEN 1 AND 1024),
 source_id text,state text CHECK(state IS NULL OR state IN('OPEN','RECOVERING','RESOLVED')),
 high_watermark bigint NOT NULL CHECK(high_watermark>=0),parent_token uuid,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '90 days',
 CHECK(expires_at>created_at)
);
CREATE INDEX incident_bookmark_scope_idx ON ouf_ingestion.incident_publication_bookmark(tenant_id,principal_id,expires_at);
CREATE INDEX incident_bookmark_expiry_idx ON ouf_ingestion.incident_publication_bookmark(expires_at);
CREATE TRIGGER incident_bookmark_scope_no_update BEFORE UPDATE ON ouf_ingestion.incident_bookmark_scope
 FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.reject_history_mutation();
CREATE FUNCTION ouf_ingestion.guard_publication_bookmark() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'ING_PUBLICATION_BOOKMARK_IMMUTABLE'; END IF;
 IF OLD.expires_at>clock_timestamp() THEN RAISE EXCEPTION 'ING_PUBLICATION_BOOKMARK_LIVE'; END IF;
 RETURN OLD;
END$$;
CREATE TRIGGER incident_publication_bookmark_immutable BEFORE UPDATE OR DELETE ON ouf_ingestion.incident_publication_bookmark
 FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.guard_publication_bookmark();
CREATE TABLE ouf_ingestion.incident_publication_history_floor(
 tenant_id text PRIMARY KEY,revision bigint NOT NULL CHECK(revision>=0)
);
CREATE FUNCTION ouf_ingestion.guard_publication_history_floor() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR NEW.revision<OLD.revision THEN RAISE EXCEPTION 'ING_HISTORY_FLOOR_CANNOT_REWIND'; END IF;
 RETURN NEW;
END$$;
CREATE TRIGGER incident_publication_history_floor_monotone BEFORE UPDATE OR DELETE ON ouf_ingestion.incident_publication_history_floor
 FOR EACH ROW EXECUTE FUNCTION ouf_ingestion.guard_publication_history_floor();
