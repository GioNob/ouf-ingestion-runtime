-- A historical run belongs only to the publication named in its immutable snapshot.
ALTER TABLE ouf_ingestion.ing_run ADD COLUMN publication_id uuid, ADD COLUMN admission_pending boolean NOT NULL DEFAULT false;
UPDATE ouf_ingestion.ing_run r SET publication_id=(s.snapshot_json#>>'{configuration,publicationId}')::uuid
FROM ouf_ingestion.runtime_configuration_snapshot s
WHERE s.run_id=r.run_id AND s.snapshot_json#>>'{configuration,publicationId}' ~*
 '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
UPDATE ouf_ingestion.ing_run SET admission_pending=true
WHERE publication_id IS NOT NULL AND mode='MANAGED_ONCE' AND state='PREFLIGHT';
CREATE INDEX ing_run_publication ON ouf_ingestion.ing_run(tenant_id,source_id,publication_id,created_at DESC);

ALTER TABLE ouf_ingestion.ing_schedule ADD COLUMN discovered_bundle jsonb;
-- Discovery fills this before the next MANAGED claim. Re-open only the legacy
-- default-zero first refusal, preserving its elapsed time and attempt count.
UPDATE ouf_ingestion.ing_schedule s SET state='ACTIVE',activation_blocked=false
FROM ouf_ingestion.source_health h WHERE h.source_id=s.source_id AND s.trigger_once
 AND s.state='PAUSED' AND s.activation_blocked AND s.publication_enabled
 AND s.consumed_publication_id IS DISTINCT FROM s.publication_id AND s.activation_failures=1
 AND s.sync_profile#>'{operationalPolicy,maxRetryAttempts}' IS NULL
 -- Both timestamps were written by the same V14 failure transaction. An
 -- unrelated later health observation is insufficient to reopen admission.
 AND h.updated_at + (CASE WHEN s.sync_profile->>'retryBackoffSeconds' ~ '^[0-9]{1,5}$'
                       THEN greatest(1,(s.sync_profile->>'retryBackoffSeconds')::int)
                       WHEN s.sync_profile->>'retryBackoffSeconds' IS NULL THEN 1 END * interval '1 second') = s.next_run_at
 AND h.last_error_code IN ('ING_ACTIVATION_GATEWAY_429','ING_ACTIVATION_GATEWAY_503');

ALTER TABLE ouf_ingestion.ing_partition
 ADD COLUMN batch_started_at timestamptz, ADD COLUMN retry_started_at timestamptz,
 ADD COLUMN retry_attempts integer NOT NULL DEFAULT 0, ADD COLUMN retry_at timestamptz, ADD COLUMN retry_code text;
-- Working evidence for the record at the restart cursor. Keep exact receipts
-- across worker changes and HUMAN budgets. PENDING without a receipt is ambiguous.
ALTER TABLE ouf_ingestion.ing_partition ADD COLUMN lake_sequence bigint,
 ADD COLUMN record_acquired_at timestamptz, ADD COLUMN handoff_resume_state text;
CREATE TABLE ouf_ingestion.lake_write_evidence (
 run_id uuid NOT NULL, partition_key text NOT NULL, sequence_no bigint NOT NULL, effect text NOT NULL,
 content_hash text NOT NULL, receipt_ref text, PRIMARY KEY(run_id,partition_key,sequence_no,effect),
 FOREIGN KEY(run_id,partition_key) REFERENCES ouf_ingestion.ing_partition(run_id,partition_key)
);
-- Existing operator pauses and quarantines contain no proof that replay is safe.
-- They retain their existing HUMAN disposition paths.
ALTER TABLE ouf_ingestion.handoff_outbox ADD COLUMN delivery_started_at timestamptz,
 ADD COLUMN delivery_stop_reason text,
 ADD COLUMN delivery_session_id uuid REFERENCES ouf_ingestion.run_recovery_session(session_id);
CREATE VIEW ouf_ingestion.handoff_budget AS
 SELECT h.handoff_id,coalesce(g.max_attempts-g.attempts,128-h.attempts) remaining,
   coalesce(g.deadline_at,h.delivery_started_at+interval '24 hours') deadline,
   coalesce(g.state='ACTIVE',true) authorized
 FROM ouf_ingestion.handoff_outbox h LEFT JOIN ouf_ingestion.run_recovery_session g ON g.session_id=h.delivery_session_id;
-- Older dispatcher versions folded denials and ambiguous receipts into one
-- generic failure. Preserve the cause and require disposition for unproven retries.
UPDATE ouf_ingestion.handoff_outbox SET state='FAILED_TERMINAL',delivery_stop_reason='ING_HANDOFF_RECONCILE_REQUIRED'
 WHERE state='FAILED_TERMINAL' OR (state='FAILED_RETRYABLE' AND
   coalesce(last_error_code,'') NOT IN ('ING_EXECUTION_GATEWAY_202','ING_EXECUTION_GATEWAY_429',
      'ING_EXECUTION_GATEWAY_502','ING_EXECUTION_GATEWAY_503','ING_EXECUTION_GATEWAY_504'));
UPDATE ouf_ingestion.handoff_outbox SET delivery_started_at=created_at WHERE attempts>0 AND state<>'ACKED';
CREATE UNIQUE INDEX handoff_incident_per_run ON ouf_ingestion.runtime_issue(run_id)
 WHERE issue_code='ING_HANDOFF_BLOCKED';

CREATE FUNCTION ouf_ingestion.block_handoff(target uuid) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 UPDATE ouf_ingestion.ing_run SET state='PAUSED',failure_code='ING_HANDOFF_BLOCKED',
  control_version=control_version+1,updated_at=clock_timestamp()
 WHERE run_id=target AND state IN ('RUNNING','DRAINING','PAUSED') AND failure_code IS DISTINCT FROM 'ING_HANDOFF_BLOCKED';
 UPDATE ouf_ingestion.ing_partition SET handoff_resume_state=state,state='PAUSED',lease_owner=null,lease_until=null
 WHERE run_id=target AND state IN ('RUNNING','DRAINING');
 INSERT INTO ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id)
 SELECT gen_random_uuid(),run_id,source_id,'DELIVERY','ERROR','ING_HANDOFF_BLOCKED','run-handoffs:'||run_id,correlation_id
 FROM ouf_ingestion.ing_run WHERE run_id=target AND state='PAUSED'
 ON CONFLICT(run_id) WHERE issue_code='ING_HANDOFF_BLOCKED' DO NOTHING;
END $$;
SELECT ouf_ingestion.block_handoff(run_id) FROM
 (SELECT DISTINCT run_id FROM ouf_ingestion.handoff_outbox WHERE state='FAILED_TERMINAL') legacy;

-- An audited abort fences pending effects. Preserve the payload, last cause, attempts
-- and every durable receipt. Generic issue resolution cannot grant another attempt.
UPDATE ouf_ingestion.runtime_issue i SET status='RESOLVED',resolution_reason='RUN_ABORTED',
 resolution_actor=a.actor_subject,authorization_decision_ref=a.authorization_decision_ref,
 resolved_at=a.created_at,lock_version=i.lock_version+1
FROM ouf_ingestion.ing_run r JOIN ouf_ingestion.audit_event a
 ON a.resource_type='RUN' AND a.resource_id=r.run_id::text AND a.tenant_id=r.tenant_id
WHERE i.run_id=r.run_id AND r.state='ABORTED' AND a.event_type='RUN_ABORTED' AND a.actor_type='HUMAN'
 AND i.issue_code='ING_RECOVERY_BUDGET_EXHAUSTED' AND i.status='OPEN';

CREATE OR REPLACE FUNCTION ouf_ingestion.refresh_pressure_state() RETURNS text LANGUAGE plpgsql AS $$
DECLARE p ouf_ingestion.runtime_pressure_policy%ROWTYPE; backlog bigint; next_state text;
BEGIN
 SELECT * INTO p FROM ouf_ingestion.runtime_pressure_policy WHERE policy_key='GLOBAL' FOR UPDATE;
 SELECT count(*) INTO backlog FROM ouf_ingestion.handoff_outbox h JOIN ouf_ingestion.ing_run r USING(run_id) WHERE h.state<>'ACKED' AND r.state<>'ABORTED';
 next_state:=CASE p.state
  WHEN 'NORMAL' THEN CASE WHEN backlog>=p.hard_outbox_limit THEN 'HARD_PRESSURE' WHEN backlog>=p.soft_outbox_limit THEN 'SOFT_PRESSURE' ELSE 'NORMAL' END
  WHEN 'SOFT_PRESSURE' THEN CASE WHEN backlog>=p.hard_outbox_limit THEN 'HARD_PRESSURE' WHEN backlog<=p.recovery_outbox_limit THEN 'NORMAL' ELSE 'SOFT_PRESSURE' END
  WHEN 'HARD_PRESSURE' THEN CASE WHEN backlog<=p.recovery_outbox_limit THEN 'RECOVERY' ELSE 'HARD_PRESSURE' END
  WHEN 'RECOVERY' THEN CASE WHEN backlog>=p.hard_outbox_limit THEN 'HARD_PRESSURE' WHEN backlog>=p.soft_outbox_limit THEN 'SOFT_PRESSURE' WHEN backlog<=p.recovery_outbox_limit THEN 'NORMAL' ELSE 'RECOVERY' END END;
 UPDATE ouf_ingestion.runtime_pressure_policy SET state=next_state,updated_at=CASE WHEN runtime_pressure_policy.state<>next_state THEN transaction_timestamp() ELSE updated_at END WHERE policy_key='GLOBAL';
 RETURN next_state;
END $$;

-- Anchor operational reads on the tenant's schedule even before a run exists.
-- Unscheduled historical runs remain visible without guessing a publication.
CREATE VIEW ouf_ingestion.current_source_status AS
WITH sources AS (
 SELECT tenant_id,source_id FROM ouf_ingestion.ing_schedule
 UNION SELECT tenant_id,source_id FROM ouf_ingestion.ing_run
)
SELECT src.tenant_id,src.source_id source_ref,s.publication_id,s.state schedule_state,
 CASE WHEN blocked.failure_code IS NOT NULL THEN 'DEGRADED' WHEN r.run_id IS NULL THEN 'UNKNOWN'
      WHEN r.failure_code IS NOT NULL OR p.retry_at IS NOT NULL THEN 'DEGRADED'
      WHEN r.publication_id IS NULL THEN coalesce(h.status,'UNKNOWN')
      WHEN r.state='PREFLIGHT' THEN 'UNKNOWN' WHEN r.state IN ('FAILED','PAUSED','ABORTED') THEN 'DEGRADED'
      ELSE 'HEALTHY' END health_state,
 CASE WHEN r.publication_id IS NOT NULL THEN coalesce(p.retry_attempts,0)
      WHEN r.run_id IS NOT NULL THEN h.consecutive_failures END consecutive_failures,
 CASE WHEN r.run_id IS NULL AND blocked.failure_code IS NOT NULL THEN blocked.failure_code
      WHEN r.run_id IS NULL AND s.activation_blocked THEN 'ING_ACTIVATION_BLOCKED'
      WHEN r.publication_id IS NOT NULL THEN coalesce(r.failure_code,p.retry_code)
      WHEN r.run_id IS NOT NULL THEN coalesce(r.failure_code,h.last_error_code) END error_code,
 CASE WHEN s.state='ACTIVE' AND s.activation_failures>0 THEN s.next_run_at
      WHEN p.retry_at IS NOT NULL THEN greatest(p.retry_at,h.retry_not_before,h.circuit_open_until)
      WHEN r.publication_id IS NULL AND r.run_id IS NOT NULL THEN h.retry_not_before END next_retry_at,
 CASE WHEN p.retry_at IS NOT NULL OR (r.publication_id IS NULL AND r.run_id IS NOT NULL) THEN h.circuit_open_until END circuit_open_until,
 r.run_id job_ref,r.control_version,
 CASE WHEN r.state='PREFLIGHT' AND s.activation_failures>0 AND s.lease_owner IS NULL THEN 'RETRY_WAIT'
      WHEN r.state='RUNNING' AND p.retry_at IS NOT NULL THEN 'RETRY_WAIT'
      WHEN r.run_id IS NOT NULL THEN r.state WHEN blocked.failure_code IS NOT NULL OR s.state='PAUSED' THEN 'BLOCKED'
      WHEN s.state='DISABLED' THEN 'DISABLED' ELSE 'AWAITING_START' END run_state,
 coalesce(r.updated_at,s.discovery_verified_at,s.next_run_at) last_seen_at,r.correlation_id
FROM sources src LEFT JOIN ouf_ingestion.ing_schedule s USING(tenant_id,source_id)
LEFT JOIN LATERAL (SELECT r.* FROM ouf_ingestion.ing_run r
 WHERE r.tenant_id=src.tenant_id AND r.source_id=src.source_id
 AND r.publication_id IS NOT DISTINCT FROM s.publication_id
 ORDER BY r.created_at DESC,r.run_id DESC LIMIT 1) r ON true
LEFT JOIN LATERAL (SELECT old.failure_code FROM ouf_ingestion.ing_run old
 WHERE old.tenant_id=src.tenant_id AND old.source_id=src.source_id AND old.state='PAUSED'
   AND old.failure_code IN ('ING_HANDOFF_BLOCKED','ING_RECOVERY_BUDGET_EXHAUSTED','ING_LAKE_RECONCILE_REQUIRED')
 ORDER BY old.created_at,old.run_id LIMIT 1) blocked ON r.run_id IS NULL
LEFT JOIN LATERAL (SELECT retry_at,retry_attempts,retry_code FROM ouf_ingestion.ing_partition p
 WHERE p.run_id=r.run_id AND p.state='RUNNING' AND retry_at IS NOT NULL
 ORDER BY retry_at,partition_key LIMIT 1) p ON true
LEFT JOIN ouf_ingestion.source_health h ON h.source_id=src.source_id;
