ALTER TABLE ouf_ingestion.ing_schedule ADD COLUMN workload_class text NOT NULL DEFAULT 'BOOTSTRAP' CHECK(workload_class IN('BOOTSTRAP','DELTA_RECOVERY','DELTA_ORDINARY'));
ALTER TABLE ouf_ingestion.source_health
  ADD COLUMN retry_window_started_at timestamptz,
  ADD COLUMN retry_window_seconds integer NOT NULL DEFAULT 900 CHECK(retry_window_seconds BETWEEN 60 AND 86400),
  ADD COLUMN retry_attempts_in_window integer NOT NULL DEFAULT 0 CHECK(retry_attempts_in_window>=0),
  ADD COLUMN first_failure_at timestamptz,
  ADD COLUMN last_failure_at timestamptz;

CREATE TABLE ouf_ingestion.runtime_pressure_policy(
  policy_key text PRIMARY KEY CHECK(policy_key='GLOBAL'),
  state text NOT NULL DEFAULT 'NORMAL' CHECK(state IN('NORMAL','SOFT_PRESSURE','HARD_PRESSURE','RECOVERY')),
  global_running_limit integer NOT NULL DEFAULT 8 CHECK(global_running_limit BETWEEN 1 AND 128),
  soft_outbox_limit integer NOT NULL DEFAULT 1000 CHECK(soft_outbox_limit>0),
  hard_outbox_limit integer NOT NULL DEFAULT 5000 CHECK(hard_outbox_limit>soft_outbox_limit),
  recovery_outbox_limit integer NOT NULL DEFAULT 500 CHECK(recovery_outbox_limit<soft_outbox_limit),
  updated_at timestamptz NOT NULL DEFAULT transaction_timestamp());
INSERT INTO ouf_ingestion.runtime_pressure_policy(policy_key) VALUES('GLOBAL');

CREATE FUNCTION ouf_ingestion.refresh_pressure_state() RETURNS text LANGUAGE plpgsql AS $$
DECLARE p ouf_ingestion.runtime_pressure_policy%ROWTYPE; backlog bigint; next_state text;
BEGIN
 SELECT * INTO p FROM ouf_ingestion.runtime_pressure_policy WHERE policy_key='GLOBAL' FOR UPDATE;
 SELECT count(*) INTO backlog FROM ouf_ingestion.handoff_outbox WHERE state<>'ACKED';
 next_state:=CASE p.state
  WHEN 'NORMAL' THEN CASE WHEN backlog>=p.hard_outbox_limit THEN 'HARD_PRESSURE' WHEN backlog>=p.soft_outbox_limit THEN 'SOFT_PRESSURE' ELSE 'NORMAL' END
  WHEN 'SOFT_PRESSURE' THEN CASE WHEN backlog>=p.hard_outbox_limit THEN 'HARD_PRESSURE' WHEN backlog<=p.recovery_outbox_limit THEN 'NORMAL' ELSE 'SOFT_PRESSURE' END
  WHEN 'HARD_PRESSURE' THEN CASE WHEN backlog<=p.recovery_outbox_limit THEN 'RECOVERY' ELSE 'HARD_PRESSURE' END
  WHEN 'RECOVERY' THEN CASE WHEN backlog>=p.hard_outbox_limit THEN 'HARD_PRESSURE' WHEN backlog>=p.soft_outbox_limit THEN 'SOFT_PRESSURE' WHEN backlog<=p.recovery_outbox_limit THEN 'NORMAL' ELSE 'RECOVERY' END END;
 UPDATE ouf_ingestion.runtime_pressure_policy SET state=next_state,updated_at=CASE WHEN runtime_pressure_policy.state<>next_state THEN transaction_timestamp() ELSE updated_at END WHERE policy_key='GLOBAL';
 RETURN next_state;
END $$;
