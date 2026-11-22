package it.comune.trieste.ouf.ingestion;

/** SQL admission predicates shared by the owner commands and the operational projection. */
final class RecoveryPredicates {
  private RecoveryPredicates() {}

  static final String CURRENT_PUBLICATION = """
    s.schedule_id=r.schedule_id and s.tenant_id=r.tenant_id
    and s.publication_id=r.publication_id and s.publication_enabled
    """;
  static final String DISCOVERY_VALID_UNTIL = "s.discovery_verified_at+interval '5 minutes'";
  static final String FRESH_DISCOVERY = DISCOVERY_VALID_UNTIL + ">clock_timestamp()";
  static final String ADMISSION_PUBLICATION = CURRENT_PUBLICATION
    + " and s.consumed_publication_id is distinct from s.publication_id";

  static final String BLOCKING_QUARANTINE = "blocking and lifecycle_state in ('OPEN','RETRY_READY','REPROCESSING')";
  static final String QUARANTINE_NOT_READY = BLOCKING_QUARANTINE + " and lifecycle_state<>'RETRY_READY'";
  static final String HANDOFF_RECOVERY_PENDING = """
    exists(select 1 from ouf_ingestion.handoff_outbox h where h.run_id=r.run_id
      and h.delivery_session_id is not null and h.state<>'ACKED')
    """;
  static final String RENEWABLE_HANDOFF = """
    ((state='FAILED_TERMINAL' and delivery_stop_reason='ING_HANDOFF_BUDGET_EXHAUSTED')
      or (delivery_session_id is not null and state in ('DELIVERING','FAILED_RETRYABLE')))
    """;

  // Called with r (run) and l (the V22 lake decision). No authorization or remote preflight is implied.
  static final String RESUME_FACTS = """
    select
      exists(select 1 from ouf_ingestion.runtime_configuration_snapshot x where x.run_id=r.run_id) has_snapshot,
      exists(select 1 from ouf_ingestion.ing_quarantine where run_id=r.run_id and %s) quarantine_not_ready,
      coalesce(r.failure_code='ING_HANDOFF_BLOCKED',false) or %s handoff_recovery,
      (select count(*) filter(where %s)=1
          and count(*) filter(where state='FAILED_TERMINAL' and (%s) is not true)=0
        from ouf_ingestion.handoff_outbox where run_id=r.run_id) handoff_renewable,
      l.lake_next_action,
      coalesce((not r.admission_pending and r.failure_code='ING_ACTIVE_PUBLICATION_CHANGED'),false)
        or r.admission_pending or (r.publication_id is not null and
          (coalesce(r.failure_code='ING_HANDOFF_BLOCKED',false) or %s or l.lake_next_action='HUMAN_RESUME_REQUIRED'))
        as publication_required,
      exists(select 1 from ouf_ingestion.ing_schedule s where %s
        and (not r.admission_pending or s.consumed_publication_id is distinct from s.publication_id)) publication_current,
      exists(select 1 from ouf_ingestion.ing_schedule s where %s and %s) publication_fresh
    """.formatted(QUARANTINE_NOT_READY, HANDOFF_RECOVERY_PENDING, RENEWABLE_HANDOFF, RENEWABLE_HANDOFF,
      HANDOFF_RECOVERY_PENDING, CURRENT_PUBLICATION, CURRENT_PUBLICATION, FRESH_DISCOVERY);

  static final String ACQUISITION_LANE = "r.state='RUNNING' and p.state='RUNNING' and not " + HANDOFF_RECOVERY_PENDING;
  static final String ACQUISITION_RETRY_AT = "greatest(p.retry_at,sh.retry_not_before,sh.circuit_open_until)";
  static final String BLOCKED_LAKE_EFFECT = "(abort_required or human_review)";
  static final String PENDING_LAKE_WRITE = """
    exists(select 1 from ouf_ingestion.lake_write_evidence e
      where e.run_id=p.run_id and e.partition_key=p.partition_key
        and (e.receipt_ref is null or %s))
    """.formatted(BLOCKED_LAKE_EFFECT);
  static final String BLOCKED_LAKE_WRITE = """
    exists(select 1 from ouf_ingestion.lake_write_evidence e
      where e.run_id=p.run_id and e.partition_key=p.partition_key and %s)
    """.formatted(BLOCKED_LAKE_EFFECT);
  static final String PARTITION_ATTEMPTS_LEFT = "p.retry_attempts<" + RetryPolicy.ATTEMPTS;
  static final String PARTITION_DEADLINE = "p.retry_started_at+interval '24 hours'";
  static final String PARTITION_EXHAUSTED = "not (" + PARTITION_ATTEMPTS_LEFT + ") or " + PARTITION_DEADLINE + "<=clock_timestamp()";
  static final String SESSION_CAN_ATTEMPT = "state='ACTIVE' and attempts<max_attempts and deadline_at>clock_timestamp()";

  static final String DELIVERY_LANE = "r.state in ('RUNNING','DRAINING')";
  static final String DELIVERY_HEAD = """
    not exists(select 1 from ouf_ingestion.handoff_outbox prior
      where prior.run_id=h.run_id and prior.partition_key=h.partition_key
      and prior.sequence_no<h.sequence_no and prior.state<>'ACKED')
    """;
  static final String DELIVERY_EXPIRED = "b.remaining<=0 or not b.authorized or b.deadline<=clock_timestamp()";
  static final String DELIVERY_RECONCILING = """
    h.state='DELIVERING' or coalesce(h.last_error_code in
      ('ING_EXECUTION_GATEWAY_UNAVAILABLE','ING_EXECUTION_GATEWAY_202','ING_EXECUTION_GATEWAY_502','ING_EXECUTION_GATEWAY_504'),false)
    """;
  static final String DELIVERY_SUPERSEDED = """
    h.delivery_session_id is not null and r.publication_id is not null and not exists(
      select 1 from ouf_ingestion.ing_schedule s where %s)
    """.formatted(CURRENT_PUBLICATION);

  static final String SCHEDULE_ACTIVE = "s.state='ACTIVE' and (not s.trigger_once or s.discovered_bundle is not null)";
  static final String SCHEDULE_NO_ACTIVE_RUN = """
    not exists(select 1 from ouf_ingestion.ing_run r where r.source_id=s.source_id
      and (r.state in ('READY','RUNNING','DRAINING') or (r.state='PREFLIGHT' and r.schedule_id is distinct from s.schedule_id)))
    """;
  static final String SCHEDULE_BLOCKERS = """
    from ouf_ingestion.ing_run blocked
      where blocked.source_id=s.source_id and blocked.tenant_id=s.tenant_id
        and blocked.state not in ('ABORTED','SUCCEEDED','COMPLETED_WITH_WARNINGS')
        and (blocked.failure_code in ('ING_RECOVERY_BUDGET_EXHAUSTED','ING_HANDOFF_BLOCKED','ING_LAKE_RECONCILE_REQUIRED')
          or (blocked.state='PAUSED' and exists(select 1 from ouf_ingestion.run_recovery_session ledger
            where ledger.run_id=blocked.run_id and ledger.state='EXHAUSTED'))
          or exists(select 1 from ouf_ingestion.runtime_issue issue where issue.run_id=blocked.run_id
            and issue.issue_code='ING_RECOVERY_BUDGET_EXHAUSTED' and issue.status='OPEN'
            and not (blocked.state='PREFLIGHT' and blocked.schedule_id=s.schedule_id)))
    """;
  static final String SCHEDULE_INCIDENT = "exists(select 1 " + SCHEDULE_BLOCKERS + ")";
  static final String MANAGED_ATTEMPTS_LEFT = """
    s.activation_failures < coalesce(g.max_attempts,
      (s.sync_profile#>>'{operationalPolicy,maxRetryAttempts}')::int+1,%d)
    """.formatted(RetryPolicy.ATTEMPTS);
  static final String MANAGED_DEADLINE = """
    coalesce(g.deadline_at, coalesce(s.activation_first_failure_at,clock_timestamp())
      + coalesce((s.sync_profile#>>'{operationalPolicy,maxRetryElapsedSeconds}')::int,86400)*interval '1 second')
    """;
}
