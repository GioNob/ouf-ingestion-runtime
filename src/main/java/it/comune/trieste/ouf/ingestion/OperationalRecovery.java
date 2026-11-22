package it.comune.trieste.ouf.ingestion;

import java.net.URI;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static it.comune.trieste.ouf.ingestion.RecoveryPredicates.*;

/** Read-only recovery projection over a bounded page of tenant-filtered runs or schedules. */
final class OperationalRecovery {
  static final String SELECT = """
    select page.*, l.lake_cause_code, coalesce(l.lake_next_action,'NONE') lake_next_action,
      r.state persisted_state, r.control_version action_version, r.admission_pending,
      f.has_snapshot, f.quarantine_not_ready, f.handoff_recovery, f.handoff_renewable,
      f.publication_required, f.publication_current, f.publication_fresh,
      acquisition.retry_at acquisition_retry_at, acquisition.reconciling lake_reconciling,
      delivery.retry_at delivery_retry_at, delivery.reconciling handoff_reconciling,
      s.state projected_schedule_state, s.activation_blocked schedule_blocked,
      case when s.schedule_id is not null then %s end schedule_incident,
      schedule.retry_at schedule_retry_at,
      case when ack.confirmed then
        case when jsonb_typeof(snap.snapshot_json#>'{configuration,publishedBundle}')='object'
          then nullif(btrim(snap.snapshot_json#>>'{configuration,publishedBundle,semanticMapping,sourceType,typeCode}'),'')
          else nullif(btrim(snap.snapshot_json#>>'{configuration,typeCode}'),'') end
      end pinned_type,
      coalesce(ack.confirmed,false) handoff_confirmed
    from page
    left join ouf_ingestion.ing_run r on r.run_id=page.job_ref and r.tenant_id=page.tenant_id
    left join ouf_ingestion.run_lake_recovery l on l.run_id=r.run_id
    left join ouf_ingestion.runtime_configuration_snapshot snap on snap.run_id=r.run_id
    left join ouf_ingestion.ing_schedule s on page.project_schedule and s.tenant_id=page.tenant_id
      and s.source_id=page.source_ref and s.publication_id is not distinct from page.publication_id
    left join ouf_ingestion.source_health sh on sh.source_id=page.source_ref
    left join ouf_ingestion.run_recovery_session g on s.schedule_id is not null and r.admission_pending
      and g.run_id=r.run_id and g.state='ACTIVE'
    left join lateral (%s where r.state='PAUSED') f on true
    left join lateral (
      select coalesce(greatest(case when p.retry_at is not null then %s end,p.lease_until),statement_timestamp()) retry_at,
        %s reconciling
      from ouf_ingestion.ing_partition p
      left join lateral (select (%s) allowed,deadline_at from ouf_ingestion.run_recovery_session
        where run_id=r.run_id order by epoch desc limit 1) budget on true
      where p.run_id=r.run_id and %s
        and (p.retry_at is not null or (p.retry_started_at is not null or p.lease_until<=statement_timestamp()) and %s)
        and not %s
        and (p.retry_started_at is null or not (%s) and greatest(%s,statement_timestamp(),p.lease_until)
          < p.retry_started_at+interval '24 hours')
        and (budget.allowed is null or budget.allowed and greatest(%s,statement_timestamp(),p.lease_until)<budget.deadline_at)
        and (r.publication_id is null or exists(select 1 from ouf_ingestion.ing_schedule s where %s))
      order by retry_at, p.partition_key limit 1
    ) acquisition on true
    left join lateral (
      select case when h.state='DELIVERING' then h.lease_until else h.next_attempt_at end retry_at,
        %s reconciling
      from ouf_ingestion.handoff_outbox h join ouf_ingestion.handoff_budget b using(handoff_id)
      where h.run_id=r.run_id and %s and %s and not (%s) and (%s) is not true
        and (h.state='FAILED_RETRYABLE' and (b.deadline is null or h.next_attempt_at<b.deadline)
          or h.state='DELIVERING' and h.lease_until<=statement_timestamp())
      order by retry_at,h.handoff_id limit 1
    ) delivery on true
    left join lateral (
      select greatest(s.next_run_at,s.lease_until,sh.retry_not_before,sh.circuit_open_until,sh.recovery_probe_until) retry_at
      where %s and not %s and %s
        and (s.activation_failures>0 or r.admission_pending and r.state='PREFLIGHT' and g.session_id is not null
          or sh.retry_not_before>statement_timestamp() or sh.circuit_open_until>statement_timestamp())
        and (not s.trigger_once or s.publication_enabled and s.consumed_publication_id is distinct from s.publication_id
          and %s and greatest(clock_timestamp(),s.next_run_at,s.lease_until,sh.retry_not_before,
            sh.circuit_open_until,sh.recovery_probe_until)<%s)
    ) schedule on true
    left join lateral (
      select count(*)>0 and bool_and(h.state='ACKED' and nullif(btrim(h.downstream_receipt_ref),'') is not null
        and h.acked_at is not null) confirmed
      from ouf_ingestion.handoff_outbox h where h.run_id=r.run_id and r.state in ('SUCCEEDED','COMPLETED_WITH_WARNINGS')
    ) ack on true
    """.formatted(SCHEDULE_INCIDENT, RESUME_FACTS, ACQUISITION_RETRY_AT, PENDING_LAKE_WRITE, SESSION_CAN_ATTEMPT, ACQUISITION_LANE,
      PENDING_LAKE_WRITE, BLOCKED_LAKE_WRITE, PARTITION_EXHAUSTED, ACQUISITION_RETRY_AT, ACQUISITION_RETRY_AT, CURRENT_PUBLICATION,
      DELIVERY_RECONCILING, DELIVERY_LANE, DELIVERY_HEAD, DELIVERY_SUPERSEDED, DELIVERY_EXPIRED,
      SCHEDULE_ACTIVE, SCHEDULE_INCIDENT, SCHEDULE_NO_ACTIVE_RUN, MANAGED_ATTEMPTS_LEFT, MANAGED_DEADLINE);

  private static final List<String> INTERNAL_COLUMNS = List.of("tenant_id", "project_schedule", "persisted_state",
    "action_version", "admission_pending", "has_snapshot", "quarantine_not_ready", "handoff_recovery",
    "handoff_renewable", "publication_required", "publication_current", "publication_fresh", "acquisition_retry_at",
    "lake_reconciling", "delivery_retry_at", "handoff_reconciling", "projected_schedule_state", "schedule_blocked",
    "schedule_incident", "schedule_retry_at", "pinned_type", "handoff_confirmed");

  private final URI thsOrigin;

  OperationalRecovery(String origin) {
    thsOrigin = origin.isBlank() ? null : URI.create(origin);
    if (thsOrigin != null && (!"https".equals(thsOrigin.getScheme()) || thsOrigin.getHost()==null
        || thsOrigin.getRawUserInfo()!=null || thsOrigin.getRawQuery()!=null || thsOrigin.getRawFragment()!=null
        || !List.of("", "/").contains(thsOrigin.getRawPath()))) {
      throw new IllegalArgumentException("ING_RECOVERY_THS_ORIGIN_INVALID");
    }
  }

  Map<String,Object> project(Map<String,Object> row) {
    String stored = (String) row.get("persisted_state");
    var state = stored == null ? null : RecoveryAction.RunState.valueOf(stored);
    var scheduleRetryAt = instant(row.get("schedule_retry_at"));
    var run = runRecovery(state,row,scheduleRetryAt);
    var schedule = scheduleAction(state,run.action(),row);
    var recovery = state == null ? new Decision(schedule,scheduleRetryAt) : run;
    boolean confirmed = RecoveryAction.flag(row,"handoff_confirmed");
    Object typeCode = row.get("pinned_type");
    row.put("runState", state == RecoveryAction.RunState.SUCCEEDED && confirmed && typeCode != null
      ? "COMPLETED" : row.get("run_state"));
    row.put("recoveryAction",recovery.action().name());
    if (recovery.retryAt() != null) row.put("retryAt",recovery.retryAt().toString());
    if (row.get("projected_schedule_state") != null) {
      row.put("scheduleRecoveryAction",schedule.name());
      if (scheduleRetryAt != null && schedule == RecoveryAction.AUTOMATIC_RETRY) row.put("scheduleRetryAt",scheduleRetryAt.toString());
    }
    if (state != null) row.put("udpHandoffConfirmed",confirmed);
    if (confirmed && typeCode != null) row.put("typeCode",typeCode);
    if (state == RecoveryAction.RunState.PAUSED) {
      Object runId = row.get("job_ref");
      long version = ((Number) row.get("action_version")).longValue();
      row.put("runId",runId);
      row.put("expectedVersion",version);
      if (thsOrigin != null && version>=0 && version<=9007199254740991L) {
        row.put("reviewUrl",thsOrigin.resolve("/trusted-human/ingestion-run-recovery/?runId="+runId
          +"&expectedVersion="+version).toString());
      }
    }
    INTERNAL_COLUMNS.forEach(row::remove);
    return row;
  }

  private record Decision(RecoveryAction action, Instant retryAt) {
    Decision(RecoveryAction action) { this(action,null); }
  }

  private static Decision runRecovery(RecoveryAction.RunState state, Map<String,Object> row, Instant scheduleRetryAt) {
    if (state == null) return new Decision(RecoveryAction.NONE);
    return switch (state) {
      case SUCCEEDED, COMPLETED_WITH_WARNINGS, FAILED, ABORTED -> new Decision(RecoveryAction.TERMINAL);
      case PAUSED -> new Decision(RecoveryAction.paused(row));
      case READY -> new Decision(RecoveryAction.NONE);
      case PREFLIGHT -> scheduleRetryAt == null ? new Decision(RecoveryAction.NONE)
        : new Decision(RecoveryAction.AUTOMATIC_RETRY,scheduleRetryAt);
      case RUNNING, DRAINING -> executionRecovery(row);
    };
  }

  private static Decision executionRecovery(Map<String,Object> row) {
    var acquisition = instant(row.get("acquisition_retry_at"));
    var delivery = instant(row.get("delivery_retry_at"));
    if (acquisition != null && (delivery == null || !acquisition.isAfter(delivery))) {
      return new Decision(automatic(RecoveryAction.flag(row,"lake_reconciling")),acquisition);
    }
    if (delivery != null) {
      return new Decision(automatic(RecoveryAction.flag(row,"handoff_reconciling")),delivery);
    }
    return new Decision(RecoveryAction.NONE);
  }

  private static RecoveryAction automatic(boolean reconciling) {
    return reconciling ? RecoveryAction.AUTOMATIC_RECONCILIATION : RecoveryAction.AUTOMATIC_RETRY;
  }

  private static RecoveryAction scheduleAction(RecoveryAction.RunState state, RecoveryAction run, Map<String,Object> row) {
    String schedule = (String) row.get("projected_schedule_state");
    if (schedule == null) return RecoveryAction.NONE;
    return switch (schedule) {
      case "DISABLED" -> RecoveryAction.TERMINAL;
      case "PAUSED" -> {
        if (state == RecoveryAction.RunState.PAUSED && RecoveryAction.flag(row,"admission_pending")) yield run;
        yield RecoveryAction.flag(row,"schedule_blocked") && row.get("publication_id") != null
          ? RecoveryAction.NEW_PUBLICATION_REQUIRED : RecoveryAction.HUMAN_SCHEDULE_REVIEW;
      }
      case "ACTIVE" -> {
        if (RecoveryAction.flag(row,"schedule_incident")) yield RecoveryAction.HUMAN_SCHEDULE_REVIEW;
        yield row.get("schedule_retry_at") != null ? RecoveryAction.AUTOMATIC_RETRY : RecoveryAction.NONE;
      }
      default -> throw new IllegalStateException("ING_SCHEDULE_STATE_UNKNOWN");
    };
  }

  private static Instant instant(Object value) {
    return value == null ? null : ((Timestamp) value).toInstant();
  }
}
