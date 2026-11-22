package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import static it.comune.trieste.ouf.ingestion.RecoveryPredicates.*;

/** Governed PAUSE/RESUME/ABORT preserving the pinned baseline and durable progress. */
@Service @ConditionalOnBean(RuntimePorts.SemanticPort.class)
public class RunLifecycleService {
  private final JdbcClient sql;private final ObjectMapper json;private final RuntimePorts.SemanticPort contracts;private final TransactionTemplate transactions;
  public RunLifecycleService(JdbcClient sql,ObjectMapper json,RuntimePorts.SemanticPort contracts,PlatformTransactionManager manager){this.sql=sql;this.json=json;this.contracts=contracts;this.transactions=new TransactionTemplate(manager);}
  @Transactional public Map<String,Object> pause(UUID run,long version,TrustedAuthorizationContext.Context actor,String correlation){Map<String,Object> current=locked(run,actor);expectVersion(current,version);String state=String.valueOf(current.get("state"));if(RecoveryAction.RunState.valueOf(state).terminal())throw new IllegalStateException("ING_RUN_TERMINAL");int n=sql.sql("update ouf_ingestion.ing_run set state='PAUSED',control_version=control_version+1,updated_at=transaction_timestamp() where run_id=:r and tenant_id=:t and control_version=:v").param("r",run).param("t",actor.tenantId()).param("v",version).update();changed(n);sql.sql("update ouf_ingestion.ing_partition set handoff_resume_state=case when state in ('RUNNING','DRAINING') and exists(select 1 from ouf_ingestion.handoff_outbox where run_id=:r and delivery_session_id is not null and state<>'ACKED') then state else handoff_resume_state end,state='PAUSED',lease_owner=null,lease_until=null where run_id=:r and state<>'SUCCEEDED'").param("r",run).update();sql.sql("update ouf_ingestion.handoff_outbox set lease_until=clock_timestamp() where run_id=:r and state='DELIVERING'").param("r",run).update();
    if(Boolean.TRUE.equals(current.get("admission_pending"))){
      sql.sql("update ouf_ingestion.ing_schedule s set state='PAUSED',activation_blocked=true,lease_owner=null,lease_until=null,lock_version=lock_version+1 from ouf_ingestion.ing_run r where r.run_id=:r and r.schedule_id=s.schedule_id and r.publication_id=s.publication_id")
        .param("r",run).update();
    }
    audit("RUN_PAUSED",run,actor,correlation,Map.of("previousState",state,"version",version));return view(run,actor);}
  public Map<String,Object> resume(UUID run,long version,TrustedAuthorizationContext.Context actor,String correlation){return resume(run,version,actor,correlation,RunRecoveryBudget.Request.defaults());}
  public Map<String,Object> resume(UUID run,long version,TrustedAuthorizationContext.Context actor,String correlation,RunRecoveryBudget.Request budget){RunRecoveryBudget.requireHuman(actor);Objects.requireNonNull(budget);var baseline=snapshot(run,actor);
    // A renewed admission passes ACTIVE and Semantic preflight in the scheduler,
    // after the original Retry-After, with a fresh lease. The grant alone cannot start it.
    if(!baseline.admissionPending())contracts.preflight(baseline.bundle());
    return transactions.execute(status->resumeVerified(run,version,actor,correlation,budget));}
  private Map<String,Object> resumeVerified(UUID run,long version,TrustedAuthorizationContext.Context actor,String correlation,RunRecoveryBudget.Request budget){Map<String,Object> current=locked(run,actor);expectVersion(current,version);if(!"PAUSED".equals(current.get("state")))throw new IllegalStateException("ING_RUN_NOT_PAUSED");
    // A HUMAN retry authorizes each blocking record before the original cursor resumes.
    sql.sql("select lifecycle_state from ouf_ingestion.ing_quarantine where run_id=:r and "
      +BLOCKING_QUARANTINE+" for update").param("r",run).query().listOfRows();
    var facts=sql.sql("""
      select f.* from ouf_ingestion.ing_run r join ouf_ingestion.run_lake_recovery l using(run_id)
      cross join lateral (%s) f where r.run_id=:r
      """.formatted(RESUME_FACTS)).param("r",run).query().singleRow();
    switch (RecoveryAction.resumeBlock(facts)) {
      case NONE -> { }
      case QUARANTINE -> throw new IllegalStateException("ING_QUARANTINE_RETRY_REQUIRED");
      case LAKE -> throw new LakeAbortRequired();
      case PUBLICATION, DISCOVERY -> throw new IllegalStateException("ING_ACTIVE_PUBLICATION_CHANGED");
      case HANDOFF -> throw new IllegalStateException("ING_EFFECT_DISPOSITION_REQUIRED");
      case SNAPSHOT -> throw new IllegalStateException("ING_RUN_NOT_PAUSED");
    }
    boolean handoff=RecoveryAction.flag(facts,"handoff_recovery");
    boolean reconcileLake="HUMAN_RESUME_REQUIRED".equals(facts.get("lake_next_action"));
    UUID session=RunRecoveryBudget.grant(sql,run,version,actor,correlation,budget);
    boolean admission=Boolean.TRUE.equals(current.get("admission_pending"));
    if(admission){
      var previous=sql.sql("""
        select s.schedule_id,s.activation_failures,s.activation_first_failure_at from ouf_ingestion.ing_schedule s
        join ouf_ingestion.ing_run r using(schedule_id) where r.run_id=:r and %s and %s for update of s
        """.formatted(ADMISSION_PUBLICATION,FRESH_DISCOVERY)).param("r",run).query().listOfRows();
      if(previous.isEmpty())throw new IllegalStateException("ING_ACTIVE_PUBLICATION_CHANGED");
      audit("PREFLIGHT_BUDGET_RENEWED",run,actor,correlation,Map.of("previousAttempts",previous.getFirst().get("activation_failures"),"sessionId",session));
      sql.sql("update ouf_ingestion.ing_schedule set state='ACTIVE',activation_blocked=false,activation_failures=0,activation_first_failure_at=null,next_run_at=greatest(clock_timestamp(),next_run_at),lease_owner=null,lease_until=null,lock_version=lock_version+1 where schedule_id=:s")
        .param("s",previous.getFirst().get("schedule_id")).update();
    }
    var priorRetries=sql.sql("select partition_key,retry_started_at,retry_attempts from ouf_ingestion.ing_partition where run_id=:r and retry_started_at is not null order by partition_key")
      .param("r",run).query().listOfRows();
    if(!priorRetries.isEmpty())audit("ACQUISITION_BUDGET_RENEWED",run,actor,correlation,Map.of("previousBudgets",priorRetries,"sessionId",session));
    String target=admission?"PREFLIGHT":"RUNNING";
    if(handoff){
      var renewed=sql.sql("""
        update ouf_ingestion.handoff_outbox set state='FAILED_RETRYABLE',delivery_stop_reason=null,delivery_session_id=:session,lease_owner=null,lease_until=null
        where run_id=:r and %s
        returning handoff_id,attempts
        """.formatted(RENEWABLE_HANDOFF)).param("session",session).param("r",run).query().listOfRows();
      if(renewed.size()!=1 || sql.sql("select exists(select 1 from ouf_ingestion.handoff_outbox where run_id=:r and state='FAILED_TERMINAL')")
          .param("r",run).query(Boolean.class).single())throw new IllegalStateException("ING_EFFECT_DISPOSITION_REQUIRED");
      if(!sql.sql("select exists(select 1 from ouf_ingestion.ing_partition where run_id=:r and state='PAUSED' and handoff_resume_state is distinct from 'DRAINING')")
          .param("r",run).query(Boolean.class).single())target="DRAINING";
      audit("HANDOFF_BUDGET_RENEWED",run,actor,correlation,Map.of("handoffId",renewed.getFirst().get("handoff_id"),
          "previousAttempts",renewed.getFirst().get("attempts"),"sessionId",session));
    }
    int n=sql.sql("update ouf_ingestion.ing_run set state=:target,failure_code=null,control_version=control_version+1,updated_at=transaction_timestamp() where run_id=:r and tenant_id=:t and state='PAUSED' and control_version=:v").param("target",target).param("r",run).param("t",actor.tenantId()).param("v",version).update();changed(n);sql.sql("update ouf_ingestion.ing_partition set state=case when :handoff then coalesce(handoff_resume_state,:target) else :target end,handoff_resume_state=null,retry_started_at=null,retry_attempts=0,retry_at=case when retry_at>clock_timestamp() then retry_at end,lease_owner=null,lease_until=null where run_id=:r and state='PAUSED'").param("handoff",handoff).param("target",target).param("r",run).update();
    if(reconcileLake){
      sql.sql("""
          update ouf_ingestion.lake_write_evidence set human_review=false,
            cause_code=coalesce(cause_code,'ING_LAKE_LEGACY_CAUSE_UNKNOWN'),cause_at=coalesce(cause_at,clock_timestamp())
          where run_id=:r and not abort_required and (human_review or (receipt_ref is null and intent_at is null and cause_code is null))
          """)
          .param("r",run).update();
      sql.sql("""
          update ouf_ingestion.ing_partition p set retry_started_at=clock_timestamp(),retry_at=greatest(retry_at,clock_timestamp())
          where run_id=:r and state='RUNNING' and exists(select 1 from ouf_ingestion.lake_write_evidence e
            where e.run_id=p.run_id and e.partition_key=p.partition_key and e.receipt_ref is null)
          """).param("r",run).update();
    }
    audit("RUN_RESUMED",run,actor,correlation,Map.of("bundleRef",bundleRef(current),"version",version,"recoverySessionId",session,"profileRef",RunRecoveryBudget.PROFILE,"maxAttempts",budget.maxAttempts(),"maxElapsedSeconds",budget.maxElapsedSeconds(),"reasonCode",budget.reasonCode()));return view(run,actor);}
  @Transactional public Map<String,Object> abort(UUID run,long version,TrustedAuthorizationContext.Context actor,String correlation){
    if(actor==null || !"HUMAN".equals(actor.actorType()) || !actor.capabilities().contains("ingestion.run.abort")
        || actor.decisionRef()==null || actor.decisionRef().isBlank())throw new SecurityException("ING_HUMAN_ABORT_AUTHORITY_REQUIRED");
    Map<String,Object> current=locked(run,actor);expectVersion(current,version);String state=String.valueOf(current.get("state"));if(RecoveryAction.RunState.valueOf(state).terminal())throw new IllegalStateException("ING_RUN_TERMINAL");int n=sql.sql("update ouf_ingestion.ing_run set state='ABORTED',failure_code='ING_OPERATOR_ABORTED',control_version=control_version+1,updated_at=transaction_timestamp() where run_id=:r and tenant_id=:t and control_version=:v").param("r",run).param("t",actor.tenantId()).param("v",version).update();changed(n);
    var pending=sql.sql("""
      select count(*) filter(where state<>'ACKED') without_ack,
             count(*) filter(where state='DELIVERING') in_flight
      from ouf_ingestion.handoff_outbox where run_id=:r
      """).param("r",run).query().singleRow();
    sql.sql("update ouf_ingestion.ing_partition set state='ABORTED',lease_owner=null,lease_until=null where run_id=:r and state<>'SUCCEEDED'").param("r",run).update();sql.sql("update ouf_ingestion.run_recovery_session set state='CANCELLED' where run_id=:r and state='ACTIVE'").param("r",run).update();sql.sql("""
      update ouf_ingestion.handoff_outbox set state='FAILED_TERMINAL',lease_owner=null,lease_until=null,
        last_error_code=case when state='DELIVERING' then 'ING_HANDOFF_OUTCOME_UNCONFIRMED' else coalesce(last_error_code,'ING_OPERATOR_ABORTED') end
      where run_id=:r and state<>'ACKED'
      """).param("r",run).update();
    if(Boolean.TRUE.equals(current.get("admission_pending"))){
      sql.sql("""
        update ouf_ingestion.ing_schedule s set state='DISABLED',
          lease_owner=null,lease_until=null,lock_version=lock_version+1
        from ouf_ingestion.ing_run r where r.run_id=:r and r.schedule_id=s.schedule_id and r.publication_id=s.publication_id
        """).param("r",run).update();
    }
    sql.sql("""
      update ouf_ingestion.runtime_issue set status='RESOLVED',resolution_reason='RUN_ABORTED',
        resolution_actor=:actor,authorization_decision_ref=:decision,resolved_at=clock_timestamp(),lock_version=lock_version+1
      where run_id=:r and status='OPEN' and issue_code in ('ING_HANDOFF_BLOCKED','ING_RECOVERY_BUDGET_EXHAUSTED')
      """).param("actor",actor.subject()).param("decision",actor.decisionRef()).param("r",run).update();
    audit("RUN_ABORTED",run,actor,correlation,Map.of("previousState",state,"version",version,"handoffsWithoutAck",pending.get("without_ack"),"inFlightHandoffs",pending.get("in_flight")));return view(run,actor);}
  public Map<String,Object> view(UUID run,TrustedAuthorizationContext.Context actor){List<Map<String,Object>> rows=sql.sql("select run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,phase,failure_code,control_version,created_at,updated_at,lake_cause_code,lake_next_action from ouf_ingestion.ing_run join ouf_ingestion.run_lake_recovery using(run_id) where run_id=:r and tenant_id=:t").param("r",run).param("t",actor.tenantId()).query().listOfRows();if(rows.isEmpty())throw new NoSuchElementException("ING_RUN_NOT_FOUND");Map<String,Object> result=new LinkedHashMap<>(rows.getFirst());var budget=sql.sql("select session_id,epoch,profile_ref,state,max_attempts,attempts,max_elapsed_seconds,deadline_at,greatest(0,max_attempts-attempts) remaining_attempts,(select sum(attempts) from ouf_ingestion.run_recovery_session history where history.run_id=:r) cumulative_attempts from ouf_ingestion.run_recovery_session where run_id=:r order by epoch desc limit 1").param("r",run).query().listOfRows();if(!budget.isEmpty())result.put("recoveryBudget",budget.getFirst());return result;}
  private Map<String,Object> locked(UUID run,TrustedAuthorizationContext.Context actor){
    // Match discovery's schedule-before-run lock order for preflight controls.
    sql.sql("select s.schedule_id from ouf_ingestion.ing_schedule s join ouf_ingestion.ing_run r using(schedule_id) where r.run_id=:r and r.tenant_id=:t and r.admission_pending for update of s")
      .param("r",run).param("t",actor.tenantId()).query(UUID.class).optional();
    List<Map<String,Object>> rows=sql.sql("select run_id,bundle_id,bundle_version,bundle_checksum,state,control_version,admission_pending,publication_id,failure_code from ouf_ingestion.ing_run where run_id=:r and tenant_id=:t for update").param("r",run).param("t",actor.tenantId()).query().listOfRows();if(rows.isEmpty())throw new NoSuchElementException("ING_RUN_NOT_FOUND");return rows.getFirst();}
  static final class LakeAbortRequired extends IllegalStateException {
    LakeAbortRequired(){super("ING_LAKE_ABORT_REQUIRED");}
  }
  private record PausedBaseline(ExecutionBundle bundle,boolean admissionPending){}
  private PausedBaseline snapshot(UUID run,TrustedAuthorizationContext.Context actor){
    var row=sql.sql("""
      select s.snapshot_json::text snapshot,r.admission_pending from ouf_ingestion.runtime_configuration_snapshot s
      join ouf_ingestion.ing_run r using(run_id) where s.run_id=:r and r.tenant_id=:t and r.state='PAUSED'
      """).param("r",run).param("t",actor.tenantId()).query().listOfRows();
    if(row.isEmpty())throw new IllegalStateException("ING_RUN_NOT_PAUSED");
    try{return new PausedBaseline(json.readValue((String)row.getFirst().get("snapshot"),ExecutionBundle.class),
      Boolean.TRUE.equals(row.getFirst().get("admission_pending")));}
    catch(Exception e){throw new IllegalStateException("ING_BUNDLE_SNAPSHOT_INVALID",e);}
  }
  private static Collection<String> pinned(ExecutionBundle bundle){Object value=bundle.configuration().get("pinnedReferences");if(!(value instanceof Collection<?> refs)||refs.isEmpty())throw new IllegalStateException("ING_PREFLIGHT_REFS_REQUIRED");return refs.stream().map(String::valueOf).toList();}
  private void audit(String event,UUID run,TrustedAuthorizationContext.Context actor,String correlation,Map<String,Object> detail){try{sql.sql("insert into ouf_ingestion.audit_event(event_id,event_type,actor_subject,actor_type,resource_type,resource_id,correlation_id,detail_json,tenant_id,authorization_decision_ref) values(:i,:e,:a,:t,'RUN',:r,:c,cast(:j as jsonb),:n,:d)").param("i",UUID.randomUUID()).param("e",event).param("a",actor.subject()).param("t",actor.actorType()).param("r",run.toString()).param("c",correlation).param("j",json.writeValueAsString(detail)).param("n",actor.tenantId()).param("d",actor.decisionRef()).update();}catch(Exception e){throw new IllegalStateException("ING_AUDIT_WRITE_FAILED",e);}}
  private static String bundleRef(Map<String,Object> row){return row.get("bundle_id")+":"+row.get("bundle_version")+":"+row.get("bundle_checksum");}
  private static void expectVersion(Map<String,Object> row,long version){if(((Number)row.get("control_version")).longValue()!=version)throw new IllegalStateException("ING_RUN_STATE_CONFLICT");}
  private static void changed(int n){if(n!=1)throw new IllegalStateException("ING_RUN_STATE_CONFLICT");}
}
