package it.comune.trieste.ouf.ingestion;

import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Owner-local durable budgets; never grants IAM authority or changes operation identity. */
final class RunRecoveryBudget {
  static final String PROFILE="ouf.run-recovery-batch.v1";
  record Request(int maxAttempts,int maxElapsedSeconds,String reasonCode) {
    Request {
      if(maxAttempts<1||maxAttempts>10000||maxElapsedSeconds<1||maxElapsedSeconds>2592000
          ||reasonCode==null||!reasonCode.matches("[A-Z][A-Z0-9_]{0,79}"))
        throw new IllegalArgumentException("ING_RECOVERY_BUDGET_INVALID");
    }
    static Request defaults(){return new Request(128,86400,"OPERATOR_REQUESTED_RECOVERY");}
  }
  static void requireHuman(TrustedAuthorizationContext.Context actor){
    if(actor==null||!"HUMAN".equals(actor.actorType())
        ||!actor.capabilities().contains("ingestion.run.resume")
        ||actor.subject()==null||actor.subject().isBlank()
        ||actor.decisionRef()==null||actor.decisionRef().isBlank())
      throw new SecurityException("ING_HUMAN_RECOVERY_AUTHORITY_REQUIRED");
  }
  private static void transaction(){
    if(!TransactionSynchronizationManager.isActualTransactionActive())
      throw new IllegalStateException("ING_RECOVERY_TRANSACTION_REQUIRED");
  }
  /** Caller holds the tenant-scoped PAUSED run row and has revalidated its baseline. */
  static UUID grant(JdbcClient sql,UUID run,long version,TrustedAuthorizationContext.Context actor,String correlation,Request budget){
    transaction();requireHuman(actor);Objects.requireNonNull(budget);
    boolean paused=sql.sql("select 1 from ouf_ingestion.ing_run where run_id=:r and tenant_id=:t and state='PAUSED' and control_version=:v for update")
      .param("r",run).param("t",actor.tenantId()).param("v",version).query(Integer.class).optional().isPresent();
    if(!paused)throw new IllegalStateException("ING_RUN_STATE_CONFLICT");
    if(correlation==null||correlation.isBlank()||correlation.length()>500)
      throw new IllegalArgumentException("ING_RECOVERY_CORRELATION_INVALID");
    sql.sql("update ouf_ingestion.run_recovery_session set state='SUPERSEDED' where run_id=:r and state='ACTIVE'").param("r",run).update();
    UUID id=UUID.randomUUID();
    sql.sql("with epoch as (select coalesce(max(epoch),0)+1 value from ouf_ingestion.run_recovery_session where run_id=:r), now as (select clock_timestamp() value) insert into ouf_ingestion.run_recovery_session(session_id,run_id,epoch,expected_control_version,profile_ref,max_attempts,max_elapsed_seconds,actor_subject,authorization_decision_ref,reason_code,correlation_id,granted_at,deadline_at) select :i,:r,epoch.value,:v,:p,:a,:e,:actor,:decision,:reason,:c,now.value,now.value+(:e*interval '1 second') from epoch,now")
      .param("i",id).param("r",run).param("v",version).param("p",PROFILE).param("a",budget.maxAttempts()).param("e",budget.maxElapsedSeconds())
      .param("actor",actor.subject()).param("decision",actor.decisionRef()).param("reason",budget.reasonCode()).param("c",correlation).update();
    return id;
  }
  /** Count one owned worker batch BEFORE adapter I/O; never retain a lock across I/O. */
  static boolean admit(JdbcClient sql,RunExecutionRepository.Claim claim){
    transaction();
    var sessions=sql.sql("select session_id,state,attempts,max_attempts,deadline_at>clock_timestamp() within_deadline from ouf_ingestion.run_recovery_session where run_id=:r order by epoch desc limit 1 for update").param("r",claim.runId()).query().listOfRows();
    if(sessions.isEmpty())return true; // Initial-run policy adoption is a separate, explicitly open tranche.
    var row=sessions.getFirst();UUID id=(UUID)row.get("session_id");
    boolean allowed="ACTIVE".equals(row.get("state"))&&Boolean.TRUE.equals(row.get("within_deadline"))
      &&((Number)row.get("attempts")).intValue()<((Number)row.get("max_attempts")).intValue();
    if(allowed){sql.sql("update ouf_ingestion.run_recovery_session set attempts=attempts+1 where session_id=:i").param("i",id).update();return true;}
    exhausted(sql,claim.runId());
    sql.sql("update ouf_ingestion.ing_partition set state='PAUSED',lease_owner=null,lease_until=null where run_id=:r and state='RUNNING'").param("r",claim.runId()).update();
    sql.sql("update ouf_ingestion.ing_run set state='PAUSED',failure_code='ING_RECOVERY_BUDGET_EXHAUSTED',updated_at=clock_timestamp() where run_id=:r and state='RUNNING'").param("r",claim.runId()).update();
    return false;
  }
  static void exhausted(JdbcClient sql,UUID run){
    transaction();
    sql.sql("update ouf_ingestion.run_recovery_session set state='EXHAUSTED' where run_id=:r and state='ACTIVE'").param("r",run).update();
    sql.sql("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id) select :i,run_id,source_id,'RECOVERY','ERROR','ING_RECOVERY_BUDGET_EXHAUSTED',:e,correlation_id from ouf_ingestion.ing_run where run_id=:r on conflict(run_id) where issue_code='ING_RECOVERY_BUDGET_EXHAUSTED' do update set status='OPEN',resolved_at=null,lock_version=runtime_issue.lock_version+1")
      .param("i",UUID.randomUUID()).param("r",run).param("e","run-recovery:"+run).update();
  }
  static void requireCurrent(JdbcClient sql,RunExecutionRepository.Claim claim){
    if(claim==null)return; // Legacy unclaimed callers are not certified by this tranche.
    boolean owned=sql.sql("select exists(select 1 from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id) where p.run_id=:r and p.partition_key=:p and p.lease_owner=:w and p.lease_generation=:g and p.lease_until>clock_timestamp() and p.state='RUNNING' and r.state='RUNNING')")
      .param("r",claim.runId()).param("p",claim.partitionKey()).param("w",claim.worker()).param("g",claim.generation()).query(Boolean.class).single();
    if(!owned)throw new IllegalStateException("ING_PARTITION_LEASE_LOST");
    var states=sql.sql("select state='ACTIVE' and deadline_at>clock_timestamp() valid from ouf_ingestion.run_recovery_session where run_id=:r order by epoch desc limit 1").param("r",claim.runId()).query(Boolean.class).list();
    if(!states.isEmpty()&&!states.getFirst())throw new IllegalStateException("ING_RECOVERY_BUDGET_EXHAUSTED");
  }
}
