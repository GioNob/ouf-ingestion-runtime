package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class RunStateRepository {
  private static final int CIRCUIT_THRESHOLD=5;
  private final JdbcClient sql;private final ObjectMapper json;
  public RunStateRepository(JdbcClient sql,ObjectMapper json){this.sql=sql;this.json=json;}

  @Transactional public Optional<ScheduleClaim> claimDue(String worker,Duration lease){
    sql.sql("select pg_advisory_xact_lock(hashtextextended('ouf-ingestion-admission',0))").query().singleRow();
    String pressure=refreshPressure();
    Optional<ScheduleClaim> claimed=sql.sql("with policy as (select global_running_limit from ouf_ingestion.runtime_pressure_policy where policy_key='GLOBAL'), admitted as (select count(distinct source_id) total from (select source_id from ouf_ingestion.ing_run where state in ('READY','PREFLIGHT','RUNNING','DRAINING') union all select source_id from ouf_ingestion.ing_schedule where lease_until>transaction_timestamp()) a), tenant_head as (select distinct on(tenant_id) schedule_id,tenant_id,next_run_at,last_dispatched_at,workload_class from ouf_ingestion.ing_schedule s where state='ACTIVE' and (publication_id is null or discovery_verified_at>transaction_timestamp()-interval '5 minutes') and next_run_at<=transaction_timestamp() and (lease_until is null or lease_until<transaction_timestamp()) and (:pressure not in ('HARD_PRESSURE','RECOVERY') or workload_class<>'BOOTSTRAP') and ((select total from admitted) < (select case when :pressure='SOFT_PRESSURE' then greatest(1,global_running_limit/2) when :pressure='RECOVERY' then 1 else global_running_limit end from policy) or exists(select 1 from ouf_ingestion.ing_run rp where rp.schedule_id=s.schedule_id and rp.state='PREFLIGHT')) and not exists(select 1 from ouf_ingestion.source_health h where h.source_id=s.source_id and (h.retry_not_before>transaction_timestamp() or h.circuit_open_until>transaction_timestamp() or h.recovery_probe_until>transaction_timestamp())) and not exists(select 1 from ouf_ingestion.ing_run r where r.source_id=s.source_id and (r.state in ('READY','RUNNING','DRAINING') or (r.state='PREFLIGHT' and r.schedule_id is distinct from s.schedule_id))) order by tenant_id,case workload_class when 'DELTA_ORDINARY' then 0 when 'DELTA_RECOVERY' then 1 else 2 end,next_run_at), candidate as (select s.schedule_id from ouf_ingestion.ing_schedule s join tenant_head h using(schedule_id) order by case h.workload_class when 'DELTA_ORDINARY' then 0 when 'DELTA_RECOVERY' then 1 else 2 end,h.last_dispatched_at nulls first,h.next_run_at,h.tenant_id for update of s skip locked limit 1) update ouf_ingestion.ing_schedule s set lease_owner=:w,lease_until=transaction_timestamp()+(:ms*interval '1 millisecond'),last_dispatched_at=transaction_timestamp(),lock_version=lock_version+1 from candidate c where s.schedule_id=c.schedule_id returning s.schedule_id,s.tenant_id,s.source_id,s.interval_seconds,s.publication_id,s.publication_checksum,s.next_run_at,s.lock_version")
      .param("pressure",pressure).param("w",worker).param("ms",lease.toMillis()).query((rs,n)->new ScheduleClaim(rs.getObject(1,UUID.class),rs.getString(2),rs.getString(3),rs.getInt(4),worker,rs.getObject(5,UUID.class),rs.getString(6),rs.getObject(7,OffsetDateTime.class),rs.getLong(8))).optional();
    claimed.ifPresent(c->sql.sql("update ouf_ingestion.source_health set recovery_probe_owner=:w,recovery_probe_until=transaction_timestamp()+(:ms*interval '1 millisecond') where source_id=:s and status='PAUSED' and circuit_open_until<=transaction_timestamp() and (recovery_probe_until is null or recovery_probe_until<=transaction_timestamp())").param("w",worker).param("ms",lease.toMillis()).param("s",c.sourceId()).update());
    return claimed;
  }
  @Transactional public void releaseSchedule(ScheduleClaim c,boolean advance){
    int changed=sql.sql("update ouf_ingestion.ing_schedule set next_run_at=case when :a then transaction_timestamp()+(interval_seconds*interval '1 second') else transaction_timestamp()+interval '60 seconds' end,state=case when :a and trigger_once then 'DISABLED' else state end,consumed_publication_id=case when :a then publication_id else consumed_publication_id end,activation_failures=case when :a then 0 else activation_failures end,activation_first_failure_at=case when :a then null else activation_first_failure_at end,activation_blocked=case when :a then false else activation_blocked end,lease_owner=null,lease_until=null,lock_version=lock_version+1 where schedule_id=:i and lease_owner=:w and lock_version=:g and lease_until>transaction_timestamp()")
     .param("a",advance).param("i",c.scheduleId()).param("w",c.worker()).param("g",c.generation()).update();if(changed!=1)throw new IllegalStateException("ING_SCHEDULE_LEASE_LOST");
  }
  @Transactional public void completePreflight(ScheduleClaim c,UUID run){checkLease(c);preflightSucceeded(run);releaseSchedule(c,true);}
  @Transactional public void failPreflight(ScheduleClaim c,UUID run,String code){checkLease(c);preflightFailed(run,code);}
  @Transactional public void activationFailed(ScheduleClaim c,UUID run,RunCoordinator.Failure failure){
    checkLease(c);if(run!=null)preflightFailed(run,failure.code());
    var row=sql.sql("select sync_profile::text policy,activation_failures,activation_first_failure_at from ouf_ingestion.ing_schedule where schedule_id=:id").param("id",c.scheduleId()).query().singleRow();
    Map<String,Object> sync;try{sync=json.readValue((String)row.get("policy"),Map.class);}catch(Exception e){throw new IllegalStateException("ING_SCHEDULE_POLICY_INVALID");}
    Map<String,Object> policy=sync.get("operationalPolicy") instanceof Map<?,?> m?(Map<String,Object>)m:Map.of();
    int attempts=((Number)row.get("activation_failures")).intValue()+1;long maxAttempts=((Number)policy.getOrDefault("maxRetryAttempts",0)).longValue();long maxElapsed=((Number)policy.getOrDefault("maxRetryElapsedSeconds",0)).longValue();long delay=((Number)sync.getOrDefault("retryBackoffSeconds",0)).longValue();
    if(failure.retryAfter()!=null)delay=Math.max(delay,failure.retryAfter().toSeconds());
    OffsetDateTime first=row.get("activation_first_failure_at")==null?OffsetDateTime.now(ZoneOffset.UTC):(OffsetDateTime)row.get("activation_first_failure_at");
    boolean retry=failure.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE&&attempts<=maxAttempts&&delay>0&&Duration.between(first,OffsetDateTime.now(ZoneOffset.UTC)).getSeconds()+delay<=maxElapsed;
    sql.sql("update ouf_ingestion.ing_schedule set activation_failures=:attempts,activation_first_failure_at=:first,activation_blocked=:blocked,state=:state,next_run_at=transaction_timestamp()+(:delay*interval '1 second'),lease_owner=null,lease_until=null,lock_version=lock_version+1 where schedule_id=:id")
      .param("attempts",attempts).param("first",first).param("blocked",!retry).param("state",retry?"ACTIVE":"PAUSED").param("delay",Math.max(1,delay)).param("id",c.scheduleId()).update();
    sql.sql("insert into ouf_ingestion.source_health(source_id,status,last_error_code) values(:s,'DEGRADED',:code) on conflict(source_id) do update set status='DEGRADED',last_error_code=:code,updated_at=transaction_timestamp()").param("s",c.sourceId()).param("code",safe(failure.code())).update();
  }
  private void checkLease(ScheduleClaim c){if(c.generation()<0)return;boolean valid=sql.sql("select 1 from ouf_ingestion.ing_schedule where schedule_id=:i and lease_owner=:w and lock_version=:g and lease_until>transaction_timestamp() for no key update").param("i",c.scheduleId()).param("w",c.worker()).param("g",c.generation()).query(Integer.class).optional().isPresent();if(!valid)throw new IllegalStateException("ING_SCHEDULE_LEASE_LOST");}


  @Transactional public UUID createPreflightRun(ScheduleClaim claim,ExecutionBundle b,String correlation){checkLease(claim);if(!claim.sourceId().equals(b.sourceId()))throw new IllegalArgumentException("ING_BUNDLE_SOURCE_MISMATCH");
    if(claim.publicationId()!=null){var existing=sql.sql("select run_id,state,bundle_checksum from ouf_ingestion.ing_run where schedule_id=:s and schedule_due_at=:d").param("s",claim.scheduleId()).param("d",claim.dueAt()).query().listOfRows().stream().findFirst();if(existing.isPresent()){var row=existing.orElseThrow();if(!"PREFLIGHT".equals(row.get("state"))||!b.checksum().equals(row.get("bundle_checksum")))throw new IllegalStateException("ING_SCHEDULE_SLOT_ALREADY_USED");return (UUID)row.get("run_id");}}
    UUID run=UUID.randomUUID();String phase=phase(b.acquisitionMode());sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id,tenant_id,phase,schedule_id,schedule_due_at) values(:r,:s,:b,:v,:h,:m,'PREFLIGHT',:c,:t,:p,:schedule,:due)").param("r",run).param("s",claim.sourceId()).param("b",b.bundleId()).param("v",b.bundleVersion()).param("h",b.checksum()).param("m",mode(phase)).param("c",correlation).param("t",claim.tenantId()).param("p",phase).param("schedule",claim.publicationId()==null?null:claim.scheduleId(),java.sql.Types.OTHER).param("due",claim.publicationId()==null?null:claim.dueAt(),java.sql.Types.TIMESTAMP_WITH_TIMEZONE).update();sql.sql("insert into ouf_ingestion.runtime_configuration_snapshot(snapshot_id,run_id,bundle_id,bundle_version,bundle_checksum,snapshot_json) values(:i,:r,:b,:v,:h,cast(:j as jsonb))").param("i",UUID.randomUUID()).param("r",run).param("b",b.bundleId()).param("v",b.bundleVersion()).param("h",b.checksum()).param("j",encode(b)).update();sql.sql("insert into ouf_ingestion.ing_partition(run_id,partition_key,state) values(:r,'default','PREFLIGHT')").param("r",run).update();return run;}
  @Transactional public void preflightSucceeded(UUID run){transition(run,"PREFLIGHT","RUNNING",null);sql.sql("update ouf_ingestion.ing_partition set state='RUNNING' where run_id=:r and state='PREFLIGHT'").param("r",run).update();}
  @Transactional public void preflightFailed(UUID run,String code){transition(run,"PREFLIGHT","FAILED",safe(code));sql.sql("update ouf_ingestion.ing_partition set state='FAILED' where run_id=:r and state='PREFLIGHT'").param("r",run).update();}
  @Transactional public void advancePhase(UUID run,String target){Map<String,Object> row=sql.sql("select phase,state from ouf_ingestion.ing_run where run_id=:r for update").param("r",run).query().singleRow();String current=String.valueOf(row.get("phase"));if(!"RUNNING".equals(row.get("state"))||!(current.equals("FULL_SNAPSHOT")&&target.equals("CATCH_UP")||current.equals("CATCH_UP")&&target.equals("DELTA")))throw new IllegalStateException("ING_PHASE_TRANSITION_INVALID");sql.sql("update ouf_ingestion.ing_run set phase=:p,updated_at=transaction_timestamp() where run_id=:r").param("p",target).param("r",run).update();}

  @Transactional public void recordSuccess(String source){
    lockSource(source);
    sql.sql("insert into ouf_ingestion.source_health(source_id,status,consecutive_failures,last_error_code) values(:s,'HEALTHY',0,null) on conflict(source_id) do update set status='HEALTHY',consecutive_failures=0,circuit_open_until=null,retry_not_before=null,recovery_probe_owner=null,recovery_probe_until=null,last_error_code=null,retry_window_started_at=null,retry_attempts_in_window=0,updated_at=transaction_timestamp()").param("s",source).update();
  }

  @Transactional public void recordFailure(String source,String code,AdapterSpi.ErrorClass errorClass,Duration retryAfter){
    Objects.requireNonNull(errorClass,"errorClass");lockSource(source);
    if(errorClass!=AdapterSpi.ErrorClass.TRANSIENT_SOURCE){
      sql.sql("insert into ouf_ingestion.source_health(source_id,status,consecutive_failures,last_error_code) values(:s,'DEGRADED',0,:c) on conflict(source_id) do update set status='DEGRADED',consecutive_failures=0,circuit_open_until=null,retry_not_before=null,recovery_probe_owner=null,recovery_probe_until=null,last_error_code=:c,updated_at=transaction_timestamp()").param("s",source).param("c",safe(code)).update();
      return;
    }
    List<Map<String,Object>> budgets=sql.sql("select consecutive_failures,retry_attempts_in_window,retry_window_started_at from ouf_ingestion.source_health where source_id=:s").param("s",source).query().listOfRows();Map<String,Object> budget=budgets.isEmpty()?Map.of():budgets.getFirst();
    int failures=((Number)budget.getOrDefault("consecutive_failures",0)).intValue()+1;
    boolean newWindow=budget.get("retry_window_started_at")==null||sql.sql("select retry_window_started_at+(retry_window_seconds*interval '1 second')<=transaction_timestamp() from ouf_ingestion.source_health where source_id=:s").param("s",source).query(Boolean.class).single();
    int attempts=newWindow?1:((Number)budget.getOrDefault("retry_attempts_in_window",0)).intValue()+1;long base=Math.min(900,5L<<Math.min(attempts-1,7));long jitter=Math.floorMod(Objects.hash(source,safe(code),attempts),Math.max(1,(int)(base/5)+1));long delay=base+jitter;
    if(retryAfter!=null){if(retryAfter.isNegative()||retryAfter.isZero())throw new IllegalArgumentException("ING_RETRY_AFTER_INVALID");delay=Math.max(delay,(retryAfter.toMillis()+999)/1000);}
    boolean open=attempts>=CIRCUIT_THRESHOLD;
    sql.sql("insert into ouf_ingestion.source_health(source_id,status,consecutive_failures,circuit_open_until,retry_not_before,last_error_code,retry_window_started_at,retry_attempts_in_window,first_failure_at,last_failure_at) values(:s,:st,:n,case when :o then transaction_timestamp()+interval '15 minutes' end,transaction_timestamp()+(:d*interval '1 second'),:c,transaction_timestamp(),:a,transaction_timestamp(),transaction_timestamp()) on conflict(source_id) do update set status=:st,consecutive_failures=:n,circuit_open_until=case when :o then transaction_timestamp()+interval '15 minutes' end,retry_not_before=transaction_timestamp()+(:d*interval '1 second'),recovery_probe_owner=null,recovery_probe_until=null,last_error_code=:c,retry_window_started_at=case when :nw then transaction_timestamp() else source_health.retry_window_started_at end,retry_attempts_in_window=:a,first_failure_at=case when :nw then transaction_timestamp() else source_health.first_failure_at end,last_failure_at=transaction_timestamp(),updated_at=transaction_timestamp()")
      .param("s",source).param("st",open?"PAUSED":"DEGRADED").param("n",failures).param("o",open).param("d",delay).param("c",safe(code)).param("a",attempts).param("nw",newWindow).update();
  }

  @Deprecated @Transactional public void recordHealth(String source,boolean success,String code){if(success)recordSuccess(source);else recordFailure(source,code,AdapterSpi.ErrorClass.CONFIGURATION,null);}
  public Map<String,Object> run(UUID run){return sql.sql("select run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,phase,failure_code,tenant_id,correlation_id,created_at,updated_at from ouf_ingestion.ing_run where run_id=:r").param("r",run).query().singleRow();}
  private void lockSource(String source){sql.sql("select pg_advisory_xact_lock(hashtextextended(:s,0))").param("s",source).query().singleRow();}
  String refreshPressure(){return sql.sql("select ouf_ingestion.refresh_pressure_state()").query(String.class).single();}
  private void transition(UUID run,String from,String to,String code){int n=sql.sql("update ouf_ingestion.ing_run set state=:t,failure_code=:c,updated_at=transaction_timestamp() where run_id=:r and state=:f").param("t",to).param("c",code).param("r",run).param("f",from).update();if(n!=1)throw new IllegalStateException("ING_RUN_TRANSITION_INVALID");}
  private String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("ING_BUNDLE_SNAPSHOT_INVALID",e);}}
  private static String phase(String acquisition){return switch(acquisition){case "MANAGED","INTERNAL_MANAGED_CSV","INTERNAL_MANAGED_XLSX","INTERNAL_MANAGED_GEOPACKAGE","INTERNAL_MANAGED_ACCESS","INTERNAL_MANAGED_SHAPEFILE"->"MANAGED_ONCE";case "REPLAY"->"REPLAY";default->"FULL_SNAPSHOT";};}
  private static String mode(String phase){return switch(phase){case "MANAGED_ONCE"->"MANAGED_ONCE";case "REPLAY"->"REPLAY";default->"FULL_SNAPSHOT";};}
  private static String safe(String code){return code!=null&&code.matches("[A-Z0-9_]{1,80}")?code:"ING_INTERNAL_FAILURE";}
  public record ScheduleClaim(UUID scheduleId,String tenantId,String sourceId,int intervalSeconds,String worker,UUID publicationId,String publicationChecksum,OffsetDateTime dueAt,long generation){
    public ScheduleClaim(UUID id,String tenant,String source,int interval,String worker){this(id,tenant,source,interval,worker,null,null,null,-1);}
  }
}
