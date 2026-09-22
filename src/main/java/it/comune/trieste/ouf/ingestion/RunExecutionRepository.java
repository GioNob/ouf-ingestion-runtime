package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class RunExecutionRepository {
  private final JdbcClient sql;private final ObjectMapper json;
  public RunExecutionRepository(JdbcClient sql,ObjectMapper json){this.sql=sql;this.json=json;}
  @Transactional public Optional<Claim> claim(String worker,Duration lease){resumeDueRetries();String pressure=sql.sql("select ouf_ingestion.refresh_pressure_state()").query(String.class).single();return sql.sql("with candidate as (select p.run_id,p.partition_key from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id) where r.state='RUNNING' and p.state='RUNNING' and (p.lease_until is null or p.lease_until<transaction_timestamp()) and (:pressure not in ('HARD_PRESSURE','RECOVERY') or r.phase='DELTA') order by case when r.phase='DELTA' then 0 else 1 end,r.created_at,p.partition_key for update of p skip locked limit 1) update ouf_ingestion.ing_partition p set lease_owner=:w,lease_until=transaction_timestamp()+(:ms*interval '1 millisecond'),lease_generation=lease_generation+1 from candidate c where p.run_id=c.run_id and p.partition_key=c.partition_key returning p.run_id,p.partition_key,p.checkpoint_json::text,p.lease_generation")
    .param("pressure",pressure).param("w",worker).param("ms",lease.toMillis()).query((rs,n)->new Claim(rs.getObject(1,UUID.class),rs.getString(2),decodeMap(rs.getString(3)),rs.getLong(4),worker)).optional();}
  /** Expired budgets become terminal even after a long process outage. Never resume a paused/aborted run. */
  private void resumeDueRetries(){
    sql.sql("update ouf_ingestion.ing_partition p set state='FAILED',retry_not_before=null where p.state='RETRY_WAIT' and p.retry_deadline<=transaction_timestamp() and exists(select 1 from ouf_ingestion.ing_run r where r.run_id=p.run_id and r.state='RETRY_WAIT')").update();
    sql.sql("update ouf_ingestion.ing_run r set state='FAILED',failure_code=coalesce(failure_code,'ING_RETRY_BUDGET_EXHAUSTED'),updated_at=transaction_timestamp() where r.state='RETRY_WAIT' and exists(select 1 from ouf_ingestion.ing_partition p where p.run_id=r.run_id and p.state='FAILED')").update();
    sql.sql("update ouf_ingestion.ing_partition p set state='RUNNING',retry_not_before=null where p.state='RETRY_WAIT' and p.retry_not_before<=transaction_timestamp() and p.retry_deadline>transaction_timestamp() and exists(select 1 from ouf_ingestion.ing_run r where r.run_id=p.run_id and r.state='RETRY_WAIT')").update();
    sql.sql("update ouf_ingestion.ing_run r set state='RUNNING',updated_at=transaction_timestamp() where r.state='RETRY_WAIT' and exists(select 1 from ouf_ingestion.ing_partition p where p.run_id=r.run_id and p.state='RUNNING') and not exists(select 1 from ouf_ingestion.ing_partition p where p.run_id=r.run_id and p.state='RETRY_WAIT')").update();
  }
  @Transactional public boolean failed(Claim c,RunCoordinator.Failure failure){
    // Partition fencing precedes all incident and retry writes; stale workers cannot create evidence.
    var row=sql.sql("select retry_count,coalesce(retry_first_failure_at,transaction_timestamp()) first_failure,transaction_timestamp() now from ouf_ingestion.ing_partition where run_id=:r and partition_key=:p and state='RUNNING' and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp() for update")
      .param("r",c.runId()).param("p",c.partitionKey()).param("w",c.worker()).param("g",c.generation()).query().listOfRows().stream().findFirst().orElseThrow(()->new IllegalStateException("ING_PARTITION_LEASE_LOST"));
    ExecutionRetryPolicy policy=ExecutionRetryPolicy.from(bundle(c.runId()));
    int count=((Number)row.get("retry_count")).intValue()+1;
    OffsetDateTime first=(OffsetDateTime)row.get("first_failure"),now=(OffsetDateTime)row.get("now");
    long delay=policy.delay(failure.retryAfter());
    var deadline=first.plusSeconds(policy.elapsedSeconds());
    boolean retry=failure.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE && count<=policy.attempts() && delay>0 && now.plusSeconds(delay).isBefore(deadline);
    String target=retry?"RETRY_WAIT":failure.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE?"FAILED":"PAUSED";
    sql.sql("update ouf_ingestion.ing_partition set state=:state,retry_count=:count,retry_first_failure_at=:first,retry_not_before=:next,retry_deadline=:deadline,lease_owner=null,lease_until=null where run_id=:r and partition_key=:p")
      .param("state",target).param("count",count).param("first",first).param("next",retry?now.plusSeconds(delay):null,java.sql.Types.TIMESTAMP_WITH_TIMEZONE).param("deadline",deadline).param("r",c.runId()).param("p",c.partitionKey()).update();
    sql.sql("update ouf_ingestion.ing_run set state=:state,retry_stage='EXECUTION',failure_code=:code,updated_at=transaction_timestamp() where run_id=:r and state in ('RUNNING','RETRY_WAIT')")
      .param("state",target).param("code",safe(failure.code())).param("r",c.runId()).update();
    return retry;
  }
  public ExecutionBundle bundle(UUID run){String raw=sql.sql("select snapshot_json::text from ouf_ingestion.runtime_configuration_snapshot where run_id=:r").param("r",run).query(String.class).single();try{return json.readValue(raw,ExecutionBundle.class);}catch(Exception e){throw new IllegalStateException("ING_BUNDLE_SNAPSHOT_INVALID",e);}}
  public RunContext context(UUID run){return sql.sql("select source_id,correlation_id from ouf_ingestion.ing_run where run_id=:r").param("r",run).query((rs,n)->new RunContext(rs.getString(1),rs.getString(2))).single();}
  @Transactional public void release(Claim c){owned(c,"update ouf_ingestion.ing_partition set lease_owner=null,lease_until=null where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()");}
  @Transactional public void nonBlockingQuarantined(Claim c,long sequence,Map<String,Object> checkpoint,Map<String,Object> watermark){int n=sql.sql("update ouf_ingestion.ing_partition set checkpoint_json=cast(:checkpoint as jsonb),committed_watermark_json=case when not exists(select 1 from ouf_ingestion.handoff_outbox h where h.run_id=:r and h.partition_key=:p and h.state<>'ACKED') then cast(:watermark as jsonb) else committed_watermark_json end,lock_version=lock_version+1 where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()").param("checkpoint",encode(checkpoint)).param("watermark",encode(watermark)).param("r",c.runId()).param("p",c.partitionKey()).param("w",c.worker()).param("g",c.generation()).update();if(n!=1)throw new IllegalStateException("ING_PARTITION_LEASE_LOST");sql.sql("insert into ouf_ingestion.checkpoint_history(checkpoint_id,run_id,partition_key,sequence_no,checkpoint_json) values(:i,:r,:p,:s,cast(:j as jsonb)) on conflict(run_id,partition_key,sequence_no) do nothing").param("i",UUID.randomUUID()).param("r",c.runId()).param("p",c.partitionKey()).param("s",sequence).param("j",encode(checkpoint)).update();}
  @Transactional public void pause(Claim c,String code){owned(c,"update ouf_ingestion.ing_partition set state='PAUSED',lease_owner=null,lease_until=null where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()");sql.sql("update ouf_ingestion.ing_run set state='PAUSED',failure_code=:c,updated_at=transaction_timestamp() where run_id=:r and state='RUNNING'").param("c",safe(code)).param("r",c.runId()).update();}
  @Transactional public void inputExhausted(Claim c){owned(c,"update ouf_ingestion.ing_partition set state='DRAINING',lease_owner=null,lease_until=null where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()");sql.sql("update ouf_ingestion.ing_run set state='DRAINING',updated_at=transaction_timestamp() where run_id=:r and state='RUNNING' and not exists(select 1 from ouf_ingestion.ing_partition where run_id=:r and state<>'DRAINING')").param("r",c.runId()).update();}
  @Transactional public int completeDrained(){sql.sql("update ouf_ingestion.ing_partition p set state='SUCCEEDED' where p.state='DRAINING' and exists(select 1 from ouf_ingestion.ing_run r where r.run_id=p.run_id and r.state='DRAINING') and not exists(select 1 from ouf_ingestion.handoff_outbox h where h.run_id=p.run_id and h.partition_key=p.partition_key and h.state<>'ACKED')").update();return sql.sql("update ouf_ingestion.ing_run r set state=case when exists(select 1 from ouf_ingestion.ing_quarantine q where q.run_id=r.run_id and not q.blocking) or exists(select 1 from ouf_ingestion.runtime_issue i where i.run_id=r.run_id and i.severity='WARNING' and i.status='OPEN') then 'COMPLETED_WITH_WARNINGS' else 'SUCCEEDED' end,updated_at=transaction_timestamp() where r.state='DRAINING' and not exists(select 1 from ouf_ingestion.ing_partition p where p.run_id=r.run_id and p.state<>'SUCCEEDED') and not exists(select 1 from ouf_ingestion.ing_quarantine q where q.run_id=r.run_id and q.blocking and q.lifecycle_state in ('OPEN','RETRY_READY','REPROCESSING'))").update();}
  private void owned(Claim c,String statement){int n=sql.sql(statement).param("r",c.runId()).param("p",c.partitionKey()).param("w",c.worker()).param("g",c.generation()).update();if(n!=1)throw new IllegalStateException("ING_PARTITION_LEASE_LOST");}
  @SuppressWarnings("unchecked")private Map<String,Object> decodeMap(String raw){try{return json.readValue(raw,Map.class);}catch(Exception e){throw new IllegalStateException("ING_CHECKPOINT_INVALID",e);}}
  private String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("ING_CHECKPOINT_INVALID",e);}}
  private static String safe(String code){return code!=null&&code.matches("ING_[A-Z0-9_]{1,76}")?code:"ING_EXECUTION_FAILED";}
  public record Claim(UUID runId,String partitionKey,Map<String,Object> checkpoint,long generation,String worker){}
  public record RunContext(String sourceId,String correlationId){}
}
