package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class RunExecutionRepository {
  private final JdbcClient sql;private final ObjectMapper json;
  public RunExecutionRepository(JdbcClient sql,ObjectMapper json){this.sql=sql;this.json=json;}
  @Transactional public Optional<Claim> claim(String worker,Duration lease){String pressure=sql.sql("select ouf_ingestion.refresh_pressure_state()").query(String.class).single();Optional<Claim> claimed=sql.sql("with candidate as (select p.run_id,p.partition_key from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id) where r.state='RUNNING' and p.state='RUNNING' and (p.lease_until is null or p.lease_until<transaction_timestamp()) and (not exists(select 1 from ouf_ingestion.run_recovery_session s where s.run_id=p.run_id) or not exists(select 1 from ouf_ingestion.ing_partition busy where busy.run_id=p.run_id and busy.lease_until>clock_timestamp())) and (:pressure not in ('HARD_PRESSURE','RECOVERY') or r.phase='DELTA') order by case when r.phase='DELTA' then 0 else 1 end,r.created_at,p.partition_key for update of p skip locked limit 1) update ouf_ingestion.ing_partition p set lease_owner=:w,lease_until=transaction_timestamp()+(:ms*interval '1 millisecond'),lease_generation=lease_generation+1 from candidate c where p.run_id=c.run_id and p.partition_key=c.partition_key returning p.run_id,p.partition_key,p.checkpoint_json::text,p.lease_generation")
    .param("pressure",pressure).param("w",worker).param("ms",lease.toMillis()).query((rs,n)->new Claim(rs.getObject(1,UUID.class),rs.getString(2),decodeMap(rs.getString(3)),rs.getLong(4),worker)).optional();
    if(claimed.isPresent()&&!RunRecoveryBudget.admit(sql,claimed.orElseThrow()))return Optional.empty();
    return claimed;
  }
  /** The owned partition serializes records; preserve every earlier failed attempt. */
  public void requireCurrent(Claim claim){RunRecoveryBudget.requireCurrent(sql,claim);}
  public int nextAttemptNumber(Claim claim,String sourceObjectId){
    RunRecoveryBudget.requireCurrent(sql,claim);
    long next=sql.sql("select coalesce((select max(a.attempt_no)::bigint from ouf_ingestion.processing_attempt a where a.run_id=p.run_id and a.source_object_id=:o),0)+1 from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id) where p.run_id=:r and p.partition_key=:p and p.lease_owner=:w and p.lease_generation=:g and p.lease_until>transaction_timestamp() and p.state='RUNNING' and r.state='RUNNING'")
      .param("o",sourceObjectId).param("r",claim.runId()).param("p",claim.partitionKey()).param("w",claim.worker()).param("g",claim.generation()).query(Long.class).optional().orElseThrow(()->new IllegalStateException("ING_PARTITION_LEASE_LOST"));
    if(next<1||next>Integer.MAX_VALUE)throw new IllegalStateException("ING_ATTEMPT_LIMIT_REACHED");
    return (int)next;
  }
  public ExecutionBundle bundle(UUID run){String raw=sql.sql("select snapshot_json::text from ouf_ingestion.runtime_configuration_snapshot where run_id=:r").param("r",run).query(String.class).single();try{return json.readValue(raw,ExecutionBundle.class);}catch(Exception e){throw new IllegalStateException("ING_BUNDLE_SNAPSHOT_INVALID",e);}}
  public RunContext context(UUID run){return sql.sql("select source_id,correlation_id from ouf_ingestion.ing_run where run_id=:r").param("r",run).query((rs,n)->new RunContext(rs.getString(1),rs.getString(2))).single();}
  @Transactional public void release(Claim c){owned(c,"update ouf_ingestion.ing_partition set lease_owner=null,lease_until=null where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()");}
  @Transactional public void nonBlockingQuarantined(Claim c,long sequence,Map<String,Object> checkpoint,Map<String,Object> watermark){RunRecoveryBudget.requireCurrent(sql,c);int n=sql.sql("update ouf_ingestion.ing_partition set checkpoint_json=cast(:checkpoint as jsonb),committed_watermark_json=case when not exists(select 1 from ouf_ingestion.handoff_outbox h where h.run_id=:r and h.partition_key=:p and h.state<>'ACKED') then cast(:watermark as jsonb) else committed_watermark_json end,lock_version=lock_version+1 where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()").param("checkpoint",encode(checkpoint)).param("watermark",encode(watermark)).param("r",c.runId()).param("p",c.partitionKey()).param("w",c.worker()).param("g",c.generation()).update();if(n!=1)throw new IllegalStateException("ING_PARTITION_LEASE_LOST");sql.sql("insert into ouf_ingestion.checkpoint_history(checkpoint_id,run_id,partition_key,sequence_no,checkpoint_json) values(:i,:r,:p,:s,cast(:j as jsonb)) on conflict(run_id,partition_key,sequence_no) do nothing").param("i",UUID.randomUUID()).param("r",c.runId()).param("p",c.partitionKey()).param("s",sequence).param("j",encode(checkpoint)).update();RunRecoveryBudget.requireCurrent(sql,c);}
  @Transactional public void pause(Claim c,String code){if("ING_RECOVERY_BUDGET_EXHAUSTED".equals(code))RunRecoveryBudget.exhausted(sql,c.runId());owned(c,"update ouf_ingestion.ing_partition set state='PAUSED',lease_owner=null,lease_until=null where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()");sql.sql("update ouf_ingestion.ing_run set state='PAUSED',failure_code=:c,updated_at=transaction_timestamp() where run_id=:r and state='RUNNING'").param("c",safe(code)).param("r",c.runId()).update();}
  @Transactional public void inputExhausted(Claim c){owned(c,"update ouf_ingestion.ing_partition set state='DRAINING',lease_owner=null,lease_until=null where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g and lease_until>transaction_timestamp()");sql.sql("update ouf_ingestion.ing_run set state='DRAINING',updated_at=transaction_timestamp() where run_id=:r and state='RUNNING' and not exists(select 1 from ouf_ingestion.ing_partition where run_id=:r and state<>'DRAINING')").param("r",c.runId()).update();}
  @Transactional public int completeDrained(){sql.sql("update ouf_ingestion.ing_partition p set state='SUCCEEDED' where p.state='DRAINING' and exists(select 1 from ouf_ingestion.ing_run r where r.run_id=p.run_id and r.state='DRAINING') and not exists(select 1 from ouf_ingestion.handoff_outbox h where h.run_id=p.run_id and h.partition_key=p.partition_key and h.state<>'ACKED')").update();int completed=sql.sql("update ouf_ingestion.ing_run r set state=case when exists(select 1 from ouf_ingestion.ing_quarantine q where q.run_id=r.run_id and not q.blocking) or exists(select 1 from ouf_ingestion.runtime_issue i where i.run_id=r.run_id and i.severity='WARNING' and i.status='OPEN') then 'COMPLETED_WITH_WARNINGS' else 'SUCCEEDED' end,updated_at=transaction_timestamp() where r.state='DRAINING' and not exists(select 1 from ouf_ingestion.ing_partition p where p.run_id=r.run_id and p.state<>'SUCCEEDED') and not exists(select 1 from ouf_ingestion.ing_quarantine q where q.run_id=r.run_id and q.blocking and q.lifecycle_state in ('OPEN','RETRY_READY','REPROCESSING'))").update();
    sql.sql("update ouf_ingestion.run_recovery_session s set state='COMPLETED' from ouf_ingestion.ing_run r where s.run_id=r.run_id and s.state='ACTIVE' and r.state in ('SUCCEEDED','COMPLETED_WITH_WARNINGS')").update();
    sql.sql("update ouf_ingestion.runtime_issue i set status='RESOLVED',resolution_reason='RECOVERY_COMPLETED',resolved_at=clock_timestamp(),lock_version=lock_version+1 from ouf_ingestion.ing_run r where i.run_id=r.run_id and i.issue_code='ING_RECOVERY_BUDGET_EXHAUSTED' and i.status='OPEN' and r.state in ('SUCCEEDED','COMPLETED_WITH_WARNINGS')").update();
    return completed;
  }
  private void owned(Claim c,String statement){int n=sql.sql(statement).param("r",c.runId()).param("p",c.partitionKey()).param("w",c.worker()).param("g",c.generation()).update();if(n!=1)throw new IllegalStateException("ING_PARTITION_LEASE_LOST");}
  @SuppressWarnings("unchecked")private Map<String,Object> decodeMap(String raw){try{return json.readValue(raw,Map.class);}catch(Exception e){throw new IllegalStateException("ING_CHECKPOINT_INVALID",e);}}
  private String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("ING_CHECKPOINT_INVALID",e);}}
  private static String safe(String code){return code!=null&&code.matches("ING_[A-Z0-9_]{1,76}")?code:"ING_EXECUTION_FAILED";}
  public record Claim(UUID runId,String partitionKey,Map<String,Object> checkpoint,long generation,String worker){}
  public record RunContext(String sourceId,String correlationId){}
}
