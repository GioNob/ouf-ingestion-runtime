package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DurablePipelineRepository {
  private final JdbcClient sql; private final ObjectMapper json;
  public DurablePipelineRepository(JdbcClient sql,ObjectMapper json){this.sql=sql;this.json=json;}

  /** Persists attempt, lineage, handoff and restart checkpoint in one short transaction. */
  @Transactional
  public UUID stage(StageCommand c){
    return stage(c,UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID());
  }

  @Transactional
  public UUID stage(StageCommand c,UUID attempt,UUID lineage,UUID handoff){
    requireRecoveryBudget(c.runId(),c.claim());if(c.claim()!=null) requireLease(c);
    String encodedPayload=encode(c.payload);sql.sql("select pg_advisory_xact_lock(hashtextextended(:i,0))").param("i",c.idempotencyKey).query().singleRow();
    if(c.claim()!=null) requireLease(c); // Recheck after any idempotency-lock wait.
    List<Map<String,Object>> existing=sql.sql("select handoff_id,payload_json=cast(:p as jsonb) payload_matches from ouf_ingestion.handoff_outbox where idempotency_key=:i").param("p",encodedPayload).param("i",c.idempotencyKey).query().listOfRows();
    if(!existing.isEmpty()){Map<String,Object> row=existing.getFirst();if(!Boolean.TRUE.equals(row.get("payload_matches")))throw new IllegalStateException("ING_IDEMPOTENCY_CONFLICT");return (UUID)row.get("handoff_id");}
    UUID checkpoint=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.processing_attempt(attempt_id,run_id,source_object_id,attempt_no,adapter_id,bundle_version,state) values(:a,:r,:o,:n,:ad,:bv,'SUCCEEDED')")
      .param("a",attempt).param("r",c.runId).param("o",c.sourceObjectId).param("n",c.attemptNo).param("ad",c.adapterId).param("bv",c.bundleVersion).update();
    sql.sql("insert into ouf_ingestion.ing_lineage(lineage_id,run_id,attempt_id,source_object_id,bundle_ref,adapter_ref,evidence_json) values(:l,:r,:a,:o,:b,:ad,cast(:e as jsonb))")
      .param("l",lineage).param("r",c.runId).param("a",attempt).param("o",c.sourceObjectId).param("b",c.bundleRef).param("ad",c.adapterId).param("e",encode(c.evidence)).update();
    sql.sql("insert into ouf_ingestion.handoff_outbox(handoff_id,run_id,lineage_id,idempotency_key,payload_json,partition_key,sequence_no,candidate_watermark_json) values(:h,:r,:l,:i,cast(:p as jsonb),:pk,:s,cast(:w as jsonb))")
      .param("h",handoff).param("r",c.runId).param("l",lineage).param("i",c.idempotencyKey).param("p",encodedPayload).param("pk",c.partitionKey).param("s",c.sequenceNo).param("w",encode(c.candidateWatermark)).update();
    sql.sql("insert into ouf_ingestion.checkpoint_history(checkpoint_id,run_id,partition_key,sequence_no,checkpoint_json) values(:c,:r,:pk,:s,cast(:v as jsonb))")
      .param("c",checkpoint).param("r",c.runId).param("pk",c.partitionKey).param("s",c.sequenceNo).param("v",encode(c.restartCheckpoint)).update();
    sql.sql("update ouf_ingestion.ing_partition set checkpoint_json=cast(:v as jsonb),lock_version=lock_version+1 where run_id=:r and partition_key=:pk")
      .param("v",encode(c.restartCheckpoint)).param("r",c.runId).param("pk",c.partitionKey).update();
    if(c.claim()!=null) requireLease(c); // Deadline/fence still valid before commit.
    return handoff;
  }

  @Transactional
  public UUID recordFailure(UUID runId,String sourceObjectId,int attemptNo,String adapterId,String bundleVersion,String reasonCode){
    UUID attempt=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.processing_attempt(attempt_id,run_id,source_object_id,attempt_no,adapter_id,bundle_version,state,reason_code) values(:a,:r,:o,:n,:ad,:bv,'FAILED',:c)")
      .param("a",attempt).param("r",runId).param("o",sourceObjectId).param("n",attemptNo).param("ad",adapterId).param("bv",bundleVersion).param("c",reasonCode).update();
    return attempt;
  }

  /** Claim transaction ends before the remote call. SKIP LOCKED supports competing workers. */
  @Transactional
  public Optional<ClaimedHandoff> claim(String worker,Duration lease){
    var rows=sql.sql("""
      select h.handoff_id,h.run_id,h.delivery_session_id,
             h.delivery_session_id is not null and r.publication_id is not null and not exists(
               select 1 from ouf_ingestion.ing_schedule s where s.schedule_id=r.schedule_id
                 and s.publication_id=r.publication_id and s.publication_enabled) as superseded,
             b.remaining<=0 or not b.authorized or b.deadline<=clock_timestamp() as expired
      from ouf_ingestion.handoff_outbox h join ouf_ingestion.ing_run r using(run_id)
      join ouf_ingestion.handoff_budget b using(handoff_id)
      where r.state in ('RUNNING','DRAINING')
        and ((h.state in ('READY','FAILED_RETRYABLE') and (h.next_attempt_at<=clock_timestamp() or b.deadline<=clock_timestamp()))
          or (h.state='DELIVERING' and h.lease_until<=clock_timestamp()))
        and not exists(select 1 from ouf_ingestion.handoff_outbox prior
          where prior.run_id=h.run_id and prior.partition_key=h.partition_key
          and prior.sequence_no<h.sequence_no and prior.state<>'ACKED')
      order by h.next_attempt_at,h.created_at,h.handoff_id limit 1 for update of r,h skip locked
      """).query().listOfRows();
    if(rows.isEmpty())return Optional.empty();
    var row=rows.getFirst(); UUID id=(UUID)row.get("handoff_id");
    UUID session=(UUID)row.get("delivery_session_id");
    if(Boolean.TRUE.equals(row.get("superseded"))){
      sql.sql("update ouf_ingestion.handoff_outbox set state='FAILED_TERMINAL',delivery_stop_reason='ING_HANDOFF_DISPOSITION_REQUIRED',last_error_code='ING_ACTIVE_PUBLICATION_CHANGED',lease_owner=null,lease_until=null where handoff_id=:h").param("h",id).update();
      block((UUID)row.get("run_id"));return Optional.empty();
    }
    boolean expired=Boolean.TRUE.equals(row.get("expired"));
    if(!expired && session!=null){
      expired=sql.sql("update ouf_ingestion.run_recovery_session set attempts=attempts+1 where session_id=:s and state='ACTIVE' and attempts<max_attempts and deadline_at>clock_timestamp()")
        .param("s",session).update()!=1;
    }
    if(expired){
      sql.sql("update ouf_ingestion.handoff_outbox set state='FAILED_TERMINAL',delivery_stop_reason='ING_HANDOFF_BUDGET_EXHAUSTED',last_error_code=coalesce(last_error_code,'ING_HANDOFF_BUDGET_EXHAUSTED'),lease_owner=null,lease_until=null where handoff_id=:h").param("h",id).update();
      block((UUID)row.get("run_id"));return Optional.empty();
    }
    return sql.sql("""
      update ouf_ingestion.handoff_outbox set state='DELIVERING',lease_owner=:w,
        delivery_started_at=coalesce(delivery_started_at,clock_timestamp()),
        lease_until=least(clock_timestamp()+(:ms * interval '1 millisecond'),
                          coalesce((select deadline from ouf_ingestion.handoff_budget where handoff_id=:h),clock_timestamp()+interval '24 hours')),
        attempts=attempts+1 where handoff_id=:h returning handoff_id,payload_json::text,idempotency_key,attempts
      """).param("h",id).param("w",worker).param("ms",lease.toMillis())
      .query((rs,n)->new ClaimedHandoff(rs.getObject(1,UUID.class),decode(rs.getString(2)),rs.getString(3),rs.getInt(4))).optional();
  }

  private void block(UUID run){
    sql.sql("""
      update ouf_ingestion.run_recovery_session g set state='EXHAUSTED' from ouf_ingestion.handoff_outbox h
      where h.run_id=:r and g.session_id=h.delivery_session_id and g.state='ACTIVE'
        and h.state='FAILED_TERMINAL' and h.delivery_stop_reason='ING_HANDOFF_BUDGET_EXHAUSTED'
      """).param("r",run).update();
    sql.sql("select ouf_ingestion.block_handoff(:r)").param("r",run).query().singleRow();
  }

  /** Serialize a late ACK or failure against the HUMAN disposition of its run. */
  private boolean deliveryAllowed(UUID handoff){
    return sql.sql("select r.state in ('RUNNING','DRAINING') from ouf_ingestion.ing_run r join ouf_ingestion.handoff_outbox h using(run_id) where h.handoff_id=:h for update of r")
      .param("h",handoff).query(Boolean.class).optional().orElse(false);
  }

  /** Duplicate ACK is harmless. Watermark advances only across an ACK-complete prefix. */
  @Transactional
  public void acknowledge(UUID handoffId,String worker,String receipt){
    int transitioned=deliveryAllowed(handoffId)?sql.sql("update ouf_ingestion.handoff_outbox set state='ACKED',downstream_receipt_ref=:rr,acked_at=transaction_timestamp(),lease_owner=null,lease_until=null where handoff_id=:h and state='DELIVERING' and lease_owner=:w and lease_until>clock_timestamp()")
      .param("rr",receipt).param("h",handoffId).param("w",worker).update():0;
    if(transitioned==0){
      boolean alreadyAcked=sql.sql("select exists(select 1 from ouf_ingestion.handoff_outbox where handoff_id=:h and state='ACKED')").param("h",handoffId).query(Boolean.class).single();
      if(alreadyAcked)return;
      throw new IllegalStateException("ING_ACK_LEASE_MISMATCH");
    }
    resolveGovernedRetries(handoffId,worker);
    sql.sql("with target as (select run_id,partition_key from ouf_ingestion.handoff_outbox where handoff_id=:h and state='ACKED'), eligible as (select x.* from ouf_ingestion.handoff_outbox x join target t using(run_id,partition_key) where x.state='ACKED' and not exists(select 1 from ouf_ingestion.handoff_outbox prior where prior.run_id=x.run_id and prior.partition_key=x.partition_key and prior.sequence_no<=x.sequence_no and prior.state<>'ACKED') order by x.sequence_no desc limit 1) insert into ouf_ingestion.ing_watermark(source_id,partition_key,watermark_json,committed_handoff_id) select r.source_id,e.partition_key,e.candidate_watermark_json,e.handoff_id from eligible e join ouf_ingestion.ing_run r on r.run_id=e.run_id on conflict(source_id,partition_key) do update set watermark_json=excluded.watermark_json,committed_handoff_id=excluded.committed_handoff_id,committed_at=transaction_timestamp(),lock_version=ouf_ingestion.ing_watermark.lock_version+1")
      .param("h",handoffId).update();
  }

  /** Resolve only HUMAN-authorized, earlier failures of this exact record after durable ACK. */
  private void resolveGovernedRetries(UUID handoffId,String worker){
    List<Map<String,Object>> resolved=sql.sql("update ouf_ingestion.ing_quarantine q set state='RELEASED',lifecycle_state='RESOLVED',lifecycle_version=q.lifecycle_version+1,resolved_at=transaction_timestamp(),resolution_reason='DURABLE_DOWNSTREAM_ACK',resolution_actor=:w from ouf_ingestion.handoff_outbox h join ouf_ingestion.ing_lineage l using(lineage_id) join ouf_ingestion.processing_attempt a on a.attempt_id=l.attempt_id join ouf_ingestion.ing_run r on r.run_id=h.run_id,ouf_ingestion.processing_attempt failed where h.handoff_id=:h and h.state='ACKED' and a.state='SUCCEEDED' and q.run_id=h.run_id and q.source_object_id=l.source_object_id and q.attempt_id=failed.attempt_id and failed.run_id=h.run_id and failed.source_object_id=l.source_object_id and failed.state='FAILED' and failed.attempt_no<a.attempt_no and q.lifecycle_state='RETRY_READY' and q.authorization_decision_ref is not null returning q.quarantine_id,q.correlation_id,q.authorization_decision_ref,r.tenant_id")
      .param("w",worker).param("h",handoffId).query().listOfRows();
    for(Map<String,Object> q:resolved){
      UUID quarantine=(UUID)q.get("quarantine_id");
      sql.sql("update ouf_ingestion.runtime_issue set status='RESOLVED',resolution_reason='DURABLE_DOWNSTREAM_ACK',resolution_actor=:w,resolved_at=transaction_timestamp(),lock_version=lock_version+1 where quarantine_id=:q and status='OPEN'").param("w",worker).param("q",quarantine).update();
      sql.sql("insert into ouf_ingestion.audit_event(event_id,event_type,actor_subject,actor_type,resource_type,resource_id,correlation_id,detail_json,tenant_id,authorization_decision_ref) values(:i,'QUARANTINE_RETRY_RESOLVED',:w,'SERVICE','QUARANTINE',:q,:c,jsonb_build_object('handoffId',cast(:h as text),'resolution','DURABLE_DOWNSTREAM_ACK'),:t,:d)")
        .param("i",UUID.randomUUID()).param("w",worker).param("q",quarantine.toString()).param("c",q.get("correlation_id")).param("h",handoffId.toString()).param("t",q.get("tenant_id")).param("d",q.get("authorization_decision_ref")).update();
    }
  }

  @Transactional
  public void retry(UUID handoffId,String worker,String safeCode,Duration delay,boolean terminal){
    if(!deliveryAllowed(handoffId))return;
    var rows=sql.sql("""
      update ouf_ingestion.handoff_outbox set
        state=case when :terminal or b.remaining<=0 or not b.authorized or clock_timestamp()+(:ms * interval '1 millisecond')>=b.deadline
                 then 'FAILED_TERMINAL' else 'FAILED_RETRYABLE' end,
        delivery_stop_reason=case when :terminal then 'ING_HANDOFF_DISPOSITION_REQUIRED'
            when b.remaining<=0 or not b.authorized or clock_timestamp()+(:ms * interval '1 millisecond')>=b.deadline
            then 'ING_HANDOFF_BUDGET_EXHAUSTED' end,
        last_error_code=:c,next_attempt_at=case when :ms>3155760000000 then 'infinity'::timestamptz else clock_timestamp()+(:ms * interval '1 millisecond') end,
        lease_owner=null,lease_until=null
      from ouf_ingestion.handoff_budget b where b.handoff_id=:h and handoff_outbox.handoff_id=b.handoff_id
        and state='DELIVERING' and lease_owner=:w and lease_until>clock_timestamp()
      returning run_id,state
      """).param("terminal",terminal).param("c",safeCode).param("ms",delay.getSeconds()>3155760000L?3155760000001L:delay.toMillis())
      .param("h",handoffId).param("w",worker).query().listOfRows();
    if(!rows.isEmpty() && "FAILED_TERMINAL".equals(rows.getFirst().get("state")))block((UUID)rows.getFirst().get("run_id"));
  }

  @Transactional public java.time.Instant acquiredAt(RunExecutionRepository.Claim c,long sequence,java.time.Instant now){
    RunRecoveryBudget.requireCurrent(sql,c);
    return sql.sql("""
      update ouf_ingestion.ing_partition set record_acquired_at=case when lake_sequence=:seq
        then record_acquired_at else :at end,lake_sequence=:seq
      where run_id=:r and partition_key=:p and lease_owner=:w and lease_generation=:g
        and lease_until>clock_timestamp() returning record_acquired_at
      """).param("seq",sequence).param("at",now.atOffset(java.time.ZoneOffset.UTC)).param("r",c.runId())
      .param("p",c.partitionKey()).param("w",c.worker()).param("g",c.generation())
      .query(java.time.OffsetDateTime.class).optional().orElseThrow(()->new IllegalStateException("ING_PARTITION_LEASE_LOST")).toInstant();
  }

  /** Short transaction before I/O. Only a confirmed matching receipt can skip a write. */
  @Transactional public Optional<String> beginLakeWrite(CanonicalRecordPipeline.Command c,String effect,String hash){
    lockPartition(c.claim());
    int inserted=sql.sql("""
      insert into ouf_ingestion.lake_write_evidence(run_id,partition_key,sequence_no,effect,content_hash)
      values(:r,:p,:seq,:effect,:hash) on conflict do nothing
      """).param("r",c.runId()).param("p",c.partitionKey()).param("seq",c.sequenceNo())
      .param("effect",effect).param("hash",hash).update();
    if(inserted==1)return Optional.empty();
    var row=sql.sql("select content_hash,receipt_ref from ouf_ingestion.lake_write_evidence where run_id=:r and partition_key=:p and sequence_no=:seq and effect=:effect")
      .param("r",c.runId()).param("p",c.partitionKey()).param("seq",c.sequenceNo()).param("effect",effect).query().singleRow();
    if(!hash.equals(row.get("content_hash")) || row.get("receipt_ref")==null)throw new IllegalStateException("ING_LAKE_RECONCILE_REQUIRED");
    return Optional.of((String)row.get("receipt_ref"));
  }

  @Transactional public void finishLakeWrite(CanonicalRecordPipeline.Command c,String effect,String hash,String receipt){
    lockPartition(c.claim());
    String statement=receipt==null?"delete from ouf_ingestion.lake_write_evidence"
      :"update ouf_ingestion.lake_write_evidence set receipt_ref=:receipt";
    var query=sql.sql(statement+" where run_id=:r and partition_key=:p and sequence_no=:seq and effect=:effect and content_hash=:hash and receipt_ref is null")
      .param("r",c.runId()).param("p",c.partitionKey()).param("seq",c.sequenceNo()).param("effect",effect).param("hash",hash);
    if(receipt!=null)query=query.param("receipt",receipt);
    if(query.update()!=1)throw new IllegalStateException("ING_LAKE_RECONCILE_REQUIRED");
  }

  private void lockPartition(RunExecutionRepository.Claim c){
    sql.sql("select 1 from ouf_ingestion.ing_partition where run_id=:r and partition_key=:p for update")
      .param("r",c.runId()).param("p",c.partitionKey()).query(Integer.class).single();
    RunRecoveryBudget.requireCurrent(sql,c);
  }

  public void requireRecoveryBudget(RunExecutionRepository.Claim claim){RunRecoveryBudget.requireCurrent(sql,claim);}
  public void requireRecoveryBudget(UUID run,RunExecutionRepository.Claim claim){
    if(claim==null){boolean recovering=sql.sql("select exists(select 1 from ouf_ingestion.run_recovery_session where run_id=:r)").param("r",run).query(Boolean.class).single();if(recovering)throw new IllegalStateException("ING_RECOVERY_CLAIM_REQUIRED");}
    else{if(!run.equals(claim.runId()))throw new IllegalStateException("ING_PARTITION_LEASE_LOST");RunRecoveryBudget.requireCurrent(sql,claim);}
  }

  private void requireLease(StageCommand c){
    var claim=c.claim();
    if(!claim.runId().equals(c.runId())||!claim.partitionKey().equals(c.partitionKey()))throw new IllegalStateException("ING_PARTITION_LEASE_LOST");
    var valid=sql.sql("select 1 from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id) where p.run_id=:r and p.partition_key=:p and p.lease_owner=:w and p.lease_generation=:g and p.lease_until>transaction_timestamp() and p.state='RUNNING' and r.state='RUNNING' for update of p")
      .param("r",c.runId()).param("p",c.partitionKey()).param("w",claim.worker()).param("g",claim.generation()).query(Integer.class).optional();
    if(valid.isEmpty())throw new IllegalStateException("ING_PARTITION_LEASE_LOST");
    RunRecoveryBudget.requireCurrent(sql,claim);
  }

  private String encode(Object value){try{return json.writeValueAsString(value);}catch(JsonProcessingException e){throw new IllegalArgumentException("ING_JSON_INVALID",e);}}
  @SuppressWarnings("unchecked") private Map<String,Object> decode(String value){try{return json.readValue(value,Map.class);}catch(JsonProcessingException e){throw new IllegalStateException("ING_STORED_JSON_INVALID",e);}}
  public record ClaimedHandoff(UUID handoffId,Map<String,Object> payload,String idempotencyKey,int attempts){}
  public record StageCommand(UUID runId,String partitionKey,long sequenceNo,String sourceObjectId,int attemptNo,String adapterId,String bundleVersion,String bundleRef,String idempotencyKey,Map<String,Object> payload,Map<String,Object> evidence,Map<String,Object> restartCheckpoint,Map<String,Object> candidateWatermark,RunExecutionRepository.Claim claim){
    public StageCommand(UUID runId,String partitionKey,long sequenceNo,String sourceObjectId,int attemptNo,String adapterId,String bundleVersion,String bundleRef,String idempotencyKey,Map<String,Object> payload,Map<String,Object> evidence,Map<String,Object> restartCheckpoint,Map<String,Object> candidateWatermark){this(runId,partitionKey,sequenceNo,sourceObjectId,attemptNo,adapterId,bundleVersion,bundleRef,idempotencyKey,payload,evidence,restartCheckpoint,candidateWatermark,null);}
  }
}
