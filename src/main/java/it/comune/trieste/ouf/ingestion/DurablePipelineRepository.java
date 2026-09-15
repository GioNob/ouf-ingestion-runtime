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
    String encodedPayload=encode(c.payload);sql.sql("select pg_advisory_xact_lock(hashtextextended(:i,0))").param("i",c.idempotencyKey).query().singleRow();
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
    return sql.sql("with candidate as (select h.handoff_id from ouf_ingestion.handoff_outbox h where ((h.state in ('READY','FAILED_RETRYABLE') and h.next_attempt_at<=transaction_timestamp()) or (h.state='DELIVERING' and h.lease_until<transaction_timestamp())) and not exists(select 1 from ouf_ingestion.handoff_outbox prior where prior.run_id=h.run_id and prior.partition_key=h.partition_key and prior.sequence_no<h.sequence_no and prior.state<>'ACKED') order by h.next_attempt_at,h.created_at for update skip locked limit 1) update ouf_ingestion.handoff_outbox h set state='DELIVERING',lease_owner=:w,lease_until=transaction_timestamp()+(:ms * interval '1 millisecond'),attempts=attempts+1 from candidate c where h.handoff_id=c.handoff_id returning h.handoff_id,h.payload_json::text,h.idempotency_key")
      .param("w",worker).param("ms",lease.toMillis()).query((rs,n)->new ClaimedHandoff(rs.getObject(1,UUID.class),decode(rs.getString(2)),rs.getString(3))).optional();
  }

  /** Duplicate ACK is harmless. Watermark advances only across an ACK-complete prefix. */
  @Transactional
  public void acknowledge(UUID handoffId,String worker,String receipt){
    int transitioned=sql.sql("update ouf_ingestion.handoff_outbox set state='ACKED',downstream_receipt_ref=:rr,acked_at=transaction_timestamp(),lease_owner=null,lease_until=null where handoff_id=:h and state='DELIVERING' and lease_owner=:w")
      .param("rr",receipt).param("h",handoffId).param("w",worker).update();
    if(transitioned==0){
      boolean alreadyAcked=sql.sql("select exists(select 1 from ouf_ingestion.handoff_outbox where handoff_id=:h and state='ACKED')").param("h",handoffId).query(Boolean.class).single();
      if(alreadyAcked)return;
      throw new IllegalStateException("ING_ACK_LEASE_MISMATCH");
    }
    sql.sql("with target as (select run_id,partition_key from ouf_ingestion.handoff_outbox where handoff_id=:h and state='ACKED'), eligible as (select x.* from ouf_ingestion.handoff_outbox x join target t using(run_id,partition_key) where x.state='ACKED' and not exists(select 1 from ouf_ingestion.handoff_outbox prior where prior.run_id=x.run_id and prior.partition_key=x.partition_key and prior.sequence_no<=x.sequence_no and prior.state<>'ACKED') order by x.sequence_no desc limit 1) insert into ouf_ingestion.ing_watermark(source_id,partition_key,watermark_json,committed_handoff_id) select r.source_id,e.partition_key,e.candidate_watermark_json,e.handoff_id from eligible e join ouf_ingestion.ing_run r on r.run_id=e.run_id on conflict(source_id,partition_key) do update set watermark_json=excluded.watermark_json,committed_handoff_id=excluded.committed_handoff_id,committed_at=transaction_timestamp(),lock_version=ouf_ingestion.ing_watermark.lock_version+1")
      .param("h",handoffId).update();
  }

  @Transactional
  public void retry(UUID handoffId,String worker,String safeCode,Duration delay,boolean terminal){
    String state=terminal?"FAILED_TERMINAL":"FAILED_RETRYABLE";
    sql.sql("update ouf_ingestion.handoff_outbox set state=:s,last_error_code=:c,next_attempt_at=transaction_timestamp()+(:ms * interval '1 millisecond'),lease_owner=null,lease_until=null where handoff_id=:h and state='DELIVERING' and lease_owner=:w")
      .param("s",state).param("c",safeCode).param("ms",delay.toMillis()).param("h",handoffId).param("w",worker).update();
  }

  private String encode(Object value){try{return json.writeValueAsString(value);}catch(JsonProcessingException e){throw new IllegalArgumentException("ING_JSON_INVALID",e);}}
  @SuppressWarnings("unchecked") private Map<String,Object> decode(String value){try{return json.readValue(value,Map.class);}catch(JsonProcessingException e){throw new IllegalStateException("ING_STORED_JSON_INVALID",e);}}
  public record ClaimedHandoff(UUID handoffId,Map<String,Object> payload,String idempotencyKey){}
  public record StageCommand(UUID runId,String partitionKey,long sequenceNo,String sourceObjectId,int attemptNo,String adapterId,String bundleVersion,String bundleRef,String idempotencyKey,Map<String,Object> payload,Map<String,Object> evidence,Map<String,Object> restartCheckpoint,Map<String,Object> candidateWatermark){}
}
