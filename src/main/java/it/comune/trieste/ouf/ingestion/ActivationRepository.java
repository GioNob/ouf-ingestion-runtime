package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class ActivationRepository {
 private final JdbcClient db;private final ObjectMapper json;
 public ActivationRepository(JdbcClient db,ObjectMapper json){this.db=db;this.json=json;}
 @Transactional public void reconcile(PublishedActivation a){
  // In-flight claims keep their exact publication; a later poll retries reconciliation.
  db.sql("insert into ouf_ingestion.ing_schedule(schedule_id,tenant_id,source_id,interval_seconds,next_run_at,state,publication_id,publication_sequence,publication_checksum,trigger_once,publication_enabled,sync_profile,discovery_verified_at) values(:id,:tenant,:source,:cadence,transaction_timestamp(),:state,:publication,:sequence,:hash,:once,:enabled,cast(:sync as jsonb),transaction_timestamp()) on conflict(source_id) do update set interval_seconds=:cadence,publication_id=:publication,publication_sequence=:sequence,publication_checksum=:hash,trigger_once=:once,publication_enabled=:enabled,sync_profile=cast(:sync as jsonb),discovery_verified_at=transaction_timestamp(),state=case when not :enabled or (:once and ing_schedule.consumed_publication_id=:publication) then 'DISABLED' when ing_schedule.state='PAUSED' then 'PAUSED' else 'ACTIVE' end,next_run_at=case when ing_schedule.publication_id is distinct from :publication then transaction_timestamp() else ing_schedule.next_run_at end,lock_version=ing_schedule.lock_version+1 where ing_schedule.tenant_id=:tenant and (ing_schedule.lease_until is null or ing_schedule.lease_until<=transaction_timestamp()) and (ing_schedule.publication_sequence is null or ing_schedule.publication_sequence<:sequence or (ing_schedule.publication_sequence=:sequence and ing_schedule.publication_id=:publication and ing_schedule.publication_checksum=:hash))")
   .param("id",UUID.randomUUID()).param("tenant",a.tenantId()).param("source",a.sourceId()).param("cadence",a.intervalSeconds()).param("state",a.enabled()?"ACTIVE":"DISABLED").param("publication",a.publicationId()).param("sequence",a.sequence()).param("hash",a.execution().checksum()).param("once",a.once()).param("enabled",a.enabled()).param("sync",encode(a.execution().configuration().get("syncProfile"))).update();
 }
 private String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("ING_SCHEDULE_POLICY_INVALID");}}
}
