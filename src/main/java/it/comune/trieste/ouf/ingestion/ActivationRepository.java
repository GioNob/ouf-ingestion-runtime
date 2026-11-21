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
  db.sql("insert into ouf_ingestion.ing_schedule(schedule_id,tenant_id,source_id,interval_seconds,next_run_at,state,publication_id,publication_sequence,publication_checksum,trigger_once,publication_enabled,sync_profile,discovered_bundle,discovery_verified_at) values(:id,:tenant,:source,:cadence,clock_timestamp(),:state,:publication,:sequence,:hash,:once,:enabled,cast(:sync as jsonb),cast(:bundle as jsonb),transaction_timestamp()) on conflict(source_id) do update set interval_seconds=:cadence,publication_id=:publication,publication_sequence=:sequence,publication_checksum=:hash,trigger_once=:once,publication_enabled=:enabled,sync_profile=cast(:sync as jsonb),discovered_bundle=cast(:bundle as jsonb),discovery_verified_at=transaction_timestamp(),state=case when not :enabled or (:once and (ing_schedule.consumed_publication_id=:publication or exists(select 1 from ouf_ingestion.ing_run disposed where disposed.schedule_id=ing_schedule.schedule_id and disposed.publication_id=:publication and disposed.admission_pending and disposed.state='ABORTED'))) then 'DISABLED' when :once and exists(select 1 from ouf_ingestion.ing_run held where held.schedule_id=ing_schedule.schedule_id and held.publication_id=:publication and held.admission_pending and held.state='PAUSED') then 'PAUSED' when ing_schedule.state='PAUSED' and (not ing_schedule.activation_blocked or ing_schedule.publication_id=:publication) then 'PAUSED' else 'ACTIVE' end,next_run_at=case when ing_schedule.publication_id is distinct from :publication then clock_timestamp() else ing_schedule.next_run_at end,activation_failures=case when ing_schedule.publication_id is distinct from :publication then 0 else ing_schedule.activation_failures end,activation_first_failure_at=case when ing_schedule.publication_id is distinct from :publication then null else ing_schedule.activation_first_failure_at end,activation_blocked=case when ing_schedule.publication_id is distinct from :publication then false else ing_schedule.activation_blocked end,lock_version=ing_schedule.lock_version+1 where ing_schedule.tenant_id=:tenant and (ing_schedule.lease_until is null or ing_schedule.lease_until<=transaction_timestamp()) and (ing_schedule.publication_sequence is null or ing_schedule.publication_sequence<:sequence or (ing_schedule.publication_sequence=:sequence and ing_schedule.publication_id=:publication and ing_schedule.publication_checksum=:hash))")
   .param("id",UUID.randomUUID()).param("tenant",a.tenantId()).param("source",a.sourceId()).param("cadence",a.intervalSeconds()).param("state",a.enabled()?"ACTIVE":"DISABLED").param("publication",a.publicationId()).param("sequence",a.sequence()).param("hash",a.execution().checksum()).param("once",a.once()).param("enabled",a.enabled()).param("bundle",encode(a.execution())).param("sync",encode(a.execution().configuration().get("syncProfile"))).update();
  db.sql("update ouf_ingestion.ing_run r set state=case when r.admission_pending then 'PAUSED' else 'FAILED' end,failure_code='ING_ACTIVATION_SUPERSEDED',updated_at=transaction_timestamp() from ouf_ingestion.ing_schedule s where r.schedule_id=s.schedule_id and s.source_id=:source and r.state='PREFLIGHT' and (s.lease_until is null or s.lease_until<=transaction_timestamp()) and (r.publication_id is distinct from s.publication_id or r.bundle_checksum<>s.publication_checksum or not s.publication_enabled)").param("source",a.sourceId()).update();
  db.sql("update ouf_ingestion.ing_partition p set state=r.state from ouf_ingestion.ing_run r where p.run_id=r.run_id and r.source_id=:source and r.state in ('FAILED','PAUSED') and r.failure_code='ING_ACTIVATION_SUPERSEDED' and p.state='PREFLIGHT'").param("source",a.sourceId()).update();

  // Adopt a pre-V21 blocked MANAGED admission lacking a controllable run.
  // The upsert above locks this source and only its exact current publication qualifies.
  if(a.once()){
    UUID run=UUID.randomUUID();
    int adopted=db.sql("""
      insert into ouf_ingestion.ing_run(run_id,source_id,tenant_id,bundle_id,bundle_version,bundle_checksum,
        mode,phase,state,failure_code,correlation_id,schedule_id,publication_id,admission_pending)
      select :r,source_id,tenant_id,:bundle,:version,publication_checksum,'MANAGED_ONCE','MANAGED_ONCE',
        'PAUSED','ING_ACTIVATION_REVIEW_REQUIRED',:corr,schedule_id,publication_id,true
      from ouf_ingestion.ing_schedule s where source_id=:source and tenant_id=:t and publication_id=:pub
        and publication_enabled and activation_blocked and state='PAUSED' and consumed_publication_id is distinct from publication_id
        and not exists(select 1 from ouf_ingestion.ing_run r where r.schedule_id=s.schedule_id
          and r.publication_id=s.publication_id and r.admission_pending)
      """).param("r",run).param("source",a.sourceId()).param("t",a.tenantId()).param("pub",a.publicationId())
      .param("bundle",a.execution().bundleId()).param("version",a.execution().bundleVersion()).param("corr",run.toString()).update();
    if(adopted==1){
      db.sql("insert into ouf_ingestion.runtime_configuration_snapshot(snapshot_id,run_id,bundle_id,bundle_version,bundle_checksum,snapshot_json) values(:id,:r,:b,:v,:h,cast(:j as jsonb))")
        .param("id",UUID.randomUUID()).param("r",run).param("b",a.execution().bundleId()).param("v",a.execution().bundleVersion())
        .param("h",a.execution().checksum()).param("j",encode(a.execution())).update();
      db.sql("insert into ouf_ingestion.ing_partition(run_id,partition_key,state) values(:r,'default','PAUSED')").param("r",run).update();
    }
  }
 }
 private String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("ING_SCHEDULE_POLICY_INVALID");}}
}
