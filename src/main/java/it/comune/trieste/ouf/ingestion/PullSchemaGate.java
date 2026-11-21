package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PullSchemaGate {
  private final JdbcClient db;
  private final ObjectMapper json;
  private final RunExecutionRepository runs;
  private final int noticeInterval;
  public PullSchemaGate(JdbcClient db,ObjectMapper json,RunExecutionRepository runs,
      @org.springframework.beans.factory.annotation.Value("${ouf.ingestion.schema-notice.update-min-seconds:0}") int noticeInterval) {
    if (noticeInterval < 0) throw new IllegalArgumentException("ING_SCHEMA_NOTICE_INTERVAL_INVALID");
    this.noticeInterval=noticeInterval;
    this.db=db; this.json=json; this.runs=runs;
  }

  public boolean hasOpen(UUID run) {
    return db.sql("""
        select exists(select 1 from ouf_ingestion.ing_run r join ouf_ingestion.pull_schema_incident g
          using(tenant_id,source_id) where r.run_id=:r and g.resumed_at is null)
        """).param("r",run).query(Boolean.class).single();
  }

  @Transactional
  public boolean admits(RunExecutionRepository.Claim claim, ExecutionBundle bundle, PullSchema.Comparison schema,
                        Map<String,Object> position, String correlation) {
    UUID run=claim.runId();
    var row=db.sql("""
        select r.tenant_id,r.schedule_id from ouf_ingestion.ing_run r
        join ouf_ingestion.ing_partition p using(run_id)
        where r.run_id=:r and p.partition_key=:p for update of r,p
        """).param("r",run).param("p",claim.partitionKey()).query().singleRow();
    runs.requireCurrent(claim);
    String tenant=(String)row.get("tenant_id");
    String source=bundle.sourceId();
    db.sql("select pg_advisory_xact_lock(hashtextextended(:s,0))").param("s",source).query().singleRow();
    if(row.get("schedule_id")!=null && db.sql("""
        select 1 from ouf_ingestion.ing_schedule where schedule_id=:id and state='ACTIVE'
          and publication_enabled and publication_checksum=:hash and publication_id::text=:publication
          and discovery_verified_at>clock_timestamp()-interval '5 minutes' for share
        """).param("id",row.get("schedule_id")).param("hash",bundle.checksum()).param("publication",bundle.configuration().get("publicationId")).query(Integer.class).optional().isEmpty()) {
      runs.pause(claim,"ING_ACTIVATION_SUPERSEDED");
      return false;
    }
    var open=db.sql("""
        select issue_id,binding_ref,frozen_checkpoint->>'collection' as collection,frozen_checkpoint->>'mode' as cursor_mode from ouf_ingestion.pull_schema_incident
        where tenant_id=:t and source_id=:s and resumed_at is null for update
        """).param("t",tenant).param("s",source).query().listOfRows();
    UUID issue=open.isEmpty()?null:(UUID)open.getFirst().get("issue_id");
    var differences=new ArrayList<>(schema.differences());
    if(issue!=null && (!Objects.equals(bundle.bindingRef(),open.getFirst().get("binding_ref"))
        || (open.getFirst().get("cursor_mode")!=null && !"REST_JSON".equals(bundle.acquisitionMode()))
        || (open.getFirst().get("collection") instanceof String collection && !collection.equals(schema.observed().path("collection").asText()))))
      differences.add("CHECKPOINT_SCOPE_CHANGED");
    if(differences.isEmpty()) {
      if (!schema.expected().path("fields").isObject()) {
        db.sql("""
            insert into ouf_ingestion.pull_verified_fields as v(tenant_id,source_id,fields_sha256,fields)
            values(:t,:s,:hash,cast(:fields as jsonb))
            on conflict(tenant_id,source_id) do update set fields_sha256=excluded.fields_sha256,fields=excluded.fields
            where v.fields_sha256<>excluded.fields_sha256
            """).param("t",tenant).param("s",source).param("hash",schema.observed().path("fieldsSha256").asText())
            .param("fields",schema.observed().path("fields").toString()).update();
      }
      if(issue!=null) {
        boolean pending=db.sql("""
            select exists(select 1 from ouf_ingestion.handoff_outbox h join ouf_ingestion.runtime_issue i using(run_id)
            where i.issue_id=:i and h.state<>'ACKED')
            """).param("i",issue).query(Boolean.class).single();
        if(pending) {
          db.sql("""
              update ouf_ingestion.pull_schema_incident set observation_polls=observation_polls+case when last_poll=:r then 0 else 1 end,
                last_poll=:r,last_observed_at=clock_timestamp(),resume_run=null where issue_id=:i
              """).param("r",run).param("i",issue).update();
          runs.pause(claim,"ING_SCHEMA_AWAITING_PREFIX_ACK");
          return false;
        }
        db.sql("""
            update ouf_ingestion.pull_schema_incident g set resume_run=:r,
              resume_reason=case when original.bundle_checksum<>:hash
                and g.opening_approval <> (cast(:expected as jsonb) - 'fields' - 'schemaRef')
                then 'REVISION_ACTIVATED' else 'SCHEMA_RETURNED' end
            from ouf_ingestion.runtime_issue i join ouf_ingestion.ing_run original using(run_id)
            where g.issue_id=:i and i.issue_id=g.issue_id
            """).param("r",run).param("i",issue).param("hash",bundle.checksum())
            .param("expected",schema.expected().toString()).update();
      }
      return true;
    }
    if(issue==null) {
      issue=UUID.randomUUID();
      db.sql("""
          insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id)
          values(:i,:r,:s,:type,'ERROR','ING_SCHEMA_CHANGED',:ref,:c)
          """).param("i",issue).param("r",run).param("s",source).param("type",bundle.configuration().get("typeCode"))
          .param("ref","ingestion:schema-incident:"+issue).param("c",correlation).update();
      db.sql("""
          insert into ouf_ingestion.pull_schema_incident(issue_id,tenant_id,source_id,binding_ref,expected_schema,observed_schema,
            differences,frozen_checkpoint,last_poll,notice_interval_seconds,opening_approval)
          values(:i,:t,:s,:b,cast(:e as jsonb),cast(:o as jsonb),cast(:d as jsonb),cast(:p as jsonb),:r,:interval,cast(:e as jsonb) - 'fields' - 'schemaRef')
          """).param("i",issue).param("t",tenant).param("s",source).param("b",position.getOrDefault("bindingRef",bundle.bindingRef()))
          .param("e",schema.expected().toString()).param("o",schema.observed().toString())
          .param("d",encode(differences)).param("p",encode(position)).param("r",run).param("interval",noticeInterval).update();
      var approvedFields=schema.expected().path("fields");
      if (!approvedFields.isObject()) {
        var verified=db.sql("""
            select fields::text from ouf_ingestion.pull_verified_fields
            where tenant_id=:t and source_id=:s and fields_sha256=:hash
            """).param("t",tenant).param("s",source).param("hash",schema.expected().path("fieldsSha256").asText())
            .query(String.class).optional();
        if (verified.isPresent()) {
          try { approvedFields=json.readTree(verified.get()); }
          catch (java.io.IOException e) { throw new IllegalStateException("ING_SCHEMA_EVIDENCE_INVALID",e); }
        }
      }
      var report=SchemaNotice.opening(tenant,source,issue,schema,approvedFields);
      boolean ready=SchemaNotice.deliverable(report);
      db.sql("""
          insert into ouf_ingestion.onboarding_review_outbox(request_id,issue_id,idempotency_key,correlation_id,
            notice_sequence,report_json,state,last_error_code)
          select :id,:i,:key,:c,1,cast(:report as jsonb) || jsonb_build_object(
            'observationPolls',observation_polls,'observedAt',date_trunc('milliseconds',last_observed_at)),
            :state,:error from ouf_ingestion.pull_schema_incident where issue_id=:i
          """).param("id",UUID.randomUUID()).param("i",issue).param("key","schema-notice:"+issue+":1")
          .param("report",report.toString()).param("c",correlation).param("state",ready?"READY":"FAILED_TERMINAL")
          .param("error",ready?null:"ING_SCHEMA_NOTICE_EVIDENCE_INCOMPLETE").update();
    } else {
      db.sql("""
          update ouf_ingestion.pull_schema_incident set expected_schema=cast(:e as jsonb),observed_schema=cast(:o as jsonb),
            differences=cast(:d as jsonb),observation_polls=observation_polls+case when last_poll=:r then 0 else 1 end,
            last_poll=:r,resume_run=null,last_observed_at=clock_timestamp() where issue_id=:i
          """).param("i",issue).param("r",run).param("e",schema.expected().toString())
          .param("o",schema.observed().toString()).param("d",encode(differences)).update();
    }
    db.sql("""
        insert into ouf_ingestion.source_health(source_id,status,last_error_code) values(:s,'DEGRADED','ING_SCHEMA_CHANGED')
        on conflict(source_id) do update set status='DEGRADED',last_error_code='ING_SCHEMA_CHANGED',updated_at=clock_timestamp()
        """).param("s",source).update();
    runs.pause(claim,"ING_SCHEMA_CHANGED");
    return false;
  }

  private String encode(Object value) {
    try { return json.writeValueAsString(value); }
    catch(Exception e) { throw new IllegalArgumentException("ING_SCHEMA_EVIDENCE_INVALID",e); }
  }
}
