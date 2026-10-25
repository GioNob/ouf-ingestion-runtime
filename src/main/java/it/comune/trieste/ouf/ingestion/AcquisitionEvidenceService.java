package it.comune.trieste.ouf.ingestion;

import it.comune.trieste.ouf.authorization.ResourceContext;
import java.sql.Types;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** One bounded repeatable-read snapshot. No payloads, source keys, file paths or credentials. */
@Service
public class AcquisitionEvidenceService {
  public static final Set<String> CAPABILITIES=Set.of("ingestion.run.read","ingestion.contract.read","ingestion.lineage.read");
  private final JdbcClient sql;
  public AcquisitionEvidenceService(JdbcClient sql){this.sql=sql;}

  @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ,timeout=10)
  public Map<String,Object> read(AcquisitionEvidenceApi.Query q,TrustedAuthorizationContext.Context actor){
    require(q,actor,"OPEN");
    // The run, tenant and source are always selected together; no cross-tenant lookup first.
    var runs=sql.sql("""
      select r.run_id,r.source_id,r.bundle_id,r.bundle_version,r.bundle_checksum,r.mode,r.state,r.phase,
        r.failure_code,r.created_at,r.updated_at,s.snapshot_id,transaction_timestamp() snapshot_observed_at,
        current_setting('transaction_read_only')='on' read_only_snapshot,
        s.snapshot_json#>>'{configuration,publicationId}' publication_id,
        s.snapshot_json#>>'{configuration,publishedBundle,extractionProfile,runtime,contentHash}' managed_content_hash,
        s.bundle_id=r.bundle_id and s.bundle_version=r.bundle_version and s.bundle_checksum=r.bundle_checksum snapshot_matches_run
      from ouf_ingestion.ing_run r left join ouf_ingestion.runtime_configuration_snapshot s using(run_id)
      where r.run_id=:r and r.source_id=:s and r.tenant_id=:t
      """).param("r",q.runId()).param("s",q.sourceId()).param("t",actor.tenantId()).query().listOfRows();
    if(runs.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"ING_ACQUISITION_NOT_VISIBLE");
    var run=new LinkedHashMap<>(runs.getFirst());
    // Only allow well-formed opaque publication/hash references to leave this projection.
    safeReference(run,"publication_id","[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    safeReference(run,"managed_content_hash","(?:sha256:)?[0-9a-f]{64}");
    var rows=sql.sql("""
      select l.lineage_id,h.handoff_id,h.state handoff_state,h.downstream_receipt_ref,h.acked_at,
        h.sequence_no,h.partition_key,h.candidate_watermark_json is not null candidate_watermark_recorded,
        h.payload_json->>'ingestionRunId'=r.run_id::text payload_run_matches,
        h.payload_json->>'handoffId'=h.handoff_id::text payload_handoff_matches,
        h.payload_json->>'lineageId'=l.lineage_id::text payload_lineage_matches,
        h.payload_json#>>'{sourceIdentity,sourceId}'=r.source_id payload_source_matches,
        l.evidence_json#>>'{data,rawHash}' raw_hash,l.evidence_json#>>'{data,outputHash}' output_hash,
        h.payload_json->'dataAccessLabels' _labels,
        (select coalesce(jsonb_agg(jsonb_build_object('propertyIri',p.key,'valueState',
          case when p.value='null'::jsonb then 'NULL'
            when p.value='""'::jsonb then 'EMPTY_STRING'
            when jsonb_typeof(p.value)='string' and btrim(p.value#>>'{}')='' then 'WHITESPACE_ONLY'
            else 'VALUE' end) order by p.key),'[]'::jsonb)
          from jsonb_each(case when jsonb_typeof(h.payload_json->'canonicalPayload')='object'
            then h.payload_json->'canonicalPayload' else '{}'::jsonb end) p) _properties
      from ouf_ingestion.ing_lineage l join ouf_ingestion.ing_run r using(run_id)
        left join ouf_ingestion.handoff_outbox h on h.lineage_id=l.lineage_id and h.run_id=l.run_id
      where l.run_id=:r and r.tenant_id=:t and r.source_id=:s
        and (cast(:a as uuid) is null or l.lineage_id>cast(:a as uuid))
      order by l.lineage_id limit :n
      """).param("r",q.runId()).param("s",q.sourceId()).param("t",actor.tenantId())
      .param("a",q.afterLineageId(),Types.OTHER).param("n",q.limit()+1).query().listOfRows();
    boolean more=rows.size()>q.limit();if(more)rows=rows.subList(0,q.limit());
    var visible=new ArrayList<Map<String,Object>>();boolean redacted=false;
    for(var row:rows){
      if(!labelsVisible(q,actor,row.get("_labels"))){redacted=true;continue;}
      var item=new LinkedHashMap<>(row);item.remove("_labels");item.remove("_properties");
      item.put("mappedProperties",decode(row.get("_properties")));
      safeReference(item,"raw_hash","sha256:[0-9a-f]{64}");safeReference(item,"output_hash","sha256:[0-9a-f]{64}");
      // A receipt is an opaque identifier, never an arbitrary URL or owner-private path.
      Object receipt=item.remove("downstream_receipt_ref");
      item.put("downstreamReceiptRecorded",receipt instanceof String s&&!s.isBlank());
      if(receipt instanceof String s&&!s.isBlank()&&s.length()<=4096)item.put("downstreamReceiptHash",hash(s));
      item.put("durableAckRecorded","ACKED".equals(row.get("handoff_state"))&&row.get("acked_at")!=null&&Boolean.TRUE.equals(item.get("downstreamReceiptRecorded")));
      visible.add(item);
    }
    var partitions=sql.sql("""
      select p.partition_key,p.state,
        w.committed_handoff_id is not null watermark_present,
        h.run_id=:r watermark_belongs_to_requested_run,
        case when h.run_id=:r then h.handoff_id end committed_handoff_id,
        case when h.run_id=:r then h.sequence_no end committed_sequence_no,
        case when h.run_id=:r then h.state='ACKED' and h.acked_at is not null
          and nullif(h.downstream_receipt_ref,'') is not null end committed_handoff_has_ack,
        case when h.run_id=:r then w.watermark_json=h.candidate_watermark_json end watermark_matches_candidate,
        case when h.run_id=:r then w.committed_at>=h.acked_at end watermark_not_before_ack,
        case when h.run_id=:r then not exists(select 1 from ouf_ingestion.handoff_outbox prior
          where prior.run_id=:r and prior.partition_key=p.partition_key and prior.sequence_no<=h.sequence_no
          and (prior.state<>'ACKED' or prior.acked_at is null or nullif(prior.downstream_receipt_ref,'') is null)) end complete_ack_prefix
      from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id)
        left join ouf_ingestion.ing_watermark w on w.source_id=r.source_id and w.partition_key=p.partition_key
        left join ouf_ingestion.handoff_outbox h on h.handoff_id=w.committed_handoff_id and h.run_id in
          (select run_id from ouf_ingestion.ing_run where tenant_id=:t and source_id=:s)
      where p.run_id=:r and r.tenant_id=:t and r.source_id=:s order by p.partition_key limit 101
      """).param("r",q.runId()).param("s",q.sourceId()).param("t",actor.tenantId()).query().listOfRows();
    boolean partitionTruncated=partitions.size()>100;if(partitionTruncated)partitions=partitions.subList(0,100);
    var counts=sql.sql("""
      select (select count(*) from ouf_ingestion.processing_attempt where run_id=:r) attempts,
        (select count(*) from ouf_ingestion.processing_attempt where run_id=:r and state='FAILED') failed_attempts,
        (select count(*) from ouf_ingestion.ing_lineage where run_id=:r) lineage_records,
        (select count(*) from ouf_ingestion.handoff_outbox where run_id=:r) handoffs,
        (select count(*) from ouf_ingestion.ing_quarantine where run_id=:r and lifecycle_state not in ('RESOLVED','DISMISSED','SUPERSEDED')) unresolved_quarantines
      """).param("r",q.runId()).query().singleRow();
    // Check again before releasing a result: a policy that became stale during I/O is not authority.
    require(q,actor,"OPEN");
    var out=new LinkedHashMap<String,Object>();out.put("schema","ouf.ingestion.acquisition-evidence.v1");out.put("run",run);
    out.put("counts",counts);out.put("items",visible);out.put("partitions",partitions);out.put("partitionTruncated",partitionTruncated);
    out.put("nextLineageId",more&&!redacted?rows.getLast().get("lineage_id"):null);out.put("partial",redacted||more||partitionTruncated);
    out.put("authorization",redacted?"REDACTED":"AUTHORIZED");out.put("authorizationDecisionRef",actor.owner().decisionRef());
    out.put("downstreamMaterializationProven",false);return out;
  }
  private boolean labelsVisible(AcquisitionEvidenceApi.Query q,TrustedAuthorizationContext.Context actor,Object raw){
    Object labels=decode(raw);if(!(labels instanceof List<?> list)||list.isEmpty())return false;
    for(Object entry:list){if(!(entry instanceof Map<?,?> m)||!(m.get("labelId") instanceof String label)
        ||!Set.of("OPEN","ANONYMOUS","PERSONAL","SENSITIVE","RESTRICTED").contains(label))return false;
      if(!permitted(q,actor,label))return false;}
    return true;
  }
  private void require(AcquisitionEvidenceApi.Query q,TrustedAuthorizationContext.Context actor,String label){
    if(!permitted(q,actor,label))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"ING_ACQUISITION_NOT_AUTHORIZED");
  }
  private boolean permitted(AcquisitionEvidenceApi.Query q,TrustedAuthorizationContext.Context actor,String label){
    if(actor.owner()==null)return false;
    var attrs=new HashMap<String,String>();attrs.put("module","INGESTION");attrs.put("sourceRef",q.sourceId());attrs.put("jobRef",q.runId().toString());attrs.put("detailLevel","TENANT_OPERATIONAL");
    if(label!=null)attrs.put("dataAccessLabel",label);
    var resource=new ResourceContext("operational",q.runId().toString(),actor.tenantId(),null,attrs);
    return CAPABILITIES.stream().allMatch(cap->{var d=actor.owner().decide(cap,resource);return d.allowed()&&"TENANT_OPERATIONAL".equals(d.permittedDetailLevel());});
  }
  private static Object decode(Object raw){
    if(raw==null)return null;
    try{return new com.fasterxml.jackson.databind.ObjectMapper().readValue(raw.toString(),Object.class);}
    catch(Exception invalid){throw new IllegalStateException("ING_ACQUISITION_STORED_METADATA_INVALID");}
  }
  private static String hash(String value){try{return "sha256:"+HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));}catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
  private static void safeReference(Map<String,Object> row,String field,String pattern){Object value=row.get(field);if(value!=null&&(!(value instanceof String s)||!s.matches(pattern)))row.put(field,null);}
}
