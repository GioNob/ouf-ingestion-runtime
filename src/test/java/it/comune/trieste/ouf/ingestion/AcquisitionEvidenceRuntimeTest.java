package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.*;

@SpringBootTest
class AcquisitionEvidenceRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){
    r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));
  }
  @Autowired JdbcClient sql;
  @Autowired AcquisitionEvidenceService service;
  final ObjectMapper json=new ObjectMapper();
  record Fixture(UUID run,String source,UUID first,UUID second){}
  TrustedAuthorizationContext.Context actor(Set<String> caps){
    var request=new MockHttpServletRequest();TestAuthorization.bind(request,"reader","HUMAN",caps);
    return new TrustedAuthorizationContext().owner(request,"ingestion.lineage.read");
  }
  AcquisitionEvidenceApi.Query query(Fixture f,int size,UUID after){return new AcquisitionEvidenceApi.Query(f.source(),f.run(),size,after);}
  @SuppressWarnings("unchecked") static Map<String,Object> map(Object value){return (Map<String,Object>)value;}
  @SuppressWarnings("unchecked") static List<Map<String,Object>> rows(Object value){return (List<Map<String,Object>>)value;}

  @Test void realSnapshotReportsAckPrefixAndMinimizedPropertyStatesWithoutReadingRawValues()throws Exception{
    Fixture f=fixture("tenant-a","OPEN");
    var before=counts(f.run());var result=service.read(query(f,100,null),actor(AcquisitionEvidenceService.CAPABILITIES));
    assertThat(result).containsEntry("authorization","AUTHORIZED").containsEntry("partial",false).containsEntry("downstreamMaterializationProven",false);
    assertThat(map(result.get("run"))).containsEntry("read_only_snapshot",true).containsEntry("snapshot_matches_run",true)
      .containsEntry("managed_content_hash","sha256:"+"a".repeat(64));
    assertThat(rows(result.get("items"))).hasSize(2).allSatisfy(item->assertThat(item).containsEntry("durableAckRecorded",true));
    var props=rows(rows(result.get("items")).getFirst().get("mappedProperties"));
    assertThat(props).extracting(x->x.get("valueState")).containsExactly("EMPTY_STRING","NULL","VALUE","WHITESPACE_ONLY");
    assertThat(rows(result.get("partitions"))).singleElement().satisfies(p->assertThat(p)
      .containsEntry("watermark_belongs_to_requested_run",true).containsEntry("complete_ack_prefix",true)
      .containsEntry("committed_handoff_has_ack",true).containsEntry("watermark_matches_candidate",true).containsEntry("watermark_not_before_ack",true));
    String serialized=json.writeValueAsString(result);
    for(String secret:List.of("private-row-key","private-file-path","do-not-print","sensitive-title","raw-receipt-private-url"))assertThat(serialized).doesNotContain(secret);
    assertThat(counts(f.run())).isEqualTo(before);
  }
  @Test void ackStateAloneDoesNotProveReceiptAndWatermarkCannotCoverAnUnackedPrefix(){
    Fixture f=fixture("tenant-a","OPEN");
    sql.sql("update ouf_ingestion.handoff_outbox set downstream_receipt_ref=null where handoff_id=:h").param("h",f.first()).update();
    var result=service.read(query(f,100,null),actor(AcquisitionEvidenceService.CAPABILITIES));
    assertThat(rows(result.get("items"))).anySatisfy(i->assertThat(i).containsEntry("durableAckRecorded",false));
    assertThat(rows(result.get("partitions"))).singleElement().satisfies(p->assertThat(p).containsEntry("complete_ack_prefix",false));
  }
  @Test void sourceAndTenantMustBothMatchAndAllThreeCapabilitiesAreRequired(){
    Fixture f=fixture("tenant-a","OPEN");var a=actor(AcquisitionEvidenceService.CAPABILITIES);
    assertThatThrownBy(()->service.read(new AcquisitionEvidenceApi.Query("other-source",f.run(),100,null),a)).hasMessageContaining("404");
    Fixture foreign=fixture("tenant-b","OPEN");assertThatThrownBy(()->service.read(query(foreign,100,null),a)).hasMessageContaining("404");
    for(String missing:List.of("ingestion.run.read","ingestion.contract.read")){
      var caps=new HashSet<>(AcquisitionEvidenceService.CAPABILITIES);caps.remove(missing);
      assertThatThrownBy(()->service.read(query(f,100,null),actor(caps))).hasMessageContaining("403");
    }
  }
  @Test void paginationCannotClaimCompletenessAndSensitiveRowsAreNotReturned(){
    Fixture f=fixture("tenant-a","OPEN");var a=actor(AcquisitionEvidenceService.CAPABILITIES);
    var first=service.read(query(f,1,null),a);assertThat(first).containsEntry("partial",true);
    UUID after=(UUID)first.get("nextLineageId");assertThat(after).isNotNull();
    var second=service.read(query(f,1,after),a);assertThat(second).containsEntry("partial",false).containsEntry("nextLineageId",null);
    assertThat(rows(first.get("items"))).hasSize(1);assertThat(rows(second.get("items"))).hasSize(1);
    assertThat(rows(first.get("items")).getFirst().get("lineage_id")).isNotEqualTo(rows(second.get("items")).getFirst().get("lineage_id"));
    Fixture sensitive=fixture("tenant-a","SENSITIVE");var hidden=service.read(query(sensitive,100,null),a);
    assertThat(hidden).containsEntry("authorization","REDACTED").containsEntry("partial",true);assertThat(rows(hidden.get("items"))).isEmpty();
  }
  @Test void currentSourceWatermarkForAnotherRunCannotBePresentedAsThisRunsCompletion(){
    Fixture first=fixture("tenant-a","OPEN"),second=fixture("tenant-a","OPEN");
    sql.sql("update ouf_ingestion.ing_watermark set committed_handoff_id=:h where source_id=:s").param("h",second.second()).param("s",first.source()).update();
    var result=service.read(query(first,100,null),actor(AcquisitionEvidenceService.CAPABILITIES));
    assertThat(rows(result.get("partitions"))).singleElement().satisfies(p->assertThat(p)
      .containsEntry("committed_handoff_id",null).containsEntry("complete_ack_prefix",null));
  }
  Map<String,Object> counts(UUID run){return sql.sql("select (select count(*) from ouf_ingestion.handoff_outbox where run_id=:r) handoffs,(select count(*) from ouf_ingestion.processing_attempt where run_id=:r) attempts,(select count(*) from ouf_ingestion.ing_lineage where run_id=:r) lineage,(select sum(attempts) from ouf_ingestion.handoff_outbox where run_id=:r) deliveries").param("r",run).query().singleRow();}
  Fixture fixture(String tenant,String label){
    UUID run=UUID.randomUUID();String source="evidence-"+run;
    sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id,tenant_id) values(:r,:s,'bundle','1','sha256:pinned','MANAGED_ONCE','SUCCEEDED','c',:t)").param("r",run).param("s",source).param("t",tenant).update();
    sql.sql("insert into ouf_ingestion.runtime_configuration_snapshot(snapshot_id,run_id,bundle_id,bundle_version,bundle_checksum,snapshot_json) values(:i,:r,'bundle','1','sha256:pinned',cast(:j as jsonb))")
      .param("i",UUID.randomUUID()).param("r",run).param("j","{\"configuration\":{\"publicationId\":\""+UUID.randomUUID()+"\",\"password\":\"do-not-print\",\"publishedBundle\":{\"extractionProfile\":{\"runtime\":{\"contentHash\":\"sha256:"+"a".repeat(64)+"\",\"stagingRef\":\"private-file-path\"}}}}}").update();
    sql.sql("insert into ouf_ingestion.ing_partition(run_id,partition_key,state) values(:r,'default','SUCCEEDED')").param("r",run).update();
    UUID first=handoff(run,1,label),second=handoff(run,2,label);
    sql.sql("insert into ouf_ingestion.ing_watermark(source_id,partition_key,watermark_json,committed_handoff_id) values(:s,'default','{\"ordinal\":2}',:h)").param("s",source).param("h",second).update();
    return new Fixture(run,source,first,second);
  }
  UUID handoff(UUID run,int n,String label){
    UUID attempt=UUID.randomUUID(),lineage=UUID.randomUUID(),handoff=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.processing_attempt(attempt_id,run_id,source_object_id,attempt_no,adapter_id,bundle_version,state) values(:a,:r,:o,1,'managed-tabular-v1','1','SUCCEEDED')").param("a",attempt).param("r",run).param("o","private-row-key-"+n).update();
    sql.sql("insert into ouf_ingestion.ing_lineage(lineage_id,run_id,attempt_id,source_object_id,bundle_ref,adapter_ref,evidence_json) values(:l,:r,:a,:o,'bundle:1:sha256:pinned','managed-tabular-v1',cast(:j as jsonb))")
      .param("l",lineage).param("r",run).param("a",attempt).param("o","private-row-key-"+n).param("j","{\"data\":{\"rawHash\":\"sha256:"+"b".repeat(64)+"\",\"outputHash\":\"sha256:"+"c".repeat(64)+"\",\"rawObjectRef\":\"private-file-path\"}}").update();
    sql.sql("insert into ouf_ingestion.handoff_outbox(handoff_id,run_id,lineage_id,idempotency_key,payload_json,state,downstream_receipt_ref,acked_at,partition_key,sequence_no,candidate_watermark_json) values(:h,:r,:l,:i,cast(:p as jsonb),'ACKED','raw-receipt-private-url',transaction_timestamp(),'default',:n,jsonb_build_object('ordinal',:n))")
      .param("h",handoff).param("r",run).param("l",lineage).param("i",handoff.toString()).param("n",n)
      .param("p","{\"dataAccessLabels\":[{\"labelId\":\""+label+"\"}],\"canonicalPayload\":{\"a\":\"\",\"b\":null,\"c\":\"sensitive-title\",\"d\":\"  \"}}").update();
    return handoff;
  }
  static String required(String name){return Objects.requireNonNull(System.getenv(name),name);}
}
