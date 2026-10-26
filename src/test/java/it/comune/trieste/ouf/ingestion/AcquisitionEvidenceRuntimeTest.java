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
    var engine=(LocalAuthorization)request.getServletContext().getAttribute(ServletAuthorization.RUNTIME);
    var old=engine.currentSnapshot().bundle();
    var grants=old.grants().stream().map(g->new it.comune.trieste.ouf.authorization.AuthorizationPolicy.Grant(
      g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),g.servicePrincipalId(),g.organizationId(),g.validFrom(),g.validUntil(),
      new it.comune.trieste.ouf.authorization.AuthorizationPolicy.GrantConstraints("ALLOW",null,"operational",null,
        Map.of("module","INGESTION"),Set.of("OPEN"),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null))).toList();
    try{TestAuthorization.install(engine,new it.comune.trieste.ouf.authorization.AuthorizationPolicy.PolicyBundle(old.bundleId(),2,old.publishedAt(),old.capabilities(),grants));}
    catch(Exception failure){throw new IllegalStateException(failure);}
    return new TrustedAuthorizationContext().owner(request,"ingestion.lineage.read");
  }
  AcquisitionEvidenceApi.Query query(Fixture f,int size,UUID after){return new AcquisitionEvidenceApi.Query(f.source(),f.run(),size,after);}
  @SuppressWarnings("unchecked") static Map<String,Object> map(Object value){return (Map<String,Object>)value;}
  @SuppressWarnings("unchecked") static List<Map<String,Object>> rows(Object value){return (List<Map<String,Object>>)value;}

  @Test @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named="EVIDENCE_READ_IMAGE",matches=".+")
  void packagedImageUsesSignedHttpReadAndDeniesMutation()throws Exception{
    Fixture f=fixture("tenant-a","OPEN");var before=counts(f.run());
    var directory=java.nio.file.Files.createTempDirectory("ingestion-packaged-read-");var key=directory.resolve("key");java.nio.file.Files.writeString(key,"cd".repeat(32));var policy=policy(directory,"human-a");
    var args=List.of("--spring.main.sources=it.comune.trieste.ouf.ingestion.IngestionRuntimeApplication","--spring.main.web-application-type=none","--spring.flyway.enabled=true","--spring.sql.init.mode=always","--ouf.ingestion.execution.enabled=true",
      "--ouf.ingestion.gateway-read.enabled=true","--ouf.ingestion.gateway-read.key-file="+key,"--ouf.ingestion.gateway-read.issuer="+GatewayReadReceiptVerifierTest.ISSUER,"--ouf.ingestion.gateway-read.audience=gateway","--ouf.ingestion.gateway-read.workload=workload",
      "--ouf.authorization.bundle-file="+policy.get("file"),"--ouf.authorization.bundle-id=fixture","--ouf.authorization.bundle-version=2","--ouf.authorization.bundle-sha256="+policy.get("hash"));
    try(var container=new PackagedReadProcess(directory,args,"OUF_ING_DB_")){
      container.ready();
      byte[] body=("{\"sourceId\":\""+f.source()+"\",\"runId\":\""+f.run()+"\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
      var receipt=GatewayReadReceiptVerifierTest.receipt();long now=java.time.Instant.now().getEpochSecond();receipt.put("iat",now);receipt.put("exp",now+30);
      receipt.put("path",GatewayReadReceiptVerifier.PREFIX+"acquisition");receipt.put("capability","ouf.ingestion.acquisition.read");receipt.put("bodyHash",HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(body)));receipt.put("scope",String.join(" ",new TreeSet<>(AcquisitionEvidenceService.CAPABILITIES)));
      var uri=java.net.URI.create("http://127.0.0.1:"+container.port+GatewayReadReceiptVerifier.PREFIX+"acquisition");
      var request=java.net.http.HttpRequest.newBuilder(uri).timeout(java.time.Duration.ofSeconds(5)).header("Content-Type","application/json")
        .header(GatewayReadReceiptVerifier.HEADER,GatewayReadReceiptVerifierTest.sign(receipt)).header("X-Correlation-ID","corr").header("X-Tool-Attempt-ID","11111111-1111-4111-8111-111111111111").header("Idempotency-Key","idem")
        .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(body)).build();
      var client=container.client();var response=client.send(request,java.net.http.HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      assertThat(new ObjectMapper().readTree(response.body()).path("authorization").asText()).isEqualTo("AUTHORIZED");
      assertThat(new ObjectMapper().readTree(response.body()).path("redactionReasons").isEmpty()).isTrue();
      var denied=client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+container.port+"/api/trusted-human/v1/ingestion/runs/"+f.run()+"/resume")).timeout(java.time.Duration.ofSeconds(5)).POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
      assertThat(denied.statusCode()).isIn(401,403);
    }
    assertThat(counts(f.run())).isEqualTo(before);
  }
  @Test void separateReadApplicationUsesRealHttpAndCannotStartWorkersMigrateOrWrite()throws Exception{
    Fixture f=fixture("tenant-a","OPEN");
    var temp=java.nio.file.Files.createTempDirectory("ingestion-evidence-http-");
    var key=temp.resolve("key");java.nio.file.Files.writeString(key,"cd".repeat(32));
    var policy=policy(temp,"human-a");
    try(var context=it.comune.trieste.ouf.evidence.IngestionAcquisitionReadApplication.application().run(
      "--server.port=0","--spring.jmx.enabled=false","--spring.main.sources=it.comune.trieste.ouf.ingestion.IngestionRuntimeApplication","--spring.main.web-application-type=none","--spring.flyway.enabled=true","--spring.sql.init.mode=always","--ouf.ingestion.execution.enabled=true",
      "--ouf.ingestion.gateway-read.enabled=true","--ouf.ingestion.gateway-read.key-file="+key,
      "--ouf.ingestion.gateway-read.issuer="+GatewayReadReceiptVerifierTest.ISSUER,"--ouf.ingestion.gateway-read.audience=gateway","--ouf.ingestion.gateway-read.workload=workload",
      "--ouf.authorization.bundle-file="+policy.get("file"),"--ouf.authorization.bundle-id=fixture","--ouf.authorization.bundle-version=2","--ouf.authorization.bundle-sha256="+policy.get("hash"))){
      assertThat(context.getBeansOfType(RunExecutionWorker.class)).isEmpty();assertThat(context.getBeansOfType(ReplayWorker.class)).isEmpty();
      assertThat(context.getBeansOfType(OutboxDispatcher.class)).isEmpty();assertThat(context.getBeansOfType(OnboardingReviewDispatcher.class)).isEmpty();
      assertThat(context.getBeansOfType(org.flywaydb.core.Flyway.class)).isEmpty();
      var ds=context.getBean(com.zaxxer.hikari.HikariDataSource.class);
      assertThat(ds.getMaximumPoolSize()).isEqualTo(2);assertThat(ds.getMinimumIdle()).isZero();
      try(var connection=ds.getConnection()){
        assertThat(connection.isReadOnly()).isTrue();assertThat(connection.getAutoCommit()).isFalse();
        assertThatThrownBy(()->connection.createStatement().execute("create table ouf_ingestion.evidence_forbidden_write(id int)")).hasMessageContaining("read-only");connection.rollback();
      }
      int port=context.getEnvironment().getRequiredProperty("local.server.port",Integer.class);
      byte[] body=("{\"sourceId\":\""+f.source()+"\",\"runId\":\""+f.run()+"\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
      var receipt=GatewayReadReceiptVerifierTest.receipt();long now=java.time.Instant.now().getEpochSecond();
      receipt.put("path",GatewayReadReceiptVerifier.PREFIX+"acquisition");receipt.put("capability","ouf.ingestion.acquisition.read");receipt.put("iat",now);receipt.put("exp",now+30);
      receipt.put("bodyHash",HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(body)));receipt.put("scope",String.join(" ",new TreeSet<>(AcquisitionEvidenceService.CAPABILITIES)));
      var request=java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+port+GatewayReadReceiptVerifier.PREFIX+"acquisition"))
        .header("Content-Type","application/json").header(GatewayReadReceiptVerifier.HEADER,GatewayReadReceiptVerifierTest.sign(receipt))
        .header("X-Correlation-ID","corr").header("X-Tool-Attempt-ID","11111111-1111-4111-8111-111111111111").header("Idempotency-Key","idem").POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(body)).build();
      var client=java.net.http.HttpClient.newHttpClient();var response=client.send(request,java.net.http.HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(200);assertThat(json.readTree(response.body()).path("run").path("read_only_snapshot").asBoolean()).isTrue();
      var denied=client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+port+"/api/trusted-human/v1/ingestion/runs/"+f.run()+"/resume")).POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build(),java.net.http.HttpResponse.BodyHandlers.ofString());
      assertThat(denied.statusCode()).isIn(401,403);
    }
    assertThat(sql.sql("select state from ouf_ingestion.ing_run where run_id=:r").param("r",f.run()).query(String.class).single()).isEqualTo("SUCCEEDED");
  }
  Map<String,String> policy(java.nio.file.Path directory,String subject)throws Exception{
    var engine=TestAuthorization.runtime(subject,"HUMAN",AcquisitionEvidenceService.CAPABILITIES);var b=engine.currentSnapshot().bundle();
    var grants=b.grants().stream().map(g->new it.comune.trieste.ouf.authorization.AuthorizationPolicy.Grant(g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),null,null,g.validFrom(),g.validUntil(),new it.comune.trieste.ouf.authorization.AuthorizationPolicy.GrantConstraints("ALLOW",null,null,null,Map.of("module","INGESTION"),Set.of("OPEN"),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null))).toList();
    var bundle=new it.comune.trieste.ouf.authorization.AuthorizationPolicy.PolicyBundle(b.bundleId(),2,b.publishedAt(),b.capabilities(),grants);
    byte[] bytes=new ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule()).writeValueAsBytes(bundle);var file=directory.resolve("policy.json");java.nio.file.Files.write(file,bytes);
    return Map.of("file",file.toString(),"hash",HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)));
  }

  @Test void realSnapshotReportsAckPrefixAndMinimizedPropertyStatesWithoutReadingRawValues()throws Exception{
    Fixture f=fixture("tenant-a","OPEN");
    var before=counts(f.run());var result=service.read(query(f,100,null),actor(AcquisitionEvidenceService.CAPABILITIES));
    assertThat(result).containsEntry("authorization","AUTHORIZED").containsEntry("partial",false).containsEntry("redactionReasons",List.of()).containsEntry("downstreamMaterializationProven",false);
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

  @Test void redactionReasonsSeparateInvalidMetadataFromDeniedLabelsWithoutDisclosingRows()throws Exception{
    Fixture f=fixture("tenant-a","OPEN");var a=actor(AcquisitionEvidenceService.CAPABILITIES);
    var before=counts(f.run());
    for(String labels:List.of("[]","{}","[{}]","[{\"labelId\":\"UNKNOWN_PRIVATE_LABEL\"}]")){
      sql.sql("update ouf_ingestion.handoff_outbox set payload_json=jsonb_set(payload_json,'{dataAccessLabels}',cast(:j as jsonb)) where run_id=:r")
        .param("j",labels).param("r",f.run()).update();
      var hidden=service.read(query(f,100,null),a);
      assertThat(hidden).containsEntry("partial",true).containsEntry("authorization","REDACTED")
        .containsEntry("redactionReasons",List.of("DATA_LABEL_METADATA_INVALID")).containsEntry("nextLineageId",null);
      assertThat(rows(hidden.get("items"))).isEmpty();
      assertThat(json.writeValueAsString(hidden)).doesNotContain("UNKNOWN_PRIVATE_LABEL",f.first().toString());
    }
    sql.sql("update ouf_ingestion.handoff_outbox set payload_json=payload_json-'dataAccessLabels' where run_id=:r").param("r",f.run()).update();
    assertThat(service.read(query(f,100,null),a)).containsEntry("redactionReasons",List.of("DATA_LABEL_METADATA_INVALID"));
    sql.sql("update ouf_ingestion.handoff_outbox set payload_json=jsonb_set(payload_json,'{dataAccessLabels}','[{\"labelId\":\"RESTRICTED\"}]'::jsonb) where run_id=:r")
      .param("r",f.run()).update();
    var denied=service.read(query(f,100,null),a);
    assertThat(denied).containsEntry("redactionReasons",List.of("DATA_LABEL_NOT_AUTHORIZED"));
    assertThat(rows(denied.get("items"))).isEmpty();
    assertThat(json.writeValueAsString(denied)).doesNotContain("RESTRICTED",f.first().toString(),"private-row-key");
    assertThat(counts(f.run())).isEqualTo(before);
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
