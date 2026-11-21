package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest @Transactional
class ManagedRepeatabilityRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry registry){
    registry.add("spring.datasource.url",()->System.getenv("OUF_ING_DB_URL"));
    registry.add("spring.datasource.username",()->System.getenv("OUF_ING_DB_USER"));
    registry.add("spring.datasource.password",()->System.getenv("OUF_ING_DB_PASSWORD"));
  }
  @Autowired JdbcClient db;
  @Autowired ObjectMapper json;
  @Autowired ActivationRepository activation;
  @Autowired RunStateRepository state;
  @Autowired RunExecutionRepository execution;
  @Autowired SchemaSurveillanceService schemas;
  @Autowired DurablePipelineRepository durable;
  @Autowired QuarantineService quarantine;
  @Autowired FrozenContractValidator contracts;
  @Autowired OperationalAwarenessService operations;
  @Autowired IngestionMetrics metrics;
  @Autowired PlatformTransactionManager transactions;

  final String source="repeat-"+UUID.randomUUID();
  final TrustedAuthorizationContext.Context human=new TrustedAuthorizationContext.Context("operator","HUMAN","tenant-a",
    Set.of("ingestion.run.resume","ingestion.run.abort"),"decision:fixture");
  final Map<String,byte[]> assets=new HashMap<>();
  final String firstCsv="id;name\n1;Library\n";
  final List<UUID> receipts=new ArrayList<>();
  PublishedActivation current;
  RuntimePorts.ActiveBundlePort bundles=id->current.execution();
  RuntimePorts.ManagedObjectPort objects=(ref,size,hash)->assets.get(hash);
  RuntimePorts.DataLakePort lake=(zone,run,object,content,hash,key)->new RuntimePorts.DataLakePort.Receipt("lake:"+key,true);
  RuntimePorts.DurableHandoffPort udp=(id,payload,key)->{
    assertThat(payload).containsEntry("handoffId",id.toString());receipts.add(id);
    return new RuntimePorts.DurableHandoffPort.Receipt("udp://handoffs/"+id,true);
  };

  @BeforeEach void isolateSchedulers(){
    // The owner claim selects globally. Roll back all fixture state after each case.
    db.sql("truncate ouf_ingestion.ing_schedule,ouf_ingestion.ing_run cascade").update();
  }

  private PublishedActivation publish(long sequence,String csv){
    return publish(sequence,csv,Map.of());
  }
  private PublishedActivation publish(long sequence,String csv,Map<String,Object> sync){
    byte[] bytes=csv.getBytes(StandardCharsets.UTF_8);
    var envelope=PublishedActivationTest.envelope(source,true,sequence);
    var b=PublishedActivation.map(envelope,"bundle");
    var config=PublishedActivation.map(new FrozenConfigurationProbeTest().candidate(),"configuration");
    for(String key:List.of("extractionProfile","semanticMapping","sourceObjectIdentityPolicy","changeRepresentationProfile","dataAccessPolicies"))
      b.put(key,config.get(key));
    PublishedActivation.map(b,"semanticMapping").put("sourceType",Map.of("sourceId",source,"typeCode","PLACE"));
    var runtime=PublishedActivation.map(PublishedActivation.map(b,"extractionProfile"),"runtime");
    String hash=ExecutionGatewayClient.hash(bytes);assets.put(hash,bytes);
    runtime.put("contentHash",hash);PublishedActivation.map(runtime,"execution").put("expectedSize",bytes.length);
    b.put("syncProfile",sync);PublishedActivationTest.hash(b);envelope.put("checksum",b.get("checksum"));
    current=PublishedActivation.decode(envelope,"tenant-a",json);activation.reconcile(current);return current;
  }
  private Optional<UUID> dispatch(){return new RunCoordinator(state,bundles,refs->{}).dispatchOne("preflight");}
  private Optional<UUID> acquire(){
    var pipeline=new CanonicalRecordPipeline(json,contracts,durable,quarantine,lake);
    return new RunExecutionWorker(execution,b->new ManagedTabularAdapter(objects),pipeline,schemas,state,
      b->assertThat(b.configuration()).containsEntry("publicationId",current.publicationId().toString())).executeOne("acquisition");
  }
  private boolean deliver(){return new OutboxDispatcher(durable,udp,metrics).dispatchOne("udp");}
  private UUID finish(UUID run){
    assertThat(acquire()).contains(run);assertThat(state.run(run)).containsEntry("state","DRAINING");
    assertThat(deliver()).isTrue();assertThat(execution.completeDrained()).isOne();
    assertThat(status()).containsEntry("job_ref",run).containsEntry("run_state","SUCCEEDED")
      .containsEntry("publication_id",current.publicationId());
    return run;
  }
  private Map<String,Object> status(){return operations.status(source,1,human).getFirst();}
  private RunLifecycleService controls(){return new RunLifecycleService(db,json,refs->{},transactions);}
  private long count(String table){return db.sql("select count(*) from ouf_ingestion."+table).query(Long.class).single();}

  @ParameterizedTest @ValueSource(strings={"id;name\n1;Library\n","id;name\n2;Station\n"})
  void twoPublicationsAcquireTheirOwnCsvAndDurableReceipt(String csv){
    var p1=publish(1,firstCsv);UUID r1=finish(dispatch().orElseThrow());
    activation.reconcile(p1);assertThat(dispatch()).isEmpty();assertThat(status()).containsEntry("job_ref",r1);
    var p2=publish(2,csv);
    assertThat(status()).containsEntry("publication_id",p2.publicationId()).containsEntry("run_state","AWAITING_START").containsEntry("job_ref",null);
    var history=operations.history(source,null,10,human);
    assertThat(history.getFirst()).containsEntry("publication_id",p2.publicationId()).containsEntry("is_current_publication",true);
    assertThat(history.getLast()).containsEntry("job_ref",r1).containsEntry("publication_id",p1.publicationId()).containsEntry("is_current_publication",false);
    UUID r2=finish(dispatch().orElseThrow());
    assertThat(r2).isNotEqualTo(r1);assertThat(receipts).hasSize(2).doesNotHaveDuplicates();
    String actual=db.sql("select payload_json#>>'{canonicalPayload,https://example.test/name}' from ouf_ingestion.handoff_outbox where run_id=:r and state='ACKED'")
      .param("r",r2).query(String.class).single();
    assertThat(actual).isEqualTo(csv.equals(firstCsv)?"Library":"Station");
  }

  @ParameterizedTest @ValueSource(ints={429,503})
  void retryBeforeActiveLookupCompletesReusesTheSamePreflight(int statusCode){
    publish(1,firstCsv);finish(dispatch().orElseThrow());var p2=publish(2,firstCsv);
    var calls=new AtomicInteger();bundles=id->{
      if(calls.getAndIncrement()==0)throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_GATEWAY_"+statusCode,
        AdapterSpi.ErrorClass.TRANSIENT_SOURCE,Duration.ofHours(2));
      return p2.execution();
    };
    assertThat(dispatch()).isEmpty();var waiting=status();
    assertThat(waiting).containsEntry("run_state","RETRY_WAIT").containsEntry("publication_id",p2.publicationId());
    UUID run=(UUID)waiting.get("job_ref");assertThat(run).isNotNull();
    activation.reconcile(p2);assertThat(dispatch()).isEmpty();
    assertThat(db.sql("select next_run_at>=transaction_timestamp()+interval '2 hours' from ouf_ingestion.ing_schedule").query(Boolean.class).single()).isTrue();
    // Advance the persisted timer (no sleeps or operator transition).
    db.sql("update ouf_ingestion.ing_schedule set next_run_at=clock_timestamp(),activation_first_failure_at=clock_timestamp()-interval '2 hours'").update();
    assertThat(dispatch()).contains(run);finish(run);assertThat(count("ing_run")).isEqualTo(2);
  }

  @Test void acquisitionAndLakeRefusalsResumeWithoutNewRunOrRecordError(){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();
    var reads=new AtomicInteger();objects=(ref,size,hash)->{
      if(reads.getAndIncrement()==0)throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_429",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
      return assets.get(hash);
    };
    var calls=new ArrayList<String>();var normalized=new ArrayList<String>();
    lake=(zone,r,o,bytes,hash,key)->{
      calls.add(zone.name());
      if(zone==RuntimePorts.DataLakePort.Zone.NORMALIZED){
        normalized.add(key+" "+hash);
        if(normalized.size()==1)throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_503",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
      }
      return new RuntimePorts.DataLakePort.Receipt("lake:"+key,true);
    };
    for(int i=0;i<2;i++){
      assertThat(acquire()).contains(run);assertThat(status()).containsEntry("run_state","RETRY_WAIT");
      assertThat(acquire()).isEmpty();assertThat(count("ing_quarantine")).isZero();assertThat(count("processing_attempt")).isZero();
      db.sql("update ouf_ingestion.ing_partition set retry_at=clock_timestamp() where run_id=:r").param("r",run).update();
      db.sql("update ouf_ingestion.source_health set retry_not_before=clock_timestamp() where source_id=:s").param("s",source).update();
    }
    finish(run);assertThat(calls).containsExactly("RAW","NORMALIZED","NORMALIZED","CURATED");
    assertThat(normalized.getFirst()).isEqualTo(normalized.getLast());assertThat(count("ing_run")).isOne();
  }

  @ParameterizedTest @ValueSource(booleans={true,false})
  void permanentOrExhaustedHandoffHasOneGovernedExit(boolean permanent){
    publish(1,firstCsv);UUID old=dispatch().orElseThrow();assertThat(acquire()).contains(old);
    var actual=udp;
    udp=(id,payload,key)->{throw new RuntimePorts.GatewayFailure(permanent?"ING_EXECUTION_GATEWAY_403":"ING_EXECUTION_GATEWAY_503",
      permanent?AdapterSpi.ErrorClass.AUTH_ROUTE:AdapterSpi.ErrorClass.TRANSIENT_SOURCE);};
    if(!permanent)db.sql("update ouf_ingestion.handoff_outbox set attempts=127,delivery_started_at=clock_timestamp()-interval '2 hours'").update();
    assertThat(deliver()).isTrue();assertThat(deliver()).isFalse();
    assertThat(state.run(old)).containsEntry("state","PAUSED").containsEntry("failure_code","ING_HANDOFF_BLOCKED");
    assertThat(count("ing_watermark")).isZero();assertThat(count("runtime_issue")).isOne();
    publish(2,firstCsv);assertThat(dispatch()).isEmpty();assertThat(status()).containsEntry("error_code","ING_HANDOFF_BLOCKED");
    controls().abort(old,1,human,"corr-dispose");
    udp=actual;assertThat(deliver()).isFalse();finish(dispatch().orElseThrow());
    assertThat(receipts).hasSize(1);assertThat(db.sql("select status from ouf_ingestion.runtime_issue").query(String.class).single()).isEqualTo("RESOLVED");
  }

  @Test void abortFencesALateDurableReceiptBeforeTheNextPublication(){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();assertThat(acquire()).contains(run);
    var actual=udp;
    udp=(id,payload,key)->{
      controls().abort(run,0,human,"abort-in-flight");
      return new RuntimePorts.DurableHandoffPort.Receipt("udp://handoffs/"+id,true);
    };
    assertThatThrownBy(this::deliver).hasMessage("ING_ACK_LEASE_MISMATCH");
    assertThat(count("ing_watermark")).isZero();assertThat(deliver()).isFalse();
    assertThat(db.sql("select last_error_code from ouf_ingestion.handoff_outbox").query(String.class).single())
      .isEqualTo("ING_HANDOFF_OUTCOME_UNCONFIRMED");
    udp=actual;publish(2,firstCsv);finish(dispatch().orElseThrow());
  }

  @Test void humanCanRenewAnExhaustedHandoffOnItsOriginalRun(){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();assertThat(acquire()).contains(run);
    UUID handoff=db.sql("select handoff_id from ouf_ingestion.handoff_outbox").query(UUID.class).single();
    db.sql("update ouf_ingestion.handoff_outbox set attempts=128,delivery_started_at=clock_timestamp()-interval '2 hours'").update();
    assertThat(deliver()).isFalse();assertThat(receipts).isEmpty();
    controls().resume(run,1,human,"redrive",new RunRecoveryBudget.Request(1,3600,"UDP_AVAILABLE"));
    assertThat(deliver()).isTrue();assertThat(execution.completeDrained()).isOne();
    assertThat(receipts).containsExactly(handoff);
    assertThat(db.sql("select attempts,state from ouf_ingestion.handoff_outbox").query().singleRow())
      .containsEntry("attempts",129).containsEntry("state","ACKED");
    assertThat(db.sql("select attempts,state from ouf_ingestion.run_recovery_session").query().singleRow())
      .containsEntry("attempts",1).containsEntry("state","COMPLETED");
    assertThat(status()).containsEntry("job_ref",run).containsEntry("run_state","SUCCEEDED");
  }

  @Test void lostUdpAcknowledgementReconcilesTheStoredPayloadAfterRestart(){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();assertThat(acquire()).contains(run);
    var sent=new HashMap<UUID,Map<String,Object>>();
    udp=(id,payload,key)->{
      var previous=sent.putIfAbsent(id,Map.copyOf(payload));
      if(previous==null)throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_UNAVAILABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
      assertThat(previous).isEqualTo(payload);return new RuntimePorts.DurableHandoffPort.Receipt("udp://handoffs/"+id,true);
    };
    assertThat(deliver()).isTrue();assertThat(deliver()).isFalse();assertThat(count("ing_watermark")).isZero();
    db.sql("update ouf_ingestion.handoff_outbox set next_attempt_at=clock_timestamp()").update();
    assertThat(deliver()).isTrue();assertThat(sent).hasSize(1);assertThat(execution.completeDrained()).isOne();
    assertThat(status()).containsEntry("run_state","SUCCEEDED");assertThat(count("ing_watermark")).isOne();
  }

  @Test void exhaustedExecutionSessionRemainsAuditableAfterAbortAndNewPublication(){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();var lifecycle=controls();
    lifecycle.pause(run,0,human,"pause");
    lifecycle.resume(run,1,human,"resume",new RunRecoveryBudget.Request(1,3600,"DEPENDENCY_REPAIRED"));
    var last=execution.claim("last",Duration.ofMinutes(2)).orElseThrow();execution.release(last);
    assertThat(execution.claim("restart",Duration.ofMinutes(2))).isEmpty();
    publish(2,firstCsv);assertThat(dispatch()).isEmpty();
    lifecycle.abort(run,2,human,"abort");
    assertThat(db.sql("select attempts,state from ouf_ingestion.run_recovery_session where run_id=:r")
      .param("r",run).query().singleRow()).containsEntry("attempts",1).containsEntry("state","EXHAUSTED");
    assertThat(db.sql("select status,resolution_reason from ouf_ingestion.runtime_issue where run_id=:r")
      .param("r",run).query().singleRow()).containsEntry("status","RESOLVED").containsEntry("resolution_reason","RUN_ABORTED");
    finish(dispatch().orElseThrow());
  }

  @Test void revocationWhileWaitingFencesAcquisitionBeforeAnotherRead(){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();
    var calls=new AtomicInteger();objects=(ref,size,hash)->{
      calls.incrementAndGet();throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_503",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
    };
    assertThat(acquire()).contains(run);
    activation.reconcile(new PublishedActivation(current.publicationId(),1,source,"tenant-a",false,true,0,current.execution()));
    db.sql("update ouf_ingestion.ing_partition set retry_at=clock_timestamp()").update();
    db.sql("update ouf_ingestion.source_health set retry_not_before=clock_timestamp() where source_id=:s").param("s",source).update();
    assertThat(acquire()).isEmpty();assertThat(calls).hasValue(1);
    assertThat(state.run(run)).containsEntry("state","PAUSED").containsEntry("failure_code","ING_ACTIVE_PUBLICATION_CHANGED");
    assertThat(count("handoff_outbox")).isZero();
  }

  @Test void abortAfterExhaustionReleasesOnlyTheNewPublication(){
    var p1=publish(1,firstCsv);bundles=id->{throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_GATEWAY_503",
      AdapterSpi.ErrorClass.TRANSIENT_SOURCE,Duration.ofHours(25));};
    assertThat(dispatch()).isEmpty();UUID old=(UUID)status().get("job_ref");
    assertThat(status()).containsEntry("run_state","PAUSED").containsEntry("error_code","ING_RECOVERY_BUDGET_EXHAUSTED");
    controls().abort(old,0,human,"corr-abort");activation.reconcile(p1);assertThat(dispatch()).isEmpty();
    assertThat(db.sql("select consumed_publication_id is null from ouf_ingestion.ing_schedule").query(Boolean.class).single()).isTrue();
    assertThat(db.sql("select resolution_reason||':'||authorization_decision_ref from ouf_ingestion.runtime_issue")
      .query(String.class).single()).isEqualTo("RUN_ABORTED:decision:fixture");
    bundles=id->current.execution();publish(2,firstCsv);finish(dispatch().orElseThrow());
  }

  @Test void preflightRenewalRequiresFreshActiveVerificationAndPreservesRunIdentity(){
    publish(1,firstCsv,Map.of("operationalPolicy",Map.of("maxRetryAttempts",0)));bundles=id->{throw new RuntimePorts.GatewayFailure("ING_ACTIVATION_GATEWAY_503",
      AdapterSpi.ErrorClass.TRANSIENT_SOURCE,Duration.ofHours(2));};
    assertThat(dispatch()).isEmpty();UUID old=(UUID)status().get("job_ref");
    controls().resume(old,0,human,"corr-grant");
    assertThat(dispatch()).isEmpty(); // HUMAN budget preserves the gateway lower bound.
    db.sql("update ouf_ingestion.ing_schedule set next_run_at=clock_timestamp()").update();
    var checks=new AtomicInteger();bundles=id->{checks.incrementAndGet();return current.execution();};
    assertThat(dispatch()).contains(old);finish(old);
    assertThat(checks).hasValue(1);assertThat(count("ing_run")).isOne();assertThat(count("run_recovery_session")).isOne();
  }
}
