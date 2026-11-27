package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.sql.Timestamp;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.io.TempDir;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest(properties="ouf.ingestion.recovery.human-decision-window=PT6H") @Transactional
class ManagedRepeatabilityRuntimeTest {
  private static final Duration DECISION_WINDOW = Duration.ofHours(6);
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
    // Claims select globally. The committed scale case uses the same disposable CI database.
    db.sql("truncate ouf_ingestion.ing_schedule,ouf_ingestion.ing_run cascade").update();
  }

  private PublishedActivation publish(long sequence,String csv){
    return publish(sequence,csv,Map.of());
  }
  private PublishedActivation publish(long sequence,String csv,Map<String,Object> sync){
    return publish(sequence,csv,sync,null);
  }
  private PublishedActivation publish(long sequence,String csv,Map<String,Object> sync,String targetClassIri){
    byte[] bytes=csv.getBytes(StandardCharsets.UTF_8);
    var envelope=PublishedActivationTest.envelope(source,true,sequence);
    var b=PublishedActivation.map(envelope,"bundle");
    var config=PublishedActivation.map(new FrozenConfigurationProbeTest().candidate(),"configuration");
    for(String key:List.of("extractionProfile","semanticMapping","sourceObjectIdentityPolicy","changeRepresentationProfile","dataAccessPolicies"))
      b.put(key,config.get(key));
    var semantic=PublishedActivation.map(b,"semanticMapping");
    semantic.put("sourceType",Map.of("sourceId",source,"typeCode","PLACE"));
    if(targetClassIri!=null)semantic.put("targetClasses",List.of(Map.of("classIri",targetClassIri)));
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
      .containsEntry("publication_id",current.publicationId()).containsEntry("runState","COMPLETED")
      .containsEntry("typeCode","PLACE").containsEntry("udpHandoffConfirmed",true).containsEntry("recoveryAction","TERMINAL");
    return run;
  }
  private Map<String,Object> status(){return operations.status(source,1,human).getFirst();}
  private Map<String,Object> latestHistory(){return operations.history(source,null,10,human).getFirst();}
  private Instant runPausedAt(UUID run){
    return db.sql("select paused_at from ouf_ingestion.ing_run where run_id=:r")
      .param("r",run).query(Timestamp.class).single().toInstant();
  }
  private Instant discoveryExpiresAt(){
    return db.sql("select discovery_verified_at+interval '5 minutes' from ouf_ingestion.ing_schedule where source_id=:s")
      .param("s",source).query(Timestamp.class).single().toInstant();
  }
  private List<Map<String,Object>> repeatedOperationalReads(){
    return List.of(status(),latestHistory(),status(),latestHistory());
  }
  private void assertHumanDeadline(String action, Instant expected){
    for(var row:repeatedOperationalReads()){
      assertThat(row).containsEntry("recoveryAction",action).containsEntry("decisionDeadline",expected.toString())
        .doesNotContainKeys("reviewUrl","run_paused_at","run_budget_deadline","schedule_paused_at","schedule_incident_at");
    }
  }
  private RunLifecycleService controls(){return new RunLifecycleService(db,json,refs->{},transactions);}
  private long count(String table){return db.sql("select count(*) from ouf_ingestion."+table).query(Long.class).single();}

  @Test @DisplayName("ING-51F-PINNED")
  void operationalReadsKeepTheCanonicalTypeOfEachRun(){
    String poi="https://w3id.org/italia/onto/POI/PointOfInterest";
    publish(1,firstCsv,Map.of(),poi);
    UUID completed=finish(dispatch().orElseThrow());
    for(var row:List.of(status(),latestHistory())){
      assertThat(row).containsEntry("runState","COMPLETED").containsEntry("canonicalType",poi).containsEntry("canonicalTypes",List.of(poi))
        .containsEntry("typeCode","PLACE").containsEntry("udpHandoffConfirmed",true);
    }

    String station="https://example.test/Station";
    publish(2,firstCsv,Map.of(),station);
    for(var row:List.of(status(),latestHistory()))assertThat(row).doesNotContainKey("canonicalType");
    UUID paused=dispatch().orElseThrow();
    controls().pause(paused,0,human,"pause");
    for(var row:List.of(status(),latestHistory())){
      assertThat(row).containsEntry("job_ref",paused).containsEntry("runState","PAUSED")
        .containsEntry("canonicalType",station).containsEntry("udpHandoffConfirmed",false).doesNotContainKey("typeCode");
    }
    assertThat(operations.history(source,null,10,human).getLast())
      .containsEntry("job_ref",completed).containsEntry("canonicalType",poi);
  }

  @Test @DisplayName("ING-51F-LEGACY")
  void completedRunWithoutAPublishedClassOmitsCanonicalType(){
    publish(1,firstCsv);
    finish(dispatch().orElseThrow());
    for(var row:List.of(status(),latestHistory())){
      assertThat(row).containsEntry("runState","COMPLETED").containsEntry("typeCode","PLACE")
        .doesNotContainKeys("canonicalType","canonicalTypes","canonical_types");
    }
  }

  @ParameterizedTest @EnumSource(RecoveryAction.RunState.class)
  void operationalRecoveryCoversEveryRunStateAndFailureClass(RecoveryAction.RunState runState){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    for(String code:Arrays.asList(null,"ING_FUTURE_FAILURE","ING_EXECUTION_GATEWAY_503","ING_BUNDLE_SNAPSHOT_INVALID",
        "ING_RECORD_QUARANTINED","ING_ACTIVE_PUBLICATION_CHANGED","ING_ACTIVATION_SUPERSEDED",
        "ING_RECOVERY_BUDGET_EXHAUSTED","ING_HANDOFF_BLOCKED","ING_LAKE_RECONCILE_REQUIRED","ING_OPERATOR_ABORTED")){
      db.sql("update ouf_ingestion.ing_run set state=:state,failure_code=:code where run_id=:r")
        .param("state",runState.name()).param("code",code).param("r",run).update();
      String action=switch(runState){
        case READY, PREFLIGHT, RUNNING, DRAINING -> "NONE";
        case SUCCEEDED, COMPLETED_WITH_WARNINGS, FAILED, ABORTED -> "TERMINAL";
        case PAUSED -> "ING_LAKE_RECONCILE_REQUIRED".equals(code)||"ING_HANDOFF_BLOCKED".equals(code)
          ? "HUMAN_ABORT_REQUIRED" : "HUMAN_RESUME_OR_ABORT";
      };
      Instant expected = runState != RecoveryAction.RunState.PAUSED ? null : runPausedAt(run).plus(DECISION_WINDOW);
      for(var row:repeatedOperationalReads()){
        assertThat(row).as("%s / %s",runState,code).containsEntry("runState",runState.name())
          .containsEntry("recoveryAction",action).containsEntry("udpHandoffConfirmed",false).doesNotContainKeys("retryAt","typeCode","reviewUrl");
        if(runState==RecoveryAction.RunState.PAUSED){
          assertThat(row).containsEntry("runId",run).containsEntry("expectedVersion",0L)
            .containsEntry("decisionDeadline",expected.toString());
        }else assertThat(row).doesNotContainKeys("runId","expectedVersion","decisionDeadline");
      }
    }
  }

  @Test void scheduleWithoutRunProjectsActivePausedDisabledAndLegacyRetry(){
    publish(1,firstCsv);
    assertThat(status()).containsEntry("runState","AWAITING_START").containsEntry("recoveryAction","NONE")
      .doesNotContainKeys("decisionDeadline","scheduleDecisionDeadline");
    db.sql("update ouf_ingestion.ing_schedule set activation_failures=1,activation_first_failure_at=clock_timestamp(),next_run_at=clock_timestamp()+interval '2 hours'").update();
    var retry=status();
    assertThat(retry).containsEntry("recoveryAction","AUTOMATIC_RETRY").containsEntry("retryAt",retry.get("scheduleRetryAt"));
    assertThat(retry).doesNotContainKeys("decisionDeadline","scheduleDecisionDeadline");
    assertThat(Instant.parse((String)retry.get("retryAt"))).isAfter(Instant.now());
    assertThat(dispatch()).isEmpty();
    db.sql("update ouf_ingestion.ing_schedule set state='PAUSED',activation_blocked=true").update();
    for(var row:repeatedOperationalReads()) assertThat(row).containsEntry("runState","BLOCKED")
      .containsEntry("recoveryAction","NEW_PUBLICATION_REQUIRED").doesNotContainKeys("retryAt","reviewUrl","decisionDeadline","scheduleDecisionDeadline");
    db.sql("update ouf_ingestion.ing_schedule set activation_blocked=false").update();
    Instant pausedAt=db.sql("select paused_at from ouf_ingestion.ing_schedule where source_id=:s")
      .param("s",source).query(Timestamp.class).single().toInstant();
    for(var row:repeatedOperationalReads()) assertThat(row)
      .containsEntry("recoveryAction","HUMAN_SCHEDULE_REVIEW").containsEntry("scheduleRecoveryAction","HUMAN_SCHEDULE_REVIEW")
      .containsEntry("decisionDeadline",pausedAt.plus(DECISION_WINDOW).toString())
      .containsEntry("scheduleDecisionDeadline",row.get("decisionDeadline")).doesNotContainKeys("runId","expectedVersion","reviewUrl");
    db.sql("update ouf_ingestion.ing_schedule set state='DISABLED'").update();
    for(var row:repeatedOperationalReads()) assertThat(row).containsEntry("runState","DISABLED").containsEntry("recoveryAction","TERMINAL")
      .doesNotContainKeys("decisionDeadline","scheduleDecisionDeadline");
  }

  @Test void humanDeadlineUsesTheMinimumOfDurableSessionAndPartitionBudgets(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    var lifecycle=controls();
    lifecycle.pause(run,0,human,"pause");
    var configuredDeadline=runPausedAt(run).plus(DECISION_WINDOW);
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",configuredDeadline);
    var defaults=new OperationalAwarenessService(db);
    assertThat(defaults.status(source,1,human).getFirst()).containsEntry("decisionDeadline",runPausedAt(run).plus(Duration.ofHours(24)).toString());

    lifecycle.resume(run,1,human,"resume",new RunRecoveryBudget.Request(10,7200,"DEPENDENCY_REPAIRED"));
    lifecycle.pause(run,2,human,"pause-with-active-session");
    var pausedAt=runPausedAt(run);
    var sessionDeadline=db.sql("select deadline_at from ouf_ingestion.run_recovery_session where run_id=:r and state='ACTIVE'")
      .param("r",run).query(Timestamp.class).single().toInstant();
    assertThat(sessionDeadline).isBefore(pausedAt.plus(DECISION_WINDOW));
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",sessionDeadline);
    lifecycle.pause(run,3,human,"pause-again");
    assertThat(runPausedAt(run)).isEqualTo(pausedAt);
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",sessionDeadline);

    var partitionDeadline=db.sql("""
      update ouf_ingestion.ing_partition set retry_started_at=clock_timestamp()-interval '25 hours',retry_attempts=1
      where run_id=:r returning retry_started_at+interval '24 hours'
      """).param("r",run).query(Timestamp.class).single().toInstant();
    assertThat(partitionDeadline).isBefore(sessionDeadline);
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",partitionDeadline);
    assertThat(lifecycle.abort(run,4,human,"abort-after-deadline")).containsEntry("state","ABORTED");
    for(var row:repeatedOperationalReads()) assertThat(row).doesNotContainKeys("decisionDeadline","scheduleDecisionDeadline");
  }

  @Test void pendingHandoffBudgetCapsTheHumanWindow(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    assertThat(acquire()).contains(run);
    var deadline=db.sql("""
      update ouf_ingestion.handoff_outbox set delivery_started_at=clock_timestamp()-interval '23 hours'
      where run_id=:r returning delivery_started_at+interval '24 hours'
      """).param("r",run).query(Timestamp.class).single().toInstant();
    controls().pause(run,0,human,"pause-before-delivery");
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",deadline);
  }

  @Test void activeScheduleUsesTheBlockingPauseForItsOwnReviewDeadline(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    controls().pause(run,0,human,"pause");
    db.sql("update ouf_ingestion.ing_schedule set state='ACTIVE' where source_id=:s").param("s",source).update();
    db.sql("update ouf_ingestion.ing_run set failure_code='ING_RECOVERY_BUDGET_EXHAUSTED' where run_id=:r")
      .param("r",run).update();
    var deadline=runPausedAt(run).plus(DECISION_WINDOW);
    for(var row:repeatedOperationalReads()) assertThat(row).containsEntry("recoveryAction","HUMAN_RESUME_OR_ABORT")
      .containsEntry("decisionDeadline",deadline.toString())
      .containsEntry("scheduleRecoveryAction","HUMAN_SCHEDULE_REVIEW").containsEntry("scheduleDecisionDeadline",deadline.toString());
    db.sql("update ouf_ingestion.ing_run set state='FAILED' where run_id=:r").param("r",run).update();
    for(var row:repeatedOperationalReads()) assertThat(row).containsEntry("recoveryAction","TERMINAL")
      .containsEntry("scheduleDecisionDeadline",deadline.toString()).doesNotContainKey("decisionDeadline");
  }

  @Test void quarantineDecisionUsesTheResumeGuardAndVersion(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    controls().pause(run,0,human,"pause");
    db.sql("""
      insert into ouf_ingestion.ing_quarantine(quarantine_id,run_id,reason_code,evidence_ref,correlation_id)
      values(:q,:r,'ING_RECORD_QUARANTINED','test-quarantine','test-correlation')
      """).param("q",UUID.randomUUID()).param("r",run).update();
    assertThat(status()).containsEntry("recoveryAction","HUMAN_QUARANTINE_REVIEW").containsEntry("expectedVersion",1L);
    assertHumanDeadline("HUMAN_QUARANTINE_REVIEW",runPausedAt(run).plus(DECISION_WINDOW));
    assertThatThrownBy(()->controls().resume(run,1,human,"blocked")).hasMessage("ING_QUARANTINE_RETRY_REQUIRED");
  }

  @Test void staleDiscoveryWaitsForVerificationOfTheSamePublication(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    controls().pause(run,0,human,"pause");
    db.sql("update ouf_ingestion.ing_run set failure_code='ING_ACTIVE_PUBLICATION_CHANGED' where run_id=:r")
      .param("r",run).update();
    // Discovery freshness is a precondition renewed by the activation loop: it never shortens the decision window.
    var decisionDeadline=runPausedAt(run).plus(DECISION_WINDOW);
    var originalDiscovery=discoveryExpiresAt();
    assertThat(originalDiscovery).isBefore(decisionDeadline);
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",decisionDeadline);
    db.sql("update ouf_ingestion.ing_schedule set discovery_verified_at=clock_timestamp()+interval '1 second'").update();
    assertThat(discoveryExpiresAt()).isAfter(originalDiscovery);
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",decisionDeadline);
    db.sql("update ouf_ingestion.ing_schedule set discovery_verified_at=clock_timestamp()-interval '10 minutes'").update();
    for(var row:repeatedOperationalReads()) assertThat(row).containsEntry("recoveryAction","WAITING_FOR_PUBLICATION_VERIFICATION")
      .containsEntry("expectedVersion",1L).doesNotContainKeys("retryAt","decisionDeadline");
    assertThatThrownBy(()->controls().resume(run,1,human,"stale")).hasMessage("ING_ACTIVE_PUBLICATION_CHANGED");
    db.sql("update ouf_ingestion.ing_schedule set discovery_verified_at=clock_timestamp()").update();
    assertHumanDeadline("HUMAN_RESUME_OR_ABORT",decisionDeadline);
  }

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
    assertThat(waiting).containsEntry("run_state","RETRY_WAIT").containsEntry("publication_id",p2.publicationId())
      .containsEntry("recoveryAction","AUTOMATIC_RETRY").containsEntry("retryAt",waiting.get("scheduleRetryAt"));
    assertThat(Instant.parse((String)waiting.get("retryAt"))).isAfter(Instant.now());
    UUID run=(UUID)waiting.get("job_ref");assertThat(run).isNotNull();
    activation.reconcile(p2);assertThat(dispatch()).isEmpty();
    assertThat(db.sql("select next_run_at>=transaction_timestamp()+interval '2 hours' from ouf_ingestion.ing_schedule").query(Boolean.class).single()).isTrue();
    // Advance the persisted timer (no sleeps or operator transition).
    db.sql("update ouf_ingestion.ing_schedule set next_run_at=clock_timestamp(),activation_first_failure_at=clock_timestamp()-interval '2 hours'").update();
    assertThat(dispatch()).contains(run);finish(run);assertThat(count("ing_run")).isEqualTo(2);
  }

  @ParameterizedTest @ValueSource(strings={"400","401","403","404","413","422"})
  void lakeRefusalBeforeAnyEffectLeavesNoAmbiguousIntentAndResumes(String status){
    publish(1,firstCsv);UUID run=dispatch().orElseThrow();
    var refused=new AtomicInteger();
    lake=(zone,r,o,bytes,hash,key)->{
      if(zone==RuntimePorts.DataLakePort.Zone.CURATED && refused.getAndIncrement()==0)
        throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_"+status,
            Set.of("401","403").contains(status)?AdapterSpi.ErrorClass.AUTH_ROUTE:AdapterSpi.ErrorClass.CONFIGURATION);
      return new RuntimePorts.DataLakePort.Receipt("lake:"+key,true);
    };
    assertThat(acquire()).contains(run);
    assertThat(db.sql("select count(*) from ouf_ingestion.lake_write_evidence where run_id=:r and receipt_ref is null")
        .param("r",run).query(Long.class).single()).isZero();
    assertThat(status()).doesNotContainEntry("lake_next_action","ABORT_REQUIRED")
        .doesNotContainEntry("error_code","ING_LAKE_RECONCILE_REQUIRED");
    controls().resume(run,0,human,"after-refusal");
    finish(run);
    assertThat(status()).containsEntry("lake_next_action","NONE");
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
      String action=i==0 ? "AUTOMATIC_RETRY" : "AUTOMATIC_RECONCILIATION";
      for(var row:repeatedOperationalReads()) assertThat(row).containsEntry("recoveryAction",action).containsKey("retryAt")
        .doesNotContainKeys("decisionDeadline","scheduleDecisionDeadline");
      assertThat(acquire()).isEmpty();assertThat(count("ing_quarantine")).isZero();assertThat(count("processing_attempt")).isZero();
      db.sql("update ouf_ingestion.ing_partition set retry_at=clock_timestamp() where run_id=:r").param("r",run).update();
      db.sql("update ouf_ingestion.source_health set retry_not_before=clock_timestamp() where source_id=:s").param("s",source).update();
    }
    finish(run);assertThat(calls).containsExactly("RAW","NORMALIZED","NORMALIZED","CURATED");
    assertThat(normalized.getFirst()).isEqualTo(normalized.getLast());assertThat(count("ing_run")).isOne();
  }

  @TempDir Path directory;

  @Test @DisplayName("ING-51B2-ATOMIC")
  void oneUnresolvablePinBlocksAdmissionBeforeAnyLakeOrHandoffEffect() throws Exception {
    try(var peer=new SemanticGatewayFixture(directory)){
      var pins=SemanticGatewayFixture.pins(1024);
      var envelope=PublishedActivationTest.envelope(source,true,1);
      var published=PublishedActivation.map(envelope,"bundle");
      published.put("semanticReferenceBindings",pins);
      published.put("semanticRefs",SemanticGatewayFixture.refs(pins));
      PublishedActivationTest.hash(published);
      envelope.put("checksum",published.get("checksum"));
      peer.server.createContext("/api/onboarding/v1/runtime/publications/",e->{try(e){peer.reply(e,200,envelope);}});
      var client=peer.client(Map.of("reference-batch-items","512"));
      current=client.decode(envelope);
      activation.reconcile(current);
      peer.revoked=SemanticGatewayFixture.id(700);
      var writes=new AtomicInteger();
      lake=(zone,run,object,content,hash,key)->{
        writes.incrementAndGet();return new RuntimePorts.DataLakePort.Receipt("lake:"+key,true);
      };
      assertThat(new RunCoordinator(state,client,client).dispatchOne("preflight")).isEmpty();
      assertThat(peer.batches.stream().map(List::size)).containsExactly(512,512);
      var item=Map.of("code","ING_SEMANTIC_REFERENCE_UNRESOLVABLE","index",700,
          "semanticId",peer.revoked,"revisionId",pins.get(700).get("revisionId"));
      for(var row:List.of(status(),latestHistory())){
        assertThat(row).containsEntry("run_state","PAUSED").containsEntry("error_code","ING_SEMANTIC_BINDING_MISMATCH")
            .containsEntry("semanticReference",item);
      }
      assertThat(acquire()).isEmpty();
      assertThat(writes).hasValue(0);
      assertThat(count("lake_write_evidence")).isZero();
      assertThat(count("checkpoint_history")).isZero();
      assertThat(count("handoff_outbox")).isZero();
      assertThat(deliver()).isFalse();
      assertThat(receipts).isEmpty();
    }
  }

  @DisplayName("ING-51B2-PROGRESS / ING-51B-SCALE / ING-51B-REVOKE")
  @ParameterizedTest @ValueSource(booleans={false,true})
  @Transactional(propagation=org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
  void widePublicationResumesOnlyAfterRecheckingEveryPin(boolean revoke) throws Exception {
    long started=System.nanoTime();
    try(var peer=new SemanticGatewayFixture(directory)){
      var pins=SemanticGatewayFixture.pins(1024);
      var fields=new ArrayList<String>();
      var mappings=new ArrayList<Map<String,String>>();
      for(int i=0;i<pins.size();i++){
        String field=i==0?"id":"column"+i;
        fields.add(field);
        mappings.add(Map.of("sourceField",field,"targetPropertyIri",SemanticGatewayFixture.id(i),"transform","IDENTITY"));
      }
      String cells=String.join(";",Collections.nCopies(1023,"value"));
      byte[] csv=(String.join(";",fields)+"\n1;"+cells+"\n2;"+cells+"\n").getBytes(StandardCharsets.UTF_8);
      String hash=ExecutionGatewayClient.hash(csv);
      assets.put(hash,csv);
      var candidate=new FrozenConfigurationProbeTest().candidate();
      var cfg=PublishedActivation.map(candidate,"configuration");
      candidate.put("sourceId",source);
      cfg.put("bundle",Map.of("bundleId","wide","bundleVersion","1","source",
          Map.of("sourceId",source,"sourceKind","INTERNAL_MANAGED","acquisitionMode","MANAGED")));
      var runtime=PublishedActivation.map(PublishedActivation.map(cfg,"extractionProfile"),"runtime");
      runtime.put("contentHash",hash);
      var executionProfile=PublishedActivation.map(runtime,"execution");
      executionProfile.put("expectedSize",csv.length);executionProfile.put("maxColumns",1024);
      cfg.put("extractionProfile",Map.of("runtime",runtime,"projection",Map.of("PLACE",fields)));
      cfg.put("semanticMapping",Map.of("sourceType",Map.of("sourceId",source,"typeCode","PLACE"),
          "semanticRefs",SemanticGatewayFixture.refs(pins),"propertyMappings",mappings));
      cfg.put("semanticReferenceBindings",pins);
      candidate.put("configurationHash",FrozenConfigurationProbe.configurationHash(json,cfg));
      var client=peer.client(Map.of());
      peer.revoked=SemanticGatewayFixture.id(1010);
      var probe=new FrozenConfigurationProbe(json,objects,client);
      assertThatThrownBy(()->probe.probe(candidate)).isInstanceOfSatisfying(ValuePins.BindingFailure.class,
          failure->{
            var expected=Map.of("code","ING_SEMANTIC_REFERENCE_UNRESOLVABLE","index",1010,
                "semanticId",peer.revoked,"revisionId",pins.get(1010).get("revisionId"));
            assertThat(failure.detail()).isEqualTo(expected);
            assertThat(new OnboardingReadinessApi(null,"tenant-a").invalidReference(failure).getBody())
                .containsEntry("code","ING_SEMANTIC_BINDING_MISMATCH").containsEntry("semanticReference",expected);
          });
      assertThat(count("handoff_outbox")).isZero();
      peer.revoked=null;peer.batches.clear();
      var proof=probe.probe(candidate);
      assertThat(proof).containsEntry("validatedRows",2L).containsEntry("semanticBindingCount",1024);

      var envelope=PublishedActivationTest.envelope(source,true,1);
      var published=PublishedActivation.map(envelope,"bundle");
      for(String section:List.of("extractionProfile","semanticMapping","semanticReferenceBindings","sourceObjectIdentityPolicy","changeRepresentationProfile","dataAccessPolicies"))
        published.put(section,cfg.get(section));
      published.put("semanticRefs",SemanticGatewayFixture.refs(pins));
      PublishedActivationTest.hash(published);envelope.put("checksum",published.get("checksum"));
      peer.server.createContext("/api/onboarding/v1/runtime/publications/",e->{try(e){peer.reply(e,200,envelope);}});
      current=client.decode(envelope);
      activation.reconcile(current);
      var snapshot=current.execution();
      client.loadHistoricalAndVerify(source,snapshot.bundleId()+":"+snapshot.bundleVersion()+":"+snapshot.checksum());
      UUID run=new RunCoordinator(state,client,client).dispatchOne("wide-preflight").orElseThrow();
      var effects=new java.util.concurrent.ConcurrentHashMap<String,String>();
      var calls=new AtomicInteger();
      peer.server.createContext("/api/internal/v1/lake/objects",e->{try(e){
        var body=json.readTree(e.getRequestBody());
        // First attempt fails before any checkpoint. Next attempt commits one full record before the second 429.
        int call=calls.incrementAndGet();
        if(call==1 || call==5){
          if(call==1 || revoke)e.getResponseHeaders().set("Retry-After",call==1?"1":"5");
          else{
            e.getResponseHeaders().set("X-RateLimit-Limit","60");
            e.getResponseHeaders().set("X-RateLimit-Remaining","0");
            e.getResponseHeaders().set("X-RateLimit-Reset","12.5");
          }
          peer.reply(e,429,Map.of());return;
        }
        String key=body.required("idempotencyKey").asText(), digest=body.required("contentHash").asText();
        if(effects.putIfAbsent(key,digest)!=null){peer.reply(e,409,Map.of());return;}
        peer.reply(e,201,Map.of("durable",true,"objectRef","lake:"+key,"contentHash",digest));
      }});
      lake=new ExecutionGatewayClient(json,execution,peer.url,peer.token.toString(),new GatewayPacing(k->k.endsWith("per-minute")?"60000":k.endsWith("server-wait-max-seconds")?"14":null));
      var worker=new RunExecutionWorker(execution,b->new ManagedTabularAdapter(objects),
          new CanonicalRecordPipeline(json,contracts,durable,quarantine,lake),schemas,state,client::verifyForRetry);
      assertThat(worker.executeOne("wide-1")).contains(run);
      assertThat(count("checkpoint_history")).isZero();
      assertThat(db.sql("select consecutive_failures from ouf_ingestion.source_health where source_id=:s").param("s",source).query(Integer.class).single()).isOne();
      var origin=db.sql("select retry_started_at from ouf_ingestion.ing_partition where run_id=:r").param("r",run).query(Timestamp.class).single();
      assertThat(awaitWideRetry(worker,"wide-2")).contains(run);
      assertThat(worker.executeOne("too-early")).isEmpty();
      assertThat(count("checkpoint_history")).isOne();
      assertThat(count("handoff_outbox")).isOne();
      assertThat(effects).hasSize(3);
      assertThat(status()).containsEntry("consecutive_failures",0);
      assertThat(db.sql("select consecutive_failures,retry_attempts_in_window from ouf_ingestion.source_health where source_id=:s")
          .param("s",source).query().singleRow()).containsEntry("consecutive_failures",0).containsEntry("retry_attempts_in_window",2);
      assertThat(db.sql("""
          select p.retry_attempts,p.retry_started_at, h.retry_not_before between h.last_failure_at+(:low*interval '1 second')
              and h.last_failure_at+(:high*interval '1 second') and p.retry_at between h.retry_not_before
              and clock_timestamp()+(:high*interval '1 second') short_delay
          from ouf_ingestion.ing_partition p join ouf_ingestion.ing_run r using(run_id)
          join ouf_ingestion.source_health h on h.source_id=r.source_id where p.run_id=:r
          """).param("r",run).param("low",revoke?5:13).param("high",revoke?6:14).query().singleRow()).containsEntry("retry_attempts",2).containsEntry("retry_started_at",origin).containsEntry("short_delay",true);
      if(revoke)peer.revoked=SemanticGatewayFixture.id(1010);
      assertThat(awaitWideRetry(worker,"wide-3")).contains(run);
      assertThat(peer.batches.stream().map(List::size)).containsExactly(1000,24,1000,24,1000,24,1000,24,1000,24);
      if(revoke){
        assertThat(status()).containsEntry("run_state","PAUSED").containsEntry("error_code","ING_SEMANTIC_BINDING_MISMATCH")
            .containsEntry("semanticReference",Map.of("code","ING_SEMANTIC_REFERENCE_UNRESOLVABLE","index",1010,"semanticId",peer.revoked,"revisionId",pins.get(1010).get("revisionId")));
        assertThat(effects).hasSize(3);assertThat(count("handoff_outbox")).isOne();
      }else{
        assertThat(deliver()).isTrue();assertThat(deliver()).isTrue();assertThat(deliver()).isFalse();
        execution.completeDrained();
        assertThat(status()).containsEntry("run_state","SUCCEEDED");
        assertThat(effects).hasSize(6);assertThat(receipts).hasSize(2).doesNotHaveDuplicates();
        assertThat(count("checkpoint_history")).isEqualTo(2);
      }
      System.out.printf("D-082 ING-51B-%s: 1024 columns/pins, %d batch POST, %.3f s%n",
          revoke?"REVOKE":"SCALE",peer.batches.size(),(System.nanoTime()-started)/1e9);
    }finally{
      db.sql("truncate ouf_ingestion.ing_schedule,ouf_ingestion.ing_run cascade").update();
      db.sql("delete from ouf_ingestion.source_health where source_id=:s").param("s",source).update();
    }
  }

  private Optional<UUID> awaitWideRetry(RunExecutionWorker worker,String owner)throws InterruptedException{
    long deadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();
    do{
      var claimed=worker.executeOne(owner);
      if(claimed.isPresent())return claimed;
      Thread.sleep(100);
    }while(System.nanoTime()<deadline);
    throw new AssertionError("Partition did not resume within its published delay");
  }

  private void retryDue(){
    db.sql("update ouf_ingestion.ing_partition set retry_at=clock_timestamp()").update();
    db.sql("update ouf_ingestion.source_health set retry_not_before=clock_timestamp(),circuit_open_until=null where source_id=:s")
        .param("s",source).update();
  }

  @ParameterizedTest @ValueSource(strings={"close","malformed","non-durable","conflict","wrong-hash","unusable"})
  void lakePostReconcilesOnlyMatchingEvidence(String fault)throws Exception{
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    Path token=directory.resolve("token");
    Files.writeString(token,"fixture-token");
    var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    var writes=new java.util.concurrent.CopyOnWriteArrayList<com.fasterxml.jackson.databind.JsonNode>();
    server.createContext("/api/internal/v1/lake/objects",exchange->{
      try{
        var body=json.readTree(exchange.getRequestBody());
        writes.add(body);
        int status=201;
        byte[] reply=json.writeValueAsBytes(Map.of("durable",true,"objectRef","lake:"+body.path("contentHash").asText(),
            "contentHash",body.path("contentHash").asText()));
        if(writes.size()==3)switch(fault){
          case "close" -> {return;}
          case "malformed" -> reply="{".getBytes(StandardCharsets.UTF_8);
          case "non-durable","wrong-hash" -> reply=json.writeValueAsBytes(Map.of("durable",!fault.equals("non-durable"),
              "objectRef","lake:fixture","contentHash",fault.equals("wrong-hash")?"sha256:"+"0".repeat(64):body.path("contentHash").asText()));
          case "conflict" -> status=409;
          case "unusable" -> {status=500;reply=json.writeValueAsBytes(Map.of("message","UDP_LAKE_DUPLICATE_NOT_USABLE"));}
          default -> throw new AssertionError(fault);
        }
        exchange.sendResponseHeaders(status,reply.length);
        exchange.getResponseBody().write(reply);
      }finally{exchange.close();}
    });
    server.start();
    try{
      lake=new ExecutionGatewayClient(json,execution,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString());
      assertThat(acquire()).contains(run);
      String cause=switch(fault){
        case "close" -> "ING_LAKE_TRANSPORT_UNCONFIRMED";
        case "malformed" -> "ING_EXECUTION_RECEIPT_INVALID";
        case "non-durable" -> "ING_DATALAKE_NOT_DURABLE";
        case "wrong-hash" -> "ING_LAKE_CONTENT_DIVERGED";
        case "conflict" -> "ING_EXECUTION_GATEWAY_409";
        case "unusable" -> "ING_LAKE_OBJECT_UNUSABLE";
        default -> throw new AssertionError(fault);
      };
      assertThat(writes).hasSize(3);
      assertThat(count("handoff_outbox")).isZero();
      assertThat(count("checkpoint_history")).isZero();
      assertThat(status()).containsEntry("lake_cause_code",cause);
      assertThat(operations.history(source,null,10,human).getFirst()).containsEntry("lake_cause_code",cause);
      assertThat(db.sql("""
          select error_code,detail_json->>'partitionKey' partition_key,detail_json->>'sequenceNo' sequence_no,
              detail_json->>'effect' effect from ouf_ingestion.operational_event
          where run_id=:r and event_type='LAKE_WRITE_FAILURE'
          """).param("r",run).query().singleRow()).containsEntry("error_code",cause).containsEntry("partition_key","default")
          .containsEntry("sequence_no","1").containsEntry("effect","curated");
      assertThat(acquire()).isEmpty();
      if(Set.of("conflict","wrong-hash","unusable").contains(fault)){
        assertThat(status()).containsEntry("lake_next_action","ABORT_REQUIRED").containsEntry("recoveryAction","HUMAN_ABORT_REQUIRED");
        assertThatThrownBy(()->controls().resume(run,0,human,"resume")).hasMessage("ING_LAKE_ABORT_REQUIRED");
        controls().abort(run,0,human,"abort");
        assertThat(writes).hasSize(3);
      }else{
        assertThat(status()).containsEntry("lake_next_action","AUTO_RECONCILING");
        retryDue();
        finish(run);
        assertThat(writes).hasSize(4);
        assertThat(writes.get(3)).isEqualTo(writes.get(2));
        assertThat(status()).containsEntry("lake_next_action","NONE").containsEntry("lake_cause_code",cause);
        assertThat(receipts).hasSize(1);
      }
    }finally{server.stop(0);}
  }

  @Test void opaqueServerErrorBlocksAnIdenticalReplayWithoutInventingAnOwnerCause(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    var keys=new ArrayList<String>();
    lake=(zone,r,id,bytes,hash,key)->{
      keys.add(key+" "+hash);
      throw new RuntimePorts.GatewayFailure("ING_EXECUTION_GATEWAY_500",AdapterSpi.ErrorClass.CONFIGURATION);
    };
    assertThat(acquire()).contains(run);
    assertThat(status()).containsEntry("lake_next_action","HUMAN_RESUME_REQUIRED");
    controls().resume(run,0,human,"inspect-and-retry");
    assertThat(acquire()).contains(run);
    assertThat(status()).containsEntry("lake_next_action","ABORT_REQUIRED").containsEntry("lake_cause_code","ING_EXECUTION_GATEWAY_500");
    assertThat(keys).hasSize(2);
    assertThat(keys.getFirst()).isEqualTo(keys.getLast());
    assertThat(count("checkpoint_history")).isZero();
  }

  @Test void connectionRefusalCancelsOnlyAFreshIntent()throws Exception{
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    Path token=directory.resolve("token");
    Files.writeString(token,"fixture");
    int closedPort;
    try(var socket=new java.net.ServerSocket(0,1,java.net.InetAddress.getByName("127.0.0.1"))){
      closedPort=socket.getLocalPort();
    }
    var unavailable=new ExecutionGatewayClient(json,execution,"http://127.0.0.1:"+closedPort,token.toString());
    lake=unavailable;
    assertThat(acquire()).contains(run);
    assertThat(status()).containsEntry("error_code","ING_EXECUTION_CONNECT_FAILED");
    assertThat(count("lake_write_evidence")).isZero();

    retryDue();
    lake=(zone,r,id,bytes,hash,key)->new RuntimePorts.DataLakePort.Receipt("lake:fixture",false);
    assertThat(acquire()).contains(run);
    assertThat(count("lake_write_evidence")).isOne();
    retryDue();
    lake=unavailable;
    assertThat(acquire()).contains(run);
    assertThat(count("lake_write_evidence")).isOne();
    assertThat(status()).containsEntry("lake_next_action","AUTO_RECONCILING");
    assertThat(count("checkpoint_history")).isZero();
  }

  @ParameterizedTest @ValueSource(ints={2,25})
  void workerLossBeforeRetrySchedulingKeepsTheOriginalDeadline(int hours){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    var calls=new ArrayList<String>();
    lake=(zone,r,id,bytes,hash,key)->{
      calls.add(key+" "+hash);
      if(calls.size()==1)throw new AssertionError("worker terminated");
      return new RuntimePorts.DataLakePort.Receipt("lake:"+key,true);
    };
    assertThatThrownBy(this::acquire).isInstanceOf(AssertionError.class).hasMessage("worker terminated");
    db.sql("update ouf_ingestion.ing_partition set lease_until=clock_timestamp()-interval '1 second'").update();
    db.sql("update ouf_ingestion.lake_write_evidence set intent_at=clock_timestamp()-(:hours*interval '1 hour')")
        .param("hours",hours).update();
    assertThat(acquire()).isEmpty();
    assertThat(calls).hasSize(1);
    assertThat(status()).containsEntry("run_state","RETRY_WAIT");
    retryDue();
    if(hours>24){
      assertThat(acquire()).isEmpty();
      assertThat(status()).containsEntry("error_code","ING_RECOVERY_BUDGET_EXHAUSTED")
          .containsEntry("lake_next_action","HUMAN_RESUME_REQUIRED");
      assertThat(calls).hasSize(1);
    }else{
      finish(run);
      assertThat(calls).hasSize(4);
      assertThat(calls.get(1)).isEqualTo(calls.getFirst());
    }
  }

  @Test void exhaustedLakeReplayWaitsForANewHumanBudget(){
    publish(1,firstCsv);
    UUID run=dispatch().orElseThrow();
    var lifecycle=controls();
    lifecycle.pause(run,0,human,"pause");
    lifecycle.resume(run,1,human,"resume",new RunRecoveryBudget.Request(1,3600,"RECOVER"));
    var accepted=lake;
    lake=(zone,r,id,bytes,hash,key)->new RuntimePorts.DataLakePort.Receipt("lake:pending",false);
    assertThat(acquire()).contains(run);
    assertThat(status()).containsEntry("error_code","ING_RECOVERY_BUDGET_EXHAUSTED")
        .containsEntry("lake_cause_code","ING_DATALAKE_NOT_DURABLE").containsEntry("lake_next_action","HUMAN_RESUME_REQUIRED");
    assertThat(acquire()).isEmpty();
    assertThat(count("checkpoint_history")).isZero();
    lifecycle.resume(run,2,human,"renew",new RunRecoveryBudget.Request(1,3600,"LAKE_RECOVERED"));
    lake=accepted;
    retryDue();
    finish(run);
    assertThat(db.sql("select state from ouf_ingestion.run_recovery_session where run_id=:r order by epoch")
        .param("r",run).query(String.class).list()).containsExactly("EXHAUSTED","COMPLETED");
  }

  @ParameterizedTest @ValueSource(booleans={false,true})
  void humanResumesLegacyLakeIntentFromTheSavedCursor(boolean diverged){
    publish(1,"id;name\n1;Museum\n2;Station\n");
    UUID run=dispatch().orElseThrow();
    var keys=new ArrayList<String>();
    var hashes=new ArrayList<String>();
    lake=(zone,r,id,bytes,hash,key)->{
      keys.add(key);hashes.add(hash);
      if(keys.size()==6)throw new IllegalStateException("unclassified fixture failure");
      return new RuntimePorts.DataLakePort.Receipt("lake:"+key,true);
    };
    assertThat(acquire()).contains(run);
    assertThat(count("checkpoint_history")).isOne();
    assertThat(state.run(run)).containsEntry("failure_code","ING_LAKE_RECONCILE_REQUIRED");
    // V21 supplied only the hash, cursor and a null receipt.
    db.sql("update ouf_ingestion.lake_write_evidence set cause_code=null,cause_at=null,intent_at=null,human_review=false where receipt_ref is null").update();
    assertThat(status()).containsEntry("lake_next_action","HUMAN_RESUME_REQUIRED").containsEntry("lake_cause_code","ING_LAKE_LEGACY_CAUSE_UNKNOWN");
    if(diverged)db.sql("update ouf_ingestion.lake_write_evidence set content_hash='sha256:'||repeat('0',64) where receipt_ref is null").update();
    controls().resume(run,0,human,"human-resume",new RunRecoveryBudget.Request(1,3600,"VERIFY_LAKE"));
    assertThat(acquire()).contains(run);
    if(diverged){
      assertThat(keys).hasSize(6);
      assertThat(status()).containsEntry("lake_next_action","ABORT_REQUIRED").containsEntry("lake_cause_code","ING_LAKE_CONTENT_DIVERGED");
      assertThat(count("checkpoint_history")).isOne();
      controls().abort(run,1,human,"abort");
    }else{
      assertThat(keys).hasSize(7);
      assertThat(keys.get(6)).isEqualTo(keys.get(5));
      assertThat(hashes.get(6)).isEqualTo(hashes.get(5));
      assertThat(deliver()).isTrue();assertThat(deliver()).isTrue();
      assertThat(execution.completeDrained()).isOne();
      assertThat(status()).containsEntry("job_ref",run).containsEntry("run_state","SUCCEEDED");
      assertThat(count("checkpoint_history")).isEqualTo(2);
      assertThat(receipts).hasSize(2).doesNotHaveDuplicates();
    }
    assertThat(db.sql("select attempts from ouf_ingestion.run_recovery_session where run_id=:r").param("r",run).query(Integer.class).single()).isOne();
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
    assertThat(status()).containsEntry("recoveryAction","HUMAN_RESUME_OR_ABORT").containsEntry("expectedVersion",1L);
    controls().resume(run,1,human,"redrive",new RunRecoveryBudget.Request(1,3600,"UDP_AVAILABLE"));
    assertThat(status()).containsEntry("recoveryAction","AUTOMATIC_RETRY").containsKey("retryAt");
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
    assertThat(status()).containsEntry("runState","DRAINING").containsEntry("recoveryAction","AUTOMATIC_RECONCILIATION")
      .containsEntry("udpHandoffConfirmed",false).containsKey("retryAt");
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
    assertThat(status()).containsEntry("recoveryAction","NEW_PUBLICATION_REQUIRED").doesNotContainKey("retryAt");
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
