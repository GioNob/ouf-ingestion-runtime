package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.test.context.*;

@SpringBootTest class HumanRecoveryBudgetRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired JdbcClient sql;@Autowired ObjectMapper json;@Autowired PlatformTransactionManager transactions;
  @Autowired RunStateRepository state;@Autowired RunExecutionRepository execution;@Autowired DurablePipelineRepository durable;
  @Autowired QuarantineService quarantine;@Autowired FrozenContractValidator validator;@Autowired SchemaSurveillanceService schemas;
  final TrustedAuthorizationContext.Context human=new TrustedAuthorizationContext.Context("human:operator","HUMAN","tenant-budget",Set.of("ingestion.run.resume"),"decision://budget");
  @BeforeEach void reset(){sql.sql("truncate table ouf_ingestion.ing_run cascade").update();sql.sql("update ouf_ingestion.runtime_pressure_policy set state='NORMAL',global_running_limit=128 where policy_key='GLOBAL'").update();}
  RunLifecycleService controls(){return new RunLifecycleService(sql,json,refs->{assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();},transactions);}
  UUID paused(){String source="budget-"+UUID.randomUUID();UUID run=state.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),human.tenantId(),source,300,"fixture"),bundle(source),"corr-budget");state.preflightSucceeded(run);sql.sql("update ouf_ingestion.ing_run set state='PAUSED',created_at='1000-01-01T00:00:00Z' where run_id=:r").param("r",run).update();sql.sql("update ouf_ingestion.ing_partition set state='PAUSED',checkpoint_json='{\"ordinal\":7}',committed_watermark_json='{\"ordinal\":5}' where run_id=:r").param("r",run).update();return run;}
  void resume(UUID run,long version,int attempts,int seconds){controls().resume(run,version,human,"corr-human",new RunRecoveryBudget.Request(attempts,seconds,"DEPENDENCY_REPAIRED"));}
  List<Map<String,Object>> sessions(UUID run){return sql.sql("select session_id,epoch,state,attempts,max_attempts,authorization_decision_ref from ouf_ingestion.run_recovery_session where run_id=:r order by epoch").param("r",run).query().listOfRows();}

  @Test void humanGrantsNewBudgetAfterExhaustionWithoutResettingHistoryOrProgress(){
    UUID run=paused();resume(run,0,1,3600);var claim=execution.claim("first",Duration.ofMinutes(2)).orElseThrow();execution.release(claim);
    assertThat(execution.claim("exhaust",Duration.ofMinutes(2))).isEmpty();assertThat(state.run(run)).containsEntry("state","PAUSED").containsEntry("failure_code","ING_RECOVERY_BUDGET_EXHAUSTED");
    resume(run,1,2,3600);var rows=sessions(run);assertThat(rows).hasSize(2);assertThat(rows.getFirst()).containsEntry("state","EXHAUSTED").containsEntry("attempts",1);assertThat(rows.getLast()).containsEntry("state","ACTIVE").containsEntry("attempts",0).containsEntry("max_attempts",2);
    assertThat(rows.getFirst().get("session_id")).isNotEqualTo(rows.getLast().get("session_id"));assertThat(sql.sql("select checkpoint_json::text||committed_watermark_json::text from ouf_ingestion.ing_partition where run_id=:r").param("r",run).query(String.class).single()).contains("7","5");
    assertThat(sql.sql("select count(*) from ouf_ingestion.audit_event where resource_id=:r and event_type='RUN_RESUMED' and jsonb_exists(detail_json,'recoverySessionId')").param("r",run.toString()).query(Long.class).single()).isEqualTo(2);
  }
  @Test void concurrentHumanCommandsCreateExactlyOneSession() throws Exception {
    UUID run=paused();CountDownLatch preflights=new CountDownLatch(2);
    RunLifecycleService service=new RunLifecycleService(sql,json,refs->{preflights.countDown();try{if(!preflights.await(5,TimeUnit.SECONDS))throw new IllegalStateException("fixture-timeout");}catch(InterruptedException e){throw new IllegalStateException(e);}},transactions);
    try(ExecutorService pool=Executors.newFixedThreadPool(2)){
      Callable<String> command=()->{try{service.resume(run,0,human,"corr-race");return "GRANTED";}catch(IllegalStateException e){return e.getMessage();}};
      Future<String> a=pool.submit(command),b=pool.submit(command);assertThat(List.of(a.get(10,TimeUnit.SECONDS),b.get(10,TimeUnit.SECONDS))).contains("GRANTED").containsAnyOf("ING_RUN_STATE_CONFLICT","ING_RUN_NOT_PAUSED");
    }
    assertThat(sessions(run)).hasSize(1);assertThat(sql.sql("select control_version from ouf_ingestion.ing_run where run_id=:r").param("r",run).query(Long.class).single()).isOne();
  }
  @Test void staleClaimCannotPublishAfterHumanPauseAndNewGrant(){
    UUID run=paused();resume(run,0,2,3600);var old=execution.claim("old",Duration.ofMinutes(2)).orElseThrow();controls().pause(run,1,human,"corr-pause");resume(run,2,2,3600);
    var current=execution.claim("new",Duration.ofMinutes(2)).orElseThrow();assertThat(current.generation()).isGreaterThan(old.generation());
    assertThatThrownBy(()->durable.requireRecoveryBudget(old)).hasMessage("ING_PARTITION_LEASE_LOST");durable.requireRecoveryBudget(current);
    assertThat(sessions(run).getFirst()).containsEntry("state","SUPERSEDED").containsEntry("attempts",1);
  }
  @Test void cumulativeAttemptsAndOneIncidentSurviveRepeatedHumanRenewal(){
    UUID run=paused();for(int i=0;i<3;i++){resume(run,i,1,3600);var c=execution.claim("cycle",Duration.ofMinutes(2)).orElseThrow();execution.release(c);assertThat(execution.claim("limit",Duration.ofMinutes(2))).isEmpty();}
    assertThat(sql.sql("select sum(attempts) from ouf_ingestion.run_recovery_session where run_id=:r").param("r",run).query(Long.class).single()).isEqualTo(3);
    assertThat(sql.sql("select count(*) from ouf_ingestion.runtime_issue where run_id=:r and issue_code='ING_RECOVERY_BUDGET_EXHAUSTED' and status='OPEN'").param("r",run).query(Long.class).single()).isOne();
  }
  @Test void expiredSessionDeniesAdmissionWithoutResetOnRepositoryRecreation() throws Exception {
    UUID run=paused();resume(run,0,5,1);Thread.sleep(1100);
    var restarted=new org.springframework.transaction.support.TransactionTemplate(transactions).execute(tx->new RunExecutionRepository(sql,json).claim("restart",Duration.ofMinutes(2)));
    assertThat(restarted).isEmpty();assertThat(sessions(run).getFirst()).containsEntry("state","EXHAUSTED").containsEntry("attempts",0);
    assertThat(state.run(run)).containsEntry("state","PAUSED");
  }
  @Test void lateRemoteResultCannotStageOrAdvanceCheckpoint() throws Exception {
    UUID run=paused();resume(run,0,5,1);var claim=execution.claim("slow",Duration.ofMinutes(2)).orElseThrow();int[] calls={0};
    RuntimePorts.DataLakePort slow=(zone,r,o,bytes,hash,key)->{calls[0]++;assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();sql.sql("select session_id from ouf_ingestion.run_recovery_session where run_id=:r for update nowait").param("r",run).query(UUID.class).single();try{Thread.sleep(1100);}catch(InterruptedException e){throw new IllegalStateException(e);}return new RuntimePorts.DataLakePort.Receipt("lake:durable",true);};
    CanonicalRecordPipeline pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,slow);
    var command=new CanonicalRecordPipeline.Command(run,"default",8,1,"test-adapter",execution.bundle(run),new AdapterSpi.SourceRecord("object",8,Map.of("name","Town Hall"),Map.of("managedObjectRef","managed:object")),"corr-late",Map.of("ordinal",8),Map.of("ordinal",8),claim);
    assertThatThrownBy(()->pipeline.process(command)).hasMessage("ING_RECOVERY_BUDGET_EXHAUSTED");assertThat(calls[0]).isOne();
    assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(Long.class).single()).isZero();assertThat(sql.sql("select checkpoint_json->>'ordinal' from ouf_ingestion.ing_partition where run_id=:r").param("r",run).query(String.class).single()).isEqualTo("7");
    execution.pause(claim,"ING_RECOVERY_BUDGET_EXHAUSTED");assertThat(sessions(run).getFirst()).containsEntry("state","EXHAUSTED");
  }
  @Test void serviceDeniesAiMissingCapabilityAndForeignTenantBeforeGrant(){
    UUID run=paused();for(var actor:List.of(new TrustedAuthorizationContext.Context("ai","AI",human.tenantId(),human.capabilities(),"decision://ai"),new TrustedAuthorizationContext.Context("human","HUMAN",human.tenantId(),Set.of(),"decision://denied"))){assertThatThrownBy(()->controls().resume(run,0,actor,"corr-denied")).hasMessage("ING_HUMAN_RECOVERY_AUTHORITY_REQUIRED");}
    var foreign=new TrustedAuthorizationContext.Context("human","HUMAN","other-tenant",human.capabilities(),"decision://foreign");assertThatThrownBy(()->controls().resume(run,0,foreign,"corr-foreign")).hasMessage("ING_RUN_NOT_PAUSED");assertThat(sessions(run)).isEmpty();
  }
  @Test void failedBaselinePreflightCreatesNoSessionAndLeavesRunPaused(){
    UUID run=paused();RunLifecycleService blocked=new RunLifecycleService(sql,json,refs->{throw new IllegalStateException("ING_REFERENCE_REVOKED");},transactions);
    assertThatThrownBy(()->blocked.resume(run,0,human,"corr-deny")).hasMessage("ING_REFERENCE_REVOKED");assertThat(sessions(run)).isEmpty();assertThat(state.run(run)).containsEntry("state","PAUSED");
  }
  @Test void historyCannotBeDeletedLimitsChangedOrCountersReset(){
    UUID run=paused();resume(run,0,2,3600);var claim=execution.claim("first",Duration.ofMinutes(2)).orElseThrow();execution.release(claim);
    for(String mutation:List.of("set attempts=0","set max_attempts=100","set deadline_at=deadline_at+interval '1 hour'"))assertThatThrownBy(()->sql.sql("update ouf_ingestion.run_recovery_session "+mutation+" where run_id=:r").param("r",run).update()).hasMessageContaining("ING_RECOVERY_HISTORY_IMMUTABLE");
    assertThatThrownBy(()->sql.sql("delete from ouf_ingestion.run_recovery_session where run_id=:r").param("r",run).update()).hasMessageContaining("ING_RECOVERY_HISTORY_IMMUTABLE");assertThat(sessions(run).getFirst()).containsEntry("attempts",1);
  }
  @Test void humanGrantDoesNotBypassAdmissionPressure(){
    UUID run=paused();resume(run,0,2,3600);sql.sql("update ouf_ingestion.runtime_pressure_policy set state='HARD_PRESSURE' where policy_key='GLOBAL'").update();
    assertThat(execution.claim("pressure",Duration.ofMinutes(2))).isEmpty();assertThat(sessions(run).getFirst()).containsEntry("attempts",0);assertThat(state.run(run)).containsEntry("state","RUNNING");
  }
  @Test void invalidBudgetIsRejectedAndAutomaticCounterIsNotARecoveryGrant(){
    assertThatThrownBy(()->new RunRecoveryBudget.Request(0,86400,"RETRY")).hasMessage("ING_RECOVERY_BUDGET_INVALID");assertThatThrownBy(()->new RunRecoveryBudget.Request(1,0,"RETRY")).hasMessage("ING_RECOVERY_BUDGET_INVALID");
    UUID run=paused();state.recordFailure(String.valueOf(state.run(run).get("source_id")),"ING_GATEWAY_503",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,null);assertThat(sessions(run)).isEmpty();
    assertThatThrownBy(()->RunRecoveryBudget.grant(sql,run,0,human,"corr",RunRecoveryBudget.Request.defaults())).hasMessage("ING_RECOVERY_TRANSACTION_REQUIRED");
  }
  @Test void schedulerCannotCreateANewRunAroundAnExhaustedIncident(){
    sql.sql("truncate table ouf_ingestion.ing_schedule cascade").update();UUID run=paused();resume(run,0,1,3600);
    var claim=execution.claim("first",Duration.ofMinutes(2)).orElseThrow();execution.release(claim);assertThat(execution.claim("exhaust",Duration.ofMinutes(2))).isEmpty();
    String source=String.valueOf(state.run(run).get("source_id"));
    sql.sql("insert into ouf_ingestion.ing_schedule(schedule_id,tenant_id,source_id,interval_seconds,next_run_at) values(:i,:t,:s,60,clock_timestamp()-interval '1 minute')").param("i",UUID.randomUUID()).param("t",human.tenantId()).param("s",source).update();
    assertThat(state.claimDue("scheduler",Duration.ofMinutes(2))).isEmpty();assertThat(sessions(run)).hasSize(1);assertThat(sessions(run).getFirst()).containsEntry("attempts",1);
  }
  @Test void parallelPartitionsDoNotSpendOrInvalidateTheLastInflightBatch(){
    UUID run=paused();sql.sql("insert into ouf_ingestion.ing_partition(run_id,partition_key,state) values(:r,'other','PAUSED')").param("r",run).update();resume(run,0,1,3600);
    var first=execution.claim("first",Duration.ofMinutes(2)).orElseThrow();assertThat(execution.claim("second",Duration.ofMinutes(2))).isEmpty();durable.requireRecoveryBudget(first);assertThat(sessions(run).getFirst()).containsEntry("attempts",1).containsEntry("state","ACTIVE");
  }
  @Test void unclaimedLegacyPublicationCannotBypassARecoverySession(){
    UUID run=paused();resume(run,0,2,3600);
    var command=new DurablePipelineRepository.StageCommand(run,"default",8,"object",1,"test-adapter","1","bundle:1","same-operation",Map.of(),Map.of(),Map.of("ordinal",8),Map.of("ordinal",8));
    assertThatThrownBy(()->durable.stage(command)).hasMessage("ING_RECOVERY_CLAIM_REQUIRED");assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(Long.class).single()).isZero();
  }
  @Test void exhaustedRunDoesNotStopAnIndependentRun(){
    UUID exhausted=paused();resume(exhausted,0,1,3600);var first=execution.claim("first",Duration.ofMinutes(2)).orElseThrow();execution.release(first);assertThat(execution.claim("exhaust",Duration.ofMinutes(2))).isEmpty();
    UUID independent=paused();resume(independent,0,2,3600);assertThat(execution.claim("healthy",Duration.ofMinutes(2)).orElseThrow().runId()).isEqualTo(independent);assertThat(state.run(exhausted)).containsEntry("state","PAUSED");
  }
  @Test void recoveryIncidentResolvesOnlyAfterActualHandoffAckAndDrain(){
    UUID run=paused();resume(run,0,1,3600);var first=execution.claim("first",Duration.ofMinutes(2)).orElseThrow();execution.release(first);assertThat(execution.claim("exhaust",Duration.ofMinutes(2))).isEmpty();resume(run,1,2,3600);
    AdapterSpi adapter=new AdapterSpi(){public String adapterId(){return "test-adapter";}public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_CSV");}public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}public RecordCursor open(ExecutionBundle b,Checkpoint c){return new RecordCursor(){boolean sent;public Optional<SourceRecord> next(){if(sent)return Optional.empty();sent=true;return Optional.of(new SourceRecord("object",8,Map.of("name","Town Hall"),Map.of("managedObjectRef","managed:object")));}public Checkpoint checkpoint(){return new Checkpoint(Map.of("ordinal",8));}public void close(){}};}};
    RuntimePorts.DataLakePort lake=(zone,r,o,bytes,hash,key)->new RuntimePorts.DataLakePort.Receipt("lake:"+zone+":"+key,true);
    RunExecutionWorker worker=new RunExecutionWorker(execution,b->adapter,new CanonicalRecordPipeline(json,validator,durable,quarantine,lake),schemas,state);
    assertThat(worker.executeOne("recovered")).contains(run);assertThat(state.run(run)).containsEntry("state","DRAINING");assertThat(sql.sql("select status from ouf_ingestion.runtime_issue where run_id=:r and issue_code='ING_RECOVERY_BUDGET_EXHAUSTED'").param("r",run).query(String.class).single()).isEqualTo("OPEN");
    var delivery=durable.claim("dispatch",Duration.ofMinutes(1)).orElseThrow();durable.acknowledge(delivery.handoffId(),"dispatch","udp:receipt");execution.completeDrained();
    assertThat(state.run(run)).containsEntry("state","SUCCEEDED");assertThat(sessions(run).getLast()).containsEntry("state","COMPLETED");assertThat(sql.sql("select status from ouf_ingestion.runtime_issue where run_id=:r and issue_code='ING_RECOVERY_BUDGET_EXHAUSTED'").param("r",run).query(String.class).single()).isEqualTo("RESOLVED");
  }
  private static ExecutionBundle bundle(String source){return new ExecutionBundle("bundle","1","sha256:pinned",source,"INTERNAL_MANAGED_CSV","managed:object",Map.ofEntries(Map.entry("pinnedReferences",List.of("schema:1","semantic:1")),Map.entry("sourceSchemaRef","schema:1"),Map.entry("sourceSchemaId","places"),Map.entry("sourceSchemaVersion","1"),Map.entry("expectedFieldNames",List.of("name")),Map.entry("typeCode","place"),Map.entry("semanticPublicationSetRef","semantic:1"),Map.entry("adapterProfileRef","adapter:test:1"),Map.entry("adapterId","test-adapter"),Map.entry("adapterRuntimeVersion","1.0.0"),Map.entry("observationPolicy","ACQUISITION_TIME"),Map.entry("propertyMappings",List.of(Map.of("sourceField","name","targetPropertyIri","https://example.test/name","transform","IDENTITY"))),Map.entry("mappingRefs",List.of("mapping:1"))));}
  private static String required(String name){String value=System.getenv(name);if(value==null)throw new IllegalStateException(name+" required");return value;}
}
