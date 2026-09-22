package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.TestAuthorization;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest class IncidentTimelineRuntimeTest {
 @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->System.getenv("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->System.getenv("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->System.getenv("OUF_ING_DB_PASSWORD"));}
 @Autowired JdbcClient sql; @Autowired ObjectMapper json; @Autowired RunStateRepository runs; @Autowired RunExecutionRepository execution;
 @Autowired OperationalAwarenessService visibility; @Autowired PlatformTransactionManager manager;

 @Test void retryIsDurableDeduplicatedAndResolvedOnlyWhenRunDrained(){
  UUID run=start(3);var first=lease(run,"first",1);
  assertThat(execution.failed(first,transientFailure(Duration.ofSeconds(5)))).isTrue();
  var incident=incident(run);UUID id=(UUID)incident.get("incident_id");
  assertThat(incident).containsEntry("lifecycle_state","RECOVERING").containsEntry("attempt_count",1);
  assertThat(incident.get("next_retry_at")).isNotNull();
  // New repository instance simulates a process with no local retry/incident state.
  var restarted=new RunExecutionRepository(sql,json);
  retryNow(run);var second=lease(run,"second",2);
  assertThat(restarted.failed(second,transientFailure(Duration.ofSeconds(9)))).isTrue();
  assertThat(incident(run)).containsEntry("incident_id",id).containsEntry("attempt_count",2);
  retryNow(run);var third=lease(run,"third",3);
  restarted.inputExhausted(third);
  assertThat(incident(run).get("lifecycle_state")).isEqualTo("RECOVERING");
  restarted.completeDrained();
  assertThat(incident(run)).containsEntry("incident_id",id).containsEntry("lifecycle_state","RESOLVED");
  String projection=sql.sql("select projection::text from ouf_ingestion.operational_incident_transition where incident_id=:id order by sequence_id desc limit 1").param("id",id).query(String.class).single();
  assertThat(projection).contains("RUN_RECOVERED","duration_ms","attempt_count");
 }
 @Test void budgetExhaustionStaysOpenAndStaleWorkerCannotWriteAnIncident(){
  UUID run=start(1);var claim=lease(run,"worker",1);
  assertThat(execution.failed(claim,transientFailure(Duration.ofSeconds(1)))).isTrue();
  long events=count(run);
  assertThatThrownBy(()->execution.failed(claim,transientFailure(Duration.ofSeconds(1)))).hasMessage("ING_PARTITION_LEASE_LOST");
  assertThat(count(run)).isEqualTo(events);
  retryNow(run);assertThat(execution.failed(lease(run,"worker",2),transientFailure(Duration.ofSeconds(1)))).isFalse();
  assertThat(runs.run(run).get("state")).isEqualTo("FAILED");
  assertThat(incident(run).get("lifecycle_state")).isEqualTo("OPEN");
  execution.completeDrained();assertThat(incident(run).get("resolved_at")).isNull();
 }
 @Test void incidentAndRunTransitionRollbackTogether(){
  UUID run=start(2);var claim=lease(run,"worker",1);
  new TransactionTemplate(manager).execute(status->{execution.failed(claim,transientFailure(Duration.ofSeconds(1)));status.setRollbackOnly();return null;});
  assertThat(runs.run(run).get("state")).isEqualTo("RUNNING");assertThat(count(run)).isZero();
 }
 @Test void retryAfterCannotExceedPinnedElapsedBudget(){
  UUID run=start(4);assertThat(execution.failed(lease(run,"worker",1),transientFailure(Duration.ofHours(2)))).isFalse();
  assertThat(runs.run(run).get("state")).isEqualTo("FAILED");assertThat(incident(run).get("next_retry_at")).isNull();
 }
 @Test void cursorSnapshotSurvivesRecoveryAndReauthorizesEachPage(){
  UUID run=start(0);String source=(String)runs.run(run).get("source_id");
  for(String code:List.of("ING_A","ING_B","ING_C"))emit(run,code,"OPEN");
  var actor=actor();var timeline=new IncidentTimeline(sql,json,visibility);
  var first=timeline.page(new IncidentTimeline.Query(source,null,null,1,null,null,null,null),actor);
  assertThat(first.get("hasMore")).isEqualTo(true);
  var ids=new HashSet<Object>();ids.add(items(first).getFirst().get("incident_id"));
  emit(run,"ING_B","RESOLVED");emit(run,"ING_D","OPEN");
  Map<String,Object> page=first;
  while(Boolean.TRUE.equals(page.get("hasMore"))){page=timeline.page(new IncidentTimeline.Query(source,null,null,1,null,(String)page.get("nextCursor"),null,null),actor);for(var row:items(page)){assertThat(row.get("lifecycle_state")).isEqualTo("OPEN");assertThat(ids.add(row.get("incident_id"))).isTrue();}}
  assertThat(ids).hasSize(3);
  var denied=new TrustedAuthorizationContext.Context(actor.subject(),"HUMAN",actor.tenantId(),Set.of(),"denied");
  var deniedPage=timeline.page(new IncidentTimeline.Query(source,null,null,1,null,(String)first.get("nextCursor"),null,null),denied);
  assertThat(items(deniedPage)).isEmpty();assertThat(deniedPage.get("partial")).isEqualTo(true);
  assertThatThrownBy(()->timeline.page(new IncidentTimeline.Query("different",null,null,1,null,(String)first.get("nextCursor"),null,null),actor)).isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
 }
 @Test void retentionNeverDeletesOpenHeldOrRecentResolvedIncidents(){
  UUID run=start(0);emit(run,"ING_RECENT","RESOLVED");emit(run,"ING_OPEN","OPEN");emit(run,"ING_HELD","RESOLVED");
  UUID held=sql.sql("select incident_id from ouf_ingestion.operational_incident where run_id=:r and error_code='ING_HELD'").param("r",run).query(UUID.class).single();
  sql.sql("insert into ouf_ingestion.operational_incident_hold values(:i,'LEGAL_HOLD','case-1')").param("i",held).update();
  sql.sql("update ouf_ingestion.operational_incident set retain_until=transaction_timestamp()-interval '40 days',resolved_at=transaction_timestamp()-interval '40 days' where incident_id=:i").param("i",held).update();
  sql.sql("select ouf_ingestion.prune_resolved_incidents(100)").query(Integer.class).single();
  assertThat(sql.sql("select count(*) from ouf_ingestion.operational_incident where run_id=:r").param("r",run).query(Long.class).single()).isEqualTo(3);
 }
 @Test void expiredUnreferencedIncidentCanBePrunedButTransitionsCannotBeRewritten(){
  UUID run=start(0);emit(run,"ING_OLD","RESOLVED");UUID id=(UUID)incident(run).get("incident_id");
  assertThatThrownBy(()->sql.sql("update ouf_ingestion.operational_incident_transition set projection='{}' where incident_id=:i").param("i",id).update()).isInstanceOf(org.springframework.dao.DataAccessException.class);
  sql.sql("update ouf_ingestion.operational_incident set retain_until=transaction_timestamp()-interval '40 days',resolved_at=transaction_timestamp()-interval '40 days' where incident_id=:i").param("i",id).update();
  sql.sql("select ouf_ingestion.prune_resolved_incidents(100)").query(Integer.class).single();
  assertThat(count(run)).isZero();
  assertThat(sql.sql("select count(*) from ouf_ingestion.operational_incident where incident_id=:i").param("i",id).query(Long.class).single()).isZero();
 }
 private UUID start(int attempts){String source="timeline-"+UUID.randomUUID();var config=Map.<String,Object>of("syncProfile",Map.of("retryBackoffSeconds",1,"operationalPolicy",Map.of("maxRetryAttempts",attempts,"maxRetryElapsedSeconds",600,"operationalRetentionDays",45)));
  UUID run=runs.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-a",source,300,"scheduler"),new ExecutionBundle("b","1","sha256:b",source,"REST_JSON","gateway://source",config),"correlation-test");runs.preflightSucceeded(run);return run;}
 private RunExecutionRepository.Claim lease(UUID run,String worker,long generation){sql.sql("update ouf_ingestion.ing_partition set lease_owner=:w,lease_generation=:g,lease_until=transaction_timestamp()+interval '2 minutes' where run_id=:r").param("w",worker).param("g",generation).param("r",run).update();return new RunExecutionRepository.Claim(run,"default",Map.of(),generation,worker);}
 private void retryNow(UUID run){sql.sql("update ouf_ingestion.ing_partition set state='RUNNING',retry_not_before=null where run_id=:r").param("r",run).update();sql.sql("update ouf_ingestion.ing_run set state='RUNNING',updated_at=transaction_timestamp() where run_id=:r").param("r",run).update();}
 private Map<String,Object> incident(UUID run){return sql.sql("select * from ouf_ingestion.operational_incident where run_id=:r").param("r",run).query().singleRow();}
 private long count(UUID run){return sql.sql("select count(*) from ouf_ingestion.operational_incident_transition t join ouf_ingestion.operational_incident i using(incident_id) where i.run_id=:r").param("r",run).query(Long.class).single();}
 private void emit(UUID run,String code,String state){sql.sql("select ouf_ingestion.record_incident(:key,:run,:code,:state,'TEST_EVENT','ERROR',null,0)").param("key",run+":"+code).param("run",run).param("code",code).param("state",state).query(UUID.class).single();}
 private RunCoordinator.Failure transientFailure(Duration after){return new RunCoordinator.Failure("ING_SOURCE_UNREACHABLE",AdapterSpi.ErrorClass.TRANSIENT_SOURCE,after);}
 private TrustedAuthorizationContext.Context actor(){var request=new MockHttpServletRequest();TestAuthorization.bind(request,"reader","HUMAN",Set.of("operations.incident.read","operations.incident.explain"));var engine=(LocalAuthorization)request.getServletContext().getAttribute(ServletAuthorization.RUNTIME);var old=engine.currentSnapshot().bundle();
  var grants=old.grants().stream().map(g->new Grant(g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),g.servicePrincipalId(),g.organizationId(),g.validFrom(),g.validUntil(),new GrantConstraints("ALLOW",null,"operational",null,Map.of("module","INGESTION"),Set.of(),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null))).toList();
  try{TestAuthorization.install(engine,new PolicyBundle(old.bundleId(),2,old.publishedAt(),old.capabilities(),grants));}catch(Exception e){throw new IllegalStateException(e);}
  return new TrustedAuthorizationContext().owner(request,"operations.incident.read");}
 @SuppressWarnings("unchecked")private List<Map<String,Object>> items(Map<String,Object> page){return (List<Map<String,Object>>)page.get("items");}
}
