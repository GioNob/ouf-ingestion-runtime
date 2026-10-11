package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@SpringBootTest
class IncidentSnapshotRuntimeTest {
 @DynamicPropertySource static void db(DynamicPropertyRegistry r){
  r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));
 }
 @Autowired org.springframework.jdbc.core.simple.JdbcClient sql;
 @Autowired DataSource dataSource;
 @Autowired IncidentSnapshotService snapshots;
 @Autowired IncidentPublicationBookmarkService bookmarks;
 @Autowired OperationalAwarenessService projection;
 @Autowired ObjectMapper json;
 @Autowired PlatformTransactionManager transactions;
 record Scope(String tenant,String source,UUID run,String subject){}

 @Test void lateLowerPrivateSequencePublishesAfterCommittedHigherSequenceWithoutLoss()throws Exception{
  var scope=scope();UUID late=UUID.randomUUID(),early;
  try(var connection=dataSource.getConnection()){
   connection.setAutoCommit(false);insert(connection,scope,late,"ERROR");
   early=issue(scope,"WARNING");
   assertThat(privateId(late,connection)).isLessThan(privateId(early,null));
   assertThat(snapshots.publish(scope.tenant(),scope.source())).isEqualTo(1);
   var first=page(scope,10);
   assertThat(ids(first)).containsExactly(early.toString());
   connection.commit();
  }
  assertThat(snapshots.publish(scope.tenant(),scope.source())).isEqualTo(1);
  var second=page(scope,10);
  assertThat(ids(second)).containsExactlyInAnyOrder(late.toString(),early.toString());
  assertThat(revision(late)).isGreaterThan(revision(early));
  assertThat(snapshots.publish(scope.tenant(),scope.source())).isZero();
 }

 @Test void captureAndPublicationRollbackAtomicallyAndVersionCannotBeForged(){
  var scope=scope();UUID id=issue(scope,"WARNING");
  long version=version(id);
  new TransactionTemplate(transactions).executeWithoutResult(tx->{
   update(id,"ERROR");
   assertThat(transitionCount(id)).isEqualTo(2);
   tx.setRollbackOnly();
  });
  assertThat(version(id)).isEqualTo(version);assertThat(transitionCount(id)).isEqualTo(1);
  new TransactionTemplate(transactions).executeWithoutResult(tx->{assertThat(snapshots.publish(scope.tenant(),scope.source())).isEqualTo(1);tx.setRollbackOnly();});
  assertThat(revision(id)).isEqualTo(-1L);
  assertThat(snapshots.publish(scope.tenant(),scope.source())).isEqualTo(1);
  assertThat(snapshots.publish(scope.tenant(),scope.source())).isZero();
  assertThatThrownBy(()->sql.sql("update ouf_ingestion.runtime_issue set operational_version=operational_version+1 where issue_id=:id").param("id",id).update()).hasStackTraceContaining("ING_INCIDENT_VERSION_OWNER_MAINTAINED");
  assertThatThrownBy(()->sql.sql("update ouf_ingestion.incident_transition set body='{}'::jsonb where issue_id=:id").param("id",id).update()).hasStackTraceContaining("ING_INCIDENT_HISTORY_IMMUTABLE");
  assertThatThrownBy(()->sql.sql("delete from ouf_ingestion.incident_transition where issue_id=:id").param("id",id).update()).hasStackTraceContaining("ING_INCIDENT_HISTORY_IMMUTABLE");
 }

 @Test void concurrentPublishersCannotDuplicateOrSkipRevisions()throws Exception{
  var scope=scope();for(int i=0;i<20;i++)issue(scope,"WARNING");
  try(var executor=Executors.newFixedThreadPool(2)){
   var barrier=new CyclicBarrier(2);
   var a=executor.submit(()->{barrier.await();return snapshots.publish(scope.tenant(),scope.source());});
   var b=executor.submit(()->{barrier.await();return snapshots.publish(scope.tenant(),scope.source());});
   assertThat(a.get(10,TimeUnit.SECONDS)+b.get(10,TimeUnit.SECONDS)).isEqualTo(20);
  }
  var count=sql.sql("select count(distinct publication_revision) from ouf_ingestion.incident_transition where tenant_id=:t and source_id=:s").param("t",scope.tenant()).param("s",scope.source()).query(Long.class).single();
  assertThat(count).isEqualTo(20L);assertThat(snapshots.publish(scope.tenant(),scope.source())).isZero();
 }

 @Test void pageMembershipPriorityAndRetryTokensSurviveLaterUpdatesAndServiceRestart(){
  var scope=scope();var original=new HashSet<String>();
  for(int i=0;i<7;i++)original.add(issue(scope,"WARNING").toString());
  UUID important=issue(scope,"ERROR");original.add(important.toString());
  var actor=actor(scope,1,null);
  var first=snapshots.page(scope.source(),null,null,2,null,actor);
  assertThat(ids(first).getFirst()).isEqualTo(important.toString());
  UUID cursor=UUID.fromString((String)first.get("nextCursor"));
  for(var id:original)update(UUID.fromString(id),"ERROR");
  snapshots.publish(scope.tenant(),scope.source());
  var restarted=new IncidentSnapshotService(sql,json,projection,transactions);
  var retryA=restarted.page(null,null,null,null,cursor,actor(scope,1,null));
  var retryB=restarted.page(null,null,null,null,cursor,actor(scope,1,null));
  assertThat(retryA).isEqualTo(retryB);
  var seen=new ArrayList<>(ids(first));var next=retryA;
  while(true){seen.addAll(ids(next));if(!next.containsKey("nextCursor"))break;next=restarted.page(null,null,null,null,UUID.fromString((String)next.get("nextCursor")),actor(scope,1,null));}
  assertThat(seen).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(original);
  assertThat(next.get("consultationRecorded")).isEqualTo(false);
  assertThat(items(retryA)).allMatch(x->"WARNING".equals(x.get("severity")));
 }

 @Test void scopedCursorsRejectPrincipalTenantPolicyFilterAndExpiryReplay(){
  var scope=scope();issue(scope,"ERROR");issue(scope,"WARNING");
  var first=page(scope,1);UUID cursor=UUID.fromString((String)first.get("nextCursor"));
  var stranger=new Scope(scope.tenant(),scope.source(),scope.run(),"other-"+scope.subject());
  assertStatus(404,()->snapshots.page(null,null,null,null,cursor,actor(stranger,1,null)));
  var foreign=new Scope("other-"+scope.tenant(),scope.source(),scope.run(),scope.subject());
  assertStatus(404,()->snapshots.page(null,null,null,null,cursor,actor(foreign,1,null)));
  assertStatus(409,()->snapshots.page(null,null,null,null,cursor,actor(scope,2,null)));
  assertStatus(400,()->snapshots.page(scope.source(),null,null,null,cursor,actor(scope,1,null)));
  assertStatus(404,()->snapshots.page(null,null,null,null,UUID.randomUUID(),actor(scope,1,null)));
  sql.sql("update ouf_ingestion.incident_read_session set created_at=clock_timestamp()-interval '1 hour',expires_at=clock_timestamp()-interval '1 minute' where session_id=:id").param("id",UUID.fromString((String)first.get("snapshotRef"))).update();
  assertStatus(410,()->snapshots.page(null,null,null,null,cursor,actor(scope,1,null)));
 }

 @Test void rowRedactionAndLiveTenantRecheckCannotLeakMaterializedHistory()throws Exception{
  var scope=scope();UUID denied=issue(scope,"ERROR"),visible=issue(scope,"WARNING");
  var redacted=snapshots.page(scope.source(),null,null,10,null,actor(scope,1,denied));
  assertThat(ids(redacted)).containsExactly(visible.toString());
  assertThat(redacted.get("partial")).isEqualTo(true);
  assertThat(json.writeValueAsString(redacted)).doesNotContain(denied.toString(),"evidence://");
  var first=page(scope,1);UUID cursor=UUID.fromString((String)first.get("nextCursor"));
  sql.sql("update ouf_ingestion.ing_run set tenant_id=:tenant where run_id=:run").param("tenant","transferred-"+scope.tenant()).param("run",scope.run()).update();
  var after=snapshots.page(null,null,null,null,cursor,actor(scope,1,null));
  assertThat(ids(after)).isEmpty();assertThat(after.get("partial")).isEqualTo(true);
 }

 @Test void priorityIsAppliedBeforePagesAndPublishedLagHistoryGapStayExplicit(){
  var scope=scope();UUID important=issue(scope,"ERROR");
  for(int i=0;i<40;i++)issue(scope,"WARNING");
  var first=page(scope,1);
  assertThat(ids(first)).containsExactly(important.toString());assertThat(first.get("truncated")).isEqualTo(true);
  var history=snapshots.page(scope.source(),null,OffsetDateTime.now().minusYears(1),100,null,actor(scope,1,null));
  assertThat(history.get("partial")).isEqualTo(true);
  assertThat(((Map<?,?>)history.get("coverage")).get("historyGap")).isEqualTo(true);
  var many=scope();for(int i=0;i<1025;i++)issue(many,"WARNING");
  var limited=page(many,100);
  assertThat(limited.get("partial")).isEqualTo(true);
  assertThat(((Map<?,?>)limited.get("coverage")).get("publicationLag")).isEqualTo(true);
 }

 @Test void cursorCapacityIsFiniteAndDoesNotPreventIndependentIncidentPersistence(){
  var scope=scope();issue(scope,"ERROR");
  for(int i=0;i<16;i++)page(scope,100);
  assertStatus(503,()->page(scope,100));
  UUID independent=issue(scope,"WARNING");assertThat(transitionCount(independent)).isEqualTo(1);
  assertStatus(400,()->snapshots.page(scope.source(),null,null,101,null,actor(scope,1,null)));
  assertStatus(400,()->snapshots.page(scope.source(),"INVALID",null,10,null,actor(scope,1,null)));
  assertThatThrownBy(()->sql.sql("select ouf_ingestion.publish_incident_transitions(:t,:s,null)").param("t",scope.tenant()).param("s",scope.source()).query(Integer.class).single()).hasStackTraceContaining("ING_INCIDENT_PUBLICATION_INVALID");
 }

 @Test void ownerHttpPageIsGovernedAndEmptyAuthorizedSourceIsNotFalseRedaction()throws Exception{
  var scope=scope();
  var empty=page(scope,10);assertThat(ids(empty)).isEmpty();assertThat(empty.get("partial")).isEqualTo(false);
  issue(scope,"ERROR");issue(scope,"WARNING");issue(scope,"WARNING");
  var http=org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(projection,org.mockito.Mockito.mock(RuntimeIssueService.class),new TrustedAuthorizationContext(),snapshots)).build();
  org.springframework.test.web.servlet.request.RequestPostProcessor authenticated=request->{
    var verified=boundRequest(scope,1,null);request.setUserPrincipal(verified.getUserPrincipal());
    request.getServletContext().setAttribute(ServletAuthorization.RUNTIME,verified.getServletContext().getAttribute(ServletAuthorization.RUNTIME));return request;
  };
  var response=http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(authenticated).contentType("application/json")
    .content("{\"snapshot\":true,\"sourceId\":\""+scope.source()+"\",\"limit\":2}"))
    .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(2)).andExpect(jsonPath("$.partial").value(false)).andReturn();
  var frame=json.readTree(response.getResponse().getContentAsString());
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(authenticated).contentType("application/json")
    .content("{\"cursor\":\""+frame.get("nextCursor").asText()+"\"}"))
    .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1)).andExpect(jsonPath("$.snapshotRef").value(frame.get("snapshotRef").asText()));
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").contentType("application/json").content("{\"snapshot\":true}")).andExpect(status().isForbidden());
 }


 @Test void publicationBookmarkResumesLateLowerSequenceWithOldObservationWithoutSkipping()throws Exception{
  var scope=scope();UUID late=UUID.randomUUID();
  String bookmark;UUID early;
  try(var c=dataSource.getConnection()){
   c.setAutoCommit(false);insert(c,scope,late,"ERROR");
   early=issue(scope,"WARNING");
   var first=bookmarks.start(scope.source(),null,10,actor(scope,1,null));
   assertThat(ids(first)).containsExactly(early.toString());
   bookmark=(String)first.get("publicationBookmark");assertThat(bookmark).isNotBlank();
   assertThat(privateId(late,c)).isLessThan(privateId(early,null));
   c.commit();
  }
  var resumed=bookmarks.resume(UUID.fromString(bookmark),10,actor(scope,1,null));
  assertThat(ids(resumed)).containsExactly(late.toString());
  assertThat(revision(late)).isGreaterThan(revision(early));
  assertThat(resumed.get("consultationRecorded")).isEqualTo(false);
  assertThat(((Map<?,?>)resumed.get("coverage")).get("parentBookmark")).isEqualTo(bookmark);
 }

 @Test void publicationBookmarkSurvivesPageExpiryServiceRecreationAndNewLogin(){
  var scope=scope();UUID old=issue(scope,"WARNING");
  var initial=bookmarks.start(scope.source(),null,10,actor(scope,1,null));UUID bookmark=UUID.fromString((String)initial.get("publicationBookmark"));
  sql.sql("update ouf_ingestion.incident_read_session set created_at=clock_timestamp()-interval '1 hour',expires_at=clock_timestamp()-interval '1 minute' where session_id=:id").param("id",UUID.fromString((String)initial.get("snapshotRef"))).update();
  UUID later=issue(scope,"ERROR");update(old,"ERROR");
  var restarted=new IncidentPublicationBookmarkService(sql,json,new IncidentSnapshotService(sql,json,projection,transactions),transactions);
  var resumed=restarted.resume(bookmark,10,actorLogin(scope,"new-login"));
  assertThat(ids(resumed)).containsExactlyInAnyOrder(old.toString(),later.toString());
  var empty=restarted.resume(UUID.fromString((String)resumed.get("publicationBookmark")),10,actorLogin(scope,"third-login"));
  assertThat(ids(empty)).isEmpty();assertThat(empty.get("partial")).isEqualTo(false);
  assertThat(empty.get("consultationRecorded")).isEqualTo(false);
 }

 @Test void publicationBookmarkRequiresAllNonRedactedPagesAndReplayIsStable(){
  var scope=scope();for(int i=0;i<3;i++)issue(scope,"WARNING");
  var first=bookmarks.start(scope.source(),null,1,actor(scope,1,null));assertThat(first).doesNotContainKey("publicationBookmark");
  UUID session=UUID.fromString((String)first.get("snapshotRef"));
  UUID denied=sql.sql("select cast(body->>'incident_id' as uuid) from ouf_ingestion.incident_read_item where session_id=:id and position=1").param("id",session).query(UUID.class).single();
  UUID middle=UUID.fromString((String)first.get("nextCursor"));
  var hidden=bookmarks.page(middle,actor(scope,1,denied));assertThat(hidden.get("partial")).isEqualTo(true);
  UUID finalCursor=UUID.fromString((String)hidden.get("nextCursor"));
  var last=bookmarks.page(finalCursor,actor(scope,1,null));assertThat(last).doesNotContainKey("publicationBookmark");
  bookmarks.page(middle,actor(scope,1,null));
  last=bookmarks.page(finalCursor,actor(scope,1,null));
  assertThat(last.get("publicationBookmark")).isNotNull();
  assertThat(bookmarks.page(finalCursor,actor(scope,1,null)).get("publicationBookmark")).isEqualTo(last.get("publicationBookmark"));
 }

 @Test void publicationBookmarkRejectsIdentityPolicyAbsentOwnerAndLegacyScope(){
  var scope=scope();issue(scope,"WARNING");
  var first=bookmarks.start(scope.source(),null,10,actor(scope,1,null));UUID bookmark=UUID.fromString((String)first.get("publicationBookmark"));
  var stranger=new Scope(scope.tenant(),scope.source(),scope.run(),"other-"+scope.subject());
  assertStatus(404,()->bookmarks.resume(bookmark,10,actor(stranger,1,null)));
  var foreign=new Scope("other-"+scope.tenant(),scope.source(),scope.run(),scope.subject());
  assertStatus(404,()->bookmarks.resume(bookmark,10,actor(foreign,1,null)));
  assertStatus(409,()->bookmarks.resume(bookmark,10,actor(scope,2,null)));
  assertStatus(403,()->bookmarks.resume(bookmark,10,new TrustedAuthorizationContext.Context(scope.subject(),"HUMAN",scope.tenant(),Set.of(IncidentSnapshotService.CAPABILITY),"fabricated")));
  var legacy=page(scope,10);
  assertStatus(404,()->bookmarks.page(UUID.fromString((String)legacy.get("cursor")),actor(scope,1,null)));
  assertStatus(400,()->bookmarks.resume(bookmark,101,actor(scope,1,null)));
 }

 @Test void publicationBookmarkHistoryFloorAndExpiredBoundaryFailExplicitly(){
  var scope=scope();issue(scope,"ERROR");
  var first=bookmarks.start(scope.source(),null,10,actor(scope,1,null));UUID bookmark=UUID.fromString((String)first.get("publicationBookmark"));
  sql.sql("insert into ouf_ingestion.incident_publication_history_floor(tenant_id,revision) values(:t,999)").param("t",scope.tenant()).update();
  assertStatus(410,()->bookmarks.resume(bookmark,10,actor(scope,1,null)));
  assertThatThrownBy(()->sql.sql("update ouf_ingestion.incident_publication_history_floor set revision=0 where tenant_id=:t").param("t",scope.tenant()).update()).hasStackTraceContaining("ING_HISTORY_FLOOR_CANNOT_REWIND");
  UUID expired=UUID.randomUUID();
  sql.sql("""
   insert into ouf_ingestion.incident_publication_bookmark(token,session_id,principal_id,tenant_id,identity_digest,policy_ref,source_id,state,high_watermark,parent_token,created_at,expires_at)
   select :token,:session,principal_id,tenant_id,identity_digest,policy_ref,source_id,state,high_watermark,parent_token,clock_timestamp()-interval '100 days',clock_timestamp()-interval '1 day'
   from ouf_ingestion.incident_publication_bookmark where token=:old
   """).param("token",expired).param("session",UUID.randomUUID()).param("old",bookmark).update();
  assertStatus(410,()->bookmarks.resume(expired,10,actor(scope,1,null)));
 }

 @Test void publicationBookmarkMetadataCannotMutateOrDropLiveCoverage(){
  var scope=scope();UUID incident=issue(scope,"WARNING");
  var first=bookmarks.start(scope.source(),null,10,actor(scope,1,null));UUID bookmark=UUID.fromString((String)first.get("publicationBookmark"));
  assertThatThrownBy(()->sql.sql("update ouf_ingestion.incident_publication_bookmark set high_watermark=high_watermark+1 where token=:id").param("id",bookmark).update()).hasStackTraceContaining("ING_PUBLICATION_BOOKMARK_IMMUTABLE");
  assertThatThrownBy(()->sql.sql("delete from ouf_ingestion.incident_publication_bookmark where token=:id").param("id",bookmark).update()).hasStackTraceContaining("ING_PUBLICATION_BOOKMARK_LIVE");
  String metadata=sql.sql("select to_jsonb(b)::text from ouf_ingestion.incident_publication_bookmark b where token=:id").param("id",bookmark).query(String.class).single();
  assertThat(metadata).doesNotContain(incident.toString(),"impact_summary","evidence://");
 }

 @Test void publicationBookmarkCapacityIsPersistentAndKeepsIncidentWritesAlive(){
  var scope=scope();issue(scope,"WARNING");
  var first=bookmarks.start(scope.source(),null,10,actor(scope,1,null));UUID bookmark=UUID.fromString((String)first.get("publicationBookmark"));
  sql.sql("""
   insert into ouf_ingestion.incident_publication_bookmark(token,session_id,principal_id,tenant_id,identity_digest,policy_ref,source_id,state,high_watermark,parent_token)
   select gen_random_uuid(),gen_random_uuid(),b.principal_id,b.tenant_id,b.identity_digest,b.policy_ref,b.source_id,b.state,b.high_watermark,b.parent_token
   from ouf_ingestion.incident_publication_bookmark b cross join generate_series(1,511) g where b.token=:old
   """).param("old",bookmark).update();
  UUID independent=issue(scope,"ERROR");assertThat(transitionCount(independent)).isEqualTo(1);
  assertStatus(503,()->bookmarks.start(scope.source(),null,10,actor(scope,1,null)));
  var recreated=new IncidentPublicationBookmarkService(sql,json,snapshots,transactions);
  assertStatus(503,()->recreated.start(scope.source(),null,10,actor(scope,1,null)));
  assertThat(recreated.page(UUID.fromString((String)first.get("cursor")),actor(scope,1,null)).get("publicationBookmark")).isEqualTo(bookmark.toString());
 }

 @Test void publicationBookmarkConcurrentFinalReadIssuesOneStablePosition()throws Exception{
  var scope=scope();issue(scope,"WARNING");issue(scope,"ERROR");
  var first=bookmarks.start(scope.source(),null,1,actor(scope,1,null));
  UUID cursor=UUID.fromString((String)first.get("nextCursor"));
  try(var pool=Executors.newFixedThreadPool(2)){
   var barrier=new CyclicBarrier(2);
   var a=pool.submit(()->{barrier.await();return bookmarks.page(cursor,actor(scope,1,null));});
   var b=pool.submit(()->{barrier.await();return bookmarks.page(cursor,actor(scope,1,null));});
   assertThat(a.get(10,TimeUnit.SECONDS).get("publicationBookmark")).isEqualTo(b.get(10,TimeUnit.SECONDS).get("publicationBookmark"));
  }
 }


 @Test void unchangedPublicationBoundaryReusesBookmarkWithoutMetadataGrowth(){
  var scope=scope();issue(scope,"WARNING");
  var first=bookmarks.start(scope.source(),null,10,actor(scope,1,null));UUID bookmark=UUID.fromString((String)first.get("publicationBookmark"));
  assertThat(bookmarks.start(scope.source(),null,10,actor(scope,1,null)).get("publicationBookmark")).isEqualTo(bookmark.toString());
  for(int i=0;i<3;i++){
   var unchanged=bookmarks.resume(bookmark,10,actorLogin(scope,"new-login-"+i));
   assertThat(ids(unchanged)).isEmpty();assertThat(unchanged.get("publicationBookmark")).isEqualTo(bookmark.toString());
   assertThat(((Map<?,?>)unchanged.get("coverage")).get("noPublicationProgress")).isEqualTo(true);
  }
  assertThat(sql.sql("select count(*) from ouf_ingestion.incident_publication_bookmark where tenant_id=:t").param("t",scope.tenant()).query(Long.class).single()).isEqualTo(1L);
 }

 @Test void changedCapturedMetadataCannotIssueCoverageBookmark(){
  var scope=scope();issue(scope,"WARNING");issue(scope,"ERROR");
  var first=bookmarks.start(scope.source(),null,1,actor(scope,1,null));
  UUID session=UUID.fromString((String)first.get("snapshotRef"));
  sql.sql("update ouf_ingestion.incident_read_session set state='OPEN' where session_id=:id").param("id",session).update();
  assertStatus(409,()->bookmarks.page(UUID.fromString((String)first.get("nextCursor")),actor(scope,1,null)));
  assertThat(sql.sql("select count(*) from ouf_ingestion.incident_publication_bookmark where tenant_id=:t").param("t",scope.tenant()).query(Long.class).single()).isZero();
 }

 private TrustedAuthorizationContext.Context actorLogin(Scope s,String login){
  var request=boundRequest(s,1,null);
  request.setUserPrincipal(new TrustedPrincipal(new PrincipalContext(s.subject(),s.tenant(),PrincipalContext.ActorType.HUMAN,null,login,"cursor-issuer","cursor-audience",Set.of(IncidentSnapshotService.CAPABILITY))));
  var owner=OwnerAuthorization.bind(request);
  return new TrustedAuthorizationContext.Context(s.subject(),"HUMAN",s.tenant(),owner.candidates(),owner.decisionRef(),owner);
 }


 @Test void ownerHttpPublicationResumePreservesSavedSourceAndFreshSdkAfterNewLogin()throws Exception{
  var scope=scope();UUID first=issue(scope,"ERROR");
  var http=bookmarkHttp();
  var response=http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(httpActor(scope,1,null,"login-one"))
   .contentType("application/json").content(json.writeValueAsString(Map.of("snapshot",true,"coverage",true,"sourceId",scope.source(),"limit",1))))
   .andExpect(status().isOk()).andExpect(jsonPath("$.consultationRecorded").value(false)).andReturn();
  var body=json.readValue(response.getResponse().getContentAsString(),Map.class);
  String bookmark=(String)body.get("publicationBookmark");assertThat(bookmark).isNotBlank();
  UUID later=issue(scope,"WARNING");
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(httpActor(scope,1,null,"login-two"))
   .contentType("application/json").content(json.writeValueAsString(Map.of("resumeFrom",bookmark,"limit",1))))
   .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1))
   .andExpect(jsonPath("$.items[0].incident_id").value(later.toString()))
   .andExpect(jsonPath("$.coverage.parentBookmark").value(bookmark))
   .andExpect(jsonPath("$.consultationRecorded").value(false));
  assertThat(later).isNotEqualTo(first);
 }

 @Test void ownerHttpCoveragePageRedactionRevocationAndPolicyRemainCurrent()throws Exception{
  var scope=scope();UUID a=issue(scope,"ERROR"),b=issue(scope,"WARNING");
  var http=bookmarkHttp();
  var response=http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(httpActor(scope,1,null,"same-login"))
   .contentType("application/json").content(json.writeValueAsString(Map.of("snapshot",true,"coverage",true,"sourceId",scope.source(),"limit",1))))
   .andExpect(status().isOk()).andExpect(jsonPath("$.publicationBookmark").doesNotExist()).andReturn();
  var first=json.readValue(response.getResponse().getContentAsString(),Map.class);
  String cursor=(String)first.get("nextCursor"),request=json.writeValueAsString(Map.of("cursor",cursor,"coverage",true));
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(httpActor(scope,1,b,"same-login"))
    .contentType("application/json").content(request)).andExpect(status().isOk())
    .andExpect(jsonPath("$.partial").value(true)).andExpect(jsonPath("$.publicationBookmark").doesNotExist());
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").contentType("application/json").content(request)).andExpect(status().isForbidden());
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(httpActor(scope,2,null,"same-login"))
    .contentType("application/json").content(request)).andExpect(status().isConflict());
  http.perform(post("/api/internal/v1/ingestion/operations/incidents").with(httpActor(scope,1,null,"same-login"))
    .contentType("application/json").content(request)).andExpect(status().isOk()).andExpect(jsonPath("$.publicationBookmark").isString());
 }

 @Test void ownerHttpBookmarkQueriesAreClosedBeforeAnyOwnerRead()throws Exception{
  var scope=scope();var http=bookmarkHttp();String token="11111111-aaaa-4111-8111-111111111111";
  var invalid=List.of(
    Map.of("coverage",true),Map.of("snapshot",true,"coverage",false),
    Map.of("snapshot",true,"coverage",true,"since","2026-10-11T00:00:00Z"),
    Map.of("resumeFrom",token,"sourceId",scope.source()),Map.of("resumeFrom",token,"state","OPEN"),
    Map.of("resumeFrom",token,"coverage",true),Map.of("resumeFrom",token,"cursor",token),
    Map.of("cursor",token,"coverage",true,"limit",1),Map.of("resumeFrom",token.toUpperCase()),
    Map.of("resumeFrom","00000000-0000-0000-0000-000000000000"),Map.of("revision",1),Map.of("limit",true));
  for(var query:invalid)http.perform(post("/api/internal/v1/ingestion/operations/incidents").contentType("application/json")
    .content(json.writeValueAsString(query))).andExpect(status().isBadRequest());
  for(String raw:List.of("{\"resumeFrom\":null}","{\"resumeFrom\":\""+token+"\",\"resumeFrom\":\""+token+"\"}"))
   http.perform(post("/api/internal/v1/ingestion/operations/incidents").contentType("application/json").content(raw)).andExpect(status().isBadRequest());
  for(String route:List.of("summary","status","history"))http.perform(post("/api/internal/v1/ingestion/operations/"+route)
   .contentType("application/json").content("{\"snapshot\":true,\"coverage\":true}")).andExpect(status().isBadRequest());
  assertThat(sql.sql("select count(*) from ouf_ingestion.incident_read_session where tenant_id=:tenant")
    .param("tenant",scope.tenant()).query(Long.class).single()).isZero();
 }

 private org.springframework.test.web.servlet.MockMvc bookmarkHttp(){
  return org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup(
    new OperationalAwarenessApi(projection,org.mockito.Mockito.mock(RuntimeIssueService.class),new TrustedAuthorizationContext(),snapshots,bookmarks)).build();
 }
 private org.springframework.test.web.servlet.request.RequestPostProcessor httpActor(Scope scope,long version,UUID denied,String login){
  return request->{
    var verified=boundRequest(scope,version,denied);
    request.setUserPrincipal(new TrustedPrincipal(new PrincipalContext(scope.subject(),scope.tenant(),PrincipalContext.ActorType.HUMAN,null,login,"cursor-issuer","cursor-audience",Set.of(IncidentSnapshotService.CAPABILITY))));
    request.getServletContext().setAttribute(ServletAuthorization.RUNTIME,verified.getServletContext().getAttribute(ServletAuthorization.RUNTIME));
    return request;
  };
 }

 private Scope scope(){
  String tenant="cursor-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();UUID run=UUID.randomUUID();
  sql.sql("insert into ouf_ingestion.ing_run(run_id,tenant_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id) values(:id,:tenant,:source,'cursor-bundle','1','sha256:cursor','DELTA','RUNNING','cursor-correlation')")
    .param("id",run).param("tenant",tenant).param("source",source).update();
  return new Scope(tenant,source,run,"reader-"+UUID.randomUUID());
 }
 private UUID issue(Scope s,String severity){
  UUID id=UUID.randomUUID();
  sql.sql("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id) values(:id,:run,:source,'TEST',:severity,'ING_CURSOR_TEST',:evidence,'cursor-correlation')")
    .param("id",id).param("run",s.run()).param("source",s.source()).param("severity",severity).param("evidence","evidence://"+id).update();return id;
 }
 private void insert(Connection c,Scope s,UUID id,String severity)throws SQLException{
  try(var statement=c.prepareStatement("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id) values(?,?,?,'TEST',?,'ING_CURSOR_TEST',?,'cursor-correlation')")){
   statement.setObject(1,id);statement.setObject(2,s.run());statement.setString(3,s.source());statement.setString(4,severity);statement.setString(5,"evidence://"+id);statement.executeUpdate();
  }
 }
 private long privateId(UUID id,Connection c)throws SQLException{
  if(c==null)return sql.sql("select transition_id from ouf_ingestion.incident_transition where issue_id=:id").param("id",id).query(Long.class).single();
  try(var st=c.prepareStatement("select transition_id from ouf_ingestion.incident_transition where issue_id=?")){st.setObject(1,id);try(var rs=st.executeQuery()){rs.next();return rs.getLong(1);}}
 }
 private Long revision(UUID id){return sql.sql("select coalesce(publication_revision,-1) from ouf_ingestion.incident_transition where issue_id=:id order by issue_version desc limit 1").param("id",id).query(Long.class).single();}
 private long version(UUID id){return sql.sql("select operational_version from ouf_ingestion.runtime_issue where issue_id=:id").param("id",id).query(Long.class).single();}
 private int transitionCount(UUID id){return sql.sql("select count(*)::integer from ouf_ingestion.incident_transition where issue_id=:id").param("id",id).query(Integer.class).single();}
 private void update(UUID id,String severity){sql.sql("update ouf_ingestion.runtime_issue set severity=:severity,resolution_reason='new investigation' where issue_id=:id").param("id",id).param("severity",severity).update();}
 private Map<String,Object> page(Scope s,int limit){return snapshots.page(s.source(),null,null,limit,null,actor(s,1,null));}
 @SuppressWarnings("unchecked") private static List<Map<String,Object>> items(Map<String,Object> result){return (List<Map<String,Object>>)result.get("items");}
 private static List<String> ids(Map<String,Object> result){return items(result).stream().map(x->x.get("incident_id").toString()).toList();}
 private static void assertStatus(int expected,Runnable call){assertThatThrownBy(call::run).isInstanceOfSatisfying(ResponseStatusException.class,e->assertThat(e.getStatusCode().value()).isEqualTo(expected));}
 private MockHttpServletRequest boundRequest(Scope s,long version,UUID denied){
  try{
   var engine=new LocalAuthorization(Clock.systemUTC(),Duration.ofHours(1));var now=Instant.now();
   var constraints=new GrantConstraints("ALLOW",null,"operational",null,Map.of("sourceRef",s.source()),Set.of(),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null);
   var grants=new ArrayList<Grant>();grants.add(new Grant("read",IncidentSnapshotService.CAPABILITY,s.tenant(),s.subject(),null,null,now.minusSeconds(60),now.plusSeconds(3600),constraints));
   if(denied!=null)grants.add(new Grant("deny",IncidentSnapshotService.CAPABILITY,s.tenant(),s.subject(),null,null,now.minusSeconds(60),now.plusSeconds(3600),new GrantConstraints("DENY",null,"operational",denied.toString(),Map.of(),Set.of(),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null)));
   TestAuthorization.install(engine,new PolicyBundle("cursor-fixture",version,now,List.of(new CapabilityDescriptor(IncidentSnapshotService.CAPABILITY,"READ",IncidentSnapshotService.CAPABILITY,Set.of(PrincipalContext.ActorType.HUMAN))),grants));
   var request=new MockHttpServletRequest();request.setUserPrincipal(new TrustedPrincipal(new PrincipalContext(s.subject(),s.tenant(),PrincipalContext.ActorType.HUMAN,null,"cursor-auth","cursor-issuer","cursor-audience",Set.of(IncidentSnapshotService.CAPABILITY))));
   request.getServletContext().setAttribute(ServletAuthorization.RUNTIME,engine);
   return request;
  }catch(Exception failure){throw new IllegalStateException(failure);}
 }
 private TrustedAuthorizationContext.Context actor(Scope s,long version,UUID denied){
  var owner=OwnerAuthorization.bind(boundRequest(s,version,denied));
  return new TrustedAuthorizationContext.Context(s.subject(),"HUMAN",s.tenant(),owner.candidates(),owner.decisionRef(),owner);
 }
 private static String required(String name){String value=System.getenv(name);if(value==null)throw new IllegalStateException(name+" required");return value;}
}
