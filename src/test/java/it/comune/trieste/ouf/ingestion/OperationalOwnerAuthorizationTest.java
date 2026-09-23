package it.comune.trieste.ouf.ingestion;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.jdbc.core.simple.JdbcClient;
import java.util.*;

class OperationalOwnerAuthorizationTest {
 private RequestPostProcessor actor(boolean detail){return request->{
  TestAuthorization.bind(request,"reader","HUMAN",Set.of("operations.status.read","ingestion.operations.read"));
  var engine=(LocalAuthorization)request.getServletContext().getAttribute(ServletAuthorization.RUNTIME);var old=engine.currentSnapshot().bundle();
  var grants=old.grants().stream().map(g->new Grant(g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),g.servicePrincipalId(),g.organizationId(),g.validFrom(),g.validUntil(),new GrantConstraints("ALLOW",null,"operational",null,Map.of("sourceRef","source-a"),Set.of(),detail?Set.of("TENANT_OPERATIONAL"):Set.of(),null,Set.of(),null))).toList();
  try{TestAuthorization.install(engine,new PolicyBundle(old.bundleId(),2,old.publishedAt(),old.capabilities(),grants));}catch(Exception failure){throw new IllegalStateException(failure);}return request;
 };}
 @Test void sourceAndDetailConstraintsMinimizeBeforeHttpSerialization()throws Exception{
  var service=spy(new OperationalAwarenessService(mock(JdbcClient.class)));var rows=List.<Map<String,Object>>of(Map.of("source_ref","source-a","job_ref","a","evidence_ref","protected-a"),Map.of("source_ref","source-b","job_ref","hidden-b"));
  doReturn(rows).when(service).status(any(),anyInt(),any());
  var http=MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(service,mock(RuntimeIssueService.class),new TrustedAuthorizationContext())).build();
  http.perform(post("/api/internal/v1/ingestion/operations/status").with(actor(true))).andExpect(status().isOk()).andExpect(jsonPath("$.partial").value(true)).andExpect(jsonPath("$.items.length()").value(1)).andExpect(jsonPath("$.items[0].source_ref").value("source-a")).andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("hidden-b")))).andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("protected-a"))));
  http.perform(post("/api/internal/v1/ingestion/operations/status").with(actor(false))).andExpect(status().isOk()).andExpect(jsonPath("$.authorization").value("REDACTED")).andExpect(jsonPath("$.items.length()").value(0));
 }
 @Test void denialCannotBecomeHealthySummary()throws Exception{
  var service=spy(new OperationalAwarenessService(mock(JdbcClient.class)));doReturn(List.of(Map.of("source_ref","source-b","lifecycle_state","OPEN"))).when(service).incidents(any(),any(),any(),anyInt(),any());
  var http=MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(service,mock(RuntimeIssueService.class),new TrustedAuthorizationContext())).build();
  http.perform(post("/api/internal/v1/ingestion/operations/summary").with(actor(true))).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UNKNOWN")).andExpect(jsonPath("$.partial").value(true));
 }

 @Test void historyRejectsInvalidWindowsAndLimitsBeforeReading()throws Exception{
  var service=spy(new OperationalAwarenessService(mock(JdbcClient.class)));
  var http=MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(service,mock(RuntimeIssueService.class),new TrustedAuthorizationContext())).build();
  for(String body:List.of("{\"limit\":201}","{\"limit\":0}","{\"since\":\"2026-01-01T00:00:00Z\"}","{\"since\":\"2026-09-21T00:00:00Z\",\"until\":\"2026-09-20T00:00:00Z\"}","{\"until\":\"2099-01-01T00:00:00Z\"}"))
   http.perform(post("/api/internal/v1/ingestion/operations/history").contentType("application/json").content(body).with(actor(true))).andExpect(status().isBadRequest());
  verify(service,never()).history(any(),any(),any(),anyInt(),any());
 }

 @Test void historyChecksRequestedSourceBeforeEmptyResults()throws Exception{
  var service=spy(new OperationalAwarenessService(mock(JdbcClient.class)));
  var http=MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(service,mock(RuntimeIssueService.class),new TrustedAuthorizationContext())).build();
  http.perform(post("/api/internal/v1/ingestion/operations/history").contentType("application/json").content("{\"sourceId\":\"source-b\"}").with(actor(true))).andExpect(status().isForbidden());
  verify(service,never()).history(any(),any(),any(),anyInt(),any());
 }

 @Test void historyReturnsEffectiveWindowAndRedactsOtherSources()throws Exception{
  var service=spy(new OperationalAwarenessService(mock(JdbcClient.class)));
  doReturn(List.of(Map.of("source_ref","source-a","job_ref","safe-run"),Map.of("source_ref","source-b","job_ref","hidden-run")))
   .when(service).history(eq("source-a"),any(),any(),eq(2),any());
  var http=MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(service,mock(RuntimeIssueService.class),new TrustedAuthorizationContext())).build();
  http.perform(post("/api/internal/v1/ingestion/operations/history").contentType("application/json")
   .content("{\"sourceId\":\"source-a\",\"since\":\"2026-09-21T00:00:00Z\",\"until\":\"2026-09-22T00:00:00Z\",\"limit\":2}").with(actor(true)))
   .andExpect(status().isOk()).andExpect(jsonPath("$.partial").value(true))
   .andExpect(jsonPath("$.items.length()").value(1)).andExpect(jsonPath("$.items[0].job_ref").value("safe-run"))
   .andExpect(jsonPath("$.until").value("2026-09-22T00:00:00Z"))
   .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("hidden-run"))));
 }

 @Test void recentPageCannotHideOlderOpenIncident()throws Exception{
  var service=spy(new OperationalAwarenessService(mock(JdbcClient.class)));
  var resolved=Map.<String,Object>of("source_ref","source-a","lifecycle_state","RESOLVED");
  var open=Map.<String,Object>of("source_ref","source-a","lifecycle_state","OPEN");
  doReturn(List.of(resolved,open)).when(service).incidents(isNull(),any(),isNull(),eq(100),any());
  doReturn(List.of(resolved)).when(service).incidents(isNull(),any(),any(),eq(1),any());
  var http=MockMvcBuilders.standaloneSetup(new OperationalAwarenessApi(service,mock(RuntimeIssueService.class),new TrustedAuthorizationContext())).build();
  http.perform(post("/api/internal/v1/ingestion/operations/summary").contentType("application/json").content("{\"limit\":1,\"since\":\"2026-09-20T00:00:00Z\"}").with(actor(true)))
   .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("DEGRADED")).andExpect(jsonPath("$.partial").value(true));
 }
}
