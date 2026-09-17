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
}
