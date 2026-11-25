package it.comune.trieste.ouf.ingestion;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * Task-60n (contract C-EX): the real {@link OperationalAwarenessApi#explain} behind the Gateway owner receipt.
 * Same receipt format as status/history; path, capability and scope are those of explain only.
 */
@SpringBootTest(classes=IngestionGatewayReadExplainHttpBoundaryTest.App.class,properties={"ouf.ingestion.iam.enabled=false","ouf.ingestion.gateway-read.enabled=true"})
@AutoConfigureMockMvc
class IngestionGatewayReadExplainHttpBoundaryTest {
  @Autowired MockMvc http;
  static final String PATH=GatewayReadReceiptVerifier.EXPLAIN_PATH,ATTEMPT=GatewayReadReceiptVerifierTest.ATTEMPT;
  static final UUID INCIDENT=UUID.fromString(GatewayReadReceiptVerifierTest.EXPLAIN_INCIDENT),RUN=UUID.fromString("5d0c7e1a-3b4f-4a62-9c18-7f2e6a1b9d34"),
      QUARANTINE=UUID.fromString("8a6b2c4d-1e3f-4a5b-8c7d-9e0f1a2b3c4d");
  @Autowired RuntimeIssueService issues;
  @TempDir static Path fixtureDirectory;
  @BeforeAll static void writeFixtureKey() throws Exception {
    Files.write(fixtureDirectory.resolve("fixture-key"), GatewayReadReceiptVerifierTest.KEY);
  }
  @DynamicPropertySource static void installation(DynamicPropertyRegistry p){
    p.add("ouf.ingestion.gateway-read.key-file",()->fixtureDirectory.resolve("fixture-key").toString());p.add("ouf.ingestion.gateway-read.issuer",()->GatewayReadReceiptVerifierTest.ISSUER);
    p.add("ouf.ingestion.gateway-read.audience",()->"gateway");p.add("ouf.ingestion.gateway-read.workload",()->"workload");
  }
  @Configuration(proxyBeanMethods=false)
  @EnableAutoConfiguration(exclude={org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration.class,
    org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration.class,AuthorizationAutoConfiguration.class})
  @Import({IngestionGatewayReadSecurityConfiguration.class,IngestionIamDisabledConfiguration.class})
  static class App {
    @Bean @Primary GatewayReadReceiptVerifier clockedReceiptVerifier(){return GatewayReadReceiptVerifierTest.verifier();}
    @Bean org.springframework.boot.health.contributor.HealthIndicator ingestionReadiness(){return ()->org.springframework.boot.health.contributor.Health.up().build();}
    @Bean RuntimeIssueService issues(){return spy(new RuntimeIssueService(mock(JdbcClient.class),new ObjectMapper()));}
    @Bean OperationalAwarenessService operationalService(){return new OperationalAwarenessService(mock(JdbcClient.class));}
    @Bean OperationalAwarenessApi operationalApi(OperationalAwarenessService service,RuntimeIssueService issues){return new OperationalAwarenessApi(service,issues,new TrustedAuthorizationContext());}
  }
  static Map<String,Object> row(String source){
    var row=new LinkedHashMap<String,Object>();row.put("issue_id",INCIDENT);row.put("run_id",RUN);row.put("source_id",source);row.put("type_code","QUARANTINE");
    row.put("severity","ERROR");row.put("issue_code","ING_QUARANTINE_ITEM");row.put("evidence_ref","protected-evidence-ref");row.put("status","OPEN");
    row.put("lock_version",1L);row.put("quarantine_id",QUARANTINE);row.put("onboarding_review_ref",null);row.put("resolution_reason",null);return row;
  }
  /** Current owner policy: the given capabilities, tenant-operational detail on source-a only when {@code detail}. */
  void policy(Set<String> capabilities,boolean detail){
    var engine=TestAuthorization.runtime("human-a","HUMAN",capabilities);var old=engine.currentSnapshot().bundle();
    var grants=old.grants().stream().map(g->new Grant(g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),g.servicePrincipalId(),g.organizationId(),g.validFrom(),g.validUntil(),
        new GrantConstraints("ALLOW",null,"operational",null,Map.of("sourceRef","source-a"),Set.of(),detail?Set.of("TENANT_OPERATIONAL"):Set.of(),null,Set.of(),null))).toList();
    try{TestAuthorization.install(engine,new PolicyBundle(old.bundleId(),2,old.publishedAt(),old.capabilities(),grants));}catch(Exception e){throw new IllegalStateException(e);}
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME,engine);
  }
  @BeforeEach void currentPolicy(){
    reset(issues);doReturn(row("source-a")).when(issues).get(eq(INCIDENT),any());
    policy(Set.of(GatewayReadReceiptVerifier.EXPLAIN_SCOPE),true);
  }
  static MockHttpServletRequestBuilder explain(String path,String proof,byte[] body){
    var r=post(path).contentType("application/json").content(body)
      .header("X-Correlation-ID","corr").header("X-Tool-Attempt-ID",ATTEMPT).header("Idempotency-Key","idem");
    if(proof!=null)r.header(GatewayReadReceiptVerifier.HEADER,proof);return r;
  }
  static MockHttpServletRequestBuilder explain(String proof,byte[] body){return explain(PATH,proof,body);}
  static String explainProof()throws Exception{return GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.explainReceipt());}
  void ownerNeverReadAnIssue(){verify(issues,never()).get(any(),any());verify(issues,never()).explain(any(),any());}

  @Test @DisplayName("ING-60n-06 valid explain receipt reaches the real endpoint and returns only the explanation")
  void validExplainReceiptReturnsTheExplanationOfAVisibleIncident()throws Exception{
    http.perform(explain(explainProof(),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)
        .header("X-OUF-Principal-ID","forged").header("X-OUF-Tenant-ID","other").header("X-OUF-Granted-Scopes","*"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.issueId").value(INCIDENT.toString())).andExpect(jsonPath("$.classification").value("ING_QUARANTINE_ITEM"))
      .andExpect(jsonPath("$.severity").value("ERROR")).andExpect(jsonPath("$.status").value("OPEN"))
      .andExpect(jsonPath("$.recommendedAction").value("INSPECT_QUARANTINE_AND_REPROCESS")).andExpect(jsonPath("$.explanationCode").value("EXPLAIN_ING_QUARANTINE_ITEM"))
      .andExpect(jsonPath("$.evidenceRef").doesNotExist()).andExpect(jsonPath("$.quarantineId").doesNotExist())
      .andExpect(content().string(not(containsString("protected-evidence-ref")))).andExpect(content().string(not(containsString(QUARANTINE.toString()))));
    var actor=ArgumentCaptor.forClass(TrustedAuthorizationContext.Context.class);
    verify(issues).explain(eq(INCIDENT),actor.capture());
    // authorization.owner(request,"operations.incident.explain") found the capability among the receipt principal's candidates.
    var owner=actor.getValue();
    assertEquals("human-a",owner.subject());assertEquals("tenant-a",owner.tenantId());assertEquals("HUMAN",owner.actorType());assertNotNull(owner.owner());
    assertTrue(owner.capabilities().contains(GatewayReadReceiptVerifier.EXPLAIN_SCOPE));
  }
  @Test @DisplayName("ING-60n-07 a status/history receipt never opens explain, an explain receipt never opens status/history")
  void statusOrHistoryReceiptIsDeniedOnExplainAndViceVersa()throws Exception{
    for(String op:List.of("status","history")){
      var r=GatewayReadReceiptVerifierTest.receipt();r.put("path",GatewayReadReceiptVerifier.PREFIX+op);r.put("capability","ouf.ingestion."+op);
      r.put("bodyHash",GatewayReadReceiptVerifierTest.hash(GatewayReadReceiptVerifierTest.EXPLAIN_BODY));r.put("scope","mcp.connect ingestion.operations.read operations.incident.explain");
      http.perform(explain(GatewayReadReceiptVerifierTest.sign(r),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isForbidden());
      http.perform(explain(GatewayReadReceiptVerifier.PREFIX+op,explainProof(),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isUnauthorized());
    }
    ownerNeverReadAnIssue();
  }
  @Test @DisplayName("ING-60n-08 scope ingestion.operations.read without operations.incident.explain is denied")
  void readScopeWithoutExplainScopeIsDenied()throws Exception{
    var r=GatewayReadReceiptVerifierTest.explainReceipt();r.put("scope","mcp.connect ingestion.operations.read");
    http.perform(explain(GatewayReadReceiptVerifierTest.sign(r),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isForbidden());
    // A correctly scoped receipt still needs the capability in the CURRENT owner policy: 403 from authorization.owner.
    policy(Set.of(GatewayReadReceiptVerifier.SCOPE),true);
    http.perform(explain(explainProof(),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isForbidden());
    ownerNeverReadAnIssue();
  }
  @Test @DisplayName("ING-60n-09 changed body bytes are denied before the endpoint")
  void changedBodyIsDeniedBeforeTheEndpoint()throws Exception{
    String proof=explainProof();
    for(String body:List.of("{\"incidentId\":\"3c2f5d3b-7a1e-4d54-8b62-1e7d2a4c8f21\"}","{\"incidentId\":\""+INCIDENT+"\",\"extra\":1}","{}"))
      http.perform(explain(proof,body.getBytes(StandardCharsets.UTF_8))).andExpect(status().isForbidden());
    ownerNeverReadAnIssue();
  }
  @Test @DisplayName("ING-60n-10 owner checks stay: other tenant, missing tenant-operational detail, invisible source")
  void ownerStillDeniesOtherTenantMissingDetailAndInvisibleIncident()throws Exception{
    // Even if a row came back for another tenant, the tenant-bound owner grant never releases it.
    var tenant=GatewayReadReceiptVerifierTest.explainReceipt();tenant.put("tenant","another-tenant");
    http.perform(explain(GatewayReadReceiptVerifierTest.sign(tenant),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isForbidden());
    policy(Set.of(GatewayReadReceiptVerifier.EXPLAIN_SCOPE),false);
    http.perform(explain(explainProof(),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isForbidden());
    policy(Set.of(GatewayReadReceiptVerifier.EXPLAIN_SCOPE),true);doReturn(row("source-b")).when(issues).get(eq(INCIDENT),any());
    http.perform(explain(explainProof(),GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andExpect(status().isForbidden())
      .andExpect(content().string(not(containsString("protected-evidence-ref"))));
    verify(issues,never()).explain(any(),any());
  }
  @Test @DisplayName("ING-60n-11 missing/duplicate receipt, bearer, query, method and sub-path never reach explain")
  void receiptBoundaryRejectsMissingDuplicateBearerQueryMethodAndSubPath()throws Exception{
    String proof=explainProof();byte[] body=GatewayReadReceiptVerifierTest.EXPLAIN_BODY;
    http.perform(explain(null,body)).andExpect(status().isUnauthorized());
    String badSignature = GatewayReadReceiptVerifierTest.signRaw(
        GatewayReadReceiptVerifierTest.JSON.writeValueAsBytes(GatewayReadReceiptVerifierTest.explainReceipt()),
        new byte[32], GatewayReadReceiptVerifier.DOMAIN);
    http.perform(explain(badSignature,body)).andExpect(status().isUnauthorized());
    http.perform(explain(proof,body).header(GatewayReadReceiptVerifier.HEADER,"second")).andExpect(status().isUnauthorized());
    http.perform(explain(proof,body).header("Authorization","Bearer arbitrary")).andExpect(status().isUnauthorized());
    http.perform(explain(proof,body).queryParam("extra","x")).andExpect(status().isBadRequest());
    http.perform(get(PATH).header(GatewayReadReceiptVerifier.HEADER,proof)).andExpect(status().isMethodNotAllowed());
    http.perform(explain(proof,new byte[65537])).andExpect(status().isPayloadTooLarge());
    var sub=GatewayReadReceiptVerifierTest.explainReceipt();sub.put("path",PATH+"/extra");
    http.perform(explain(PATH+"/extra",GatewayReadReceiptVerifierTest.sign(sub),body)).andExpect(status().is4xxClientError());
    ownerNeverReadAnIssue();
  }
}
