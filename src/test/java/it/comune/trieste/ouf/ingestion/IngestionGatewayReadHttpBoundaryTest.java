package it.comune.trieste.ouf.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import it.comune.trieste.ouf.authorization.*;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.*;
import org.springframework.web.bind.annotation.*;

@SpringBootTest(classes=IngestionGatewayReadHttpBoundaryTest.App.class,properties={"ouf.ingestion.iam.enabled=false","ouf.ingestion.gateway-read.enabled=true"})
@AutoConfigureMockMvc
class IngestionGatewayReadHttpBoundaryTest {
  @Autowired MockMvc http;
  static final AtomicInteger CALLS=new AtomicInteger();
  static final Path KEY_FILE=keyFile();
  static Path keyFile(){try{var p=Files.createTempFile("ingestion-read-test-key-",".txt");Files.write(p,GatewayReadReceiptVerifierTest.KEY);return p;}catch(Exception e){throw new IllegalStateException(e);}}
  @DynamicPropertySource static void installation(DynamicPropertyRegistry p){
    p.add("ouf.ingestion.gateway-read.key-file",()->KEY_FILE.toString());p.add("ouf.ingestion.gateway-read.issuer",()->GatewayReadReceiptVerifierTest.ISSUER);
    p.add("ouf.ingestion.gateway-read.audience",()->"gateway");p.add("ouf.ingestion.gateway-read.workload",()->"workload");
  }
  @Configuration(proxyBeanMethods=false)
  @EnableAutoConfiguration(exclude={org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration.class,
    org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration.class,AuthorizationAutoConfiguration.class})
  @Import({IngestionGatewayReadSecurityConfiguration.class,IngestionIamDisabledConfiguration.class,Probe.class})
  static class App {
    @Bean @Primary GatewayReadReceiptVerifier clockedReceiptVerifier(){return GatewayReadReceiptVerifierTest.verifier();}
    @Bean org.springframework.boot.health.contributor.HealthIndicator ingestionReadiness(){return ()->org.springframework.boot.health.contributor.Health.up().build();}
  }
  @RestController static class Probe {
    @PostMapping({"/api/internal/v1/ingestion/operations/status","/api/internal/v1/ingestion/operations/history"})
    Map<String,Object> read(@RequestBody Map<String,Object> body,HttpServletRequest request){
      var c=new TrustedAuthorizationContext().require(request,GatewayReadReceiptVerifier.SCOPE,false);CALLS.incrementAndGet();
      return Map.of("subject",c.subject(),"tenant",c.tenantId(),"actor",c.actorType(),"body",body);
    }
    @PostMapping("/api/internal/v1/ingestion/mutation") String mutation(){CALLS.incrementAndGet();return "MUST_NOT_RUN";}
    @GetMapping("/api/trusted-human/v1/ingestion/runs/r") String human(){CALLS.incrementAndGet();return "MUST_NOT_RUN";}
  }
  @BeforeEach void currentPolicy(){
    CALLS.set(0);http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME,TestAuthorization.runtime("human-a","HUMAN",Set.of(GatewayReadReceiptVerifier.SCOPE)));
  }
  static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request(String proof,byte[] body){
    var r=post(GatewayReadReceiptVerifierTest.PATH).contentType("application/json").content(body)
      .header("X-Correlation-ID","corr").header("X-Tool-Attempt-ID","11111111-1111-4111-8111-111111111111").header("Idempotency-Key","idem");
    if(proof!=null)r.header(GatewayReadReceiptVerifier.HEADER,proof);return r;
  }
  @Test void actualReceiptBuildsSdkTrustedPrincipalWithoutIamBearerAndIgnoresIdentityHeaders()throws Exception{
    String proof=GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.receipt());
    http.perform(request(proof,GatewayReadReceiptVerifierTest.BODY).header("X-OUF-Principal-ID","forged").header("X-OUF-Tenant-ID","other").header("X-OUF-Granted-Scopes","*"))
      .andExpect(status().isOk()).andExpect(jsonPath("$.subject").value("human-a")).andExpect(jsonPath("$.actor").value("HUMAN")).andExpect(jsonPath("$.body.sourceId").value("source-a"));
    assertEquals(1,CALLS.get());
    http.perform(request(null,GatewayReadReceiptVerifierTest.BODY)).andExpect(status().isUnauthorized());assertEquals(1,CALLS.get());
  }
  @Test void actualLuaVectorsPassTheHttpAndOwnerPolicyBoundaries()throws Exception{
    String file=System.getenv("OUF_INGESTION_READ_LUA_VECTORS");Assumptions.assumeTrue(file!=null,"cross-repository gate supplies actual Lua vectors");
    var root=GatewayReadReceiptVerifierTest.JSON.readTree(Files.readAllBytes(Path.of(file)));
    for(var v:root.get("vectors"))http.perform(post(v.get("path").asText()).contentType("application/json").content(v.get("body").asText())
        .header(GatewayReadReceiptVerifier.HEADER,v.get("proof").asText()).header("X-Correlation-ID",v.get("correlationId").asText())
        .header("X-Tool-Attempt-ID",v.get("attemptId").asText()).header("Idempotency-Key",v.get("idempotencyKey").asText()))
      .andExpect(status().isOk()).andExpect(jsonPath("$.subject").value("human-a"));
    assertEquals(2,CALLS.get());
  }
  @Test void missingBadOrDuplicateReceiptAndChangedBytesAreDeniedBeforeController()throws Exception{
    String proof=GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.receipt());
    for(String bad:Arrays.asList(null,"bad.proof",proof.substring(0,proof.length()-3)+"xxx"))http.perform(request(bad,GatewayReadReceiptVerifierTest.BODY)).andExpect(status().isUnauthorized());
    http.perform(request(proof,"{}".getBytes(StandardCharsets.UTF_8))).andExpect(status().isUnauthorized());
    http.perform(request(proof,GatewayReadReceiptVerifierTest.BODY).header(GatewayReadReceiptVerifier.HEADER,"second")).andExpect(status().isUnauthorized());assertEquals(0,CALLS.get());
  }
  @Test void identityHeadersOrBearerNeverReplaceOwnerReceipt()throws Exception{
    http.perform(request(null,GatewayReadReceiptVerifierTest.BODY).header("X-OUF-Gateway-Verified","true").header("X-OUF-Actor-Type","HUMAN").header("X-OUF-Principal-ID","human-a"))
      .andExpect(status().isUnauthorized());
    String proof=GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.receipt());
    http.perform(request(proof,GatewayReadReceiptVerifierTest.BODY).header("Authorization","Bearer arbitrary")).andExpect(status().isUnauthorized());assertEquals(0,CALLS.get());
  }
  @Test void ownerStillDeniesDifferentTenantAndCurrentPolicyWithoutGrant()throws Exception{
    var receipt=GatewayReadReceiptVerifierTest.receipt();receipt.put("tenant","another-tenant");
    http.perform(request(GatewayReadReceiptVerifierTest.sign(receipt),GatewayReadReceiptVerifierTest.BODY)).andExpect(status().isForbidden());
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME,TestAuthorization.runtime("human-a","HUMAN",Set.of()));
    http.perform(request(GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.receipt()),GatewayReadReceiptVerifierTest.BODY)).andExpect(status().isForbidden());assertEquals(0,CALLS.get());
  }
  @Test void receiptCannotAuthorizeHumanMutationOtherMethodsQueryOrOversizedBody()throws Exception{
    String proof=GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.receipt());
    http.perform(post("/api/internal/v1/ingestion/mutation").contentType("application/json").content("{}").header(GatewayReadReceiptVerifier.HEADER,proof)).andExpect(status().is4xxClientError());
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r").header(GatewayReadReceiptVerifier.HEADER,proof)).andExpect(status().is4xxClientError());
    http.perform(get(GatewayReadReceiptVerifierTest.PATH).header(GatewayReadReceiptVerifier.HEADER,proof)).andExpect(status().isMethodNotAllowed());
    http.perform(request(proof,GatewayReadReceiptVerifierTest.BODY).queryParam("extra","x")).andExpect(status().isBadRequest());
    http.perform(request(proof,new byte[65537])).andExpect(status().isPayloadTooLarge());assertEquals(0,CALLS.get());
  }
  @AfterAll static void removeFixtureKey()throws Exception{Files.deleteIfExists(KEY_FILE);}
}
