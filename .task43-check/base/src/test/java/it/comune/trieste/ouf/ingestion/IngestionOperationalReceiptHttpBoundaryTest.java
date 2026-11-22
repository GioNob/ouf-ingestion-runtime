package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.receipt.OperationalReceiptFixtures.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.JsonNode;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.receipt.SummaryReceiptFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.*;
import org.springframework.web.bind.annotation.*;

@SpringBootTest(classes = IngestionOperationalReceiptHttpBoundaryTest.App.class, properties = {
    "ouf.summary.enabled=true", "ouf.ingestion.iam.enabled=true", "ouf.ingestion.gateway-read.enabled=true",
    "ouf.ingestion.iam.issuer=https://auth.test/realms/ouf", "ouf.ingestion.iam.audience=gateway"})
@AutoConfigureMockMvc
class IngestionOperationalReceiptHttpBoundaryTest {
  @Autowired MockMvc http;
  @Autowired OperationalAwarenessService service;
  @MockitoBean(name = "ingestionJwtDecoder") JwtDecoder decoder;
  @MockitoBean(name = "operationalReceiptClock") Clock receiptClock;
  static final Path OPERATIONAL_KEY = keyFile(KEY.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
  static final Path READ_KEY = keyFile(GatewayReadReceiptVerifierTest.KEY);
  static Path keyFile(byte[] bytes) {
    try { var file = Files.createTempFile("ouf-public-receipt-test-", ".txt"); Files.write(file, bytes); return file; }
    catch (Exception e) { throw new IllegalStateException(e); }
  }
  @DynamicPropertySource static void installation(DynamicPropertyRegistry p) {
    p.add("ouf.summary.receipt-key-file", OPERATIONAL_KEY::toString);
    p.add("ouf.summary.tenant-id", () -> "tenant-a"); p.add("ouf.summary.issuer", () -> ISSUER);
    p.add("ouf.summary.audience", () -> "gateway"); p.add("ouf.summary.workload", () -> "workload");
    p.add("ouf.ingestion.gateway-read.key-file", READ_KEY::toString);
    p.add("ouf.ingestion.gateway-read.issuer", () -> ISSUER);
    p.add("ouf.ingestion.gateway-read.audience", () -> "gateway");
    p.add("ouf.ingestion.gateway-read.workload", () -> "workload");
  }
  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration(exclude = {org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration.class,
      org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration.class, AuthorizationAutoConfiguration.class})
  @Import({SummaryIdentityConfiguration.class, IngestionIamSecurityConfiguration.class,
      IngestionGatewayReadSecurityConfiguration.class, Probe.class})
  static class App {
    @Bean OperationalAwarenessService operationalService() { return spy(new OperationalAwarenessService(mock(JdbcClient.class))); }
    @Bean OperationalAwarenessApi operationalApi(OperationalAwarenessService service) {
      return new OperationalAwarenessApi(service, mock(RuntimeIssueService.class), new TrustedAuthorizationContext());
    }
    @Bean @Primary GatewayReadReceiptVerifier fixedReadVerifier() { return GatewayReadReceiptVerifierTest.verifier(); }
    @Bean org.springframework.boot.health.contributor.HealthIndicator ingestionReadiness() {
      return () -> org.springframework.boot.health.contributor.Health.up().build();
    }
  }
  @RestController static class Probe {
    @PostMapping({"/api/internal/v1/ingestion/mutation", "/api/internal/v1/ingestion/operations/summary/extra",
        "/api/internal/v1/ingestion/operations/incidents/extra", "/api/internal/v1/another-module/read"})
    Map<String, String> other(HttpServletRequest request) {
      return Map.of("subject", new TrustedAuthorizationContext().owner(request, "operations.status.read").subject());
    }
  }
  static void iamDecoder(JwtDecoder decoder) {
    doThrow(new JwtException("invalid test token")).when(decoder).decode(anyString());
    doReturn(Jwt.withTokenValue("valid-iam").header("alg", "RS256")
        .issuer(ISSUER).audience(List.of("gateway")).subject("human-a")
        .issuedAt(Instant.now().minusSeconds(10)).expiresAt(Instant.now().plusSeconds(300))
        .claim("tenant_id", "tenant-a").claim("ouf_actor_type", "HUMAN").claim("acr", "1")
        .claim("scope", "operations.status.read operations.incident.read ingestion.operations.read")
        .claim("externalRoleRefs", List.of("ouf:viewer")).build()).when(decoder).decode("valid-iam");
  }
  static void stubRows(OperationalAwarenessService service) {
    reset(service);
    doReturn(List.of(Map.of("incident_id", "safe-incident", "source_ref", "source-a", "lifecycle_state", "OPEN", "evidence_ref", "private")))
        .when(service).incidents(any(), any(), any(), anyInt(), any());
    doReturn(List.of()).when(service).status(any(), anyInt(), any());
    doReturn(List.of()).when(service).history(any(), any(), anyInt(), any());
    clearInvocations(service);
  }
  @BeforeEach void before() throws Exception {
    when(receiptClock.instant()).thenReturn(CLOCK.instant());
    iamDecoder(decoder); stubRows(service);
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME, engine(6, true, true));
  }
  static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder mediated(JsonNode vector) throws Exception {
    return post(path(vector)).contentType("application/json").content(vector.get("body").asText())
        .header(SummaryReceiptFilter.HEADER, vector.get("receipt").asText());
  }

  @Test void independentLuaVectorsReachRealControllersWithTheExpectedOwnerAndBody() throws Exception {
    for (var vector : vectors()) {
      clearInvocations(service);
      http.perform(mediated(vector).header("X-OUF-Principal-ID", "forged").header("X-OUF-Tenant-ID", "wrong")
          .header("X-OUF-Actor-Type", "SERVICE").header("X-OUF-Service-Principal", "forged-workload")
          .header("X-OUF-Granted-Scopes", "*").header("X-OUF-External-Role-Refs", "ouf:admin")
          .header("X-OUF-Authorization-Decision-Ref", "forged"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.authorization").value("AUTHORIZED"))
        .andExpect(jsonPath("$.items[0].incident_id").value("safe-incident"))
        .andExpect(jsonPath("$.items[0].evidence_ref").doesNotExist());
      var actor = ArgumentCaptor.forClass(TrustedAuthorizationContext.Context.class);
      verify(service).incidents(isNull(), isNull(), isNull(), eq(JSON.readTree(vector.get("body").asText()).get("limit").asInt()), actor.capture());
      var principal = actor.getValue().owner().principal();
      assertEquals("human-a", principal.subjectId()); assertEquals("HUMAN", principal.actorType().name());
      assertEquals("tenant-a", principal.tenantId()); assertEquals("chatgpt", principal.servicePrincipalId());
      assertEquals("1", principal.authenticationContextRef()); assertEquals("1", principal.claims().acr());
      assertEquals(ISSUER, principal.issuer()); assertEquals("gateway", principal.audience());
      assertEquals(Set.of("ouf:viewer"), principal.claims().externalRoleRefs());
      assertEquals(Set.copyOf(Arrays.asList(payload(vector).get("scope").asText().split(" "))), principal.scopes());
    }
    verifyNoInteractions(decoder);
  }

  @Test void missingInvalidCrossOperationAndBodyChangedReceiptsNeverFallBackToIam() throws Exception {
    for (var vector : vectors()) {
      http.perform(post(path(vector)).contentType("application/json").content(vector.get("body").asText()))
          .andExpect(status().isForbidden()).andExpect(status().reason("INVALID_OPERATIONAL_RECEIPT"));
      http.perform(post(path(vector)).header("Authorization", "Bearer valid-iam").contentType("application/json").content("{}"))
          .andExpect(status().isForbidden());
      http.perform(mediated(vector).header("Authorization", "Bearer valid-iam")).andExpect(status().isForbidden());
      http.perform(mediated(vector).content(vector.get("body").asText() + " ")).andExpect(status().isForbidden());
      http.perform(mediated(vector).header(SummaryReceiptFilter.HEADER, "duplicate")).andExpect(status().isForbidden());
      String otherPath = path(vector).equals(SummaryReceiptFilter.SUMMARY_PATH) ? SummaryReceiptFilter.INCIDENTS_PATH : SummaryReceiptFilter.SUMMARY_PATH;
      http.perform(post(otherPath).contentType("application/json").content(vector.get("body").asText())
          .header(SummaryReceiptFilter.HEADER, vector.get("receipt").asText())).andExpect(status().isForbidden());
      http.perform(get(path(vector)).header("Authorization", "Bearer valid-iam")).andExpect(status().isMethodNotAllowed());
    }
    verifyNoInteractions(service, decoder);
  }

  @Test void missingKeyReturns503AndDoesNotAffectOtherIamPaths() throws Exception {
    byte[] fixture = Files.readAllBytes(OPERATIONAL_KEY);
    try {
      Files.delete(OPERATIONAL_KEY);
      for (var vector : vectors()) http.perform(mediated(vector)).andExpect(status().isServiceUnavailable())
          .andExpect(status().reason("OWNER_IDENTITY_UNAVAILABLE"));
      http.perform(post("/api/internal/v1/another-module/read").header("Authorization", "Bearer valid-iam"))
          .andExpect(status().isOk());
      verifyNoInteractions(service);
    } finally { Files.write(OPERATIONAL_KEY, fixture); }
  }

  @Test void decisionDriftAndRoleRevocationDenyBeforeTheService() throws Exception {
    for (var vector : vectors()) {
      var v = payload(vector); v.put("decisionRef", "bundle:7:" + v.get("capability").asText());
      http.perform(post(path(vector)).contentType("application/json").content(vector.get("body").asText())
          .header(SummaryReceiptFilter.HEADER, sign(v)))
          .andExpect(status().isForbidden()).andExpect(status().reason("NOT_AUTHORIZED"));
    }
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME, engine(6, false, true));
    for (var vector : vectors()) http.perform(mediated(vector)).andExpect(status().isForbidden());
    verifyNoInteractions(service);
  }

  @Test void localOperationalDetailGrantsStillControlSerialization() throws Exception {
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME, engine(6, true, false));
    for (var vector : vectors()) {
      http.perform(mediated(vector)).andExpect(status().isOk()).andExpect(jsonPath("$.authorization").value("REDACTED"))
          .andExpect(jsonPath("$.partial").value(true)).andExpect(jsonPath("$.items.length()").value(0));
      if (path(vector).equals(SummaryReceiptFilter.SUMMARY_PATH))
        http.perform(mediated(vector)).andExpect(jsonPath("$.status").value("UNKNOWN"));
    }
  }

  @Test void statusHistoryKeepTheirSeparateReceiptAndOtherInternalPathsKeepIam() throws Exception {
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME,
        TestAuthorization.runtime("human-a", "HUMAN", Set.of(GatewayReadReceiptVerifier.SCOPE)));
    for (String operation : List.of("status", "history")) {
      var receipt = GatewayReadReceiptVerifierTest.receipt();
      receipt.put("path", GatewayReadReceiptVerifier.PREFIX + operation);
      receipt.put("capability", "ouf.ingestion." + operation);
      http.perform(post(GatewayReadReceiptVerifier.PREFIX + operation).contentType("application/json")
          .content(GatewayReadReceiptVerifierTest.BODY).header(GatewayReadReceiptVerifier.HEADER, GatewayReadReceiptVerifierTest.sign(receipt))
          .header("X-Correlation-ID", "corr").header("X-Tool-Attempt-ID", "11111111-1111-4111-8111-111111111111").header("Idempotency-Key", "idem"))
          .andExpect(status().isOk());
      http.perform(post(GatewayReadReceiptVerifier.PREFIX + operation).header(SummaryReceiptFilter.HEADER, vectors().getFirst().get("receipt").asText())
          .contentType("application/json").content("{}")).andExpect(status().isUnauthorized());
    }
    before();
    for (String path : List.of("/api/internal/v1/ingestion/mutation", SummaryReceiptFilter.SUMMARY_PATH + "/extra",
        SummaryReceiptFilter.INCIDENTS_PATH + "/extra", "/api/internal/v1/another-module/read")) {
      http.perform(post(path).header(SummaryReceiptFilter.HEADER, vectors().getFirst().get("receipt").asText())).andExpect(status().isUnauthorized());
      http.perform(post(path).header("Authorization", "Bearer valid-iam")).andExpect(status().isOk());
    }
    for (String path : List.of(SummaryReceiptFilter.SUMMARY_PATH + "/", SummaryReceiptFilter.INCIDENTS_PATH + "/",
        "/api/internal/v1/ingestion/operations/incidents/explain"))
      http.perform(post(path).header(SummaryReceiptFilter.HEADER, vectors().getFirst().get("receipt").asText())).andExpect(status().isUnauthorized());
  }
  @AfterAll static void removeKeys() throws Exception { Files.deleteIfExists(OPERATIONAL_KEY); Files.deleteIfExists(READ_KEY); }
}
