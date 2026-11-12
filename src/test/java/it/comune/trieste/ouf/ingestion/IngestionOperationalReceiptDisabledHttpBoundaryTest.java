package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.receipt.OperationalReceiptFixtures.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import it.comune.trieste.ouf.authorization.ServletAuthorization;
import it.comune.trieste.ouf.receipt.SummaryReceiptFilter;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(classes = IngestionOperationalReceiptHttpBoundaryTest.App.class, properties = {
    "ouf.summary.enabled=false", "ouf.ingestion.iam.enabled=true", "ouf.ingestion.gateway-read.enabled=false",
    "ouf.ingestion.iam.issuer=https://auth.test/realms/ouf", "ouf.ingestion.iam.audience=gateway"})
@AutoConfigureMockMvc
class IngestionOperationalReceiptDisabledHttpBoundaryTest {
  @Autowired MockMvc http;
  @Autowired OperationalAwarenessService service;
  @MockitoBean(name = "ingestionJwtDecoder") JwtDecoder decoder;
  @BeforeEach void before() throws Exception {
    IngestionOperationalReceiptHttpBoundaryTest.iamDecoder(decoder);
    IngestionOperationalReceiptHttpBoundaryTest.stubRows(service);
    http.getDispatcherServlet().getServletContext().setAttribute(ServletAuthorization.RUNTIME, engine(6, true, true));
  }
  @Test void disabledBoundaryPreservesTheExistingIamAuthenticationOnBothOperations() throws Exception {
    for (var vector : vectors()) {
      http.perform(IngestionOperationalReceiptHttpBoundaryTest.mediated(vector)).andExpect(status().isUnauthorized());
      http.perform(post(path(vector)).contentType("application/json").content(vector.get("body").asText())
          .header("Authorization", "Bearer valid-iam")).andExpect(status().isOk());
      // An irrelevant operational receipt does not change existing JWT behavior when disabled.
      http.perform(post(path(vector)).contentType("application/json").content(vector.get("body").asText())
          .header("Authorization", "Bearer valid-iam").header(SummaryReceiptFilter.HEADER, "invalid"))
          .andExpect(status().isOk());
    }
    verify(decoder, times(4)).decode("valid-iam");
  }
  @Test void missingOptInDoesNotInstallTheReceiptChain() {
    new WebApplicationContextRunner().withUserConfiguration(SummaryIdentityConfiguration.class).run(context -> {
      assertNull(context.getStartupFailure());
      assertFalse(context.containsBean("operationalSummaryIdentity"));
      assertFalse(context.containsBean("operationalReceiptClock"));
    });
  }
}
