package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.receipt.OperationalReceiptFixtures.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(classes = IngestionOperationalReceiptHttpBoundaryTest.App.class, properties = {
    "ouf.summary.enabled=true", "ouf.ingestion.iam.enabled=true", "ouf.ingestion.gateway-read.enabled=false",
    "ouf.summary.receipt-key-file=", "ouf.summary.tenant-id=", "ouf.summary.issuer=",
    "ouf.summary.audience=", "ouf.summary.workload=",
    "ouf.ingestion.iam.issuer=https://auth.test/realms/ouf", "ouf.ingestion.iam.audience=gateway"})
@AutoConfigureMockMvc
class IngestionOperationalReceiptUnavailableHttpBoundaryTest {
  @Autowired MockMvc http;
  @Autowired OperationalAwarenessService service;
  @MockitoBean(name = "ingestionJwtDecoder") JwtDecoder decoder;
  @Test void missingInstallationConfigurationReturns503WithoutIamFallbackOrControllerCalls() throws Exception {
    clearInvocations(service);
    for (var vector : vectors()) http.perform(IngestionOperationalReceiptHttpBoundaryTest.mediated(vector))
        .andExpect(status().isServiceUnavailable()).andExpect(status().reason("OWNER_IDENTITY_UNAVAILABLE"));
    verifyNoInteractions(service, decoder);
  }
}
