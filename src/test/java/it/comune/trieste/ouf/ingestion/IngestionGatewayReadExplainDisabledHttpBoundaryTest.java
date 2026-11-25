package it.comune.trieste.ouf.ingestion;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

/** D-080: with the opt-in boundary disabled (predecessor profile) a valid explain receipt opens nothing. */
@SpringBootTest(classes=IngestionGatewayReadExplainHttpBoundaryTest.App.class,
    properties={"ouf.ingestion.iam.enabled=false","ouf.ingestion.gateway-read.enabled=false"})
@AutoConfigureMockMvc
class IngestionGatewayReadExplainDisabledHttpBoundaryTest {
  @Autowired MockMvc http;
  @Autowired RuntimeIssueService issues;
  @Test @DisplayName("ING-60n-12 disabled profile still rejects a valid explain owner receipt")
  void disabledProfileStillRejectsAValidExplainReceipt()throws Exception{
    var result = http.perform(IngestionGatewayReadExplainHttpBoundaryTest.explain(
        IngestionGatewayReadExplainHttpBoundaryTest.explainProof(), GatewayReadReceiptVerifierTest.EXPLAIN_BODY)).andReturn();
    int status = result.getResponse().getStatus();
    assertTrue(status == 401 || status == 403, "HTTP " + status);
    verify(issues, never()).get(any(), any());
    verify(issues, never()).explain(any(), any());
  }
}
