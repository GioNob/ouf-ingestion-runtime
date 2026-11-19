package it.comune.trieste.ouf.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

/** The opt-in boundary never turns the prior disabled deployment into an open API. */
@SpringBootTest(classes=IngestionGatewayReadHttpBoundaryTest.App.class,
    properties={"ouf.ingestion.iam.enabled=false","ouf.ingestion.gateway-read.enabled=false"})
@AutoConfigureMockMvc
class IngestionGatewayReadDisabledHttpBoundaryTest {
  @Autowired MockMvc http;
  @Test void disabledProfileStillRejectsEvenAValidOwnerReceipt()throws Exception{
    IngestionGatewayReadHttpBoundaryTest.CALLS.set(0);
    http.perform(IngestionGatewayReadHttpBoundaryTest.request(GatewayReadReceiptVerifierTest.sign(GatewayReadReceiptVerifierTest.receipt()),GatewayReadReceiptVerifierTest.BODY))
      .andExpect(status().is4xxClientError());
    assertEquals(0,IngestionGatewayReadHttpBoundaryTest.CALLS.get());
  }
}
