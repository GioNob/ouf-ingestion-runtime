package it.comune.trieste.ouf.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import org.junit.jupiter.api.*;

class AcquisitionReceiptPairwiseTest {
  @Test void actualGatewayLuaReceiptIsAcceptedOnlyForExactBodyPathScopesAndAuditBinding()throws Exception{
    String file=System.getenv("OUF_ACQUISITION_VECTORS");Assumptions.assumeTrue(file!=null,"composed Gateway gate supplies actual Lua receipts");
    var root=GatewayReadReceiptVerifierTest.JSON.readTree(Files.readAllBytes(Path.of(file)));int seen=0;
    for(var v:root.get("vectors"))if(v.path("capability").asText().equals("ouf.ingestion.acquisition.read")){
      seen++;String proof=v.get("receipt").asText(),path=v.get("path").asText();byte[] body=v.get("body").asText().getBytes(StandardCharsets.UTF_8);
      var verifier=GatewayReadReceiptVerifierTest.verifier();
      var actor=verifier.verify(proof,"POST",path,body,"corr","11111111-1111-4111-8111-111111111111","idem");
      assertTrue(actor.scopes().containsAll(AcquisitionEvidenceService.CAPABILITIES));assertEquals("human-a",actor.subjectId());
      assertThrows(SecurityException.class,()->verifier.verify(proof,"POST",path,"{}".getBytes(StandardCharsets.UTF_8),"corr","11111111-1111-4111-8111-111111111111","idem"));
      assertThrows(SecurityException.class,()->verifier.verify(proof,"POST",GatewayReadReceiptVerifier.PREFIX+"status",body,"corr","11111111-1111-4111-8111-111111111111","idem"));
    }
    assertEquals(1,seen);
  }
}
