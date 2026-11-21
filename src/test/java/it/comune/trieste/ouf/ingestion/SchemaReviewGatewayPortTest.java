package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SchemaReviewGatewayPortTest {
  @Test void postAndReconciliationValidateTheSameOwnerReceipt() throws Exception {
    var json=new ObjectMapper();
    var gateway=mock(ExecutionGatewayClient.class);
    var queue=mock(OnboardingReviewRepository.class);
    var port=new SchemaReviewGatewayPort(gateway,json,queue);
    UUID incident=UUID.randomUUID(), remote=UUID.randomUUID();
    String key="ingestion-schema:"+incident,source="places",path="/api/onboarding/v1/sources/places/schema-surveillance";
    var detail=Map.of("tenant","tenant-a","sourceId",source,"incidentId",incident.toString(),"idempotencyKey",key);
    var report=json.valueToTree(Map.of("observedSchemaRef","ingestion:schema-incident:"+incident,
        "previousFingerprint","old","observedFingerprint","new","classification","BREAKING","detail",detail));
    when(queue.report(incident)).thenReturn(report.toString());
    var receipt=Map.of("source_id",source,"issue_id",remote,"observed_schema_ref","ingestion:schema-incident:"+incident,
        "previous_fingerprint","old","observed_fingerprint","new","classification","BREAKING","detail",detail);
    when(gateway.exchange(path,report,65536,201,"correlation",key)).thenReturn(json.writeValueAsBytes(receipt));
    when(gateway.exchange(path,null,1048576,200,incident.toString(),null)).thenReturn(
        json.writeValueAsBytes(List.of(receipt)),json.writeValueAsBytes(List.of(receipt,receipt)),json.writeValueAsBytes(List.of()));
    var confirmed=port.request(incident,source,"evidence","correlation",key);
    assertThat(confirmed.reviewRef()).isEqualTo("onboarding:schema-surveillance:"+remote);
    assertThat(port.reconcile(incident,source,key)).contains(confirmed);
    assertThatThrownBy(()->port.reconcile(incident,source,key)).hasMessage("ING_REVIEW_RECEIPT_INVALID");
    assertThat(port.reconcile(incident,source,key)).isEmpty();
    verify(gateway,times(1)).exchange(path,report,65536,201,"correlation",key);
  }
}
