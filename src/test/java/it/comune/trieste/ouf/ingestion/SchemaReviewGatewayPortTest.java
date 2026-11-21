package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SchemaReviewGatewayPortTest {
  @Test void reconciliationRequiresTheOwnerIdentityAndTheRequestedProgress() throws Exception {
    var json=new ObjectMapper();
    var gateway=mock(ExecutionGatewayClient.class);
    var queue=mock(OnboardingReviewRepository.class);
    var port=new SchemaReviewGatewayPort(gateway,json,queue);
    UUID issue=UUID.randomUUID(), owner=UUID.randomUUID();
    String path="/api/onboarding/v1/sources/places/schema-surveillance", key="schema-notice:"+issue+":2";
    String incident="ingestion:t:places:"+issue, at="2026-11-21T09:00:01Z";
    var report=json.createObjectNode().put("incidentKey",incident).put("state","UPDATE")
        .put("observationPolls",2).put("observedAt",at);
    report.putObject("observedSchema").put("fieldsSha256","hash");
    when(queue.report(issue,key)).thenAnswer(call->report.toString());
    String ref="onboarding:schema-surveillance:"+owner;
    when(queue.ownerRef(issue)).thenReturn(Optional.of(ref));
    var row=json.createObjectNode().put("source_id","places").put("issue_id",owner.toString())
        .put("incident_key",incident).put("observed_fingerprint","hash").put("state","OPEN")
        .put("observation_polls",2).put("last_observed_at","2026-11-21T09:00:00Z");
    when(gateway.exchange(path,null,1048576,200,issue.toString(),null))
        .thenAnswer(call->json.writeValueAsBytes(List.of(row)));
    assertThat(port.reconcile(issue,"places",key)).isEmpty();
    row.put("last_observed_at",at);
    assertThat(port.reconcile(issue,"places",key)).contains(new RuntimePorts.OnboardingReviewPort.Receipt(ref,true));
    report.put("state","RESOLVED").put("reason","REVISION_ACTIVATED");
    assertThat(port.reconcile(issue,"places",key)).isEmpty();
    row.put("state","RESOLVED").put("resolution_reason","REVISION_ACTIVATED");
    assertThat(port.reconcile(issue,"places",key)).contains(new RuntimePorts.OnboardingReviewPort.Receipt(ref,true));
    row.put("issue_id",UUID.randomUUID().toString());
    assertThatThrownBy(()->port.reconcile(issue,"places",key)).hasMessage("ING_REVIEW_OWNER_ID_CHANGED");
  }

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
    when(queue.report(incident,key)).thenReturn(report.toString());
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
