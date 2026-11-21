package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name="ouf.ingestion.execution.enabled",havingValue="true")
final class SchemaReviewGatewayPort implements RuntimePorts.OnboardingReviewPort {
  private final ExecutionGatewayClient gateway;
  private final ObjectMapper json;
  private final OnboardingReviewRepository queue;
  SchemaReviewGatewayPort(ExecutionGatewayClient gateway,ObjectMapper json,OnboardingReviewRepository queue) {
    this.gateway=gateway;this.json=json;this.queue=queue;
  }
  public Receipt request(UUID issue,String source,String evidence,String correlation,String key) {
    JsonNode report=decode(queue.report(issue));
    byte[] response=gateway.exchange(path(source),report,65536,201,correlation,key);
    return receipt(decodeBytes(response),source,report);
  }
  public Optional<Receipt> reconcile(UUID issue,String source,String key) {
    JsonNode report=decode(queue.report(issue));
    JsonNode rows=decodeBytes(gateway.exchange(path(source),null,1048576,200,issue.toString(),null));
    if(!rows.isArray()) throw invalid();
    Receipt found=null;
    for(JsonNode row:rows) {
      if(!key.equals(row.path("detail").path("idempotencyKey").asText())) continue;
      if(found!=null) throw invalid();
      found=receipt(row,source,report);
    }
    return Optional.ofNullable(found);
  }
  private static Receipt receipt(JsonNode row,String source,JsonNode report) {
    if(!source.equals(row.path("source_id").asText()) || !report.get("detail").equals(row.path("detail"))
        || !report.get("observedSchemaRef").equals(row.path("observed_schema_ref"))
        || !report.get("observedFingerprint").equals(row.path("observed_fingerprint"))
        || !report.get("previousFingerprint").equals(row.path("previous_fingerprint"))
        || !report.get("classification").equals(row.path("classification"))) throw invalid();
    try { return new Receipt("onboarding:schema-surveillance:"+UUID.fromString(row.path("issue_id").asText()),true); }
    catch(IllegalArgumentException e){throw invalid();}
  }
  private static String path(String source) {
    if(!source.matches("[A-Za-z0-9_-][A-Za-z0-9._-]{0,254}")) throw new IllegalArgumentException("ING_SOURCE_ID_INVALID");
    return "/api/onboarding/v1/sources/"+source+"/schema-surveillance";
  }
  private JsonNode decode(String value) {
    try { return json.readTree(value); } catch(Exception e){throw invalid();}
  }
  private JsonNode decodeBytes(byte[] value) {
    try { JsonNode row=json.readTree(value); if(row==null)throw invalid(); return row; }
    catch(Exception e){throw invalid();}
  }
  private static IllegalStateException invalid(){return new IllegalStateException("ING_REVIEW_RECEIPT_INVALID");}
}
