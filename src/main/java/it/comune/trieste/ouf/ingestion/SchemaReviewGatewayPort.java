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
    JsonNode report=decode(queue.report(issue,key));
    byte[] response=gateway.exchange(path(source),report,65536,201,correlation,key);
    JsonNode row=decodeBytes(response);
    Receipt receipt=receipt(row,source,report);
    if (report.has("incidentKey")) {
      verifyOwner(issue,receipt);
      if (!applied(row,report)) throw invalid();
    }
    return receipt;
  }
  public Optional<Receipt> reconcile(UUID issue,String source,String key) {
    JsonNode report=decode(queue.report(issue,key));
    JsonNode rows=decodeBytes(gateway.exchange(path(source),null,1048576,200,issue.toString(),null));
    if(!rows.isArray()) throw invalid();
    Receipt found=null;
    boolean matched=false;
    for(JsonNode row:rows) {
      boolean notice=report.has("incidentKey");
      if (notice ? !report.get("incidentKey").equals(row.path("incident_key"))
          : !key.equals(row.path("detail").path("idempotencyKey").asText())) continue;
      if(matched) throw invalid();
      matched=true;
      Receipt candidate=receipt(row,source,report);
      if (notice) verifyOwner(issue,candidate);
      if (!notice || applied(row,report)) found=candidate;
    }
    return Optional.ofNullable(found);
  }
  private static Receipt receipt(JsonNode row,String source,JsonNode report) {
    if(!source.equals(row.path("source_id").asText())) throw invalid();
    if(!report.has("incidentKey") && (!report.get("detail").equals(row.path("detail"))
        || !report.get("observedSchemaRef").equals(row.path("observed_schema_ref"))
        || !report.get("observedFingerprint").equals(row.path("observed_fingerprint"))
        || !report.get("previousFingerprint").equals(row.path("previous_fingerprint"))
        || !report.get("classification").equals(row.path("classification")))) throw invalid();
    String id=row.path("issue_id").asText();
    try {
      UUID uuid=UUID.fromString(id);
      if (!uuid.toString().equalsIgnoreCase(id)) throw invalid();
      return new Receipt("onboarding:schema-surveillance:"+uuid,true);
    }
    catch(IllegalArgumentException e){throw invalid();}
  }
  private void verifyOwner(UUID issue,Receipt receipt) {
    if (queue.ownerRef(issue).filter(ref->!ref.equals(receipt.reviewRef())).isPresent())
      throw new IllegalStateException("ING_REVIEW_OWNER_ID_CHANGED");
  }

  private static boolean applied(JsonNode row,JsonNode report) {
    if (!report.get("incidentKey").equals(row.path("incident_key"))
        || !report.path("observedSchema").path("fieldsSha256").equals(row.path("observed_fingerprint"))
        || !row.path("observation_polls").isIntegralNumber() || !row.path("observation_polls").canConvertToLong()
        || row.path("observation_polls").longValue()<0) throw invalid();
    String state=row.path("state").asText();
    if ("RESOLVED".equals(state)) {
      // Corrective activation can close the owner notice before the producer resumes.
      String reason=row.path("resolution_reason").asText();
      if ("REVISION_ACTIVATED".equals(reason)) return true;
      if (!"SCHEMA_RETURNED".equals(reason) ||
          ("RESOLVED".equals(report.path("state").asText()) && !reason.equals(report.path("reason").asText()))) throw invalid();
      return true;
    }
    if (!Set.of("OPEN","ACKNOWLEDGED").contains(state)) throw invalid();
    if ("RESOLVED".equals(report.path("state").asText())) return false;
    try {
      var observed=java.time.Instant.parse(report.path("observedAt").asText());
      var latest=java.time.Instant.parse(row.path("last_observed_at").asText());
      return row.path("observation_polls").longValue()>=report.path("observationPolls").longValue()
          && !latest.isBefore(observed);
    } catch (java.time.DateTimeException e) { throw invalid(); }
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
