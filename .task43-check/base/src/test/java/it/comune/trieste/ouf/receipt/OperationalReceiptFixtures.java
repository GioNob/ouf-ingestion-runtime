package it.comune.trieste.ouf.receipt;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Synthetic public fixtures; positive proofs are copied verbatim from the Gateway Lua vectors. */
public final class OperationalReceiptFixtures {
  public static final ObjectMapper JSON = new ObjectMapper();
  public static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(1000), ZoneOffset.UTC);
  public static final String ISSUER = "https://auth.test/realms/ouf";
  public static final String KEY = "ab".repeat(32);
  private OperationalReceiptFixtures() {}

  public static List<JsonNode> vectors() throws Exception {
    var result = new ArrayList<JsonNode>();
    for (String file : List.of("/operational-receipts.json", "/incident-receipts.json")) {
      try (var input = OperationalReceiptFixtures.class.getResourceAsStream(file)) {
        for (JsonNode v : JSON.readTree(input))
          if ("ingestion".equals(v.path("owner").asText()) && v.path("hasRole").asBoolean()) result.add(v);
      }
    }
    if (result.size() != 2) throw new IllegalStateException("missing independent producer vectors");
    return result;
  }

  public static ObjectNode payload(JsonNode vector) throws Exception {
    return (ObjectNode) JSON.readTree(Base64.getUrlDecoder().decode(vector.get("receipt").asText().split("\\.")[0]));
  }
  public static String path(JsonNode vector) throws Exception { return payload(vector).get("path").asText(); }
  public static String scope(JsonNode vector) throws Exception {
    return path(vector).equals(SummaryReceiptFilter.SUMMARY_PATH) ? "operations.status.read" : "operations.incident.read";
  }
  public static String sign(JsonNode payload) throws Exception {
    return signRaw(JSON.writeValueAsBytes(payload), "ouf-operational-owner-v1.");
  }
  // Only for negative/mutated cases; independent positive receipts are never regenerated here.
  public static String signRaw(byte[] payload, String domain) throws Exception {
    var base64 = Base64.getUrlEncoder().withoutPadding();
    String encoded = base64.encodeToString(payload);
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(KEY.getBytes(StandardCharsets.US_ASCII), "HmacSHA256"));
    return encoded + "." + base64.encodeToString(mac.doFinal((domain + encoded).getBytes(StandardCharsets.US_ASCII)));
  }

  public static LocalAuthorization engine(long version, boolean producerGrants, boolean operationalGrants) throws Exception {
    var descriptors = new ArrayList<CapabilityDescriptor>();
    var grants = new ArrayList<Grant>();
    for (String operation : List.of("summary", "incidents")) {
      String cap = "ouf.ingestion.operations." + operation;
      String scope = operation.equals("summary") ? "operations.status.read" : "operations.incident.read";
      descriptors.add(new CapabilityDescriptor(cap, "READ", scope, Set.of(PrincipalContext.ActorType.HUMAN)));
      descriptors.add(new CapabilityDescriptor(scope, "READ", scope, Set.of(PrincipalContext.ActorType.HUMAN)));
      if (producerGrants) grants.add(grant(cap, "capability"));
      if (operationalGrants) grants.add(grant(scope, "operational"));
    }
    descriptors.add(new CapabilityDescriptor("ingestion.operations.read", "READ", "ingestion.operations.read", Set.of(PrincipalContext.ActorType.HUMAN)));
    grants.add(grant("ingestion.operations.read", "capability"));
    var engine = new LocalAuthorization(CLOCK, Duration.ofMinutes(5));
    TestAuthorization.install(engine, new PolicyBundle("bundle", version, Instant.EPOCH, descriptors, grants));
    return engine;
  }
  private static Grant grant(String cap, String resourceType) {
    var constraints = new GrantConstraints("ALLOW", "ouf:viewer", resourceType, null, Map.of(),
        Set.of(), Set.of("TENANT_OPERATIONAL"), null, Set.of(), null);
    return new Grant("grant:" + cap, cap, "tenant-a", null, null, null,
        Instant.EPOCH, Instant.ofEpochSecond(2000), constraints);
  }
}
