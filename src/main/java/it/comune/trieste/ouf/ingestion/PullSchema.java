package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;

/** Only declaration metadata crosses the surveillance boundary, never an item or sample. */
final class PullSchema {
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  private static final Set<String> TYPES = Set.of("string","integer","decimal","boolean","date","datetime","geometry");
  record Comparison(ObjectNode expected, ObjectNode observed, List<String> differences) {
    boolean matches() { return differences.isEmpty(); }
  }

  static JsonNode profile(ExecutionBundle bundle) {
    JsonNode config = JSON.valueToTree(bundle.configuration());
    JsonNode extraction = config.path("extractionProfile");
    for (JsonNode candidate : List.of(extraction.path("sourceUnitProfile"),config.path("publishedBundle").path("sourceUnitProfile"), config.path("sourceUnitProfile"),
        extraction, config)) {
      if (candidate.has("readParameters")) return candidate;
    }
    return config;
  }

  static boolean declaresVerticalContract(ExecutionBundle bundle) {
    return declaresVerticalContract(bundle,profile(bundle).path("readParameters").path("api"));
  }

  static boolean declaresVerticalContract(ExecutionBundle bundle, JsonNode api) {
    var config = bundle.configuration();
    var published = config.get("publishedBundle");
    String contract = "ouf.vertical-source.v1";
    return contract.equals(config.get("sourceContract"))
        || (published instanceof Map<?,?> publication && contract.equals(publication.get("sourceContract")))
        || api.has("schemaVersion");
  }

  static Comparison compare(ExecutionBundle bundle, JsonNode page, JsonNode declaration) {
    JsonNode config = JSON.valueToTree(bundle.configuration());
    JsonNode unit = profile(bundle);
    JsonNode api = unit.path("readParameters").path("api");
    JsonNode approved = config.path("sourceSchema");
    ObjectNode expected = JSON.createObjectNode();
    expected.set("schemaRef", config.path("sourceSchemaRef"));
    expected.set("version", api.isMissingNode() ? config.path("sourceSchemaVersion") : api.path("schemaVersion"));
    JsonNode hash = api.isMissingNode() ? approved.path("fieldsSha256") : api.path("fieldsSha256");
    JsonNode pinnedFields = approved.path("fields");
    String pinnedHash = hash.asText("");
    if (pinnedHash.isEmpty() && api.isMissingNode()) pinnedHash = fingerprint(pinnedFields);
    if (pinnedHash.matches("[0-9a-f]{64}")) pinnedHash = "sha256:" + pinnedHash;
    expected.put("fieldsSha256", pinnedHash);
    expected.set("fields", pinnedFields.isArray() ? descriptors(pinnedFields) :
        unit.has("columns") ? descriptors(unit.path("columns")) : config.path("expectedFieldNames"));
    JsonNode keys = unit.path("keys").path("declared");
    expected.set("key", keys.isArray() ? keys.path(0) :
        approved.has("key") ? approved.get("key") : config.path("identityFields"));
    JsonNode geometry = unit.has("geometry") ? unit.get("geometry") : approved.path("geometry");
    expected.set("geometry", normalizeGeometry(geometry));
    String collection = unit.path("origin").path("collection").asText(config.path("collection").asText(""));
    expected.put("collection", collection);

    ObjectNode observed = JSON.createObjectNode();
    JsonNode fields = declaration.path("fields");
    observed.set("fields", descriptors(fields));
    observed.put("fieldsSha256", fingerprint(fields));
    observed.set("version", page.path("schemaVersion"));
    observed.set("declarationVersion", declaration.path("schemaVersion"));
    observed.set("collection", page.path("collection"));
    observed.set("declarationCollection", declaration.path("collection"));
    observed.set("key", declaration.path("key"));
    observed.set("geometry", normalizeGeometry(declaration.path("geometry")));

    List<String> differences = new ArrayList<>();
    JsonNode originBinding=unit.path("origin").path("bindingRef");
    if(originBinding.isTextual() && !originBinding.textValue().equals(bundle.bindingRef())) differences.add("BINDING");
    if (!text(expected.path("version")) || !expected.path("version").equals(page.path("schemaVersion"))
        || !page.path("schemaVersion").equals(declaration.path("schemaVersion"))) differences.add("VERSION");
    if (!text(page.path("collection")) || !page.path("collection").equals(declaration.path("collection"))
        || (!collection.isEmpty() && !collection.equals(page.path("collection").asText()))) differences.add("COLLECTION");
    if (pinnedHash.isEmpty()) differences.add("APPROVED_FINGERPRINT_MISSING");
    else if (!pinnedHash.equals(observed.path("fieldsSha256").asText())) differences.add("FIELDS");
    if (!expected.path("key").isArray() || expected.path("key").isEmpty()
        || !expected.path("key").equals(declaration.path("key"))) differences.add("KEY");
    JsonNode actualGeometry=declaration.path("geometry");
    boolean geometryDeclared=actualGeometry.isNull() || (text(actualGeometry.path("field")) &&
        text(actualGeometry.path("type")) && text(actualGeometry.path("crs")));
    if (!geometryDeclared || !expected.path("geometry").equals(observed.path("geometry"))) differences.add("GEOMETRY");
    return new Comparison(expected, observed, List.copyOf(differences));
  }

  static String fingerprint(JsonNode fields) {
    if (!fields.isArray() || fields.isEmpty() || fields.size() > 1024) return "";
    SortedMap<String,Object> byName = new TreeMap<>();
    for (JsonNode field : fields) {
      if (!text(field.path("name")) || !TYPES.contains(field.path("type").asText(""))
          || !field.path("nullable").isBoolean()) return "";
      if (byName.put(field.get("name").textValue(), JSON.convertValue(field, Map.class)) != null) return "";
    }
    try { return ExecutionGatewayClient.hash(JSON.writeValueAsBytes(byName.values())); }
    catch (Exception e) { throw new IllegalStateException("ING_SCHEMA_EVIDENCE_INVALID",e); }
  }

  private static JsonNode descriptors(JsonNode fields) {
    ObjectNode result = JSON.createObjectNode();
    if (fields.isArray() && fields.size() <= 1024) for (JsonNode field : fields) {
      if (!text(field.path("name"))) continue;
      ObjectNode shape = result.putObject(field.get("name").asText());
      shape.set("type", field.has("type") ? field.get("type") : field.path("nativeType"));
      shape.set("nullable", field.path("nullable"));
    }
    return result;
  }

  private static JsonNode normalizeGeometry(JsonNode geometry) {
    if (geometry.isNull() || geometry.isMissingNode()) return NullNode.instance;
    ObjectNode value = JSON.createObjectNode();
    value.set("field", geometry.has("column") ? geometry.get("column") : geometry.path("field"));
    value.set("type", geometry.path("type"));
    value.set("crs", geometry.has("sourceCrs") ? geometry.get("sourceCrs") : geometry.path("crs"));
    return value;
  }

  private static boolean text(JsonNode node) { return node.isTextual() && !node.textValue().isBlank(); }
}
