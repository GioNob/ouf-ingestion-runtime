package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;

final class SchemaNotice {
  private SchemaNotice() {}

  static ObjectNode opening(String tenant, String source, UUID issue, PullSchema.Comparison comparison,
                            JsonNode approvedFields) {
    var expected = comparison.expected();
    var observed = comparison.observed();
    var report = JsonNodeFactory.instance.objectNode();
    report.put("incidentKey", "ingestion:" + tenant + ":" + source + ":" + issue);
    report.put("state", "OPEN");
    report.set("expectedSchemaRef", expected.path("schemaRef"));
    report.put("frozenCheckpointRef", "ingestion:schema-incident:" + issue + ":frozen-checkpoint");
    var declaration = comparison.declaration();
    report.set("observedSchema", declaration);

    var differences = report.putArray("differences");
    var actual = observed.path("fields");
    if (approvedFields.isObject() && !approvedFields.isEmpty()) {
      var names = new TreeSet<String>();
      approvedFields.fieldNames().forEachRemaining(names::add);
      actual.fieldNames().forEachRemaining(names::add);
      for (String name : names) {
        var old = approvedFields.path(name);
        var current = actual.path(name);
        if (old.isMissingNode()) add(differences, name, "ADDED");
        else if (current.isMissingNode()) add(differences, name, "REMOVED");
        else {
          if (old.path("type").isTextual() && !old.path("type").equals(current.path("type")))
            add(differences, name, "TYPE_CHANGED");
          if (old.path("nullable").isBoolean() && !old.path("nullable").equals(current.path("nullable")))
            add(differences, name, "NULLABILITY_CHANGED");
        }
      }
    } else {
      // A projection can prove a removal but can omit valid source fields.
      for (JsonNode name : approvedFields)
        if (name.isTextual() && !actual.has(name.textValue())) add(differences, name.textValue(), "REMOVED");
    }
    JsonNode oldKey = expected.path("key"), newKey = observed.path("key");
    if (oldKey.isArray() && newKey.isArray() && !oldKey.equals(newKey)) {
      var names = new TreeSet<String>();
      for (JsonNode key : List.of(oldKey, newKey))
        for (JsonNode name : key) if (name.isTextual()) names.add(name.textValue());
      for (String name : names)
        if (keyPosition(oldKey, name) != keyPosition(newKey, name)) add(differences, name, "KEY_CHANGED");
    }
    if (declaration.has("geometry") && !expected.path("geometry").equals(observed.path("geometry"))) {
      var names = new TreeSet<String>();
      for (var schema : List.of(expected, observed)) {
        var name = schema.path("geometry").path("field");
        if (name.isTextual()) names.add(name.textValue());
      }
      names.forEach(name -> add(differences, name, "GEOMETRY_CHANGED"));
    }
    return report;
  }

  static boolean deliverable(ObjectNode report) {
    var schema = report.path("observedSchema");
    return report.path("incidentKey").asText().length() <= 512 && report.path("expectedSchemaRef").isTextual()
        && !report.path("expectedSchemaRef").asText().isBlank()
        && schema.path("schemaVersion").isTextual() && !schema.path("schemaVersion").asText().isBlank()
        && schema.path("fields").isArray() && !schema.path("fields").isEmpty()
        && !report.path("differences").isEmpty() && report.path("differences").size() <= 4096;
  }

  private static int keyPosition(JsonNode key, String name) {
    for (int i = 0; i < key.size(); i++) if (name.equals(key.get(i).textValue())) return i;
    return -1;
  }

  private static void add(ArrayNode differences, String field, String change) {
    if (!field.isBlank()) differences.addObject().put("field", field).put("change", change);
  }
}
