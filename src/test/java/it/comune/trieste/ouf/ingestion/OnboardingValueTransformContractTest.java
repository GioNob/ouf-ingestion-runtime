package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

class OnboardingValueTransformContractTest {
  @Test void sharedContractHasGovernedDigest() throws Exception {
    byte[] bytes = Files.readAllBytes(Path.of("contracts/ouf-value-types-v1.json"));
    assertThat(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
        .isEqualTo("3b85052e253b985f1082527eb595d2f02e5336047203deb3503a9766c1324295");
  }

  @Test void sharedValueMapContractHasGovernedDigest() throws Exception {
    byte[] bytes = Files.readAllBytes(Path.of("contracts/ouf-value-maps-v1.json"));
    assertThat(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
        .isEqualTo("40b247ad3422e09245a8314fdcc59ffe0b039e25733a2ca8f31aa6ba03ff2d2e");
  }

  private static final String XSD = "http://www.w3.org/2001/XMLSchema#";

  private static Map<String,String> contract() throws IOException {
    return new ObjectMapper().readValue(Path.of("contracts/onboarding-value-transforms-v1.json").toFile(),
        new TypeReference<Map<String,String>>() {});
  }

  @Test void runtimeImplementsEveryContractEntry() throws IOException {
    var types = contract();
    assertThat(types).hasSize(5);
    record Example(String datatype, String cell, String expected) {}
    for (var entry : types.entrySet()) {
      var rule = ConfiguredValueTransform.resolve(entry.getKey());
      var example = switch (rule) {
        case IDENTITY -> new Example("string", " 012 ", " 012 ");
        case DECIMAL_DOT_V1 -> new Example("decimal", "12.300", "12.3");
        case DECIMAL_COMMA_V1 -> new Example("decimal", "12,300", "12.3");
        case DATE_ISO_V1 -> new Example("date", "2024-02-29", "2024-02-29");
        case DATE_DMY_V1 -> new Example("date", "29/02/2024", "2024-02-29");
        default -> throw new AssertionError("Unexpected legacy reader");
      };
      assertThat(entry.getValue()).as(entry.getKey()).isEqualTo(XSD + example.datatype());
      assertThat(rule.apply(example.cell(), 1)).as(entry.getKey()).isEqualTo(example.expected());
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "OUF_ONBOARDING_CHECKOUT", matches = ".+")
  void contractEqualsOwnerDatatypes() throws IOException {
    Path checkout = Path.of(System.getenv("OUF_ONBOARDING_CHECKOUT"));
    Path owner = checkout.resolve("contracts/ouf-value-types-v1.json");
    if (!Files.notExists(owner)) {
      assertThat(Files.readAllBytes(owner)).isEqualTo(Files.readAllBytes(Path.of("contracts/ouf-value-types-v1.json")));
      Path ownerMaps = checkout.resolve("contracts/ouf-value-maps-v1.json");
      if (Files.exists(ownerMaps)) {
        assertThat(Files.readAllBytes(ownerMaps)).isEqualTo(Files.readAllBytes(Path.of("contracts/ouf-value-maps-v1.json")));
      }
      return;
    }
    // The owner can roll out D-084 after Ingestion. Keep checking the deployed vocabulary until then.
    Path source = checkout.resolve("src/main/java/it/comune/trieste/ouf/onboarding/application/ManagedFileExecutionProposal.java");
    var declaration = Pattern.compile("\\bDATATYPES\\s*=\\s*Map\\.of\\s*\\(([^;]*)\\)\\s*;")
        .matcher(Files.readString(source));
    assertThat(declaration.find()).as("Legacy Onboarding DATATYPES initializer").isTrue();
    String[] literals = declaration.group(1).strip().split("\\s*,\\s*", -1);
    assertThat(literals.length % 2).isZero();
    var legacy = new LinkedHashMap<String,String>();
    for (int i = 0; i < literals.length; i += 2) {
      assertThat(literals[i]).matches("\"[A-Za-z0-9_]+\"");
      assertThat(literals[i + 1]).matches("\"[A-Za-z0-9_]+\"");
      String rule = literals[i].substring(1, literals[i].length() - 1);
      String datatype = literals[i + 1].substring(1, literals[i + 1].length() - 1);
      assertThat(legacy.put(rule, XSD + datatype)).isNull();
    }
    assertThat(legacy).isEqualTo(contract());
  }

  @Test void sharedContractCoversRuntimeReaders() throws IOException {
    var readers = new ObjectMapper().readTree(Path.of("contracts/ouf-value-types-v1.json").toFile()).required("readers");
    var types = new LinkedHashMap<String,String>();
    readers.forEach(r -> types.put(r.required("id").asText(),r.required("output").asText()));
    assertThat(types.keySet()).containsExactlyInAnyOrderElementsOf(
        Arrays.stream(ConfiguredValueTransform.values()).map(Enum::name).toList());
    types.forEach((id,type) -> assertThat(ConfiguredValueTransform.resolve(id).output).isEqualTo(type));
  }

  @Test void sharedWhitespaceVectorsRunThroughCellReaders() throws IOException {
    var vectors = new ObjectMapper().readTree(Path.of("contracts/ouf-value-types-v1.json").toFile())
        .required("whitespace").required("vectors");
    String set = "00000000-0000-0000-0000-000000000001";
    String ref = "urn:map@" + set;
    String target = "urn:vocabulary@" + set;
    String concept = "urn:concept:A1";
    var definition = Map.of("targetVocabulary", target, "entries", List.of(Map.of("code", "A1", "concept", concept)));
    var pins = ValuePins.embedded(List.of(Map.of("semanticId", "urn:map", "publicationSetId", set, "revisionId", set,
        "valueMapRef", ref, "targetVocabulary", target, "definition", definition, "definitionSha256", ValuePins.digest(definition))));
    for (var vector : vectors) {
      var rule = ConfiguredValueTransform.resolve(vector.required("reader").asText());
      var reader = rule.bind(rule == ConfiguredValueTransform.VALUE_MAP_V1 ? Map.of("valueMapRef", ref) : Map.of(), pins);
      String cell = vector.required("cell").asText();
      String expected = vector.required("result").asText();
      switch (expected) {
        case "INVALID" -> {
          var error = assertThrows(ConfiguredValueTransform.InvalidValue.class, () -> reader.apply(cell, 7));
          assertThat(error.getMessage()).isEqualTo("ING_TRANSFORM_VALUE_INVALID");
          assertThat(error.rowOrdinal()).isEqualTo(7L);
        }
        case "ABSENT" -> assertThat(reader.apply(cell, 7)).as(rule.name()).isNull();
        case "concept mapped from code 'A1'" -> assertThat(reader.apply(cell, 7)).isEqualTo(concept);
        default -> assertThat(reader.apply(cell, 7)).as(rule.name()).isEqualTo(expected);
      }
    }
  }
}
