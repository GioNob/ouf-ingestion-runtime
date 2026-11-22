package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

class OnboardingValueTransformContractTest {
  @Test void sharedContractHasGovernedDigest() throws Exception {
    byte[] bytes = Files.readAllBytes(Path.of("contracts/ouf-value-types-v1.json"));
    assertThat(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
        .isEqualTo("4b90e85ecd1573404612ef0a0ee627feffaa9c189fe4363e82079bd07824c333");
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
    Path owner = Path.of(System.getenv("OUF_ONBOARDING_CHECKOUT"), "contracts/ouf-value-types-v1.json");
    assertThat(Files.readAllBytes(owner)).isEqualTo(Files.readAllBytes(Path.of("contracts/ouf-value-types-v1.json")));
  }

  @Test void sharedContractCoversRuntimeReaders() throws IOException {
    var readers = new ObjectMapper().readTree(Path.of("contracts/ouf-value-types-v1.json").toFile()).required("readers");
    var types = new LinkedHashMap<String,String>();
    readers.forEach(r -> types.put(r.required("id").asText(),r.required("output").asText()));
    assertThat(types.keySet()).containsExactlyInAnyOrderElementsOf(
        Arrays.stream(ConfiguredValueTransform.values()).map(Enum::name).toList());
    types.forEach((id,type) -> assertThat(ConfiguredValueTransform.resolve(id).output).isEqualTo(type));
  }
}
