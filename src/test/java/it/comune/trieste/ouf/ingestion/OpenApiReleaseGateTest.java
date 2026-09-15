package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import org.junit.jupiter.api.Test;

class OpenApiReleaseGateTest {
  @Test void openApi31ContractsHaveUniqueOperationsSecurityAndControllerParity() throws Exception {
    String trusted=Files.readString(Path.of("openapi/ingestion-trusted-human-v1.yaml")),internal=Files.readString(Path.of("openapi/ingestion-internal-v1.yaml"));
    for(String spec:List.of(trusted,internal)){assertThat(spec).startsWith("openapi: 3.1.0").contains("security:","securitySchemes:","operationId:","responses:");Matcher matcher=Pattern.compile("operationId: ([A-Za-z0-9]+)").matcher(spec);Set<String> ids=new HashSet<>();int count=0;while(matcher.find()){count++;assertThat(ids.add(matcher.group(1))).as("unique operationId "+matcher.group(1)).isTrue();}assertThat(count).isGreaterThan(0);}
    assertThat(trusted).contains("/runs/{id}:","/runs/{id}/pause:","/runs/{id}/resume:","/runs/{id}/abort:","/issues:","/issues/{id}:","/issues/{id}/explain:","/issues/schema-observations:","/quarantine:","/quarantine/{id}:","/logs/search:","/logs/aggregate:");
    assertThat(internal).contains("/contracts/runs/{runId}:","/runs/{runId}/lineage:","/contracts/deletion-check:","/contracts/assert-removable:","/contracts/legal-hold:");
  }
}
