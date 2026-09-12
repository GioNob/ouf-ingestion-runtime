package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.nio.file.*;
import org.junit.jupiter.api.Test;

class TrustedHumanOpenApiTest {
  @Test void quarantineOperationsAreExplicitlyNonMcpAndCapabilityBound() throws Exception {
    String yaml=Files.readString(Path.of("openapi/ingestion-trusted-human-v1.yaml"));
    assertThat(yaml).contains("openapi: 3.1.0","/quarantine/{id}/replay:","x-ouf-authorization-capability: ingestion.quarantine.replay");
    assertThat(yaml).contains("/logs/search:","/logs/aggregate:","ingestion.log.read","ingestion.log.aggregate");
    assertThat(yaml.split("x-ouf-mcp-tool-eligible: false",-1)).hasSize(7);
    assertThat(yaml).doesNotContain("x-ouf-mcp-tool-eligible: true");
  }
}
