package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.nio.file.*;
import org.junit.jupiter.api.Test;

class TrustedHumanOpenApiTest {
  @Test void technicalOperationsStayNonMcpWhileConversationalIssueCapabilitiesAreExplicit() throws Exception {
    String yaml=Files.readString(Path.of("openapi/ingestion-trusted-human-v1.yaml"));
    assertThat(yaml).contains("openapi: 3.1.0","/quarantine/{id}/replay:","x-ouf-authorization-capability: ingestion.quarantine.replay");
    assertThat(yaml).contains("/logs/search:","/logs/aggregate:","ingestion.log.read","ingestion.log.aggregate");
    assertThat(yaml.split("x-ouf-mcp-tool-eligible: false",-1)).hasSize(11);
    assertThat(yaml).contains("/issues/schema-observations:","ouf.ingestion.issue.search","ouf.ingestion.issue.resolve","ouf.ingestion.onboarding-review.request","x-ouf-requires-human-user: true","x-ouf-mcp-tool-eligible: true");
    assertThat(yaml).contains("/issues/{id}/explain:","ouf.ingestion.issue.explain","/quarantine/{id}/preview:","ouf.ingestion.quarantine.inspect","/quarantine/{id}/retry:","ouf.ingestion.quarantine.retry","/quarantine/{id}/reprocess:","RETRY_READY","REPROCESSING","SUPERSEDED");
    assertThat(yaml).contains("/runs/{id}/pause:","/runs/{id}/resume:","/runs/{id}/abort:","REPRODUCE","REPROCESS_CURRENT","REPROCESS_TARGET");
    String internal=Files.readString(Path.of("openapi/ingestion-internal-v1.yaml"));assertThat(internal).contains("/contracts/runs/{runId}:","ingestion.contract.read","x-ouf-mcp-tool-eligible: false","/contracts/deletion-check:","/contracts/assert-removable:","/contracts/legal-hold:");
  }
}
