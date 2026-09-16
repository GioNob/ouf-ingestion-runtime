package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class AuthorizationIngestionPairwiseTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void sharedAuthorizationSdkAndTrustedContextUseSameActorVocabularyAndDecisionRef() throws Exception {
    String root = System.getenv("AUTHORIZATION_PAIRWISE_ROOT");
    assertThat(root).as("AUTHORIZATION_PAIRWISE_ROOT must pin Source Onboarding in CI").isNotBlank();
    Path contract = Path.of(root, "contracts", "authorization", "authorization-sdk-v1.json");
    assertThat(Files.isRegularFile(contract)).isTrue();

    JsonNode schema = JSON.readTree(contract.toFile());
    assertThat(schema.at("/properties/principal/properties/actorType/enum").findValuesAsText(""))
        .isEmpty();
    assertThat(schema.at("/properties/principal/properties/actorType/enum").toString())
        .isEqualTo("[\"HUMAN\",\"SERVICE\",\"AI_AGENT\"]");
    assertThat(schema.at("/x-ouf-decision/required").toString())
        .contains("decisionRef", "bundleId", "bundleVersion");

    HttpServletRequest request = mock(HttpServletRequest.class);
    Principal principal = () -> "human:alice";
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.getAttribute(TrustedAuthorizationContext.ACTOR_TYPE)).thenReturn("HUMAN");
    when(request.getAttribute(TrustedAuthorizationContext.TENANT)).thenReturn("tenant-a");
    when(request.getAttribute(TrustedAuthorizationContext.CAPABILITIES)).thenReturn(Set.of("operations.status.read"));
    when(request.getAttribute(TrustedAuthorizationContext.DECISION)).thenReturn("baseline:7:ouf.operations.summary");

    var context = new TrustedAuthorizationContext().require(request, "operations.status.read", true);
    assertThat(context.actorType()).isEqualTo("HUMAN");
    assertThat(context.tenantId()).isEqualTo("tenant-a");
    assertThat(context.decisionRef()).isEqualTo("baseline:7:ouf.operations.summary");
  }

  @Test
  void trustedContextFailsClosedForLegacyOrMissingAuthorizationAttributes() {
    HttpServletRequest legacy = mock(HttpServletRequest.class);
    when(legacy.getUserPrincipal()).thenReturn((Principal) () -> "human:legacy");
    when(legacy.getAttribute(TrustedAuthorizationContext.ACTOR_TYPE)).thenReturn("HUMAN_USER");
    when(legacy.getAttribute(TrustedAuthorizationContext.TENANT)).thenReturn("tenant-a");
    when(legacy.getAttribute(TrustedAuthorizationContext.CAPABILITIES)).thenReturn(Set.of("operations.status.read"));
    when(legacy.getAttribute(TrustedAuthorizationContext.DECISION)).thenReturn("decision:legacy");
    assertThatThrownBy(() -> new TrustedAuthorizationContext().resolve(legacy))
        .isInstanceOf(ResponseStatusException.class)
        .hasMessageContaining("ING_ACTOR_TYPE_INVALID");

    HttpServletRequest missingDecision = mock(HttpServletRequest.class);
    when(missingDecision.getUserPrincipal()).thenReturn((Principal) () -> "service:mcp");
    when(missingDecision.getAttribute(TrustedAuthorizationContext.ACTOR_TYPE)).thenReturn("SERVICE");
    when(missingDecision.getAttribute(TrustedAuthorizationContext.TENANT)).thenReturn("tenant-a");
    when(missingDecision.getAttribute(TrustedAuthorizationContext.CAPABILITIES)).thenReturn(Set.of("operations.status.read"));
    assertThatThrownBy(() -> new TrustedAuthorizationContext().resolve(missingDecision))
        .isInstanceOf(ResponseStatusException.class)
        .hasMessageContaining("ING_TRUST_CONTEXT_REQUIRED");
  }
}
