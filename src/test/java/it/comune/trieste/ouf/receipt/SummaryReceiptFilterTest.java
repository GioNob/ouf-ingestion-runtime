package it.comune.trieste.ouf.receipt;

import static it.comune.trieste.ouf.receipt.OperationalReceiptFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import it.comune.trieste.ouf.ingestion.TrustedAuthorizationContext;
import jakarta.servlet.http.HttpServletRequest;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.*;
import org.springframework.security.core.context.SecurityContextHolder;

class SummaryReceiptFilterTest {
  @TempDir Path tmp;
  Path key;
  @BeforeEach void keyFile() throws Exception { key = tmp.resolve("public-test-key"); Files.writeString(key, KEY); }
  SummaryReceiptFilter filter(Clock clock) {
    return new SummaryReceiptFilter("tenant-a", ISSUER, "gateway", "workload", key.toString(), clock);
  }
  MockHttpServletRequest request(JsonNode vector, String proof, String body) throws Exception {
    var request = new MockHttpServletRequest("POST", path(vector));
    request.setContent(body.getBytes(StandardCharsets.UTF_8));
    request.getServletContext().setAttribute(ServletAuthorization.RUNTIME, engine(6, true, true));
    if (proof != null) request.addHeader(SummaryReceiptFilter.HEADER, proof);
    request.addHeader("X-OUF-Principal-ID", "forged-admin");
    request.addHeader("X-OUF-External-Role-Refs", "ouf:admin");
    return request;
  }
  MockHttpServletResponse denied(SummaryReceiptFilter filter, MockHttpServletRequest request, int status, String code) throws Exception {
    var response = new MockHttpServletResponse();
    filter.doFilter(request, response, (r, s) -> fail("controller must not run"));
    assertEquals(status, response.getStatus());
    if (code != null) assertEquals(code, response.getErrorMessage());
    return response;
  }
  void invalid(JsonNode vector, String proof) throws Exception {
    denied(filter(CLOCK), request(vector, proof, vector.get("body").asText()), 403, "INVALID_OPERATIONAL_RECEIPT");
  }

  @Test void independentLuaVectorsBindTheFullHumanPrincipalAndPreserveExactBody() throws Exception {
    for (var vector : vectors()) {
      String body = vector.get("body").asText();
      var req = request(vector, vector.get("receipt").asText(), body);
      var response = new MockHttpServletResponse();
      var ran = new AtomicBoolean();
      var previous = SecurityContextHolder.getContext();
      filter(CLOCK).doFilter(req, response, (r, s) -> {
        ran.set(true);
        var request = (HttpServletRequest) r;
        var principal = (TrustedPrincipal) request.getUserPrincipal();
        var c = principal.context();
        assertEquals("human-a", c.subjectId()); assertEquals("tenant-a", c.tenantId());
        assertEquals(PrincipalContext.ActorType.HUMAN, c.actorType());
        assertEquals("chatgpt", c.servicePrincipalId()); assertEquals("1", c.authenticationContextRef());
        assertEquals(ISSUER, c.issuer()); assertEquals("gateway", c.audience());
        assertEquals(Set.of("ouf:viewer"), c.claims().externalRoleRefs());
        assertEquals("1", c.claims().acr()); assertEquals(Set.of(), c.claims().amr());
        assertNull(c.claims().authenticatedAt());
        try {
          assertEquals(Set.copyOf(Arrays.asList(payload(vector).get("scope").asText().split(" "))), c.scopes());
          var owner = new TrustedAuthorizationContext().owner(request, scope(vector));
          assertEquals(c, owner.owner().principal());
        } catch (Exception e) { throw new AssertionError(e); }
        assertEquals("human-a", request.getRemoteUser());
        assertEquals(principal, SecurityContextHolder.getContext().getAuthentication().getPrincipal());
        assertArrayEquals(body.getBytes(StandardCharsets.UTF_8), request.getInputStream().readAllBytes());
        assertEquals(body, request.getReader().readLine());
        assertEquals(body.length(), request.getContentLength()); assertEquals(body.length(), request.getContentLengthLong());
      });
      assertTrue(ran.get()); assertEquals(200, response.getStatus());
      assertSame(previous, SecurityContextHolder.getContext());
    }
  }

  @Test void everyBindingAndRequiredScopeIsCheckedEvenWithAValidSignature() throws Exception {
    for (var vector : vectors()) {
      for (String field : List.of("purpose", "method", "path", "capability", "tenant", "issuer", "audience", "workload", "bodyHash", "scope")) {
        var changed = payload(vector); changed.put(field, "wrong"); invalid(vector, sign(changed));
      }
      var v = payload(vector); v.put("scope", scope(vector).equals("operations.status.read") ? "operations.incident.read" : "operations.status.read");
      invalid(vector, sign(v));
      v = payload(vector); v.put("bodyHash", v.get("bodyHash").asText().toUpperCase(Locale.ROOT)); invalid(vector, sign(v));
      for (String field : List.of("v", "iat", "exp")) {
        v = payload(vector); v.put(field, "1"); invalid(vector, sign(v));
        v = payload(vector); v.put(field, 1.0); invalid(vector, sign(v));
        v = payload(vector); v.put(field, BigInteger.ONE.shiftLeft(80)); invalid(vector, sign(v));
      }
      v = payload(vector); v.put("v", 4294967297L); invalid(vector, sign(v));
      v = payload(vector); v.put("roles", java.util.stream.IntStream.range(0, 33)
          .mapToObj(i -> "role:" + i).collect(java.util.stream.Collectors.joining(" "))); invalid(vector, sign(v));
    }
  }

  @Test void allRequiredFieldsRejectMissingWrongTypeBlankControlAndOverlongText() throws Exception {
    for (var vector : vectors()) {
      var original = payload(vector);
      for (Iterator<String> fields = original.fieldNames(); fields.hasNext();) {
        String field = fields.next();
        var changed = original.deepCopy(); changed.remove(field); invalid(vector, sign(changed));
        changed = original.deepCopy(); changed.putNull(field); invalid(vector, sign(changed));
        if (!original.get(field).isTextual()) continue;
        for (String bad : List.of(" ", "x\u0000", "x\u007f", "x".repeat(4097))) {
          changed = original.deepCopy(); changed.put(field, bad); invalid(vector, sign(changed));
        }
        if (!field.equals("roles")) { changed = original.deepCopy(); changed.put(field, ""); invalid(vector, sign(changed)); }
        changed = original.deepCopy(); changed.put(field, 12); invalid(vector, sign(changed));
      }
    }
  }

  @Test void invalidTimesIncludingOverflowCannotExtendTheThirtySecondWindow() throws Exception {
    for (var vector : vectors()) {
      for (long[] times : List.of(new long[]{1001, 1030}, new long[]{970, 1000}, new long[]{1000, 1000},
          new long[]{1000, 999}, new long[]{1000, 1031}, new long[]{Long.MIN_VALUE, Long.MAX_VALUE})) {
        var v = payload(vector); v.put("iat", times[0]); v.put("exp", times[1]); invalid(vector, sign(v));
      }
      for (long now : new long[]{999, 1030})
        denied(filter(Clock.fixed(Instant.ofEpochSecond(now), ZoneOffset.UTC)),
            request(vector, vector.get("receipt").asText(), vector.get("body").asText()), 403, "INVALID_OPERATIONAL_RECEIPT");
    }
  }

  @Test void malformedSignatureWrongDomainDuplicateJsonAndTrailingJsonFailClosed() throws Exception {
    for (var vector : vectors()) {
      String proof = vector.get("receipt").asText();
      for (String bad : Arrays.asList(null, "", "a.b", "a.b.c", "x".repeat(16385), proof + "=",
          proof.substring(0, proof.lastIndexOf('.') + 1) + "A".repeat(43))) invalid(vector, bad);
      invalid(vector, signRaw(JSON.writeValueAsBytes(payload(vector)), "ouf-ingestion-read-owner-v1."));
      String raw = payload(vector).toString();
      for (String bad : List.of("[]", "null", raw + " {}", raw.replaceFirst("\\{", "{\"v\":1,")))
        invalid(vector, signRaw(bad.getBytes(StandardCharsets.UTF_8), "ouf-operational-owner-v1."));
      var request = request(vector, proof, vector.get("body").asText());
      request.addHeader(SummaryReceiptFilter.HEADER, proof);
      denied(filter(CLOCK), request, 403, "INVALID_OPERATIONAL_RECEIPT");
    }
  }

  @Test void bodyBytesAndOperationCannotBeReplayedOrOverriddenByAuthorization() throws Exception {
    for (var vector : vectors()) {
      String proof = vector.get("receipt").asText(), body = vector.get("body").asText();
      denied(filter(CLOCK), request(vector, proof, body + " "), 403, "INVALID_OPERATIONAL_RECEIPT");
      var request = request(vector, proof, body);
      request.setRequestURI(path(vector).equals(SummaryReceiptFilter.SUMMARY_PATH) ? SummaryReceiptFilter.INCIDENTS_PATH : SummaryReceiptFilter.SUMMARY_PATH);
      denied(filter(CLOCK), request, 403, "INVALID_OPERATIONAL_RECEIPT");
      for (String bearer : List.of("Bearer valid-or-invalid", "")) {
        request = request(vector, proof, body); request.addHeader("Authorization", bearer);
        denied(filter(CLOCK), request, 403, "INVALID_OPERATIONAL_RECEIPT");
      }
      request = request(vector, proof, body); request.setContent(new byte[65537]);
      denied(filter(CLOCK), request, 413, null);
      request = request(vector, proof, body); request.setMethod("GET");
      denied(filter(CLOCK), request, 405, null);
    }
  }

  @Test void policyRevocationDecisionDriftAndRemovedRolesDenyLocally() throws Exception {
    for (var vector : vectors()) {
      for (var engine : List.of(engine(7, true, true), engine(6, false, true))) {
        var request = request(vector, vector.get("receipt").asText(), vector.get("body").asText());
        request.getServletContext().setAttribute(ServletAuthorization.RUNTIME, engine);
        denied(filter(CLOCK), request, 403, "NOT_AUTHORIZED");
      }
      var changed = payload(vector); changed.put("decisionRef", "bundle:6:wrong-capability");
      denied(filter(CLOCK), request(vector, sign(changed), vector.get("body").asText()), 403, "NOT_AUTHORIZED");
      changed = payload(vector); changed.put("roles", "");
      // Empty roles are valid receipt syntax, but cannot satisfy the local role grant.
      denied(filter(CLOCK), request(vector, sign(changed), vector.get("body").asText()), 403, "NOT_AUTHORIZED");
      var request = request(vector, vector.get("receipt").asText(), vector.get("body").asText());
      request.getServletContext().removeAttribute(ServletAuthorization.RUNTIME);
      denied(filter(CLOCK), request, 403, "NOT_AUTHORIZED");
    }
  }

  @Test void emptyRolesCanAuthenticateWhenTheCurrentPolicyAuthorizesTheSubject() throws Exception {
    var roleless = new LocalAuthorization(CLOCK, Duration.ofMinutes(5));
    var original = engine(6, true, true).currentSnapshot().bundle();
    var grants = original.grants().stream().map(g -> {
      var c = g.constraints();
      var constraints = new GrantConstraints(
          "ALLOW", null, c.resourceType(), null, Map.of(), Set.of(), Set.of("TENANT_OPERATIONAL"), null, Set.of(), null);
      return new Grant(g.grantId(), g.capabilityId(),
          g.tenantId(), "human-a", null, null, g.validFrom(), g.validUntil(), constraints);
    }).toList();
    TestAuthorization.install(roleless, new PolicyBundle(
        original.bundleId(), original.version(), original.publishedAt(), original.capabilities(), grants));
    for (var vector : vectors()) {
      var changed = payload(vector); changed.put("roles", "");
      var req = request(vector, sign(changed), vector.get("body").asText());
      req.getServletContext().setAttribute(ServletAuthorization.RUNTIME, roleless);
      var ran = new AtomicBoolean();
      var response = new MockHttpServletResponse();
      filter(CLOCK).doFilter(req, response, (r, s) -> {
        ran.set(true);
        assertEquals(Set.of(), ((TrustedPrincipal) ((HttpServletRequest) r).getUserPrincipal()).context().claims().externalRoleRefs());
      });
      assertTrue(ran.get()); assertEquals(200, response.getStatus());
    }
  }

  @Test void missingIdentityBindingsAndUnavailableOrMalformedKeyReturn503() throws Exception {
    var vector = vectors().getFirst();
    for (int missing = 0; missing < 5; missing++) {
      String[] config = {"tenant-a", ISSUER, "gateway", "workload", key.toString()};
      config[missing] = null;
      denied(new SummaryReceiptFilter(config[0], config[1], config[2], config[3], config[4], CLOCK),
          request(vector, vector.get("receipt").asText(), vector.get("body").asText()), 503, "OWNER_IDENTITY_UNAVAILABLE");
    }
    for (String badKey : List.of("", "ab".repeat(31), "gh".repeat(32))) {
      Files.writeString(key, badKey);
      denied(filter(CLOCK), request(vector, vector.get("receipt").asText(), vector.get("body").asText()), 503, "OWNER_IDENTITY_UNAVAILABLE");
    }
    Files.delete(key);
    denied(filter(CLOCK), request(vector, vector.get("receipt").asText(), vector.get("body").asText()), 503, "OWNER_IDENTITY_UNAVAILABLE");
  }
}
