package it.comune.trieste.ouf.receipt;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.*;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.Principal;
import java.time.Clock;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

/** Adapted from 3d8dd34: only a verified, body-bound receipt creates a HUMAN principal. */
public final class SummaryReceiptFilter extends OncePerRequestFilter {
  public static final String HEADER = "X-OUF-Operational-Receipt";
  public static final String SUMMARY_PATH = "/api/internal/v1/ingestion/operations/summary";
  public static final String INCIDENTS_PATH = "/api/internal/v1/ingestion/operations/incidents";
  private static final String DETAIL = "TENANT_OPERATIONAL";
  private final String tenant, issuer, audience, workload, keyFile;
  private final Clock clock;
  private final ObjectMapper json = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  public SummaryReceiptFilter(String tenant, String issuer, String audience, String workload,
                              String keyFile, Clock clock) {
    this.tenant = tenant; this.issuer = issuer; this.audience = audience;
    this.workload = workload; this.keyFile = keyFile; this.clock = clock;
  }

  @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                            FilterChain chain) throws IOException, ServletException {
    if (!"POST".equals(request.getMethod())) { response.sendError(405); return; }
    final byte[] key;
    try {
      for (String binding : new String[]{tenant, issuer, audience, workload}) validateText(binding, false);
      if (keyFile == null || keyFile.isBlank()) throw new IllegalArgumentException();
      String secret = Files.readString(Path.of(keyFile), StandardCharsets.US_ASCII).strip();
      if (!secret.matches("[a-fA-F0-9]{64}")) throw new IllegalArgumentException();
      // The hex characters are ASCII key bytes, NOT decoded hexadecimal bytes.
      key = secret.getBytes(StandardCharsets.US_ASCII);
    } catch (IOException | IllegalArgumentException | SecurityException unavailable) {
      response.sendError(503, "OWNER_IDENTITY_UNAVAILABLE"); return;
    }

    byte[] body = request.getInputStream().readNBytes(65537);
    if (body.length > 65536) { response.sendError(413); return; }
    final TrustedPrincipal principal;
    final String decisionRef;
    final String capability;
    try {
      if (request.getHeaders("Authorization").hasMoreElements()) throw new SecurityException();
      String operation;
      if (SUMMARY_PATH.equals(request.getRequestURI())) operation = "summary";
      else if (INCIDENTS_PATH.equals(request.getRequestURI())) operation = "incidents";
      else throw new SecurityException();
      capability = "ouf.ingestion.operations." + operation;
      String requiredScope = "summary".equals(operation) ? "operations.status.read" : "operations.incident.read";
      var values = request.getHeaders(HEADER);
      if (!values.hasMoreElements()) throw new SecurityException();
      String signed = values.nextElement();
      if (values.hasMoreElements() || signed.length() > 16384) throw new SecurityException();
      String[] parts = signed.split("\\.", -1);
      if (parts.length != 2) throw new SecurityException();
      byte[] payload = decode(parts[0]), signature = decode(parts[1]);
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      if (!MessageDigest.isEqual(mac.doFinal(("ouf-operational-owner-v1." + parts[0])
          .getBytes(StandardCharsets.US_ASCII)), signature)) throw new SecurityException();
      JsonNode receipt = json.readTree(payload);
      if (receipt == null || !receipt.isObject() || integer(receipt, "v") != 1) throw new SecurityException();
      long issued = integer(receipt, "iat"), expires = integer(receipt, "exp"), now = clock.instant().getEpochSecond();
      if (issued > now || expires <= now || expires <= issued || Math.subtractExact(expires, issued) > 30)
        throw new SecurityException();
      var expected = Map.of("purpose", "operational-summary-owner", "method", "POST",
          "path", request.getRequestURI(), "capability", capability, "tenant", tenant,
          "issuer", issuer, "audience", audience, "workload", workload);
      for (var entry : expected.entrySet())
        if (!entry.getValue().equals(text(receipt, entry.getKey()))) throw new SecurityException();
      String bodyHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
      if (!bodyHash.equals(text(receipt, "bodyHash"))) throw new SecurityException();
      Set<String> scopes = words(receipt, "scope", false), roles = words(receipt, "roles", true);
      if (!scopes.contains(requiredScope) || roles.size() > 32) throw new SecurityException();
      decisionRef = text(receipt, "decisionRef");
      String acr = text(receipt, "acr");
      principal = new TrustedPrincipal(new PrincipalContext(text(receipt, "subject"), tenant,
          PrincipalContext.ActorType.HUMAN, text(receipt, "client"), acr, issuer, audience, scopes,
          new PrincipalContext.IdentityClaims(roles, acr, Set.of(), null)));
    } catch (IOException | GeneralSecurityException | IllegalArgumentException | ArithmeticException | SecurityException invalid) {
      response.sendError(403, "INVALID_OPERATIONAL_RECEIPT"); return;
    }

    var wrapped = new BodyRequest(request, body, principal);
    try {
      var owner = OwnerAuthorization.bind(wrapped);
      var decision = owner.require(capability,
          new ResourceContext("capability", null, tenant, null, Map.of("detailLevel", DETAIL)));
      if (!DETAIL.equals(decision.permittedDetailLevel()) || !decisionRef.equals(decision.decisionRef()))
        throw new SecurityException();
    } catch (SecurityException denied) {
      response.sendError(403, "NOT_AUTHORIZED"); return;
    }
    var previous = SecurityContextHolder.getContext();
    var security = SecurityContextHolder.createEmptyContext();
    security.setAuthentication(new UsernamePasswordAuthenticationToken(principal, "", List.of()));
    SecurityContextHolder.setContext(security);
    try { chain.doFilter(wrapped, response); }
    finally { SecurityContextHolder.setContext(previous); }
  }

  private static byte[] decode(String encoded) {
    if (!encoded.matches("[A-Za-z0-9_-]+")) throw new SecurityException();
    byte[] decoded = Base64.getUrlDecoder().decode(encoded);
    if (!Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(encoded)) throw new SecurityException();
    return decoded;
  }

  private static long integer(JsonNode receipt, String field) {
    JsonNode value = receipt.path(field);
    if (!value.isIntegralNumber() || !value.canConvertToLong()) throw new SecurityException();
    return value.longValue();
  }

  private static void validateText(String value, boolean emptyAllowed) {
    if (value == null || (!emptyAllowed && value.isBlank()) || value.length() > 4096
        || value.chars().anyMatch(Character::isISOControl)) throw new SecurityException();
    if (emptyAllowed && !value.isEmpty() && value.isBlank()) throw new SecurityException();
  }

  private static String text(JsonNode receipt, String field) {
    JsonNode value = receipt.path(field);
    if (!value.isTextual()) throw new SecurityException();
    validateText(value.textValue(), false);
    return value.textValue();
  }

  private static Set<String> words(JsonNode receipt, String field, boolean emptyAllowed) {
    JsonNode value = receipt.path(field);
    if (!value.isTextual()) throw new SecurityException();
    String words = value.textValue();
    validateText(words, emptyAllowed);
    if (words.isEmpty()) return Set.of();
    return Set.copyOf(Arrays.asList(words.split(" +")));
  }

  private static final class BodyRequest extends HttpServletRequestWrapper {
    private final byte[] body;
    private final Principal principal;
    BodyRequest(HttpServletRequest request, byte[] body, Principal principal) {
      super(request); this.body = body; this.principal = principal;
    }
    @Override public Principal getUserPrincipal() { return principal; }
    @Override public String getRemoteUser() { return principal.getName(); }
    @Override public int getContentLength() { return body.length; }
    @Override public long getContentLengthLong() { return body.length; }
    @Override public BufferedReader getReader() {
      return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
    }
    @Override public ServletInputStream getInputStream() {
      var input = new ByteArrayInputStream(body);
      return new ServletInputStream() {
        @Override public int read() { return input.read(); }
        @Override public int read(byte[] bytes, int offset, int length) { return input.read(bytes, offset, length); }
        @Override public boolean isFinished() { return input.available() == 0; }
        @Override public boolean isReady() { return true; }
        @Override public void setReadListener(ReadListener listener) { throw new IllegalStateException("SYNCHRONOUS_RECEIPT_REQUIRED"); }
      };
    }
  }
}
