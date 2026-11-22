package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.PrincipalContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Authenticates only the two closed READ operations; never trusts ordinary identity headers. */
public final class GatewayReadReceiptVerifier {
  public static final String HEADER="X-OUF-Ingestion-Read-Receipt", SCOPE="ingestion.operations.read";
  static final String PREFIX="/api/internal/v1/ingestion/operations/", DOMAIN="ouf-ingestion-read-owner-v1.";
  private final ObjectMapper json;
  private final byte[] key;
  private final String issuer,audience,workload;
  private final Clock clock;
  public record Receipt(int v,String purpose,String method,String path,String bodyHash,String capability,long iat,long exp,
      String issuer,String audience,String workload,String actor,String subject,String tenant,String acr,String roles,String scope,
      String idempotencyKey,String correlationId,String attemptId,String decisionRef){}
  public GatewayReadReceiptVerifier(String file,String issuer,String audience,String workload) {
    this(readKey(file),issuer,audience,workload,Clock.systemUTC());
  }
  GatewayReadReceiptVerifier(byte[] key,String issuer,String audience,String workload,Clock clock) {
    if(key==null || !new String(key,StandardCharsets.US_ASCII).matches("[0-9a-fA-F]{64}"))
      throw new IllegalArgumentException("INGESTION_READ_OWNER_KEY_REQUIRED");
    var uri=java.net.URI.create(issuer);
    if(!"https".equals(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null
        ||!text(audience,4096)||!text(workload,4096))throw new IllegalArgumentException("INGESTION_READ_INSTALLATION_BINDINGS_REQUIRED");
    this.key=key.clone();this.issuer=issuer;this.audience=audience;this.workload=workload;this.clock=Objects.requireNonNull(clock);
    json=new ObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT).disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);
  }
  private static byte[] readKey(String file) {
    try {
      Path path=Path.of(file);if(!path.isAbsolute())throw new IllegalArgumentException();
      try(var input=Files.newInputStream(path)) {
        byte[] raw=input.readNBytes(129);if(raw.length>128)throw new IllegalArgumentException();
        return new String(raw,StandardCharsets.US_ASCII).trim().getBytes(StandardCharsets.US_ASCII);
      }
    }catch(Exception e){throw new IllegalStateException("INGESTION_READ_OWNER_KEY_UNAVAILABLE");}
  }
  static boolean text(String s,int max){return s!=null&&!s.isBlank()&&s.length()<=max&&s.chars().noneMatch(c->Character.isISOControl(c));}
  static String capability(String path) {
    return switch(path){case PREFIX+"status"->"ouf.ingestion.status";case PREFIX+"history"->"ouf.ingestion.history";default->throw denied();};
  }
  static SecurityException denied(){return new SecurityException("INGESTION_GATEWAY_READ_RECEIPT_INVALID");}
  static byte[] decode(String value) {
    if(value==null||!value.matches("[A-Za-z0-9_-]+"))throw denied();
    byte[] raw=Base64.getUrlDecoder().decode(value);
    if(!Base64.getUrlEncoder().withoutPadding().encodeToString(raw).equals(value))throw denied();return raw;
  }
  public PrincipalContext verify(String proof,String method,String path,byte[] body,String correlation,String attempt,String idempotency) {
    try {
      if(proof==null||proof.length()>16384||body==null||body.length>65536||!"POST".equals(method))throw denied();
      String capability=capability(path);String[] parts=proof.split("\\.",-1);if(parts.length!=2)throw denied();
      byte[] payload=decode(parts[0]),signature=decode(parts[1]);if(signature.length!=32||payload.length>12288)throw denied();
      var mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(key,"HmacSHA256"));
      if(!MessageDigest.isEqual(mac.doFinal((DOMAIN+parts[0]).getBytes(StandardCharsets.US_ASCII)),signature))throw denied();
      Receipt r=json.readValue(payload,Receipt.class);long now=clock.instant().getEpochSecond();
      if(r.v()!=1||!"ingestion-read-owner".equals(r.purpose())||!method.equals(r.method())||!path.equals(r.path())
          ||!capability.equals(r.capability())||!"HUMAN".equals(r.actor())||r.iat()>now||r.exp()<=now||r.exp()<=r.iat()
          ||r.iat()<now-30||r.exp()>r.iat()+30||!issuer.equals(r.issuer())||!audience.equals(r.audience())||!workload.equals(r.workload())
          ||!HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body)).equals(r.bodyHash())
          ||!text(r.subject(),4096)||!text(r.tenant(),200)||!text(r.acr(),4096)||!text(r.decisionRef(),4096)
          ||!text(r.correlationId(),256)||!text(r.attemptId(),256)||!text(r.idempotencyKey(),256)
          ||!r.correlationId().equals(correlation)||!r.attemptId().equals(attempt)||!r.idempotencyKey().equals(idempotency))throw denied();
      if(r.roles()==null||r.roles().length()>4096||r.scope()==null||r.scope().length()>8192)throw denied();
      var roles=new TreeSet<String>();
      if(!r.roles().isEmpty())for(String role:r.roles().split(" ",-1))
        if(!role.matches("[A-Za-z0-9_:./-]{1,128}")||!roles.add(role)||roles.size()>32)throw denied();
      if(!String.join(" ",roles).equals(r.roles()))throw denied();
      var scopes=new HashSet<String>();for(String scope:r.scope().split(" ",-1))
        if(!scope.matches("[A-Za-z0-9_:./-]{1,256}")||!scopes.add(scope)||scopes.size()>128)throw denied();
      if(!scopes.contains(SCOPE))throw denied();
      return new PrincipalContext(r.subject(),r.tenant(),PrincipalContext.ActorType.HUMAN,r.workload(),r.acr(),r.issuer(),r.audience(),
          scopes,new PrincipalContext.IdentityClaims(roles,r.acr(),Set.of(),null));
    }catch(Exception e){throw denied();}
  }
}
