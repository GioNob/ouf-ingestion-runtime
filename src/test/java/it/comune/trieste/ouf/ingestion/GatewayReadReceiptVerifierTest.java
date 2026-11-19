package it.comune.trieste.ouf.ingestion;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.*;

class GatewayReadReceiptVerifierTest {
  static final ObjectMapper JSON=new ObjectMapper();
  static final byte[] KEY="cd".repeat(32).getBytes(StandardCharsets.US_ASCII);
  static final String ISSUER="https://auth.test/realms/ouf",PATH=GatewayReadReceiptVerifier.PREFIX+"status";
  static final byte[] BODY="{\"sourceId\":\"source-a\",\"limit\":10}".getBytes(StandardCharsets.UTF_8);
  static final Clock CLOCK=Clock.fixed(Instant.ofEpochSecond(1000),ZoneOffset.UTC);
  static GatewayReadReceiptVerifier verifier(){return new GatewayReadReceiptVerifier(KEY,ISSUER,"gateway","workload",CLOCK);}
  static Map<String,Object> receipt()throws Exception {
    var r=new LinkedHashMap<String,Object>();r.put("v",1);r.put("purpose","ingestion-read-owner");r.put("method","POST");r.put("path",PATH);
    r.put("bodyHash",HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(BODY)));r.put("capability","ouf.ingestion.status");r.put("iat",1000);r.put("exp",1030);
    r.put("issuer",ISSUER);r.put("audience","gateway");r.put("workload","workload");r.put("actor","HUMAN");r.put("subject","human-a");r.put("tenant","tenant-a");r.put("acr","1");r.put("roles","ouf:reader ouf:viewer");r.put("scope","mcp.connect ingestion.operations.read");
    r.put("idempotencyKey","idem");r.put("correlationId","corr");r.put("attemptId","11111111-1111-4111-8111-111111111111");r.put("decisionRef","bundle:6:ouf.ingestion.status");return r;
  }
  static String sign(Map<String,Object> r)throws Exception{return signRaw(JSON.writeValueAsBytes(r),KEY,GatewayReadReceiptVerifier.DOMAIN);}
  static String signRaw(byte[] raw,byte[] key,String domain)throws Exception{
    var b64=Base64.getUrlEncoder().withoutPadding();String p=b64.encodeToString(raw);var h=Mac.getInstance("HmacSHA256");h.init(new SecretKeySpec(key,"HmacSHA256"));return p+"."+b64.encodeToString(h.doFinal((domain+p).getBytes(StandardCharsets.US_ASCII)));
  }
  static void verify(String proof,byte[] body){verifier().verify(proof,"POST",PATH,body,"corr","11111111-1111-4111-8111-111111111111","idem");}
  @Test void signedReceiptCreatesOnlyTheVerifiedHumanPrincipal()throws Exception{
    var c=verifier().verify(sign(receipt()),"POST",PATH,BODY,"corr","11111111-1111-4111-8111-111111111111","idem");
    assertEquals("human-a",c.subjectId());assertEquals("HUMAN",c.actorType().name());assertEquals("tenant-a",c.tenantId());assertEquals("workload",c.servicePrincipalId());assertEquals(Set.of("ouf:reader","ouf:viewer"),c.claims().externalRoleRefs());
  }
  @Test void realLuaVectorsCrossTheJavaOwnerVerifier()throws Exception{
    String path=System.getenv("OUF_INGESTION_READ_LUA_VECTORS");Assumptions.assumeTrue(path!=null,"cross-repository gate supplies actual Lua vectors");
    var values=JSON.readTree(Files.readAllBytes(Path.of(path)));assertEquals(2,values.get("vectors").size());
    for(var v:values.get("vectors")){
      var c=verifier().verify(v.get("proof").asText(),"POST",v.get("path").asText(),v.get("body").asText().getBytes(StandardCharsets.UTF_8),v.get("correlationId").asText(),v.get("attemptId").asText(),v.get("idempotencyKey").asText());
      assertEquals("human-a",c.subjectId());assertTrue(c.scopes().contains(GatewayReadReceiptVerifier.SCOPE));
    }
  }
  @Test void everyBoundFieldRejectsEvenACorrectlySignedChangedReceipt()throws Exception{
    var changes=Map.<String,Object>ofEntries(Map.entry("v",2),Map.entry("purpose","managed-file-owner"),Map.entry("method","GET"),Map.entry("path",GatewayReadReceiptVerifier.PREFIX+"history"),Map.entry("bodyHash","0".repeat(64)),Map.entry("capability","ouf.ingestion.run"),Map.entry("iat",1001),Map.entry("exp",1000),Map.entry("issuer","https://other.invalid"),Map.entry("audience","other"),Map.entry("workload","other"),Map.entry("actor","SERVICE"),Map.entry("subject",""),Map.entry("tenant",""),Map.entry("acr",""),Map.entry("scope","mcp.connect"),Map.entry("roles","b a"),Map.entry("idempotencyKey","other"),Map.entry("correlationId","other"),Map.entry("attemptId","other"),Map.entry("decisionRef",""));
    for(var e:changes.entrySet()){var r=receipt();r.put(e.getKey(),e.getValue());String p=sign(r);assertThrows(SecurityException.class,()->verify(p,BODY),e.getKey());}
    var r=receipt();r.put("exp",1031);assertThrows(SecurityException.class,()->verify(sign(r),BODY));
  }
  @Test void signaturesMacDomainBodyMethodPathAndAuditHeadersCannotBeReused()throws Exception{
    String p=sign(receipt());assertThrows(SecurityException.class,()->verify(p,new byte[0]));
    assertThrows(SecurityException.class,()->verify(signRaw(JSON.writeValueAsBytes(receipt()),KEY,"ouf-managed-file-owner-v1."),BODY));
    assertThrows(SecurityException.class,()->verify(signRaw(JSON.writeValueAsBytes(receipt()),"ab".repeat(32).getBytes(),GatewayReadReceiptVerifier.DOMAIN),BODY));
    for(String path:List.of(PATH+"/extra",GatewayReadReceiptVerifier.PREFIX+"run","/api/trusted-human/v1/ingestion/runs/r"))assertThrows(SecurityException.class,()->verifier().verify(p,"POST",path,BODY,"corr","11111111-1111-4111-8111-111111111111","idem"));
    assertThrows(SecurityException.class,()->verifier().verify(p,"GET",PATH,BODY,"corr","11111111-1111-4111-8111-111111111111","idem"));
    assertThrows(SecurityException.class,()->verifier().verify(p,"POST",PATH,BODY,"changed","11111111-1111-4111-8111-111111111111","idem"));
  }
  @Test void duplicateUnknownFieldsMalformedProofAndOversizedInputFailClosed()throws Exception{
    byte[] raw=JSON.writeValueAsBytes(receipt());String json=new String(raw,StandardCharsets.UTF_8);
    for(String invalid:List.of(json.replaceFirst("\\{","{\"v\":1,"),json.substring(0,json.length()-1)+",\"unknown\":true}"))assertThrows(SecurityException.class,()->verify(signRaw(invalid.getBytes(StandardCharsets.UTF_8),KEY,GatewayReadReceiptVerifier.DOMAIN),BODY));
    for(String proof:Arrays.asList(null,"","a.b.c","a.b","x".repeat(16385)))assertThrows(SecurityException.class,()->verify(proof,BODY));
    String p=sign(receipt());assertThrows(SecurityException.class,()->verify(p,new byte[65537]));
  }
}
