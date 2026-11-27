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

  // --- task-60n: incidents/explain (contract C-EX), same receipt format, own path, capability and scope ---
  static final String ATTEMPT="11111111-1111-4111-8111-111111111111",EXPLAIN_INCIDENT="2b1e4c2a-6f0d-4c43-9a51-0d6c1f3b7e10";
  static final byte[] EXPLAIN_BODY=("{\"incidentId\":\""+EXPLAIN_INCIDENT+"\"}").getBytes(StandardCharsets.UTF_8);
  static String hash(byte[] body)throws Exception{return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(body));}
  static Map<String,Object> explainReceipt()throws Exception {
    var r=receipt();r.put("path",GatewayReadReceiptVerifier.EXPLAIN_PATH);r.put("bodyHash",hash(EXPLAIN_BODY));r.put("capability","ouf.operations.explain");
    r.put("scope","mcp.connect "+GatewayReadReceiptVerifier.EXPLAIN_SCOPE);r.put("decisionRef","bundle:6:ouf.operations.explain");return r;
  }
  static it.comune.trieste.ouf.authorization.PrincipalContext verifyAt(String path,String proof,byte[] body){
    return verifier().verify(proof,"POST",path,body,"corr",ATTEMPT,"idem");
  }
  @Test @DisplayName("ING-60n-01 explain receipt verifies with path, capability and explain scope bound")
  void explainReceiptCreatesTheVerifiedHumanPrincipalWithTheExplainScope()throws Exception{
    var c=verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,sign(explainReceipt()),EXPLAIN_BODY);
    assertEquals("human-a",c.subjectId());assertEquals("HUMAN",c.actorType().name());assertEquals("tenant-a",c.tenantId());assertEquals("workload",c.servicePrincipalId());
    assertEquals(Set.of("mcp.connect",GatewayReadReceiptVerifier.EXPLAIN_SCOPE),c.scopes());assertEquals(Set.of("ouf:reader","ouf:viewer"),c.claims().externalRoleRefs());
  }
  @Test @DisplayName("ING-60n-02 status/history and explain receipts are not interchangeable")
  void statusHistoryAndExplainReceiptsNeverCrossPaths()throws Exception{
    for(String op:List.of("status","history")){
      // A genuine status/history receipt, even over the explain bytes and with both scopes, never opens explain.
      var r=receipt();r.put("path",GatewayReadReceiptVerifier.PREFIX+op);r.put("capability","ouf.ingestion."+op);r.put("bodyHash",hash(EXPLAIN_BODY));
      r.put("scope","mcp.connect ingestion.operations.read operations.incident.explain");String p=sign(r);
      assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,p,EXPLAIN_BODY),op);
      assertNotNull(verifyAt(GatewayReadReceiptVerifier.PREFIX+op,p,EXPLAIN_BODY),op);
    }
    String explain=sign(explainReceipt());
    for(String op:List.of("status","history"))assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.PREFIX+op,explain,EXPLAIN_BODY),op);
    for(String capability:List.of("ouf.ingestion.status","ouf.ingestion.history","operations.incident.explain","ouf.operations.incidents")){
      var r=explainReceipt();r.put("capability",capability);String p=sign(r);
      assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,p,EXPLAIN_BODY),capability);
    }
    for(String op:List.of("status","history")){
      var r=receipt();r.put("path",GatewayReadReceiptVerifier.PREFIX+op);r.put("capability","ouf.operations.explain");r.put("scope","ingestion.operations.read operations.incident.explain");String p=sign(r);
      assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.PREFIX+op,p,BODY),op);
    }
  }
  @Test @DisplayName("ING-60n-03 scope is required per path: operations.incident.explain for explain, ingestion.operations.read for status/history")
  void eachPathRequiresItsOwnScope()throws Exception{
    for(String scope:List.of("mcp.connect ingestion.operations.read","ingestion.operations.read","mcp.connect","operations.incident.read","mcp.connect operations.incident.read ouf.operations.explain")){
      var r=explainReceipt();r.put("scope",scope);String p=sign(r);
      assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,p,EXPLAIN_BODY),scope);
    }
    var both=explainReceipt();both.put("scope","mcp.connect ingestion.operations.read operations.incident.explain");
    assertTrue(verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,sign(both),EXPLAIN_BODY).scopes().contains(GatewayReadReceiptVerifier.EXPLAIN_SCOPE));
    for(String op:List.of("status","history")){
      var r=receipt();r.put("path",GatewayReadReceiptVerifier.PREFIX+op);r.put("capability","ouf.ingestion."+op);r.put("scope","mcp.connect operations.incident.explain");String p=sign(r);
      assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.PREFIX+op,p,BODY),op);
      r.put("scope","mcp.connect ingestion.operations.read");assertNotNull(verifyAt(GatewayReadReceiptVerifier.PREFIX+op,sign(r),BODY),op);
    }
  }
  @Test @DisplayName("ING-60n-04 explain receipt is bound to the exact body, audit headers, method and 30 s window")
  void explainBodyHeadersMethodAndWindowCannotChange()throws Exception{
    String p=sign(explainReceipt());
    for(String body:List.of("{\"incidentId\":\"3c2f5d3b-7a1e-4d54-8b62-1e7d2a4c8f21\"}","{\"incidentId\":\""+EXPLAIN_INCIDENT+"\" }","{}",""))
      assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,p,body.getBytes(StandardCharsets.UTF_8)),body);
    assertThrows(SecurityException.class,()->verifier().verify(p,"GET",GatewayReadReceiptVerifier.EXPLAIN_PATH,EXPLAIN_BODY,"corr",ATTEMPT,"idem"));
    assertThrows(SecurityException.class,()->verifier().verify(p,"POST",GatewayReadReceiptVerifier.EXPLAIN_PATH,EXPLAIN_BODY,"other",ATTEMPT,"idem"));
    assertThrows(SecurityException.class,()->verifier().verify(p,"POST",GatewayReadReceiptVerifier.EXPLAIN_PATH,EXPLAIN_BODY,"corr","22222222-2222-4222-8222-222222222222","idem"));
    assertThrows(SecurityException.class,()->verifier().verify(p,"POST",GatewayReadReceiptVerifier.EXPLAIN_PATH,EXPLAIN_BODY,"corr",ATTEMPT,"other"));
    var late=explainReceipt();late.put("exp",1031);assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,sign(late),EXPLAIN_BODY));
    var service=explainReceipt();service.put("actor","SERVICE");assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,sign(service),EXPLAIN_BODY));
    assertThrows(SecurityException.class,()->verifyAt(GatewayReadReceiptVerifier.EXPLAIN_PATH,signRaw(JSON.writeValueAsBytes(explainReceipt()),KEY,"ouf-managed-file-owner-v1."),EXPLAIN_BODY));
  }
  @Test @DisplayName("ING-60n-05 closed table: only status, history and incidents/explain verify")
  void onlyTheThreeExactOwnerPathsVerify()throws Exception{
    assertEquals(Set.of(PATH, GatewayReadReceiptVerifier.PREFIX + "history", GatewayReadReceiptVerifier.EXPLAIN_PATH),
        Set.of(GatewayReadReceiptVerifier.paths()));
    assertEquals("/api/internal/v1/ingestion/operations/incidents/explain",GatewayReadReceiptVerifier.EXPLAIN_PATH);
    for(String path:List.of(GatewayReadReceiptVerifier.PREFIX+"incidents",GatewayReadReceiptVerifier.PREFIX+"summary",GatewayReadReceiptVerifier.EXPLAIN_PATH+"/",
        GatewayReadReceiptVerifier.EXPLAIN_PATH+"/extra",GatewayReadReceiptVerifier.PREFIX+"incidents/EXPLAIN",GatewayReadReceiptVerifier.PREFIX+"incidents/explain;x","/api/trusted-human/v1/ingestion/issues/explain")){
      var r=explainReceipt();r.put("path",path);String p=sign(r);
      assertThrows(SecurityException.class,()->verifyAt(path,p,EXPLAIN_BODY),path);
    }
    assertThrows(SecurityException.class,()->verifyAt(null,sign(explainReceipt()),EXPLAIN_BODY));
  }
}
