package it.comune.trieste.ouf.ingestion;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import it.comune.trieste.ouf.receipt.SummaryReceiptFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest @AutoConfigureMockMvc @DirtiesContext
class SummaryIdentityRuntimeTest {
  static final String CAP="ouf.ingestion.operations.summary",SOURCE="receipt-source-"+UUID.randomUUID();
  static final Path KEY=key();
  static Path key(){try{var p=Files.createTempFile("summary-receipt", ".key");Files.writeString(p,"ab".repeat(32));p.toFile().deleteOnExit();return p;}catch(Exception e){throw new IllegalStateException(e);}}
  @DynamicPropertySource static void properties(DynamicPropertyRegistry r){
    r.add("spring.datasource.url",()->System.getenv("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->System.getenv("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->System.getenv("OUF_ING_DB_PASSWORD"));
    r.add("ouf.summary.receipt-key-file",KEY::toString);r.add("ouf.summary.tenant-id",()->"tenant-a");r.add("ouf.summary.issuer",()->"https://auth.test");r.add("ouf.summary.audience",()->"gateway");r.add("ouf.summary.workload",()->"workload");
  }
  @Autowired MockMvc http;@Autowired LocalAuthorization auth;@Autowired JdbcClient sql;
  void policy(long version,boolean grant)throws Exception{
    var now=Instant.now();var producer=new GrantConstraints("ALLOW","ouf:viewer","capability",null,Map.of(),Set.of(),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null);
    var source=new GrantConstraints("ALLOW","ouf:viewer","operational",null,Map.of("sourceRef",SOURCE),Set.of(),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null);
    var capabilities=List.of(new CapabilityDescriptor(CAP,"READ","operations.status.read",Set.of(PrincipalContext.ActorType.HUMAN)),new CapabilityDescriptor("operations.status.read","READ","operations.status.read",Set.of(PrincipalContext.ActorType.HUMAN)));
    var grants=grant?List.of(new Grant("producer",CAP,"tenant-a",null,null,null,now.minusSeconds(60),now.plusSeconds(3600),producer),new Grant("source","operations.status.read","tenant-a",null,null,null,now.minusSeconds(60),now.plusSeconds(3600),source)):List.<Grant>of();
    TestAuthorization.install(auth,new PolicyBundle("receipt-runtime",version,now,capabilities,grants));
  }
  String receipt(String body,String roles,long version)throws Exception{
    long now=Instant.now().getEpochSecond();var r=new LinkedHashMap<String,Object>();r.put("v",1);r.put("purpose","operational-summary-owner");r.put("method","POST");r.put("path","/api/internal/v1/ingestion/operations/summary");r.put("capability",CAP);r.put("bodyHash",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8))));r.put("iat",now);r.put("exp",now+30);r.put("issuer","https://auth.test");r.put("audience","gateway");r.put("workload","workload");r.put("subject","human-a");r.put("tenant","tenant-a");r.put("client","chatgpt");r.put("acr","1");r.put("roles",roles);r.put("scope","operations.status.read");r.put("decisionRef","receipt-runtime:"+version+":"+CAP);
    var encoder=Base64.getUrlEncoder().withoutPadding();String encoded=encoder.encodeToString(new ObjectMapper().writeValueAsBytes(r));var mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec("ab".repeat(32).getBytes(StandardCharsets.US_ASCII),"HmacSHA256"));return encoded+"."+encoder.encodeToString(mac.doFinal(("ouf-operational-owner-v1."+encoded).getBytes(StandardCharsets.US_ASCII)));
  }
  @Test void realPolicyFilterAndDatabasePreserveSourceScopeAndRevocation()throws Exception{
    policy(1,true);UUID run=UUID.randomUUID(),issue=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id,tenant_id) values(:r,:s,'b','1','sha256:x','REPLAY','RUNNING','corr','tenant-a')").param("r",run).param("s",SOURCE).update();
    sql.sql("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id) values(:i,:r,:s,'TEST','ERROR','ING_TEST','protected-evidence','corr')").param("i",issue).param("r",run).param("s",SOURCE).update();
    String body="{\"limit\":5,\"sourceId\":\""+SOURCE+"\"}",path="/api/internal/v1/ingestion/operations/summary";
    http.perform(post(path).contentType("application/json").content(body).header(SummaryReceiptFilter.HEADER,receipt(body,"ouf:viewer",1))).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("DEGRADED")).andExpect(jsonPath("$.items[0].source_ref").value(SOURCE)).andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("protected-evidence"))));
    http.perform(post(path).contentType("application/json").content(body).header(SummaryReceiptFilter.HEADER,receipt(body,"",1)).header("X-OUF-External-Role-Refs","ouf:viewer")).andExpect(status().isForbidden());
    String other="{\"sourceId\":\"other-source\"}";
    http.perform(post(path).contentType("application/json").content(other).header(SummaryReceiptFilter.HEADER,receipt(other,"ouf:viewer",1))).andExpect(status().isForbidden());
    http.perform(post(path).contentType("application/json").content(body).header("X-OUF-Gateway-Verified","true")).andExpect(status().isForbidden());
    String prior=receipt(body,"ouf:viewer",1);policy(2,false);
    http.perform(post(path).contentType("application/json").content(body).header(SummaryReceiptFilter.HEADER,prior)).andExpect(status().isForbidden());
  }
}
