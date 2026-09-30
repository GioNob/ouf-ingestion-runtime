package it.comune.trieste.ouf.ingestion;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.*;
import it.comune.trieste.ouf.authorization.*;
import jakarta.servlet.http.HttpServletRequest;
import java.security.*;
import java.security.interfaces.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.*;

@SpringBootTest(classes=IngestionIamHttpBoundaryTest.App.class, properties={
    "ouf.ingestion.iam.enabled=true", "ouf.ingestion.iam.issuer=https://issuer.invalid/realms/test",
    "ouf.ingestion.iam.audience=expected-audience"})
@AutoConfigureMockMvc
class IngestionIamHttpBoundaryTest {
  static final String ISSUER="https://issuer.invalid/realms/test", CAP="ingestion.run.read";
  static final KeyPair KEY=key();
  @Autowired MockMvc http;
  @MockitoBean(name="ingestionJwtDecoder") JwtDecoder decoder;

  @Configuration(proxyBeanMethods=false)
  @EnableAutoConfiguration(exclude={org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
      org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration.class,AuthorizationAutoConfiguration.class})
  @Import({IngestionIamSecurityConfiguration.class, Probe.class})
  static class App {
    @Bean ServletContextInitializer authorization(){return c->c.setAttribute(ServletAuthorization.RUNTIME,
        TestAuthorization.runtime("iam-subject","HUMAN",Set.of(CAP)));}
  }
  @RestController static class Probe {
    @GetMapping("/api/trusted-human/v1/ingestion/runs/{id}") Map<String,String> read(HttpServletRequest request){
      var context=new TrustedAuthorizationContext().require(request,CAP,true);
      return Map.of("subject",context.subject(),"tenant",context.tenantId(),"actor",context.actorType());
    }
  }
  static KeyPair key(){try{var g=KeyPairGenerator.getInstance("RSA");g.initialize(2048);return g.generateKeyPair();}catch(Exception e){throw new IllegalStateException(e);}}
  @BeforeEach void realVerification(){
    var real=NimbusJwtDecoder.withPublicKey((RSAPublicKey)KEY.getPublic()).build();
    real.setJwtValidator(IngestionIamSecurityConfiguration.validators(ISSUER,"expected-audience"));
    when(decoder.decode(anyString())).thenAnswer(i->real.decode(i.getArgument(0,String.class)));
  }
  static String token(KeyPair key,String issuer,String audience,String tenant,String actor,String scope,Instant expiry,Map<String,Object> extra)throws Exception{
    var claims=new JWTClaimsSet.Builder().issuer(issuer).subject("iam-subject").audience(audience)
        .issueTime(Date.from(Instant.now().minusSeconds(10))).claim("tenant_id",tenant)
        .claim("ouf_actor_type",actor).claim("acr","1").claim("scope",scope);
    if(expiry!=null)claims.expirationTime(Date.from(expiry));
    extra.forEach(claims::claim);
    var jwt=new SignedJWT(new JWSHeader(JWSAlgorithm.RS256),claims.build());
    jwt.sign(new RSASSASigner((RSAPrivateKey)key.getPrivate()));return jwt.serialize();
  }
  String good(String tenant,String actor,String scope,Map<String,Object> extra)throws Exception{
    return token(KEY,ISSUER,"expected-audience",tenant,actor,scope,Instant.now().plusSeconds(300),extra);
  }
  @Test void signedHumanBindsSdkPrincipalAndIgnoresSpoofedHeaders()throws Exception{
    http.perform(get("/api/trusted-human/v1/ingestion/runs/any-source-run")
        .header("Authorization","Bearer "+good("tenant-a","HUMAN",CAP,Map.of()))
        .header("X-OUF-Principal-ID","spoofed").header("X-OUF-Tenant-ID","wrong"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.subject").value("iam-subject"))
        .andExpect(jsonPath("$.tenant").value("tenant-a"));
  }
  @Test void humanUserAliasNormalizesToHuman()throws Exception{
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("Authorization","Bearer "+good("tenant-a","HUMAN_USER",CAP,Map.of())))
        .andExpect(status().isOk()).andExpect(jsonPath("$.actor").value("HUMAN"));
  }
  @Test void headersWithoutBearerDoNotAuthenticate()throws Exception{
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("X-OUF-Gateway-Verified","true").header("X-OUF-Actor-Type","HUMAN")
        .header("X-OUF-Granted-Scopes",CAP)).andExpect(status().isUnauthorized());
  }
  @Test void wrongSignatureIssuerAudienceExpiredAndMissingExpiryFail()throws Exception{
    var expiry=Instant.now().plusSeconds(300);
    var bad=List.of(token(key(),ISSUER,"expected-audience","tenant-a","HUMAN",CAP,expiry,Map.of()),
        token(KEY,"https://wrong.invalid","expected-audience","tenant-a","HUMAN",CAP,expiry,Map.of()),
        token(KEY,ISSUER,"other-audience","tenant-a","HUMAN",CAP,expiry,Map.of()),
        token(KEY,ISSUER,"expected-audience","tenant-a","HUMAN",CAP,Instant.now().minusSeconds(120),Map.of()),
        token(KEY,ISSUER,"expected-audience","tenant-a","HUMAN",CAP,null,Map.of()));
    for(String value:bad)http.perform(get("/api/trusted-human/v1/ingestion/runs/r").header("Authorization","Bearer "+value))
        .andExpect(status().isUnauthorized());
  }
  @Test void scopeHeadersDoNotGrantMissingScopeAndTenantMismatchDenied()throws Exception{
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("Authorization","Bearer "+good("tenant-a","HUMAN","other.scope",Map.of()))
        .header("X-OUF-Granted-Scopes",CAP)).andExpect(status().isForbidden());
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("Authorization","Bearer "+good("another-tenant","HUMAN",CAP,Map.of())))
        .andExpect(status().isForbidden());
  }
  @Test void serviceCannotUseHumanApiAndUnknownActorOrSubjectConflictFails()throws Exception{
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("Authorization","Bearer "+good("tenant-a","SERVICE",CAP,Map.of("azp","workload"))))
        .andExpect(status().isForbidden());
    for(String actor:List.of("UNKNOWN","SERVICE_IDENTITY"))http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("Authorization","Bearer "+good("tenant-a",actor,CAP,Map.of())))
        .andExpect(status().isUnauthorized());
    http.perform(get("/api/trusted-human/v1/ingestion/runs/r")
        .header("Authorization","Bearer "+good("tenant-a","HUMAN",CAP,Map.of("ouf_subject","other-subject"))))
        .andExpect(status().isUnauthorized());
  }
}
