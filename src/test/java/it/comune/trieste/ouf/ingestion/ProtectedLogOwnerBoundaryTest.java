package it.comune.trieste.ouf.ingestion;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import java.util.*;

class ProtectedLogOwnerBoundaryTest {
 private RequestPostProcessor actor(boolean detail,String type){return request->{
  TestAuthorization.bind(request,"operator",type,Set.of("ingestion.log.read","ingestion.log.aggregate"));
  var engine=(LocalAuthorization)request.getServletContext().getAttribute(ServletAuthorization.RUNTIME);var old=engine.currentSnapshot().bundle();
  var grants=old.grants().stream().map(g->new Grant(g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),g.servicePrincipalId(),g.organizationId(),g.validFrom(),g.validUntil(),new GrantConstraints("ALLOW",null,"protected-log",null,Map.of("module","INGESTION"),Set.of(),detail?Set.of("SECURITY_SENSITIVE"):Set.of(),null,Set.of(),null))).toList();
  try{TestAuthorization.install(engine,new PolicyBundle(old.bundleId(),2,old.publishedAt(),old.capabilities(),grants));}catch(Exception failure){throw new IllegalStateException(failure);}return request;
 };}
 @Test void bothRoutesRequireHumanExplicitDetailAndConfiguredOwnerNamespace()throws Exception{
  String query="{\"from\":\"2026-09-17T01:00:00Z\",\"to\":\"2026-09-17T02:00:00Z\",\"purpose\":\"investigation\"}";
  for(String namespace:List.of("tenant-a","another-tenant",""))for(String route:List.of("search","aggregate")){
   var logs=mock(ProtectedOperationalLogService.class);when(logs.search(any(),any(),any())).thenReturn(Map.of("items",List.of()));when(logs.aggregate(any(),any(),any(),any())).thenReturn(List.of());
   var http=MockMvcBuilders.standaloneSetup(new ProtectedLogTrustedHumanApi(logs,new TrustedAuthorizationContext(),namespace)).build();
   String body=route.equals("search")?query:"{\"query\":"+query+",\"dimension\":\"severity\"}";
   http.perform(post("/api/trusted-human/v1/ingestion/logs/"+route).contentType("application/json").content(body).with(actor(false,"HUMAN"))).andExpect(status().isForbidden());
   http.perform(post("/api/trusted-human/v1/ingestion/logs/"+route).contentType("application/json").content(body).with(actor(true,"AI_AGENT"))).andExpect(status().isForbidden());
   verify(logs,never()).search(any(),any(),any());verify(logs,never()).aggregate(any(),any(),any(),any());
   http.perform(post("/api/trusted-human/v1/ingestion/logs/"+route).contentType("application/json").content(body).with(actor(true,"HUMAN"))).andExpect(status().is(namespace.equals("tenant-a")?200:403));
   if(namespace.equals("tenant-a")){
    if(route.equals("search"))verify(logs).search(any(),argThat(a->a.decisionRef().endsWith(":2:ingestion.log.read")),any());
    else verify(logs).aggregate(any(),any(),argThat(a->a.decisionRef().endsWith(":2:ingestion.log.aggregate")),any());
   }else{verify(logs,never()).search(any(),any(),any());verify(logs,never()).aggregate(any(),any(),any(),any());}
  }
 }
}
