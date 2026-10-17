package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class PublicationGatewayClientTest {
 @Test void frozenConfigurationUsesOnlyTheServiceReadRouteWithExactHash()throws Exception{
  Path token=Files.createTempFile("frozen-config-",".token");Files.writeString(token,"test-workload-token");
  var path=new AtomicReference<String>();var method=new AtomicReference<String>();var bearer=new AtomicReference<String>();
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/",exchange->{try{path.set(exchange.getRequestURI().toString());method.set(exchange.getRequestMethod());
   bearer.set(exchange.getRequestHeaders().getFirst("Authorization"));byte[] body="{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
   exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);}finally{exchange.close();}});server.start();
  try{
   var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");
   UUID version=UUID.randomUUID();String hash="sha256:"+"a".repeat(64);client.frozenConfiguration("source",version,hash);
   assertThat(method.get()).isEqualTo("GET");assertThat(bearer.get()).isEqualTo("Bearer test-workload-token");
   assertThat(path.get()).startsWith("/api/internal/v1/onboarding/ingestion-configuration?")
       .contains("sourceId=source","onboardingVersionId="+version,"configurationHash=sha256%3A"+"a".repeat(64));
   assertThat(path.get()).doesNotContain("/api/onboarding/v1/sources/");
  }finally{server.stop(0);Files.deleteIfExists(token);}
 }
 @Test void nativeClassAndTwentyPropertiesRemainExactlyPinnedWithinTheBound()throws Exception{
  for(int count:List.of(21,32,33)){
   var envelope=PublishedActivationTest.envelope("native-source",true,1);var published=PublishedActivation.map(envelope,"bundle");
   var bindings=new ArrayList<Map<String,Object>>();var refs=new ArrayList<String>();
   for(int i=0;i<count;i++){String id="urn:native:"+i;refs.add(id+"@1");bindings.add(Map.of("semanticId",id,"semanticVersion","1","revisionId",new UUID(0,i+1).toString(),"publicationSetId",new UUID(1,1).toString()));}
   published.put("semanticReferenceBindings",bindings);published.put("semanticRefs",refs);PublishedActivationTest.hash(published);envelope.put("checksum",published.get("checksum"));
   var calls=new AtomicInteger();var corruptLast=new AtomicBoolean();Path token=Files.createTempFile("native-client-",".token");Files.writeString(token,"test-token");var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
   server.createContext("/",exchange->{try{if(!"Bearer test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))){exchange.sendResponseHeaders(403,-1);return;}calls.incrementAndGet();var query=new HashMap<String,String>();for(String part:exchange.getRequestURI().getRawQuery().split("&")){String[] pair=part.split("=",2);query.put(pair[0],URLDecoder.decode(pair[1],java.nio.charset.StandardCharsets.UTF_8));}String id=query.get("semanticId");int index=Integer.parseInt(id.substring(id.lastIndexOf(':')+1));var response=Map.of("semantic_id",id,"semantic_version",corruptLast.get()&&index==count-1?"2":"1","revision_id",query.get("revisionId"),"publication_set_id",query.get("publicationSetId"),"status","ACTIVE");byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(response);exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);}finally{exchange.close();}});server.start();
   try{var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");var bundle=client.decode(envelope).execution();
    if(count>32){assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_EXACT_SEMANTIC_BINDINGS_REQUIRED");assertThat(calls).hasValue(0);}
    else{client.preflight(bundle);assertThat(calls).hasValue(count);corruptLast.set(true);assertThatThrownBy(()->client.preflight(bundle)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");assertThat(calls).hasValue(count*2);}
   }finally{server.stop(0);Files.deleteIfExists(token);}
  }
 }
 @Test void exactReferenceAndActivePublicationAreRecheckedWithWorkloadCredential()throws Exception{
  var envelope=PublishedActivationTest.envelope("test-source",false,1);var mode=new AtomicInteger();var query=new AtomicReference<String>();Path token=Files.createTempFile("activation-client-",".token");Files.writeString(token,"test-token");HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/",exchange->{try{if(!"Bearer test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))||mode.get()==3){exchange.sendResponseHeaders(403,-1);return;}
   Map<String,Object> response;if(exchange.getRequestURI().getPath().startsWith("/api/semantic")){query.set(exchange.getRequestURI().getQuery());response=Map.of("semantic_id","core","semantic_version",mode.get()==1?"2":"1","revision_id","00000000-0000-0000-0000-000000000001","publication_set_id","00000000-0000-0000-0000-000000000002","status","ACTIVE");}else{response=new LinkedHashMap<>(envelope);if(mode.get()==2)response.put("publicationId",UUID.randomUUID().toString());}
   byte[] body=PublishedActivationTest.JSON.writeValueAsBytes(response);exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
  }finally{exchange.close();}});server.start();
  try{var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");var a=client.decode(envelope);var claim=new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-a","test-source",60,"worker",a.publicationId(),a.execution().checksum(),java.time.OffsetDateTime.now(),1);
   assertThat(client.loadAndVerify(claim).checksum()).isEqualTo(a.execution().checksum());client.preflight(a.execution());assertThat(query.get()).contains("revisionId=00000000-0000-0000-0000-000000000001","publicationSetId=00000000-0000-0000-0000-000000000002");
   mode.set(1);assertThatThrownBy(()->client.preflight(a.execution())).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");mode.set(2);assertThatThrownBy(()->client.loadAndVerify(claim)).hasMessage("ING_ACTIVE_PUBLICATION_CHANGED");mode.set(3);assertThatThrownBy(()->client.loadAndVerify(claim)).isInstanceOf(RuntimePorts.GatewayFailure.class).hasMessage("ING_ACTIVATION_GATEWAY_403");
  }finally{server.stop(0);Files.deleteIfExists(token);}
 }
}
