package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class PublicationGatewayClientTest {
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
