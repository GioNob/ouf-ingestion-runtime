package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class HistoricalPublicationClientTest {
 @Test void historicalResolutionNeverFallsBackToActiveAndRetainsPublishedRetiredReferences()throws Exception{
  var envelope=PublishedActivationTest.envelope("test-source",false,1);
  var status=new AtomicReference<>("RETIRED");var fail=new AtomicBoolean();var calls=new ArrayList<String>();
  var token=Files.createTempFile("historical-",".token");Files.writeString(token,"test-token");
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/",e->{try{
   calls.add(e.getRequestURI().getPath());
   if(fail.get()){e.sendResponseHeaders(404,-1);return;}
   Object value=e.getRequestURI().getPath().startsWith("/api/semantic")?Map.of("semantic_id","core","semantic_version","1","revision_id","00000000-0000-0000-0000-000000000001","publication_set_id","00000000-0000-0000-0000-000000000002","status",status.get()):envelope;
   byte[] bytes=PublishedActivationTest.JSON.writeValueAsBytes(value);e.sendResponseHeaders(200,bytes.length);e.getResponseBody().write(bytes);
  }finally{e.close();}});server.start();
  try{
   var client=new PublicationGatewayClient(PublishedActivationTest.JSON,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");
   var original=client.decode(envelope).execution();String ref=original.bundleId()+":"+original.bundleVersion()+":"+original.checksum();
   assertThat(client.loadHistoricalAndVerify("test-source",ref)).isEqualTo(original);
   assertThatThrownBy(()->client.preflight(original)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");
   assertThatThrownBy(()->client.loadHistoricalAndVerify("other-source",ref)).hasMessage("ING_HISTORICAL_REFERENCE_MISMATCH");
   assertThatThrownBy(()->client.loadHistoricalAndVerify("test-source",ref+"wrong")).hasMessage("ING_HISTORICAL_REFERENCE_MISMATCH");
   status.set("DRAFT");assertThatThrownBy(()->client.loadHistoricalAndVerify("test-source",ref)).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");
   fail.set(true);assertThatThrownBy(()->client.loadHistoricalAndVerify("test-source",ref)).hasMessage("ING_ACTIVATION_GATEWAY_404");
   assertThat(calls).allMatch(p->p.equals("/api/onboarding/v1/runtime/publications/resolve")||p.equals("/api/semantic/v1/references:resolve"));
  }finally{server.stop(0);Files.deleteIfExists(token);}
 }
}
