package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;

class HttpGatewaySourceClientTest {
  private HttpServer server;private final AtomicReference<Response> response=new AtomicReference<>();private final AtomicReference<String> requestBody=new AtomicReference<>();private final ObjectMapper json=new ObjectMapper();

  @BeforeEach void start() throws Exception {
    server=HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(),0),0);
    server.createContext("/fetch",exchange->{try{
      requestBody.set(new String(exchange.getRequestBody().readAllBytes(),StandardCharsets.UTF_8));Response r=response.get();
      if(r.delayMillis>0)Thread.sleep(r.delayMillis);r.headers.forEach((k,v)->exchange.getResponseHeaders().add(k,v));
      exchange.sendResponseHeaders(r.status,r.body.length);exchange.getResponseBody().write(r.body);
    }catch(InterruptedException e){Thread.currentThread().interrupt();}finally{exchange.close();}});
    server.start();
  }
  @AfterEach void stop(){server.stop(0);}

  @Test void postsGovernedBindingAndReturnsSuccessfulBody() throws Exception {
    respond(200,"{\"items\":[{\"id\":1}]}");byte[] body=client(1000).fetch("gateway://rest/objects",Map.of("pageSize",10),"corr-1");
    assertThat(new String(body,StandardCharsets.UTF_8)).contains("\"id\":1");JsonNode sent=json.readTree(requestBody.get());assertThat(sent.path("bindingRef").asText()).isEqualTo("gateway://rest/objects");assertThat(sent.path("parameters").path("pageSize").asInt()).isEqualTo(10);
  }

  @Test void classifies429AndHonorsDeltaSecondsRetryAfter(){
    respond(429,"limited",Map.of("Retry-After","90"));var failure=catchThrowableOfType(()->fetch(client(1000)),RuntimePorts.GatewayFailure.class);
    assertThat(failure.safeCode()).isEqualTo("ING_GATEWAY_429");assertThat(failure.errorClass()).isEqualTo(AdapterSpi.ErrorClass.TRANSIENT_SOURCE);assertThat(failure.retryAfter()).contains(Duration.ofSeconds(90));
  }

  @Test void classifies503AsTransient(){
    respond(503,"unavailable");assertClass(503,AdapterSpi.ErrorClass.TRANSIENT_SOURCE,"ING_GATEWAY_503");
  }

  @Test void classifiesAuthenticationAndAuthorizationFailures(){
    respond(401,"unauthorized");assertFailure(AdapterSpi.ErrorClass.AUTH_ROUTE,"ING_GATEWAY_401");respond(403,"forbidden");assertFailure(AdapterSpi.ErrorClass.AUTH_ROUTE,"ING_GATEWAY_403");
  }

  @Test void classifiesMissingGovernedBindingAsConfiguration(){
    respond(404,"missing");assertFailure(AdapterSpi.ErrorClass.CONFIGURATION,"ING_GATEWAY_404");
  }

  @Test void timeoutIsTransient(){
    response.set(new Response(200,"ok".getBytes(StandardCharsets.UTF_8),Map.of(),400));var failure=catchThrowableOfType(()->fetch(client(40)),RuntimePorts.GatewayFailure.class);assertThat(failure.safeCode()).isEqualTo("ING_GATEWAY_UNAVAILABLE");assertThat(failure.errorClass()).isEqualTo(AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
  }

  @Test void refusedConnectionIsTransient() throws Exception {
    int port;try(java.net.ServerSocket socket=new java.net.ServerSocket(0,1,InetAddress.getLoopbackAddress())){port=socket.getLocalPort();}
    URI unavailable=URI.create("http://127.0.0.1:"+port+"/fetch");var failure=catchThrowableOfType(()->fetch(client(unavailable,300,1_000_000)),RuntimePorts.GatewayFailure.class);
    assertThat(failure.safeCode()).isEqualTo("ING_GATEWAY_UNAVAILABLE");assertThat(failure.errorClass()).isEqualTo(AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
  }

  @Test void oversizedSuccessfulResponseIsDeterministicDataFailure(){
    respond(200,"123456");var failure=catchThrowableOfType(()->fetch(client(endpoint(),1000,5)),RuntimePorts.GatewayFailure.class);assertThat(failure.safeCode()).isEqualTo("ING_GATEWAY_RESPONSE_TOO_LARGE");assertThat(failure.errorClass()).isEqualTo(AdapterSpi.ErrorClass.DATA);
  }

  @Test void invalidSuccessfulPayloadIsDeterministicDataFailure(){
    respond(200,"not-json");var cursor=new GatewayJsonAdapter(client(1000),json).open(bundle(),null);AdapterSpi.AdapterException failure=catchThrowableOfType(cursor::next,AdapterSpi.AdapterException.class);assertThat(failure.code()).isEqualTo("ING_JSON_INVALID");assertThat(failure.errorClass()).isEqualTo(AdapterSpi.ErrorClass.DATA);
  }

  @Test void rejectsRawSourceUrlBeforeNetworkCall(){
    respond(200,"ok");var failure=catchThrowableOfType(()->client(1000).fetch("https://source.example/data",Map.of(),"corr"),RuntimePorts.GatewayFailure.class);assertThat(failure.safeCode()).isEqualTo("ING_GATEWAY_BINDING_REQUIRED");assertThat(failure.errorClass()).isEqualTo(AdapterSpi.ErrorClass.CONFIGURATION);assertThat(requestBody.get()).isNull();
  }

  private void assertClass(int status,AdapterSpi.ErrorClass type,String code){assertFailure(type,code);}
  private void assertFailure(AdapterSpi.ErrorClass type,String code){var failure=catchThrowableOfType(()->fetch(client(1000)),RuntimePorts.GatewayFailure.class);assertThat(failure.safeCode()).isEqualTo(code);assertThat(failure.errorClass()).isEqualTo(type);}
  private void fetch(HttpGatewaySourceClient client){client.fetch("gateway://rest/objects",Map.of("pageSize",10),"corr-1");}
  private HttpGatewaySourceClient client(long timeoutMs){return client(endpoint(),timeoutMs,1_000_000);}
  private URI endpoint(){return URI.create("http://"+server.getAddress().getHostString()+":"+server.getAddress().getPort()+"/fetch");}
  private HttpGatewaySourceClient client(URI endpoint,long timeoutMs,int maxBytes){return new HttpGatewaySourceClient(HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build(),endpoint,json,Duration.ofMillis(timeoutMs),maxBytes,Clock.fixed(Instant.parse("2026-09-14T00:00:00Z"),ZoneOffset.UTC));}
  private void respond(int status,String body){respond(status,body,Map.of());}
  private void respond(int status,String body,Map<String,String> headers){response.set(new Response(status,body.getBytes(StandardCharsets.UTF_8),headers,0));}
  private static ExecutionBundle bundle(){return new ExecutionBundle("b","1","h","s","REST_JSON","gateway://rest/objects",Map.of("identityFields",List.of("id"),"typeCode","OBJECT","pageSize",10));}
  private record Response(int status,byte[] body,Map<String,String> headers,long delayMillis){}
}
