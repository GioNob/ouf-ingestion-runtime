package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ExecutionGatewayClientTest {
  @TempDir Path directory;

  @Test void rereadsTheTokenFileAcrossRotation()throws Exception{
    Path token=directory.resolve("token");
    Files.writeString(token,"");
    var waits=new ArrayList<Long>();
    String value=WorkloadTokenFile.read(token,millis->{
      waits.add(millis);
      String replacement=waits.size()==1?"eyJhbGciOiJSUzI1NiJ9.":"complete-token";
      assertThatCode(()->Files.writeString(token,replacement)).doesNotThrowAnyException();
    });
    assertThat(waits).containsExactly(25L,50L);
    assertThat(value).isEqualTo("complete-token");
  }

  @ParameterizedTest @ValueSource(strings={"", "eyJhbGciOiJSUzI1NiJ9.", "partial token"})
  void incompleteTokenStopsBeforePostAndRotationRecovers(String incomplete)throws Exception{
    Path token=directory.resolve("token");
    Files.writeString(token,incomplete);
    var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    var requests=new AtomicInteger();
    server.createContext("/",exchange->{
      requests.incrementAndGet();
      try{exchange.sendResponseHeaders(200,-1);}finally{exchange.close();}
    });
    server.start();
    try{
      var client=new ExecutionGatewayClient(new ObjectMapper(),mock(RunExecutionRepository.class),
          "http://127.0.0.1:"+server.getAddress().getPort(),token.toString());
      assertThatThrownBy(()->client.fetch("gateway://fixture",Map.of(),"corr"))
          .isInstanceOfSatisfying(RuntimePorts.GatewayFailure.class,f->{
            assertThat(f.safeCode()).isEqualTo("ING_WORKLOAD_TOKEN_INVALID");
            assertThat(f.errorClass()).isEqualTo(AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
          });
      assertThat(requests).hasValue(0);
      Files.writeString(token,"rotated-token");
      client.fetch("gateway://fixture",Map.of(),"corr");
      assertThat(requests).hasValue(1);
    }finally{server.stop(0);}
  }

  @Test void rejectsAcceptedWithoutDurableAckAndChecksManagedBytes()throws Exception{
    var json=new ObjectMapper();var mode=new AtomicInteger();UUID handoff=UUID.randomUUID();
    Path token=Files.createTempFile("execution-client-",".token");Files.writeString(token,"test-token");
    var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    server.createContext("/",exchange->{try{
      if(!"Bearer test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))){exchange.sendResponseHeaders(403,-1);return;}
      byte[] response=exchange.getRequestURI().getPath().contains("content")?new byte[]{1,2,4}:json.writeValueAsBytes(Map.of("durable",true,"receiptRef","udp://handoffs/"+handoff));
      exchange.sendResponseHeaders(mode.get()==0?202:exchange.getRequestURI().getPath().contains("content")?200:201,response.length);exchange.getResponseBody().write(response);
    }finally{exchange.close();}});server.start();
    try{
      var client=new ExecutionGatewayClient(json,mock(RunExecutionRepository.class),"http://127.0.0.1:"+server.getAddress().getPort(),token.toString());
      assertThatThrownBy(()->client.deliver(handoff,Map.of("handoffId",handoff.toString()),"key")).hasMessage("ING_EXECUTION_GATEWAY_202");
      mode.set(1);assertThat(client.deliver(handoff,Map.of("handoffId",handoff.toString()),"key").durable()).isTrue();
      assertThatThrownBy(()->client.read("object://file/asset",3,ExecutionGatewayClient.hash(new byte[]{1,2,3}))).hasMessage("ING_MANAGED_INTEGRITY_MISMATCH");
      Files.writeString(token,"rotated-token");assertThatThrownBy(()->client.deliver(handoff,Map.of("handoffId",handoff.toString()),"key")).hasMessage("ING_EXECUTION_GATEWAY_403");
    }finally{server.stop(0);Files.deleteIfExists(token);}
  }
}
