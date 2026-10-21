package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Real HTTP transport must use the owner SERVICE contract, never the HUMAN wildcard. */
class OnboardingReadinessGatewayContractTest {
  @Test void exactWorkloadConfigurationReadReconcilesAndNeverFallsBackAfterDenial() throws Exception {
    var json=new ObjectMapper();
    var candidate=new FrozenConfigurationProbeTest().candidate();
    UUID version=UUID.fromString((String)candidate.get("onboardingVersionId"));
    String hash=(String)candidate.get("configurationHash");
    String configurationPath="/api/internal/v1/onboarding/ingestion-configuration";
    String compatibilityPath="/api/internal/v1/onboarding/compatibility/ingestion-runtime";
    var attestation=Map.of("attestation_id",UUID.randomUUID().toString(),
        "onboarding_version_id",version.toString(),"configuration_hash",hash,
        "compatible",true,"consumer","INGESTION_RUNTIME");
    var requests=Collections.synchronizedList(new ArrayList<Map<String,Object>>());
    var failureStatus=new AtomicInteger();
    var token=Files.createTempFile("readiness-contract-",".token");
    Files.writeString(token,"fixture-workload-token");
    var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    server.createContext("/",exchange->{
      try {
        String path=exchange.getRequestURI().getPath();
        var query=new HashMap<String,String>();
        String raw=exchange.getRequestURI().getRawQuery();
        if(raw!=null)for(String item:raw.split("&")){
          String[] pair=item.split("=",2);
          query.put(URLDecoder.decode(pair[0],StandardCharsets.UTF_8),
              pair.length==2?URLDecoder.decode(pair[1],StandardCharsets.UTF_8):"");
        }
        requests.add(Map.of("path",path,"method",exchange.getRequestMethod(),"query",query));
        if(!"GET".equals(exchange.getRequestMethod())
            ||!"Bearer fixture-workload-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))
            ||!Set.of(configurationPath,compatibilityPath).contains(path)){
          exchange.sendResponseHeaders(403,-1);return;
        }
        if(!query.equals(Map.of("sourceId","source","onboardingVersionId",version.toString(),"configurationHash",hash))){
          exchange.sendResponseHeaders(412,-1);return;
        }
        if(failureStatus.get()!=0){exchange.sendResponseHeaders(failureStatus.get(),-1);return;}
        Map<String,Object> response=path.equals(configurationPath)
            ?Map.of("source_id","source","onboarding_version_id",version.toString(),"configuration_hash",hash,
                "configuration",candidate.get("configuration"),"state","APPROVED","version",1)
            :Map.of("status","PRESENT","compatible",true,"attestation",attestation);
        byte[] bytes=json.writeValueAsBytes(response);
        exchange.sendResponseHeaders(200,bytes.length);
        exchange.getResponseBody().write(bytes);
      } finally {exchange.close();}
    });
    server.start();
    var objects=mock(ExecutionGatewayClient.class);
    var client=new PublicationGatewayClient(json,"http://127.0.0.1:"+server.getAddress().getPort(),token.toString(),"tenant-a");
    var service=new OnboardingReadinessService(json,client,objects);
    try {
      assertThat(service.verify("source",version,hash,()->{}))
          .containsEntry("status","PASS").containsEntry("reconciled",true).containsEntry("attestationSubmitted",true);
      assertThat(requests).hasSize(2);
      assertThat(requests.getFirst().get("path")).isEqualTo(configurationPath);
      assertThat(requests.get(1).get("path")).isEqualTo(compatibilityPath);
      verifyNoInteractions(objects);
      for(int code:List.of(403,412)){
        failureStatus.set(code);int before=requests.size();
        assertThatThrownBy(()->service.verify("source",version,hash,()->{}))
            .isInstanceOf(RuntimePorts.GatewayFailure.class).hasMessage("ING_ACTIVATION_GATEWAY_"+code);
        assertThat(requests).hasSize(before+1);
        assertThat(requests.getLast().get("path")).isEqualTo(configurationPath);
      }
      assertThat(requests).allSatisfy(request->{
        assertThat(request.get("method")).isEqualTo("GET");
        assertThat(request.get("query")).isEqualTo(Map.of("sourceId","source","onboardingVersionId",version.toString(),"configurationHash",hash));
        assertThat((String)request.get("path")).doesNotStartWith("/api/onboarding/v1/sources/");
      });
    } finally {service.close();server.stop(0);Files.deleteIfExists(token);}
  }
}
