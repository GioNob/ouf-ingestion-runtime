package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.comune.trieste.ouf.authorization.TestAuthorization;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class OnboardingReadinessTest {
  final ObjectMapper json=new ObjectMapper();final PublicationGatewayClient gateway=mock(PublicationGatewayClient.class);
  final ExecutionGatewayClient objects=mock(ExecutionGatewayClient.class);
  final OnboardingReadinessService service=new OnboardingReadinessService(json,gateway,objects);
  final Map<String,Object> candidate=new FrozenConfigurationProbeTest().candidate();
  final UUID version=UUID.fromString((String)candidate.get("onboardingVersionId"));final String hash=(String)candidate.get("configurationHash");
  @AfterEach void close(){service.close();Thread.interrupted();}
  Map<String,Object> attestation(){return Map.of("attestation_id",UUID.randomUUID().toString(),"onboarding_version_id",version.toString(),"configuration_hash",hash,"compatible",true,"consumer","INGESTION_RUNTIME");}
  void frozen(){when(gateway.frozenConfiguration("source",version)).thenReturn(Map.of("source_id","source","onboarding_version_id",version.toString(),"configuration_hash",hash,"configuration",candidate.get("configuration"),"state","APPROVED","version",1));}
  @Test void realProbeValidatesEveryRowBeforeExactOwnerAcknowledgment(){
    frozen();when(gateway.compatibility("source",version,hash)).thenReturn(Map.of("status","ABSENT"));
    when(objects.read(anyString(),anyLong(),anyString())).thenReturn("\ufeffid;name\r\n1;Town Hall\r\n2;Library\r\n".getBytes(StandardCharsets.UTF_8));
    when(gateway.attestCompatibility("source",version,hash,2)).thenReturn(attestation());
    assertThat(service.verify("source",version,hash,()->{})).containsEntry("status","PASS").containsEntry("validatedRows",2L).containsEntry("attestationSubmitted",true).containsEntry("reconciled",false);
    var order=inOrder(gateway,objects);order.verify(gateway).frozenConfiguration("source",version);order.verify(gateway).compatibility("source",version,hash);order.verify(gateway).preflight(any(ExecutionBundle.class));order.verify(objects).read(anyString(),anyLong(),anyString());order.verify(gateway).attestCompatibility("source",version,hash,2);
  }
  @Test void lostAcknowledgmentReconcilesWithoutReissuingMutationOrReadingAsset(){
    frozen();when(gateway.compatibility("source",version,hash)).thenReturn(Map.of("status","PRESENT","compatible",true,"attestation",attestation()));
    assertThat(service.verify("source",version,hash,()->{})).containsEntry("reconciled",true);
    verifyNoInteractions(objects);verify(gateway,never()).attestCompatibility(anyString(),any(),anyString(),anyLong());
  }
  @Test void driftOrNegativeAttestationCannotProduceCompatibility(){
    frozen();when(gateway.compatibility("source",version,hash)).thenReturn(Map.of("compatible",false));
    assertThatThrownBy(()->service.verify("source",version,hash,()->{})).hasMessage("ING_READINESS_EXISTING_ATTESTATION_REJECTED");
    assertThatThrownBy(()->service.verify("source",version,"sha256:"+"0".repeat(64),()->{})).hasMessage("ING_READINESS_FROZEN_IDENTITY_CHANGED");
    verifyNoInteractions(objects);verify(gateway,never()).attestCompatibility(anyString(),any(),anyString(),anyLong());
  }
  @Test void cancellationDuringSemanticResolutionNeverAttests(){
    frozen();when(gateway.compatibility("source",version,hash)).thenReturn(Map.of("status","ABSENT"));
    doAnswer(invocation->{Thread.currentThread().interrupt();return null;}).when(gateway).preflight(any(ExecutionBundle.class));
    assertThatThrownBy(()->service.verify("source",version,hash,()->{})).hasMessage("ING_COMPAT_CANCELLED");
    verifyNoInteractions(objects);verify(gateway,never()).attestCompatibility(anyString(),any(),anyString(),anyLong());
  }
  @Test void humanAndResourcePolicyAreRequiredBeforeAnyWork()throws Exception{
    var mocked=mock(OnboardingReadinessService.class);
    var http=MockMvcBuilders.standaloneSetup(new OnboardingReadinessApi(mocked,"tenant-a")).setControllerAdvice(new OwnerAuthorizationErrors()).build();
    String body=json.writeValueAsString(Map.of("sourceId","source","onboardingVersionId",version,"configurationHash",hash));
    for(String type:List.of("SERVICE","AI_AGENT"))http.perform(post("/api/trusted-human/v1/ingestion/onboarding-readiness").contentType("application/json").content(body).with(request->{TestAuthorization.bind(request,"actor",type,Set.of("ouf.onboarding.configuration.write"));return request;})).andExpect(status().isForbidden());
    http.perform(post("/api/trusted-human/v1/ingestion/onboarding-readiness").contentType("application/json").content(body).with(request->{TestAuthorization.bind(request,"actor","HUMAN",Set.of());return request;})).andExpect(status().isForbidden());
    verifyNoInteractions(mocked);
  }
}
