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
  void frozen(){when(gateway.frozenConfiguration(eq("source"),eq(version),anyString())).thenReturn(Map.of("source_id","source","onboarding_version_id",version.toString(),"configuration_hash",hash,"configuration",candidate.get("configuration"),"state","APPROVED","version",1));}
  @Test void realProbeValidatesEveryRowBeforeExactOwnerAcknowledgment(){
    frozen();when(gateway.compatibility("source",version,hash)).thenReturn(Map.of("status","ABSENT"));
    when(objects.read(anyString(),anyLong(),anyString())).thenReturn("\ufeffid;name\r\n1;Town Hall\r\n2;Library\r\n".getBytes(StandardCharsets.UTF_8));
    when(gateway.attestCompatibility("source",version,hash,2)).thenReturn(attestation());
    assertThat(service.verify("source",version,hash,()->{})).containsEntry("status","PASS").containsEntry("validatedRows",2L).containsEntry("attestationSubmitted",true).containsEntry("reconciled",false);
    var order=inOrder(gateway,objects);order.verify(gateway).frozenConfiguration("source",version,hash);order.verify(gateway).compatibility("source",version,hash);order.verify(gateway).preflight(any(ExecutionBundle.class));order.verify(objects).read(anyString(),anyLong(),anyString());order.verify(gateway).attestCompatibility("source",version,hash,2);
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
  @Test void invalidCsvReturnsSafe422AndTheNextFrozenCsvPasses()throws Exception{
    var http=MockMvcBuilders.standaloneSetup(new OnboardingReadinessApi(service,"tenant-a")).build();
    var invalid=new FrozenConfigurationProbeTest().candidate();
    var badConfig=PublishedActivation.map(invalid,"configuration");
    var runtime=PublishedActivation.map(PublishedActivation.map(badConfig,"extractionProfile"),"runtime");
    byte[] duplicate="id;name\r\n1;First\r\n1;Second\r\n".getBytes(StandardCharsets.UTF_8);
    PublishedActivation.map(runtime,"execution").put("expectedSize",duplicate.length);
    runtime.put("contentHash",ExecutionGatewayClient.hash(duplicate));
    String badHash=FrozenConfigurationProbe.configurationHash(json,badConfig);
    UUID badVersion=UUID.fromString((String)invalid.get("onboardingVersionId"));
    when(gateway.frozenConfiguration("source",badVersion,badHash)).thenReturn(Map.of("source_id","source",
      "onboarding_version_id",badVersion.toString(),"configuration_hash",badHash,"configuration",badConfig,"state","APPROVED","version",1));
    when(gateway.compatibility("source",badVersion,badHash)).thenReturn(Map.of("status","ABSENT"));
    when(objects.read(anyString(),anyLong(),eq(ExecutionGatewayClient.hash(duplicate)))).thenReturn(duplicate);
    http.perform(authorized(badVersion,badHash)).andExpect(status().is(422))
      .andExpect(jsonPath("$.code").value("ING_MANAGED_DUPLICATE_IDENTITY"));
    verify(gateway,never()).attestCompatibility(anyString(),any(),anyString(),anyLong());

    frozen();when(gateway.compatibility("source",version,hash)).thenReturn(Map.of("status","ABSENT"));
    byte[] valid="\ufeffid;name\r\n1;Town Hall\r\n2;Library\r\n".getBytes(StandardCharsets.UTF_8);
    when(objects.read(anyString(),anyLong(),eq(ExecutionGatewayClient.hash(valid)))).thenReturn(valid);
    when(gateway.attestCompatibility("source",version,hash,2)).thenReturn(attestation());
    http.perform(authorized(version,hash)).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("PASS"));
  }

  @Test void contractErrorExposesOnlyTheOwnerCode()throws Exception{
    var mocked=mock(OnboardingReadinessService.class);
    when(mocked.verify(eq("source"),eq(version),eq(hash),any())).thenThrow(
      new FrozenContractValidator.ContractViolation("ING_CONTRACT_INVALID",List.of("private-field")));
    var http=MockMvcBuilders.standaloneSetup(new OnboardingReadinessApi(mocked,"tenant-a")).build();
    http.perform(authorized(version,hash)).andExpect(status().is(422))
      .andExpect(jsonPath("$.code").value("ING_CONTRACT_INVALID")).andExpect(jsonPath("$.paths").doesNotExist());
  }

  @Test void typedDataFailureReturnsOnlyCodeAndRow()throws Exception{
    var mocked=mock(OnboardingReadinessService.class);
    when(mocked.verify(eq("source"),eq(version),eq(hash),any())).thenThrow(new ConfiguredValueTransform.InvalidValue(2));
    var http=MockMvcBuilders.standaloneSetup(new OnboardingReadinessApi(mocked,"tenant-a")).build();
    var response=http.perform(authorized(version,hash)).andExpect(status().is(422)).andReturn().getResponse();
    assertThat(json.readTree(response.getContentAsString())).isEqualTo(json.readTree(
        "{\"code\":\"ING_TRANSFORM_VALUE_INVALID\",\"rowOrdinal\":2}"));
  }

  private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder authorized(UUID id,String digest)throws Exception{
    return post("/api/trusted-human/v1/ingestion/onboarding-readiness").contentType("application/json")
      .content(json.writeValueAsString(Map.of("sourceId","source","onboardingVersionId",id,"configurationHash",digest)))
      .with(request->{TestAuthorization.bind(request,"operator","HUMAN",Set.of("ouf.onboarding.configuration.write"));return request;});
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
