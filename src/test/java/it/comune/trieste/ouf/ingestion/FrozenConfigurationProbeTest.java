package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;

class FrozenConfigurationProbeTest {
  private final ObjectMapper json=new ObjectMapper();
  private final byte[] csv="\ufeffid;name\r\n1;Town Hall\r\n2;Library\r\n".getBytes(StandardCharsets.UTF_8);
  private int reads,semanticChecks;

  @Test void realCsvMapperAndFrozenOutputContractsValidateAllRowsWithoutPersistence(){
    var proof=probe().probe(candidate());
    assertThat(proof).containsEntry("status","PASS").containsEntry("validatedRows",2L).containsEntry("attestationSubmitted",false);
    assertThat(reads).isEqualTo(1);assertThat(semanticChecks).isEqualTo(1);
  }
  @Test void tamperedFrozenConfigurationFailsBeforeNetwork(){
    var c=candidate();configuration(c).put("changed",true);
    assertThatThrownBy(()->probe().probe(c)).hasMessage("ING_COMPAT_CONFIGURATION_HASH_MISMATCH");
    assertThat(reads+semanticChecks).isZero();
  }
  @Test void draftCannotGenerateACompatibilityProof(){
    var c=candidate();c.put("state","DRAFT");
    assertThatThrownBy(()->probe().probe(c)).hasMessage("ING_COMPAT_VERSION_NOT_FROZEN");
    assertThat(reads+semanticChecks).isZero();
  }
  @Test void unsupportedAdapterVersionFailsBeforeNetwork(){
    var c=candidate();execution(c).put("adapterRuntimeVersion","99.0.0");rehash(c);
    assertThatThrownBy(()->probe().probe(c)).hasMessage("ING_COMPAT_ADAPTER_VERSION_UNSUPPORTED");
    assertThat(reads+semanticChecks).isZero();
  }
  @Test void semanticFailureCannotBecomeCompatible(){
    RuntimePorts.SemanticPort deny=new RuntimePorts.SemanticPort(){public void preflight(Collection<String> refs){throw new IllegalArgumentException("ING_SEMANTIC_BINDING_MISMATCH");}};
    assertThatThrownBy(()->new FrozenConfigurationProbe(json,objects(),deny).probe(candidate())).hasMessage("ING_SEMANTIC_BINDING_MISMATCH");
    assertThat(reads).isZero();
  }
  @Test void missingMappingFieldFailsBeforeAnyOutputOrAttestation(){
    var c=candidate();var semantic=PublishedActivation.map(configuration(c),"semanticMapping");
    semantic.put("propertyMappings",List.of(Map.of("sourceField","missing","targetPropertyIri","https://example.test/name","transform","IDENTITY")));rehash(c);
    assertThatThrownBy(()->probe().probe(c)).hasMessage("ING_MAPPING_SOURCE_FIELD_MISSING");
  }
  @Test void unsupportedTransformUsesTheProductionFailure(){
    var c=candidate();PublishedActivation.map(configuration(c),"semanticMapping").put("propertyMappings",List.of(Map.of("sourceField","name","targetPropertyIri","https://example.test/name","transform","UPPERCASE")));rehash(c);
    assertThatThrownBy(()->probe().probe(c)).hasMessage("ING_TRANSFORM_UNSUPPORTED");
  }
  @Test void assetIntegrityFailurePropagatesWithoutPositiveProof(){
    RuntimePorts.ManagedObjectPort broken=(ref,size,hash)->{throw new IllegalArgumentException("ING_MANAGED_INTEGRITY_MISMATCH");};
    assertThatThrownBy(()->new FrozenConfigurationProbe(json,broken,semantics()).probe(candidate())).hasMessage("ING_MANAGED_INTEGRITY_MISMATCH");
  }
  @Test void sourceIdentityStrategyIsCheckedIndependentlyOfSourceFields(){
    var c=candidate();PublishedActivation.map(configuration(c),"sourceObjectIdentityPolicy").put("strategy","GUESS_ADDRESS");rehash(c);
    assertThatThrownBy(()->probe().probe(c)).hasMessage("ING_COMPAT_IDENTITY_POLICY_UNSUPPORTED");
  }
  @Test void rowOrdinalIdentityUsesTheProductionManagedIdentity(){
    var c=candidate();configuration(c).put("sourceObjectIdentityPolicy",Map.of("strategy","ASSET_AND_ROW_ORDINAL","sourceFields",List.of("$managedRowOrdinal")));rehash(c);
    assertThat(probe().probe(c)).containsEntry("validatedRows",2L);
  }
  @Test void lateInvalidRowFailsRatherThanSamplingTheFirstRow(){
    byte[] bad="id;name\r\n1;First\r\n1;Second\r\n".getBytes(StandardCharsets.UTF_8);
    var c=candidate();execution(c).put("expectedSize",bad.length);runtime(c).put("contentHash",ExecutionGatewayClient.hash(bad));rehash(c);
    assertThatThrownBy(()->new FrozenConfigurationProbe(json,(ref,size,hash)->bad,semantics()).probe(c)).hasMessage("ING_MANAGED_DUPLICATE_IDENTITY");
  }
  @Test void unresolvedTransportPlaceholderIsRejected(){
    var p=new Properties();p.setProperty("url","${MISSING}");
    assertThatThrownBy(()->FrozenConfigurationProbeMain.required(p,"url")).hasMessage("ING_COMPAT_TRANSPORT_CONFIG_REQUIRED");
  }

  private FrozenConfigurationProbe probe(){return new FrozenConfigurationProbe(json,objects(),semantics());}
  private RuntimePorts.ManagedObjectPort objects(){return (ref,size,hash)->{reads++;assertThat(size).isEqualTo(csv.length);assertThat(hash).isEqualTo(ExecutionGatewayClient.hash(csv));return csv;};}
  private RuntimePorts.SemanticPort semantics(){return new RuntimePorts.SemanticPort(){public void preflight(Collection<String> refs){semanticChecks++;assertThat(refs).containsExactly("places@1");}};}
  private Map<String,Object> candidate(){
    var exec=new LinkedHashMap<String,Object>();
    exec.putAll(Map.of("acquisitionMode","INTERNAL_MANAGED_CSV","adapterId","managed-tabular-v1","adapterRuntimeVersion","1.0.0","sourceSchemaRef","schema:1","sourceSchemaId","places","sourceSchemaVersion","1","semanticPublicationSetRef","semantic:1","adapterProfileRef","adapter:1","observationPolicy","ACQUISITION_TIME"));
    exec.putAll(Map.of("expectedSize",csv.length,"maxRows",100,"maxColumns",20,"maxCellChars",1000));
    var runtime=new LinkedHashMap<String,Object>(Map.of("execution",exec,"stagingRef","object://files/approved.csv","contentHash",ExecutionGatewayClient.hash(csv),"csvDelimiter",";"));
    var semantic=new LinkedHashMap<String,Object>(Map.of("sourceType",Map.of("sourceId","source","typeCode","PLACE"),"semanticRefs",List.of("places@1"),"propertyMappings",List.of(Map.of("sourceField","name","targetPropertyIri","https://example.test/name","transform","IDENTITY"))));
    var config=new LinkedHashMap<String,Object>();
    config.put("bundle",Map.of("bundleId","bundle","bundleVersion","1","source",Map.of("sourceId","source","sourceKind","INTERNAL_MANAGED","acquisitionMode","MANAGED")));
    config.put("extractionProfile",Map.of("runtime",runtime,"projection",Map.of("PLACE",List.of("id","name"))));
    config.put("semanticMapping",semantic);config.put("semanticReferenceBindings",List.of(Map.of("semanticId","places","semanticVersion","1","revisionId",UUID.randomUUID().toString(),"publicationSetId",UUID.randomUUID().toString())));
    config.put("sourceObjectIdentityPolicy",new LinkedHashMap<>(Map.of("strategy","NATIVE_KEY","sourceFields",List.of("id"))));
    config.put("changeRepresentationProfile",Map.of("mode","FULL_SNAPSHOT"));config.put("dataAccessPolicies",List.of(Map.of("target","https://example.test/name","label","OPEN","scope","PROPERTY")));
    var out=new LinkedHashMap<String,Object>(Map.of("sourceId","source","onboardingVersionId",UUID.randomUUID().toString(),"version",1,"state","IN_REVIEW","configuration",config));rehash(out);return out;
  }
  private Map<String,Object> configuration(Map<String,Object> candidate){return PublishedActivation.map(candidate,"configuration");}
  private Map<String,Object> runtime(Map<String,Object> candidate){return PublishedActivation.map(PublishedActivation.map(configuration(candidate),"extractionProfile"),"runtime");}
  private Map<String,Object> execution(Map<String,Object> candidate){return PublishedActivation.map(runtime(candidate),"execution");}
  private void rehash(Map<String,Object> candidate){candidate.put("configurationHash",FrozenConfigurationProbe.configurationHash(json,configuration(candidate)));}
}
