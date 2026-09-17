package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class PublishedExecutionMapperTest {
  @Test void executesOnlyPinnedApprovedMappingAndManagedAsset(){
    var mapped=PublishedExecutionMapper.map(snapshot());
    assertThat(mapped.acquisitionMode()).isEqualTo("INTERNAL_MANAGED_CSV");
    assertThat(mapped.bindingRef()).isEqualTo("object://files/approved.csv");
    assertThat(mapped.checksum()).isEqualTo("sha256:pinned");
    assertThat(mapped.configuration().get("propertyMappings")).isEqualTo(List.of(Map.of("sourceField","name","targetPropertyIri","https://example.org/name","transform","IDENTITY")));
    assertThat(mapped.configuration().get("expectedFieldNames")).isEqualTo(List.of("id","name"));
  }
  @Test void missingApprovedExecutionProfileFailsClosed(){
    var b=snapshot();var published=new LinkedHashMap<>(PublishedActivation.map(b.configuration(),"publishedBundle"));
    published.put("extractionProfile",Map.of("runtime",Map.of()));
    assertThatThrownBy(()->PublishedExecutionMapper.map(new ExecutionBundle(b.bundleId(),b.bundleVersion(),b.checksum(),b.sourceId(),b.acquisitionMode(),null,Map.of("publishedBundle",published)))).isInstanceOf(IllegalArgumentException.class);
  }
  private ExecutionBundle snapshot(){
    var execution=new LinkedHashMap<String,Object>();
    execution.putAll(Map.of("acquisitionMode","INTERNAL_MANAGED_CSV","adapterId","managed-tabular-v1","adapterRuntimeVersion","1.0.0","sourceSchemaRef","schema:1","sourceSchemaId","file","sourceSchemaVersion","1","semanticPublicationSetRef","semantic:1","adapterProfileRef","adapter:1","observationPolicy","ACQUISITION_TIME"));
    var published=Map.<String,Object>of("dataAccessPolicies",List.of(Map.of("target","https://example.org/name","label","OPEN","scope","PROPERTY")),"extractionProfile",Map.of("runtime",Map.of("execution",execution,"stagingRef","object://files/approved.csv","contentHash","sha256:asset"),"projection",Map.of("FILE",List.of("id","name"))),"semanticMapping",Map.of("sourceType",Map.of("sourceId","source","typeCode","FILE"),"propertyMappings",List.of(Map.of("sourceField","name","targetPropertyIri","https://example.org/name","transform","IDENTITY"))),"sourceObjectIdentityPolicy",Map.of("sourceFields",List.of("id")),"changeRepresentationProfile",Map.of("mode","FULL_SNAPSHOT"));
    return new ExecutionBundle("bundle","1","sha256:pinned","source","MANAGED",null,Map.of("publishedBundle",published));
  }
}
