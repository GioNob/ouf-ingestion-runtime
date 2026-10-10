package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.*;

/** Read-only consumer proof for an unpublished, frozen managed-source configuration. */
public final class FrozenConfigurationProbe {
  private final ObjectMapper json;
  private final RuntimePorts.ManagedObjectPort objects;
  private final RuntimePorts.SemanticPort semantics;

  public FrozenConfigurationProbe(ObjectMapper json,RuntimePorts.ManagedObjectPort objects,RuntimePorts.SemanticPort semantics){
    this.json=json;this.objects=objects;this.semantics=semantics;
  }

  public Map<String,Object> probe(Map<String,Object> candidate){
    String source=PublishedActivation.text(candidate,"sourceId");
    UUID version=UUID.fromString(PublishedActivation.text(candidate,"onboardingVersionId"));
    String hash=PublishedActivation.text(candidate,"configurationHash");
    if(!Set.of("IN_REVIEW","APPROVED").contains(candidate.get("state")))throw invalid("ING_COMPAT_VERSION_NOT_FROZEN");
    Map<String,Object> configuration=PublishedActivation.map(candidate,"configuration");
    if(!hash.matches("sha256:[a-f0-9]{64}")||!hash.equals(configurationHash(json,configuration)))throw invalid("ING_COMPAT_CONFIGURATION_HASH_MISMATCH");
    var template=PublishedActivation.map(configuration,"bundle");
    var sourceConfig=PublishedActivation.map(template,"source");
    if(!source.equals(sourceConfig.get("sourceId"))||!"INTERNAL_MANAGED".equals(sourceConfig.get("sourceKind"))||!"MANAGED".equals(sourceConfig.get("acquisitionMode")))throw invalid("ING_COMPAT_SOURCE_UNSUPPORTED");
    var published=new LinkedHashMap<String,Object>(template);
    for(String key:List.of("extractionProfile","semanticMapping","sourceObjectIdentityPolicy","changeRepresentationProfile","dataAccessPolicies","semanticReferenceBindings")){
      if(!configuration.containsKey(key))throw invalid("ING_COMPAT_SECTION_REQUIRED");
      published.put(key,configuration.get(key));
    }
    var semantic=PublishedActivation.map(configuration,"semanticMapping");
    Object refs=semantic.get("semanticRefs");
    if(!(refs instanceof List<?> list)||list.isEmpty()||list.size()>20||list.stream().anyMatch(x->!(x instanceof String s)||!s.matches("[^@\\s]+@[^@\\s]+"))||new HashSet<>(list).size()!=list.size())throw invalid("ING_PREFLIGHT_EXACT_REF_REQUIRED");
    published.put("semanticRefs",refs);
    String bundleId=PublishedActivation.text(template,"bundleId");
    Object rawVersion=template.getOrDefault("bundleVersion",candidate.get("version"));
    if(rawVersion==null||String.valueOf(rawVersion).isBlank())throw invalid("ING_COMPAT_BUNDLE_VERSION_REQUIRED");
    var snapshot=new ExecutionBundle(bundleId,String.valueOf(rawVersion),hash,source,"MANAGED",null,Map.of("publishedBundle",published,"pinnedReferences",refs));
    var bundle=PublishedExecutionMapper.map(snapshot);
    AdapterSpi adapter=switch(bundle.acquisitionMode()){
      case "INTERNAL_MANAGED_CSV","INTERNAL_MANAGED_XLSX" -> new ManagedTabularAdapter(objects);
      case "INTERNAL_MANAGED_GEOPACKAGE" -> new ManagedGeoPackageAdapter(objects);
      default -> throw invalid("ING_COMPAT_ADAPTER_UNSUPPORTED");
    };
    if(!adapter.adapterId().equals(bundle.configuration().get("adapterId"))||!adapter.compatibility().current().equals(bundle.configuration().get("adapterRuntimeVersion")))throw invalid("ING_COMPAT_ADAPTER_VERSION_UNSUPPORTED");
    var identity=PublishedActivation.map(configuration,"sourceObjectIdentityPolicy");
    String strategy=PublishedActivation.text(identity,"strategy");
    Object fields=identity.get("sourceFields");
    if("MANAGED_DETERMINISTIC".equals(strategy)){
      var runtime=PublishedActivation.map(PublishedActivation.map(configuration,"extractionProfile"),"runtime");
      if(!List.of("$managedRowOrdinal").equals(fields)||!"managed-tabular-v1".equals(adapter.adapterId())
          ||!"normalization://managed-file/asset-row-ordinal-v1".equals(identity.get("normalizationRuleRef"))
          ||!"ASSET_AND_ROW_ORDINAL".equals(runtime.get("rowIdentityBasis")))throw invalid("ING_COMPAT_IDENTITY_POLICY_UNSUPPORTED");
    }else if(!Set.of("NATIVE_KEY","COMPOSITE_NATIVE_KEY").contains(strategy)||!(fields instanceof List<?> keys)
        ||keys.isEmpty()||keys.stream().anyMatch(x->!(x instanceof String s)||s.isBlank()||s.startsWith("$"))
        ||new HashSet<>(keys).size()!=keys.size()||("NATIVE_KEY".equals(strategy)?keys.size()!=1:keys.size()<2))throw invalid("ING_COMPAT_IDENTITY_POLICY_UNSUPPORTED");
    Object rawExpected=bundle.configuration().get("expectedFieldNames");
    if(!(rawExpected instanceof List<?> expected)||expected.stream().anyMatch(x->!(x instanceof String s)||s.isBlank())||new HashSet<>(expected).size()!=expected.size())throw invalid("ING_EXPECTED_FIELDS_INVALID");
    // Exact historical identities are resolved through the same Gateway port used by run preflight.
    semantics.preflight(snapshot);
    var pipeline=new CanonicalRecordPipeline(json,new FrozenContractValidator(json),null,null,null);
    UUID run=UUID.randomUUID();long count=0;
    try(var cursor=adapter.open(bundle,new AdapterSpi.Checkpoint(Map.of()))){
      Optional<AdapterSpi.SourceRecord> next;
      while((next=cursor.next()).isPresent()){
        var record=next.get();
        if(!record.payload().keySet().containsAll(expected))throw invalid("ING_SCHEMA_INCOMPATIBLE");
        pipeline.validateForCompatibility(new CanonicalRecordPipeline.Command(run,"compatibility-probe",++count,1,adapter.adapterId(),bundle,record,"compatibility-probe",Map.of(),Map.of()));
      }
    }
    if(count==0)throw invalid("ING_COMPAT_EMPTY_ASSET_UNPROVEN");
    return Map.of("schema","ouf.ingestion.compatibility-probe.v1","status","PASS","onboardingVersionId",version.toString(),"configurationHash",hash,"contentHash",PublishedActivation.text(bundle.configuration(),"expectedHash"),"adapterId",adapter.adapterId(),"adapterRuntimeVersion",adapter.compatibility().current(),"validatedRows",count,"semanticBindingCount",list.size(),"attestationSubmitted",false);
  }

  static String configurationHash(ObjectMapper json,Object configuration){
    try{return ExecutionGatewayClient.hash(json.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,true).writeValueAsBytes(configuration));}
    catch(Exception e){throw invalid("ING_COMPAT_CONFIGURATION_INVALID");}
  }
  private static IllegalArgumentException invalid(String code){return new IllegalArgumentException(code);}
}
