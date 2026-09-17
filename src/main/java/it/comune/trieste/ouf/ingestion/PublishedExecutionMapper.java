package it.comune.trieste.ouf.ingestion;

import java.util.*;

/** Deterministic projection of approved publication data; never consults ACTIVE during execution. */
public final class PublishedExecutionMapper {
  private PublishedExecutionMapper(){}
  public static ExecutionBundle map(ExecutionBundle snapshot){
    if(!snapshot.configuration().containsKey("publishedBundle"))return snapshot;
    var published=PublishedActivation.map(snapshot.configuration(),"publishedBundle");
    var extraction=PublishedActivation.map(published,"extractionProfile");
    var runtime=PublishedActivation.map(extraction,"runtime");
    var execution=PublishedActivation.map(runtime,"execution");
    var semantic=PublishedActivation.map(published,"semanticMapping");
    var type=PublishedActivation.map(semantic,"sourceType");
    if(!snapshot.sourceId().equals(PublishedActivation.text(type,"sourceId")))throw invalid();
    String typeCode=PublishedActivation.text(type,"typeCode");
    var config=new LinkedHashMap<String,Object>(execution);
    config.putAll(snapshot.configuration());
    config.put("typeCode",typeCode);
    config.put("propertyMappings",semantic.get("propertyMappings"));
    if(!(published.get("dataAccessPolicies") instanceof List<?> policies)||policies.isEmpty())throw new IllegalArgumentException("ING_DATA_ACCESS_POLICY_REQUIRED");
    var labels=new LinkedHashSet<Map<String,Object>>();
    for(Object item:policies){if(!(item instanceof Map<?,?> policy)||!(policy.get("label") instanceof String label)||!Set.of("OPEN","ANONYMOUS","PERSONAL","SENSITIVE","RESTRICTED").contains(label))throw new IllegalArgumentException("ING_DATA_ACCESS_POLICY_INVALID");labels.add(Map.of("labelId",label));}
    config.put("dataAccessLabels",List.copyOf(labels));
    config.put("changeRepresentationProfile",published.get("changeRepresentationProfile"));
    var projection=PublishedActivation.map(extraction,"projection");
    config.put("expectedFieldNames",projection.get(typeCode));
    config.put("identityFields",PublishedActivation.map(published,"sourceObjectIdentityPolicy").get("sourceFields"));
    for(String key:List.of("adapterId","adapterRuntimeVersion","sourceSchemaRef","sourceSchemaId","sourceSchemaVersion","semanticPublicationSetRef","adapterProfileRef","observationPolicy"))PublishedActivation.text(config,key);
    if(!(config.get("propertyMappings") instanceof List<?> mappings)||mappings.isEmpty())throw new IllegalArgumentException("ING_PROPERTY_MAPPING_REQUIRED");
    if(!(config.get("expectedFieldNames") instanceof List<?> fields)||fields.isEmpty())throw new IllegalArgumentException("ING_EXPECTED_FIELDS_REQUIRED");
    if(!(config.get("identityFields") instanceof List<?> keys)||keys.isEmpty())throw new IllegalArgumentException("ING_IDENTITY_POLICY_REQUIRED");
    String mode=PublishedActivation.text(execution,"acquisitionMode");
    String binding=snapshot.bindingRef();
    if("MANAGED".equals(snapshot.acquisitionMode())){
      if("INTERNAL_MANAGED_GEOPACKAGE".equals(mode)){
        if(!"managed-geopackage-v1".equals(config.get("adapterId"))||keys.equals(List.of("$managedRowOrdinal")))throw invalid();
        for(String key:List.of("layer","sourceCrs","geometryColumn")){String approved=PublishedActivation.text(runtime,key);if(!approved.equals(config.get(key)))throw invalid();}
      }else if(!Set.of("INTERNAL_MANAGED_CSV","INTERNAL_MANAGED_XLSX").contains(mode)||!"managed-tabular-v1".equals(config.get("adapterId")))throw invalid();
      binding=PublishedActivation.text(runtime,"stagingRef");
      config.put("expectedHash",PublishedActivation.text(runtime,"contentHash"));
      if(keys.equals(List.of("$managedRowOrdinal")))config.put("identityFields",List.of());
    }else if(!Set.of("REST_JSON","WFS").contains(mode))throw invalid();
    if(config.get("changeRepresentationProfile")==null)throw new IllegalArgumentException("ING_CHANGE_PROFILE_REQUIRED");
    return new ExecutionBundle(snapshot.bundleId(),snapshot.bundleVersion(),snapshot.checksum(),snapshot.sourceId(),mode,binding,config);
  }
  private static IllegalArgumentException invalid(){return new IllegalArgumentException("ING_EXECUTION_PROFILE_INVALID");}
}
