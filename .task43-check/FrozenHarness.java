package it.comune.trieste.ouf.ingestion;
import java.util.*;import com.fasterxml.jackson.databind.*;
public class FrozenHarness {
 public static void main(String[] args)throws Exception{
  var json=new ObjectMapper();
  for(boolean invalid:List.of(false,true)){
   byte[] bytes=("x\n01\n"+(invalid?"private":"02")).getBytes(java.nio.charset.StandardCharsets.UTF_8);
   var exec=new LinkedHashMap<String,Object>(Map.of("acquisitionMode","INTERNAL_MANAGED_CSV","sourceSchemaRef","schema:1","sourceSchemaId","places","sourceSchemaVersion","1","semanticPublicationSetRef","sem:1","adapterProfileRef","adapter:1","observationPolicy","ACQUISITION_TIME","adapterId","managed-tabular-v1","adapterRuntimeVersion","1.0.0"));exec.put("expectedSize",bytes.length);
   String set=UUID.randomUUID().toString(),id="urn:t";
   var def=Map.of("components",List.of(Map.of("name","ids","type","integer","required",true,"cardinality","MANY")));
   var pin=Map.of("semanticId",id,"semanticVersion","1","revisionId",UUID.randomUUID().toString(),"publicationSetId",set,"definition",def,"definitionSha256",ValuePins.digest(def));
   var comp=Map.of("kind","STRUCTURED","dataAccessLabel","OPEN","targetPropertyIri","urn:out","typeRef",id+"@"+set,"components",List.of(Map.of("component","ids","items",List.of(Map.of("fieldName","x","reader","INTEGER_V1")))));
   var config=Map.of("bundle",Map.of("source",Map.of("sourceId","s","sourceKind","INTERNAL_MANAGED","acquisitionMode","MANAGED"),"bundleId","bundle","bundleVersion","1"),
     "extractionProfile",Map.of("projection",Map.of("PLACE",List.of("x")),"runtime",Map.of("execution",exec,"stagingRef","object://test","contentHash",ExecutionGatewayClient.hash(bytes))),
     "semanticMapping",Map.of("sourceType",Map.of("sourceId","s","typeCode","PLACE"),"semanticRefs",List.of(id+"@1"),"propertyMappings",List.of(),"compositions",List.of(comp)),
     "sourceObjectIdentityPolicy",Map.of("strategy","NATIVE_KEY","sourceFields",List.of("x")),"dataAccessPolicies",List.of(Map.of("target","urn:out","label","OPEN","scope","PROPERTY")),
     "changeRepresentationProfile",Map.of("mode","FULL_SNAPSHOT"),"semanticReferenceBindings",List.of(pin));
   var candidate=Map.<String,Object>of("sourceId","s","onboardingVersionId",UUID.randomUUID().toString(),"configuration",config,"state","IN_REVIEW","configurationHash",FrozenConfigurationProbe.configurationHash(json,config));
   var probe=new FrozenConfigurationProbe(json,(ref,size,hash)->bytes,refs->{assert refs.equals(List.of(id+"@1"));});
   if(!invalid){var proof=probe.probe(candidate);assert proof.get("validatedRows").equals(2L);}
   else{try{probe.probe(candidate);throw new AssertionError();}catch(ConfiguredValueTransform.InvalidValue e){assert e.rowOrdinal()==2;assert e.getMessage().equals("ING_TRANSFORM_VALUE_INVALID");}}
  }System.out.println("PASS actual FrozenConfigurationProbe with fixture adapter: structured-only pass and reject at ordinal 2");
 }
}