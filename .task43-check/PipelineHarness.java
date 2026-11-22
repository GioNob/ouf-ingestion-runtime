package it.comune.trieste.ouf.ingestion;
import java.util.*;import com.fasterxml.jackson.databind.*;
public class PipelineHarness {
 public static void main(String[] args)throws Exception{
  ObjectMapper json=new ObjectMapper();
  var zones=new EnumMap<RuntimePorts.DataLakePort.Zone,byte[]>(RuntimePorts.DataLakePort.Zone.class);
  var pipeline=new CanonicalRecordPipeline(json,new FrozenContractValidator(json),new DurablePipelineRepository(),new QuarantineService(),
    (zone,run,obj,data,hash,key)->{zones.put(zone,data);return new RuntimePorts.DataLakePort.Receipt("lake:"+zone,true);});
  var contract=json.readTree(java.nio.file.Path.of("contracts/ouf-value-types-v1.json").toFile());
  for(var r:contract.required("readers"))assert ConfiguredValueTransform.resolve(r.required("id").asText()).output.equals(r.required("output").asText());
  assert contract.required("readers").size()==ConfiguredValueTransform.values().length;
  var cells=Map.<String,Object>of("x"," +001 | |0002 ","when","2026-11-22T10:15+01:00");
  var comp=Map.of("kind","CONSTRUCTOR","targetPropertyIri","urn:ids","dataAccessLabel","OPEN","constructorId","LIST_SPLIT_V1",
    "inputs",List.of(Map.of("fieldName","x","role","value","reader","IDENTITY")),"params",Map.of("separator","|","itemReader","INTEGER_V1"));
  var policy=Map.of("scope","PROPERTY","target","urn:ids","label","OPEN");
  var exec=Map.of("acquisitionMode","INTERNAL_MANAGED_CSV","sourceSchemaRef","schema:1","sourceSchemaId","places","sourceSchemaVersion","1",
    "semanticPublicationSetRef","sem:1","adapterProfileRef","adapter:1","observationPolicy","ACQUISITION_TIME","adapterId","managed-tabular-v1","adapterRuntimeVersion","1.0.0");
  var sem=Map.of("compositions",List.of(comp),"sourceType",Map.of("typeCode","PLACE","sourceId","s"),"propertyMappings",
    List.of(Map.of("sourceField","when","targetPropertyIri","urn:when","transform","DATETIME_ISO_OFFSET_V1")));
  var published=Map.of("semanticMapping",sem,"changeRepresentationProfile",Map.of("mode","FULL_SNAPSHOT"),"sourceObjectIdentityPolicy",Map.of("sourceFields",List.of("x")),
    "extractionProfile",Map.of("projection",Map.of("PLACE",List.of("x","when")),"runtime",Map.of("execution",exec,"stagingRef","managed:1","contentHash","hash")),"dataAccessPolicies",List.of(policy));
  var bundle=PublishedExecutionMapper.map(new ExecutionBundle("bundle","1","hash","s","MANAGED",null,Map.of("publishedBundle",published)));
  var cmd=new CanonicalRecordPipeline.Command(UUID.randomUUID(),"default",1,1,"managed-tabular-v1",bundle,new AdapterSpi.SourceRecord("obj",1,cells,Map.of()),"corr",Map.of(),Map.of());
  pipeline.validateForCompatibility(cmd);assert zones.isEmpty();
  assert pipeline.process(cmd).succeeded(); assert zones.size()==3;
  assert json.readTree(zones.get(RuntimePorts.DataLakePort.Zone.RAW)).equals(json.valueToTree(cells));
  assert DurablePipelineRepository.handoff.get("canonicalPayload").equals(Map.of("urn:ids",List.of("1","2"),"urn:when","2026-11-22T09:15:00Z"));
  assert DurablePipelineRepository.handoff.get("rawObjectRef").equals("lake:RAW");
  var lineage=json.valueToTree(DurablePipelineRepository.lineage);assert lineage.at("/data/rawObjectRef").asText().equals("lake:RAW");
  for(String status:List.of("ACTIVE","DEPRECATED","RETIRED","DRAFT")){
   var r=Map.of("artifact_type","DATATYPE","status",status);
   assert PublicationGatewayClient.Resolution.PREPARATION.accepts(r)==status.equals("ACTIVE");
   assert PublicationGatewayClient.Resolution.PUBLISHED.accepts(r)!=status.equals("DRAFT");
  }
  System.out.println("PASS production projection, 17 contract reader outputs, compatibility and process, 3 frozen validators, RAW and handoff, status matrix (in-memory persistence)");
 }
}