package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

/** Record-level boundary: configured mapping, immutable lake evidence, rc3 contracts and durable outbox. */
@Service
@ConditionalOnBean(RuntimePorts.DataLakePort.class)
public final class CanonicalRecordPipeline {
  private static final String ENVELOPE="/contracts/rc3/canonical-data-envelope-v1.json",LINEAGE="/contracts/rc3/lineage-record-v1.json",HANDOFF="/contracts/rc3/handoff-payload-v1.json";
  private final ObjectMapper json;private final FrozenContractValidator contracts;private final DurablePipelineRepository durable;private final QuarantineService quarantine;private final RuntimePorts.DataLakePort lake;private final Clock clock;
  public CanonicalRecordPipeline(ObjectMapper json,FrozenContractValidator contracts,DurablePipelineRepository durable,QuarantineService quarantine,RuntimePorts.DataLakePort lake){this(json,contracts,durable,quarantine,lake,Clock.systemUTC());}
  CanonicalRecordPipeline(ObjectMapper json,FrozenContractValidator contracts,DurablePipelineRepository durable,QuarantineService quarantine,RuntimePorts.DataLakePort lake,Clock clock){this.json=json;this.contracts=contracts;this.durable=durable;this.quarantine=quarantine;this.lake=lake;this.clock=clock;}

  public Result process(Command c){
    UUID attempt=UUID.randomUUID(),lineage=UUID.randomUUID(),handoff=UUID.randomUUID();String rawRef=sourceRef(c.record());
    try{
      Config cfg=configuration(c.bundle());Instant acquired=clock.instant();String observed=observedAt(c.record(),cfg,acquired);
      byte[] raw=bytes(c.record().payload());String rawHash=hash(raw);
      RuntimePorts.DataLakePort.Receipt rawReceipt=persist(RuntimePorts.DataLakePort.Zone.RAW,c,raw,rawHash,"raw");rawRef=rawReceipt.objectRef();
      RecordMetadata metadata=metadata(c.record());Map<String,Object> mapped=metadata.operation()==Operation.UPSERT?map(c.record().payload(),cfg.mappings()):Map.of();
      Map<String,Object> envelope=envelope(c,cfg,metadata,mapped,observed,acquired,rawRef);
      contracts.validate(ENVELOPE,envelope);byte[] normalized=bytes(envelope);String normalizedHash=hash(normalized);
      persist(RuntimePorts.DataLakePort.Zone.NORMALIZED,c,normalized,normalizedHash,"normalized");
      Map<String,Object> candidate=Map.copyOf(mapped);Map<String,Object> curatedValue=metadata.operation()==Operation.UPSERT?candidate:tombstoneEvidence(c,metadata);
      byte[] curated=bytes(curatedValue);String outputHash=hash(curated);
      persist(RuntimePorts.DataLakePort.Zone.CURATED,c,curated,outputHash,"curated");
      Map<String,Object> refs=contractRefs(c.bundle(),cfg);
      Map<String,Object> evidence=lineage(c,cfg,metadata,attempt,lineage,handoff,rawRef,rawHash,normalizedHash,outputHash,observed,acquired,refs);
      Map<String,Object> payload=handoff(c,cfg,metadata,lineage,handoff,rawRef,outputHash,observed,acquired,refs,candidate);
      contracts.validate(LINEAGE,evidence);contracts.validate(HANDOFF,payload);
      durable.stage(new DurablePipelineRepository.StageCommand(c.runId(),c.partitionKey(),c.sequenceNo(),c.record().sourceObjectId(),c.attemptNo(),c.adapterId(),c.bundle().bundleVersion(),bundleRef(c.bundle()),idempotency(c,"handoff"),payload,evidence,c.restartCheckpoint(),c.candidateWatermark()),attempt,lineage,handoff);
      return new Result(attempt,lineage,handoff,null);
    }catch(RuntimeException failure){
      String code=safeCode(failure);UUID failed=durable.recordFailure(c.runId(),c.record().sourceObjectId(),c.attemptNo(),c.adapterId(),c.bundle().bundleVersion(),code);
      UUID quarantineId=quarantine.quarantine(c.runId(),failed,c.record().sourceObjectId(),code,rawRef,"contract-or-mapping:"+code,c.correlationId());
      return new Result(failed,null,null,quarantineId);
    }
  }

  public Result reject(Command c,String reasonCode){
    String code=reasonCode!=null&&reasonCode.matches("ING_[A-Z0-9_]{1,76}")?reasonCode:"ING_PROCESSING_REJECTED";String rawRef=sourceRef(c.record());
    try{byte[] raw=bytes(c.record().payload());String rawHash=hash(raw);rawRef=persist(RuntimePorts.DataLakePort.Zone.RAW,c,raw,rawHash,"rejected-raw").objectRef();}catch(RuntimeException ignored){}
    UUID failed=durable.recordFailure(c.runId(),c.record().sourceObjectId(),c.attemptNo(),c.adapterId(),c.bundle().bundleVersion(),code);UUID quarantineId=quarantine.quarantine(c.runId(),failed,c.record().sourceObjectId(),code,rawRef,"schema-surveillance:"+code,c.correlationId());return new Result(failed,null,null,quarantineId);
  }

  private Config configuration(ExecutionBundle b){Map<String,Object> c=b.configuration();return new Config(required(c,"sourceSchemaRef"),required(c,"sourceSchemaId"),required(c,"sourceSchemaVersion"),required(c,"typeCode"),required(c,"semanticPublicationSetRef"),required(c,"adapterProfileRef"),optional(c,"authorityPolicyRef"),strings(c.get("mappingRefs")),strings(c.get("relationshipResolutionStrategyRefs")),labels(c.get("dataAccessLabels")),mappings(c.get("propertyMappings")),required(c,"observationPolicy"),changeProfile(c.get("changeRepresentationProfile")));}
  private Map<String,Object> map(Map<String,Object> source,List<Mapping> mappings){Map<String,Object> out=new LinkedHashMap<>();for(Mapping m:mappings){if(!"IDENTITY".equals(m.transform()))throw failure("ING_TRANSFORM_UNSUPPORTED");if(!source.containsKey(m.sourceField()))throw failure("ING_MAPPING_SOURCE_FIELD_MISSING");if(out.put(m.targetPropertyIri(),source.get(m.sourceField()))!=null)throw failure("ING_MAPPING_TARGET_DUPLICATE");}return out;}
  private String observedAt(AdapterSpi.SourceRecord record,Config cfg,Instant acquired){if("ACQUISITION_TIME".equals(cfg.observationPolicy()))return acquired.toString();if("PROVENANCE_OBSERVED_AT".equals(cfg.observationPolicy())){Object v=record.provenance().get("observedAt");if(v==null)throw failure("ING_OBSERVED_AT_MISSING");try{return OffsetDateTime.parse(String.valueOf(v)).toInstant().toString();}catch(Exception e){throw failure("ING_OBSERVED_AT_INVALID");}}throw failure("ING_OBSERVATION_POLICY_UNSUPPORTED");}
  private Map<String,Object> envelope(Command c,Config cfg,RecordMetadata metadata,Map<String,Object> data,String observed,Instant acquired,String rawRef){Map<String,Object> p=new LinkedHashMap<>(c.record().provenance());p.put("correlationId",c.correlationId());Map<String,Object> e=linked("envelopeVersion","1.0","ingestionId",idempotency(c,"ingestion"),"runId",c.runId().toString(),"sourceId",c.bundle().sourceId(),"typeCode",cfg.typeCode(),"sourceObjectId",c.record().sourceObjectId(),"sourceSchemaId",cfg.sourceSchemaId(),"sourceSchemaVersion",cfg.sourceSchemaVersion(),"configurationBundleId",c.bundle().bundleId(),"configurationBundleVersion",c.bundle().bundleVersion(),"operation",metadata.operation().name(),"observedAt",observed,"acquiredAt",acquired.toString(),"contentType","application/json","sourceDataRef",rawRef,"provenance",p);if(metadata.operation()==Operation.UPSERT)e.put("data",data);return e;}
  private Map<String,Object> lineage(Command c,Config cfg,RecordMetadata metadata,UUID attempt,UUID lineage,UUID handoff,String rawRef,String rawHash,String normalizedHash,String outputHash,String observed,Instant acquired,Map<String,Object> refs){Map<String,Object> source=linked("sourceId",c.bundle().sourceId(),"typeCode",cfg.typeCode(),"sourceObjectId",c.record().sourceObjectId(),"sourceObservedAt",observed);put(source,"sourceRevision",metadata.sourceRevision());return linked("lineageId",lineage.toString(),"ingestionRunId",c.runId().toString(),"ingestionId",idempotency(c,"ingestion"),"handoffId",handoff.toString(),"source",source,"contracts",refs,"processing",linked("pipelineVersion","1.1.0","transformationIds",cfg.mappings().stream().map(Mapping::transform).distinct().toList(),"processingAttemptId",attempt.toString()),"data",linked("rawObjectRef",rawRef,"rawHash",rawHash,"normalizedHash",normalizedHash,"outputHash",outputHash),"times",linked("acquiredAt",acquired.toString(),"processedAt",acquired.toString()),"trace",linked("correlationId",c.correlationId()),"outcome",linked("status","SUCCESS","issueRefs",List.of()));}
  private Map<String,Object> handoff(Command c,Config cfg,RecordMetadata metadata,UUID lineage,UUID handoff,String rawRef,String outputHash,String observed,Instant acquired,Map<String,Object> refs,Map<String,Object> candidate){Map<String,Object> identity=linked("sourceId",c.bundle().sourceId(),"typeCode",cfg.typeCode(),"sourceObjectId",c.record().sourceObjectId(),"observedAt",observed);put(identity,"sourceRevision",metadata.sourceRevision());put(identity,"validFrom",metadata.validFrom());put(identity,"validTo",metadata.validTo());Map<String,Object> payload=linked("handoffId",handoff.toString(),"ingestionRunId",c.runId().toString(),"ingestionId",idempotency(c,"ingestion"),"sourceIdentity",identity,"operation",metadata.operation().name(),"rawObjectRef",rawRef,"contractRefs",refs,"lineageId",lineage.toString(),"contentHash",outputHash,"occurredAt",observed,"acquiredAt",acquired.toString(),"changeRepresentation",changeRepresentation(cfg.changeProfile(),metadata,metadata.operation()==Operation.UPSERT?outputHash:null),"correlationId",c.correlationId());if(metadata.operation()==Operation.UPSERT)payload.put("canonicalPayload",candidate);if(!cfg.dataAccessLabels().isEmpty())payload.put("dataAccessLabels",cfg.dataAccessLabels());return payload;}
  private Map<String,Object> contractRefs(ExecutionBundle b,Config cfg){Map<String,Object> m=linked("sourceSchemaRef",cfg.sourceSchemaRef(),"bundleRef",bundleRef(b),"semanticPublicationSetRef",cfg.semanticPublicationSetRef(),"adapterProfileRef",cfg.adapterProfileRef());if(!cfg.mappingRefs().isEmpty())m.put("mappingRefs",cfg.mappingRefs());if(!cfg.relationshipResolutionStrategyRefs().isEmpty())m.put("relationshipResolutionStrategyRefs",cfg.relationshipResolutionStrategyRefs());put(m,"authorityPolicyRef",cfg.authorityPolicyRef());return m;}
  private RuntimePorts.DataLakePort.Receipt persist(RuntimePorts.DataLakePort.Zone zone,Command c,byte[] content,String hash,String suffix){var r=lake.persist(zone,c.runId(),c.record().sourceObjectId(),content,hash,idempotency(c,suffix));if(!r.durable())throw failure("ING_DATALAKE_NOT_DURABLE");return r;}
  private byte[] bytes(Object value){try{return json.writeValueAsBytes(value);}catch(JsonProcessingException e){throw failure("ING_JSON_INVALID");}}
  private static String hash(byte[] value){try{return "sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));}catch(Exception e){throw new IllegalStateException("ING_HASH_UNAVAILABLE",e);}}
  private static String idempotency(Command c,String suffix){return c.runId()+":"+c.partitionKey()+":"+c.sequenceNo()+":"+suffix;}
  private static String bundleRef(ExecutionBundle b){return b.bundleId()+":"+b.bundleVersion()+":"+b.checksum();}
  private static String sourceRef(AdapterSpi.SourceRecord r){Object ref=r.provenance().get("managedObjectRef");return ref==null?"source-object:"+r.sourceObjectId():String.valueOf(ref);}
  private static RecordMetadata metadata(AdapterSpi.SourceRecord r){Map<String,Object> p=r.provenance();Operation operation=parseOperation(p.get("operation"));String validFrom=instant(p,"validFrom"),validTo=instant(p,"validTo");if(validFrom!=null&&validTo!=null&&Instant.parse(validTo).isBefore(Instant.parse(validFrom)))throw failure("ING_VALID_TIME_INVALID");return new RecordMetadata(operation,optional(p,"sourceRevision"),validFrom,validTo,optional(p,"baseRevisionRef"),optional(p,"baseContentHash"),scalar(p,"eventSequence"),optional(p,"geometryHash"),optional(p,"relationshipsHash"),optional(p,"contractEvidenceHash"));}
  private static Operation parseOperation(Object value){if(value==null)return Operation.UPSERT;try{return Operation.valueOf(String.valueOf(value));}catch(Exception e){throw failure("ING_OPERATION_UNSUPPORTED");}}
  private static ChangeProfile changeProfile(Object value){if(value==null)return new ChangeProfile(ChangeMode.FULL_SNAPSHOT);Object mode=value instanceof Map<?,?> m?m.get("mode"):value;try{return new ChangeProfile(ChangeMode.valueOf(String.valueOf(mode)));}catch(Exception e){throw failure("ING_CHANGE_REPRESENTATION_UNSUPPORTED");}}
  private static Map<String,Object> changeRepresentation(ChangeProfile profile,RecordMetadata metadata,String outputHash){Map<String,Object> out=linked("mode",profile.mode().name());put(out,"canonicalContentHash",outputHash);if(profile.mode()==ChangeMode.DELTA_PATCH&&metadata.baseRevisionRef()==null&&metadata.baseContentHash()==null)throw failure("ING_DELTA_BASE_REQUIRED");if(profile.mode()==ChangeMode.PROPERTY_EVENTS&&metadata.eventSequence()==null)throw failure("ING_EVENT_SEQUENCE_REQUIRED");put(out,"baseRevisionRef",metadata.baseRevisionRef());put(out,"baseContentHash",metadata.baseContentHash());put(out,"eventSequence",metadata.eventSequence());put(out,"geometryHash",metadata.geometryHash());put(out,"relationshipsHash",metadata.relationshipsHash());put(out,"contractEvidenceHash",metadata.contractEvidenceHash());return out;}
  private static Map<String,Object> tombstoneEvidence(Command c,RecordMetadata metadata){Map<String,Object> value=linked("operation",metadata.operation().name(),"sourceObjectId",c.record().sourceObjectId());put(value,"sourceRevision",metadata.sourceRevision());return value;}
  private static String instant(Map<String,Object> values,String key){String value=optional(values,key);if(value==null)return null;try{return OffsetDateTime.parse(value).toInstant().toString();}catch(Exception e){throw failure("ING_TEMPORAL_METADATA_INVALID");}}
  private static String required(Map<String,Object> c,String key){Object v=c.get(key);if(v==null||String.valueOf(v).isBlank())throw failure("ING_PIPELINE_CONFIG_MISSING");return String.valueOf(v);}
  private static String optional(Map<?,?> values,String key){Object value=values.get(key);return value==null||String.valueOf(value).isBlank()?null:String.valueOf(value);}
  private static Object scalar(Map<String,Object> values,String key){Object value=values.get(key);if(value==null||value instanceof String||value instanceof Number)return value;throw failure("ING_PIPELINE_METADATA_INVALID");}
  private static List<String> strings(Object v){if(v==null)return List.of();if(!(v instanceof Collection<?> x))throw failure("ING_PIPELINE_CONFIG_INVALID");List<String> out=x.stream().map(String::valueOf).toList();if(out.stream().anyMatch(String::isBlank)||new HashSet<>(out).size()!=out.size())throw failure("ING_PIPELINE_CONFIG_INVALID");return out;}
  private static List<Map<String,Object>> labels(Object value){if(value==null)return List.of();if(!(value instanceof Collection<?> items))throw failure("ING_PIPELINE_CONFIG_INVALID");List<Map<String,Object>> out=new ArrayList<>();for(Object item:items){if(!(item instanceof Map<?,?> raw))throw failure("ING_PIPELINE_CONFIG_INVALID");Map<String,Object> label=new LinkedHashMap<>();raw.forEach((k,v)->label.put(String.valueOf(k),v));out.add(Collections.unmodifiableMap(label));}if(new HashSet<>(out).size()!=out.size())throw failure("ING_PIPELINE_CONFIG_INVALID");return List.copyOf(out);}
  private static List<Mapping> mappings(Object v){if(!(v instanceof Collection<?> x)||x.isEmpty())throw failure("ING_PROPERTY_MAPPING_REQUIRED");List<Mapping> out=new ArrayList<>();for(Object item:x){if(!(item instanceof Map<?,?> m))throw failure("ING_PIPELINE_CONFIG_INVALID");out.add(new Mapping(text(m,"sourceField"),text(m,"targetPropertyIri"),text(m,"transform")));}return List.copyOf(out);}
  private static String text(Map<?,?> m,String key){Object v=m.get(key);if(v==null||String.valueOf(v).isBlank())throw failure("ING_PIPELINE_CONFIG_INVALID");return String.valueOf(v);}
  private static IllegalArgumentException failure(String code){return new IllegalArgumentException(code);}
  private static String safeCode(RuntimeException e){if(e instanceof FrozenContractValidator.ContractViolation c)return c.safeCode();String m=e.getMessage();return m!=null&&m.matches("ING_[A-Z0-9_]{1,76}")?m:"ING_PROCESSING_FAILED";}
  private static Map<String,Object> linked(Object... values){Map<String,Object> m=new LinkedHashMap<>();for(int i=0;i<values.length;i+=2)m.put((String)values[i],values[i+1]);return m;}
  private static void put(Map<String,Object> target,String key,Object value){if(value!=null)target.put(key,value);}
  private enum Operation { UPSERT,DELETE,TOMBSTONE }
  private enum ChangeMode { FULL_SNAPSHOT,DELTA_PATCH,PROPERTY_EVENTS }
  private record ChangeProfile(ChangeMode mode){}
  private record RecordMetadata(Operation operation,String sourceRevision,String validFrom,String validTo,String baseRevisionRef,String baseContentHash,Object eventSequence,String geometryHash,String relationshipsHash,String contractEvidenceHash){}
  private record Config(String sourceSchemaRef,String sourceSchemaId,String sourceSchemaVersion,String typeCode,String semanticPublicationSetRef,String adapterProfileRef,String authorityPolicyRef,List<String> mappingRefs,List<String> relationshipResolutionStrategyRefs,List<Map<String,Object>> dataAccessLabels,List<Mapping> mappings,String observationPolicy,ChangeProfile changeProfile){}
  private record Mapping(String sourceField,String targetPropertyIri,String transform){}
  public record Command(UUID runId,String partitionKey,long sequenceNo,int attemptNo,String adapterId,ExecutionBundle bundle,AdapterSpi.SourceRecord record,String correlationId,Map<String,Object> restartCheckpoint,Map<String,Object> candidateWatermark){public Command{Objects.requireNonNull(runId);Objects.requireNonNull(bundle);Objects.requireNonNull(record);restartCheckpoint=Map.copyOf(restartCheckpoint);candidateWatermark=Map.copyOf(candidateWatermark);}}
  public record Result(UUID processingAttemptId,UUID lineageId,UUID handoffId,UUID quarantineId){public boolean succeeded(){return handoffId!=null;}}
}
