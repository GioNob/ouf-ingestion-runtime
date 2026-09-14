package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;

@SpringBootTest class CanonicalRecordPipelineRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired ObjectMapper json;@Autowired FrozenContractValidator validator;@Autowired DurablePipelineRepository durable;@Autowired QuarantineService quarantine;@Autowired JdbcClient sql;
  private final List<UUID> createdRuns=new ArrayList<>();

  @AfterEach void removeClaimableOutboxFromSharedDatabase(){for(UUID run:createdRuns)sql.sql("update ouf_ingestion.handoff_outbox set state='ACKED',acked_at=transaction_timestamp() where run_id=:r and state<>'ACKED'").param("r",run).update();createdRuns.clear();}

  @Test void persistsThreeZonesAndStagesContractAlignedHandoff(){
    UUID run=run();RecordingLake lake=new RecordingLake();CanonicalRecordPipeline pipeline=pipeline(lake);
    var result=pipeline.process(command(run,Map.of("name","Town Hall"),List.of(mapping("name","https://example.test/name"))));
    assertThat(result.succeeded()).isTrue();assertThat(lake.zones).containsExactly(RuntimePorts.DataLakePort.Zone.RAW,RuntimePorts.DataLakePort.Zone.NORMALIZED,RuntimePorts.DataLakePort.Zone.CURATED);
    Map<String,Object> row=sql.sql("select payload_json::text as payload,evidence_json::text as evidence from ouf_ingestion.handoff_outbox h join ouf_ingestion.ing_lineage l using(lineage_id) where h.handoff_id=:h").param("h",result.handoffId()).query().singleRow();
    Map<String,Object> handoff=json.readValue(string(row,"payload"),Map.class);Map<String,Object> canonical=(Map<String,Object>)handoff.get("canonicalPayload");Map<String,Object> refs=(Map<String,Object>)handoff.get("contractRefs");
    assertThat(canonical).containsExactly(entry("https://example.test/name","Town Hall")).doesNotContainKeys("properties","typeCode","sourceObjectId");
    assertThat(refs.get("relationshipResolutionStrategyRefs")).isEqualTo(List.of("relationship-strategy:place-address:1"));
    assertThat(string(row,"payload")).contains(result.lineageId().toString());
    assertThat(string(row,"evidence")).contains(result.processingAttemptId().toString(),"sha256:");
    assertThat(sql.sql("select count(*) from ouf_ingestion.ing_quarantine where run_id=:r").param("r",run).query(Long.class).single()).isZero();
  }

  @Test void mappingFailureCreatesFailedAttemptAndQuarantineWithoutCheckpointOrOutbox(){
    UUID run=run();CanonicalRecordPipeline pipeline=pipeline(new RecordingLake());
    var result=pipeline.process(command(run,Map.of("name","Town Hall"),List.of(mapping("missing","https://example.test/name"))));
    assertThat(result.succeeded()).isFalse();assertThat(result.quarantineId()).isNotNull();
    assertThat(sql.sql("select state||':'||reason_code from ouf_ingestion.processing_attempt where attempt_id=:a").param("a",result.processingAttemptId()).query(String.class).single()).isEqualTo("FAILED:ING_MAPPING_SOURCE_FIELD_MISSING");
    assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(Long.class).single()).isZero();
    assertThat(sql.sql("select checkpoint_json from ouf_ingestion.ing_partition where run_id=:r and partition_key='default'").param("r",run).query(String.class).single()).isEqualTo("{}");
  }

  private CanonicalRecordPipeline pipeline(RuntimePorts.DataLakePort lake){return new CanonicalRecordPipeline(json,validator,durable,quarantine,lake,Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"),ZoneOffset.UTC));}
  private CanonicalRecordPipeline.Command command(UUID run,Map<String,Object> payload,List<Map<String,Object>> mappings){Map<String,Object> cfg=new LinkedHashMap<>(Map.of("sourceSchemaRef","schema:places:1","sourceSchemaId","places","sourceSchemaVersion","1","typeCode","place","semanticPublicationSetRef","sem:1","adapterProfileRef","adapter:managed:1","observationPolicy","ACQUISITION_TIME","propertyMappings",mappings,"mappingRefs",List.of("mapping:places:1"),"relationshipResolutionStrategyRefs",List.of("relationship-strategy:place-address:1")));ExecutionBundle b=new ExecutionBundle("bundle","1","sha256:bundle","source-1","INTERNAL_MANAGED_CSV","managed:object",cfg);AdapterSpi.SourceRecord record=new AdapterSpi.SourceRecord("object-1",1,payload,Map.of("managedObjectRef","managed:object"));return new CanonicalRecordPipeline.Command(run,"default",1,1,"managed-tabular-v1",b,record,"corr-1",Map.of("ordinal",1),Map.of("ordinal",1));}
  private UUID run(){UUID r=UUID.randomUUID();createdRuns.add(r);sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id) values(:r,'source-1','bundle','1','sha256:bundle','MANAGED_ONCE','RUNNING','corr-1')").param("r",r).update();sql.sql("insert into ouf_ingestion.ing_partition(run_id,partition_key,state) values(:r,'default','RUNNING')").param("r",r).update();return r;}
  private static Map<String,Object> mapping(String source,String target){return Map.of("sourceField",source,"targetPropertyIri",target,"transform","IDENTITY");}
  private static String string(Map<String,Object> row,String key){return String.valueOf(row.get(key));}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
  private static final class RecordingLake implements RuntimePorts.DataLakePort {final List<Zone> zones=new ArrayList<>();public Receipt persist(Zone zone,UUID run,String object,byte[] content,String hash,String key){zones.add(zone);return new Receipt("lake:"+zone.name().toLowerCase()+":"+key,true);}}
}
