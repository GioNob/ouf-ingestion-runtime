package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SchemaSurveillanceService {
  private final JdbcClient sql;private final ObjectMapper json;
  public SchemaSurveillanceService(JdbcClient sql,ObjectMapper json){this.sql=sql;this.json=json;}
  @Transactional public Observation observe(UUID run,ExecutionBundle bundle,AdapterSpi.SourceRecord record,String correlation){Set<String> expected=fields(bundle.configuration().get("expectedFieldNames"));Set<String> actual=new TreeSet<>(record.payload().keySet());String expectedVersion=required(bundle.configuration(),"sourceSchemaVersion");Object rawVersion=record.provenance().get("schemaVersion");String observedVersion=rawVersion==null?null:String.valueOf(rawVersion);Outcome outcome;if(observedVersion!=null&&!expectedVersion.equals(observedVersion))outcome=Outcome.UNKNOWN_VERSION;else if(actual.equals(expected))outcome=Outcome.MATCH;else if(actual.containsAll(expected))outcome=Outcome.COMPATIBLE_ADDITIVE;else outcome=Outcome.INCOMPATIBLE;String checksum=hash(actual);String fieldsJson=encode(actual);UUID id=sql.sql("insert into ouf_ingestion.schema_observation(observation_id,run_id,source_id,type_code,schema_checksum,outcome,expected_schema_version,observed_schema_version,field_names) values(:i,:r,:s,:t,:h,:o,:e,:v,cast(:f as jsonb)) on conflict(source_id,type_code,schema_checksum,expected_schema_version,observed_schema_version) do update set observation_count=schema_observation.observation_count+1,last_observed_at=transaction_timestamp() returning observation_id").param("i",UUID.randomUUID()).param("r",run).param("s",bundle.sourceId()).param("t",required(bundle.configuration(),"typeCode")).param("h",checksum).param("o",outcome.name()).param("e",expectedVersion).param("v",observedVersion).param("f",fieldsJson).query(UUID.class).single();if(outcome.blocking())issue(run,bundle,outcome,checksum,correlation);return new Observation(id,outcome,checksum);}
  private void issue(UUID run,ExecutionBundle bundle,Outcome outcome,String checksum,String correlation){String code="ING_SCHEMA_"+outcome.name();sql.sql("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id) values(:i,:r,:s,:t,'ERROR',:c,:e,:x) on conflict(source_id,type_code,issue_code,evidence_ref) where status='OPEN' do nothing").param("i",UUID.randomUUID()).param("r",run).param("s",bundle.sourceId()).param("t",required(bundle.configuration(),"typeCode")).param("c",code).param("e",checksum).param("x",correlation).update();}
  private static Set<String> fields(Object value){if(!(value instanceof Collection<?> c)||c.isEmpty())throw new IllegalArgumentException("ING_EXPECTED_FIELDS_REQUIRED");Set<String> out=new TreeSet<>();for(Object x:c)if(!out.add(String.valueOf(x)))throw new IllegalArgumentException("ING_EXPECTED_FIELDS_INVALID");return out;}
  private static String required(Map<String,Object> c,String key){Object v=c.get(key);if(v==null||String.valueOf(v).isBlank())throw new IllegalArgumentException("ING_SCHEMA_CONFIG_MISSING");return String.valueOf(v);}
  private String encode(Object value){try{return json.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("ING_SCHEMA_EVIDENCE_INVALID",e);}}
  private static String hash(Collection<String> names){try{return "sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(String.join("\n",names).getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException("ING_HASH_UNAVAILABLE",e);}}
  public enum Outcome {MATCH,COMPATIBLE_ADDITIVE,UNKNOWN_VERSION,INCOMPATIBLE;public boolean blocking(){return this==UNKNOWN_VERSION||this==INCOMPATIBLE;}}
  public record Observation(UUID observationId,Outcome outcome,String schemaChecksum){}
}
