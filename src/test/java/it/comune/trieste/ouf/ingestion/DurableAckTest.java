package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;

@SpringBootTest class DurableAckTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired DurablePipelineRepository repository; @Autowired JdbcClient sql;
  private final Map<UUID,String> sources=new HashMap<>();
  @AfterEach void closeOutboxFixtures(){for(UUID run:sources.keySet())sql.sql("update ouf_ingestion.handoff_outbox set state='ACKED',acked_at=transaction_timestamp(),lease_owner=null,lease_until=null where run_id=:r and state<>'ACKED'").param("r",run).update();sources.clear();}

  @Test void restartCheckpointAdvancesBeforeAckButWatermarkWaitsForCompletePrefix(){
    UUID run=run();UUID first=stage(run,1),second=stage(run,2);
    assertThat(checkpoint(run)).isEqualTo("2");assertThat(watermarks(run)).isZero();
    var c1=repository.claim("w1",Duration.ofMinutes(1)).orElseThrow();assertThat(c1.handoffId()).isEqualTo(first);assertThat(repository.claim("w2",Duration.ofMinutes(1))).isEmpty();
    repository.acknowledge(first,"w1","receipt-1");var c2=repository.claim("w2",Duration.ofMinutes(1)).orElseThrow();assertThat(c2.handoffId()).isEqualTo(second);repository.acknowledge(second,"w2","receipt-2");
    assertThat(sql.sql("select watermark_json->>'ordinal' from ouf_ingestion.ing_watermark where source_id=:s").param("s",sources.get(run)).query(String.class).single()).isEqualTo("2");
    repository.acknowledge(first,"ignored-worker","duplicate-receipt");assertThat(watermarks(run)).isOne();
  }

  @Test void retryReleasesLeaseAndKeepsSafeErrorOnly(){
    UUID h=stage(run(),1);var claim=repository.claim("worker",Duration.ofMinutes(1)).orElseThrow();assertThat(claim.handoffId()).isEqualTo(h);
    repository.retry(h,"worker","ING_DOWNSTREAM_UNAVAILABLE",Duration.ZERO,false);
    assertThat(sql.sql("select state||':'||last_error_code from ouf_ingestion.handoff_outbox where handoff_id=:h").param("h",h).query(String.class).single()).isEqualTo("FAILED_RETRYABLE:ING_DOWNSTREAM_UNAVAILABLE");
    assertThat(repository.claim("other",Duration.ofMinutes(1))).isPresent();
  }

  @Test void equivalentStageIsIdempotentAndConflictingPayloadFailsClosed(){UUID run=run();String key="stable-"+run;var command=command(run,1,key,Map.of("row",1));UUID first=repository.stage(command);assertThat(repository.stage(command)).isEqualTo(first);assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where idempotency_key=:i").param("i",key).query(Long.class).single()).isOne();assertThatThrownBy(()->repository.stage(command(run,1,key,Map.of("row",2)))).hasMessage("ING_IDEMPOTENCY_CONFLICT");}

  @Test void expiredDeliveringLeaseIsReclaimedAfterCrash(){UUID handoff=stage(run(),1);var lost=repository.claim("crashed",Duration.ofMinutes(1)).orElseThrow();assertThat(lost.handoffId()).isEqualTo(handoff);sql.sql("update ouf_ingestion.handoff_outbox set lease_until=transaction_timestamp()-interval '1 second' where handoff_id=:h").param("h",handoff).update();assertThat(repository.claim("recovery",Duration.ofMinutes(1)).orElseThrow().handoffId()).isEqualTo(handoff);}

  @Test void expiredOrSupersededExecutionCannotStageOrAdvanceCheckpoint(){
    UUID run=run();
    sql.sql("update ouf_ingestion.ing_partition set lease_owner='old',lease_generation=1,lease_until=transaction_timestamp()-interval '1 second' where run_id=:r").param("r",run).update();
    var old=new RunExecutionRepository.Claim(run,"default",Map.of(),1,"old");
    assertThatThrownBy(()->repository.stage(fenced(run,old))).hasMessage("ING_PARTITION_LEASE_LOST");
    assertThat(checkpoint(run)).isNull();
    sql.sql("update ouf_ingestion.ing_partition set lease_owner='new',lease_generation=2,lease_until=transaction_timestamp()+interval '1 minute' where run_id=:r").param("r",run).update();
    assertThatThrownBy(()->repository.stage(fenced(run,old))).hasMessage("ING_PARTITION_LEASE_LOST");
    assertThat(sql.sql("select count(*) from ouf_ingestion.processing_attempt where run_id=:r").param("r",run).query(Long.class).single()).isZero();
    repository.stage(fenced(run,new RunExecutionRepository.Claim(run,"default",Map.of(),2,"new")));
    assertThat(checkpoint(run)).isEqualTo("1");assertThat(watermarks(run)).isZero();
  }
  private DurablePipelineRepository.StageCommand fenced(UUID run,RunExecutionRepository.Claim lease){
    var c=command(run,1,"fenced-"+run,Map.of("row",1));
    return new DurablePipelineRepository.StageCommand(c.runId(),c.partitionKey(),c.sequenceNo(),c.sourceObjectId(),c.attemptNo(),c.adapterId(),c.bundleVersion(),c.bundleRef(),c.idempotencyKey(),c.payload(),c.evidence(),c.restartCheckpoint(),c.candidateWatermark(),lease);
  }

  private UUID run(){UUID r=UUID.randomUUID();String source="source-"+r;sources.put(r,source);sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id) values(:r,:s,'b','1','sha256:x','MANAGED_ONCE','RUNNING','corr')").param("r",r).param("s",source).update();sql.sql("insert into ouf_ingestion.ing_partition(run_id,partition_key,state) values(:r,'default','RUNNING')").param("r",r).update();return r;}
  private UUID stage(UUID r,long n){return repository.stage(command(r,n,"idem-"+r+'-'+n,Map.of("row",n)));}
  private DurablePipelineRepository.StageCommand command(UUID r,long n,String key,Map<String,Object> payload){return new DurablePipelineRepository.StageCommand(r,"default",n,"object-"+n,1,"managed-tabular-v1","1","bundle:1",key,payload,Map.of("safe","evidence"),Map.of("ordinal",n),Map.of("ordinal",n));}
  private String checkpoint(UUID r){return sql.sql("select checkpoint_json->>'ordinal' from ouf_ingestion.ing_partition where run_id=:r").param("r",r).query(String.class).single();}
  private long watermarks(UUID r){return sql.sql("select count(*) from ouf_ingestion.ing_watermark where source_id=:s").param("s",sources.get(r)).query(Long.class).single();}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
