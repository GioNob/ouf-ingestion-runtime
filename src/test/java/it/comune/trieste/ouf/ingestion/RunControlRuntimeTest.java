package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;

@SpringBootTest class RunControlRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired RunStateRepository state;@Autowired JdbcClient sql;
  @Test void everyDispatchedRunPinsBundleAndPassesPreflightBeforeRunning(){String source=schedule("tenant-a");AtomicInteger checks=new AtomicInteger();RuntimePorts.ActiveBundlePort bundles=id->bundle(id);RuntimePorts.SemanticPort semantic=refs->{assertThat(refs).containsExactly("schema://1","semantic://set/7");checks.incrementAndGet();};RunCoordinator coordinator=new RunCoordinator(state,bundles,semantic);UUID run=coordinator.dispatchOne("worker-a").orElseThrow();assertThat(checks).hasValue(1);assertThat(state.run(run)).containsEntry("state","RUNNING").containsEntry("phase","FULL_SNAPSHOT").containsEntry("bundle_checksum","sha256:pinned").containsEntry("tenant_id","tenant-a");assertThat(state.claimDue("worker-b",Duration.ofMinutes(1))).isEmpty();}
  @Test void phasesOnlyAdvanceInNormativeOrder(){String source=schedule("tenant-phase");var claim=state.claimDue("phase-worker",Duration.ofMinutes(1)).orElseThrow();UUID run=state.createPreflightRun(claim,bundle(source),"corr");state.preflightSucceeded(run);assertThatThrownBy(()->state.advancePhase(run,"DELTA")).hasMessage("ING_PHASE_TRANSITION_INVALID");state.advancePhase(run,"CATCH_UP");state.advancePhase(run,"DELTA");assertThat(state.run(run)).containsEntry("phase","DELTA");state.releaseSchedule(claim,true);}
  @Test void repeatedPreflightFailuresOpenCircuitAndHealthRecoversAfterSuccess(){String source="health-"+UUID.randomUUID();for(int i=0;i<5;i++)state.recordHealth(source,false,"ING_PREFLIGHT_FAILED");Map<String,Object> paused=sql.sql("select status,consecutive_failures,last_error_code,circuit_open_until from ouf_ingestion.source_health where source_id=:s").param("s",source).query().singleRow();assertThat(paused).containsEntry("status","PAUSED").containsEntry("consecutive_failures",5).containsEntry("last_error_code","ING_PREFLIGHT_FAILED");state.recordHealth(source,true,null);assertThat(sql.sql("select status from ouf_ingestion.source_health where source_id=:s").param("s",source).query(String.class).single()).isEqualTo("HEALTHY");}
  private String schedule(String tenant){String source="source-"+UUID.randomUUID();sql.sql("insert into ouf_ingestion.ing_schedule(schedule_id,tenant_id,source_id,interval_seconds,next_run_at) values(:i,:t,:s,60,transaction_timestamp()-interval '1 minute')").param("i",UUID.randomUUID()).param("t",tenant).param("s",source).update();return source;}
  private static ExecutionBundle bundle(String source){return new ExecutionBundle("bundle-1","7","sha256:pinned",source,"REST_JSON","gateway://binding/1",Map.of("pinnedReferences",List.of("schema://1","semantic://set/7")));}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
