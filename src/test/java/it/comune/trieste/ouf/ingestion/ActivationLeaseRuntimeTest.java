package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;

@SpringBootTest
class ActivationLeaseRuntimeTest {
 @DynamicPropertySource static void database(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
 @Autowired ActivationRepository activations;@Autowired RunStateRepository state;@Autowired RunExecutionRepository snapshots;@Autowired JdbcClient db;
 @BeforeEach void isolate(){db.sql("update ouf_ingestion.ing_schedule set state='DISABLED'").update();db.sql("update ouf_ingestion.runtime_pressure_policy set global_running_limit=128,soft_outbox_limit=1000,hard_outbox_limit=5000,recovery_outbox_limit=500,state='NORMAL' where policy_key='GLOBAL'").update();}
 @Test void expiredPreflightResumesSameRunAndFencesOldWorker(){String source="lease-"+UUID.randomUUID();var a=PublishedActivation.decode(PublishedActivationTest.envelope(source,true,1),"tenant-a",PublishedActivationTest.JSON);activations.reconcile(a);var first=state.claimDue("old",Duration.ofMinutes(2)).orElseThrow();UUID run=state.createPreflightRun(first,a.execution(),"corr");db.sql("update ouf_ingestion.ing_schedule set lease_until=transaction_timestamp()-interval '1 second' where source_id=:s").param("s",source).update();var recovered=state.claimDue("new",Duration.ofMinutes(2)).orElseThrow();assertThat(state.createPreflightRun(recovered,a.execution(),"corr-retry")).isEqualTo(run);assertThatThrownBy(()->state.completePreflight(first,run)).hasMessage("ING_SCHEDULE_LEASE_LOST");state.completePreflight(recovered,run);activations.reconcile(a);assertThat(db.sql("select state from ouf_ingestion.ing_schedule where source_id=:s").param("s",source).query(String.class).single()).isEqualTo("DISABLED");assertThat(db.sql("select count(*) from ouf_ingestion.ing_run where source_id=:s").param("s",source).query(Integer.class).single()).isEqualTo(1);}
 @Test void publicationChangePreservesRunSnapshotAndRejectsRegression(){String source="version-"+UUID.randomUUID();var a=PublishedActivation.decode(PublishedActivationTest.envelope(source,false,1),"tenant-a",PublishedActivationTest.JSON);activations.reconcile(a);var claim=state.claimDue("worker",Duration.ofMinutes(2)).orElseThrow();UUID run=state.createPreflightRun(claim,a.execution(),"corr");state.completePreflight(claim,run);var b=PublishedActivation.decode(PublishedActivationTest.envelope(source,false,2),"tenant-a",PublishedActivationTest.JSON);activations.reconcile(b);activations.reconcile(a);assertThat(snapshots.bundle(run).checksum()).isEqualTo(a.execution().checksum());assertThat(db.sql("select publication_id from ouf_ingestion.ing_schedule where source_id=:s").param("s",source).query(UUID.class).single()).isEqualTo(b.publicationId());assertThat(state.claimDue("parallel",Duration.ofMinutes(2))).isEmpty();}
 private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
