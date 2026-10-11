package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class OperationalPriorityRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r) {
    r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));
    r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));
    r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));
  }
  @Autowired JdbcClient sql;
  @Autowired OperationalAwarenessService service;
  @Autowired PlatformTransactionManager transactions;

  @Test void oldestActiveErrorSurvivesLimitDespiteRecentWarningsAndResolvedErrors() {
    String tenant="priority-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID run=run(tenant,source);
    UUID important=issue(run,source,"ERROR",OffsetDateTime.now().minusDays(2));
    for(int i=0;i<12;i++){
      issue(run,source,"WARNING",OffsetDateTime.now());
      UUID resolved=issue(run,source,"ERROR",OffsetDateTime.now());
      resolve(resolved,OffsetDateTime.now());
    }
    var rows=service.incidents(null,source,null,1,actor(tenant));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("incident_id",important).containsEntry("lifecycle_state","OPEN").containsEntry("severity","ERROR");
  }

  @Test void catchupIncludesResolutionOfOldIssueAndCannotCrossTenant() {
    String tenant="catchup-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID run=run(tenant,source);
    UUID changed=issue(run,source,"ERROR",OffsetDateTime.now().minusDays(3));
    UUID unchanged=issue(run,source,"ERROR",OffsetDateTime.now().minusDays(3));
    UUID hidden=issue(run("other-"+tenant,source),source,"ERROR",OffsetDateTime.now());
    resolve(changed,OffsetDateTime.now());
    var rows=service.incidents(null,source,OffsetDateTime.now().minusHours(1),100,actor(tenant));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("incident_id",changed).containsEntry("lifecycle_state","RESOLVED");
    assertThat(rows).noneMatch(x->unchanged.equals(x.get("incident_id"))||hidden.equals(x.get("incident_id")));
  }

  @Test void resolvedStateFilterAndInvalidStateRemainEnforced() {
    String tenant="filter-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID run=run(tenant,source),open=issue(run,source,"ERROR",OffsetDateTime.now());
    UUID resolved=issue(run,source,"WARNING",OffsetDateTime.now());
    resolve(resolved,OffsetDateTime.now());
    assertThat(service.incidents("RESOLVED",source,null,10,actor(tenant))).hasSize(1).allMatch(x->resolved.equals(x.get("incident_id")));
    assertThatThrownBy(()->service.incidents("INVALID",source,null,10,actor(tenant))).hasMessage("ING_OPERATIONAL_STATE_INVALID");
  }

  @Test void metadataUpdateOfOldOpenIncidentAppearsInCatchupAndStaysTenantScoped() {
    String tenant="metadata-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID run=run(tenant,source);
    UUID changed=issue(run,source,"WARNING",OffsetDateTime.now().minusDays(3));
    UUID unchanged=issue(run,source,"ERROR",OffsetDateTime.now().minusDays(3));
    UUID hidden=issue(run("other-"+tenant,source),source,"WARNING",OffsetDateTime.now().minusDays(3));
    for(UUID id:List.of(changed,hidden)){
      sql.sql("update ouf_ingestion.runtime_issue set severity='ERROR',resolution_reason='owner investigation' where issue_id=:id").param("id",id).update();
    }
    var rows=service.incidents(null,source,OffsetDateTime.now().minusHours(1),100,actor(tenant));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("incident_id",changed).containsEntry("lifecycle_state","OPEN").containsEntry("severity","ERROR");
    assertThat(rows).noneMatch(x->unchanged.equals(x.get("incident_id"))||hidden.equals(x.get("incident_id")));
  }

  @Test void noOpUpdateDoesNotManufactureCatchupActivity() {
    String tenant="no-op-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID id=issue(run(tenant,source),source,"ERROR",OffsetDateTime.now().minusDays(2));
    var before=observation(id);
    sql.sql("update ouf_ingestion.runtime_issue set severity=severity where issue_id=:id").param("id",id).update();
    assertThat(observation(id)).isEqualTo(before);
    assertThat(service.incidents(null,source,OffsetDateTime.now().minusHours(1),100,actor(tenant))).isEmpty();
  }

  @Test void observationCannotBeBackdatedOrRaisedByDirectCaller() {
    String tenant="tamper-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID id=issue(run(tenant,source),source,"ERROR",OffsetDateTime.now().minusDays(2));
    var before=observation(id);
    for(var at:List.of(before.minusDays(1),before.plusDays(1))){
      assertThatThrownBy(()->sql.sql("update ouf_ingestion.runtime_issue set last_updated_at=:at where issue_id=:id").param("at",at).param("id",id).update())
        .hasStackTraceContaining("ING_INCIDENT_OBSERVATION_OWNER_MAINTAINED");
      assertThat(observation(id)).isEqualTo(before);
    }
  }

  @Test void rolledBackIncidentChangeDoesNotPublishCatchupObservation() {
    String tenant="rollback-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID id=issue(run(tenant,source),source,"WARNING",OffsetDateTime.now().minusDays(2));
    var before=observation(id);
    new TransactionTemplate(transactions).executeWithoutResult(tx->{
      sql.sql("update ouf_ingestion.runtime_issue set severity='ERROR' where issue_id=:id").param("id",id).update();
      assertThat(observation(id)).isAfter(before);
      tx.setRollbackOnly();
    });
    assertThat(observation(id)).isEqualTo(before);
    assertThat(service.incidents(null,source,OffsetDateTime.now().minusHours(1),100,actor(tenant))).isEmpty();
  }

  @Test void resolutionMetadataUpdateAndReopeningKeepMonotoneObservation() {
    String tenant="reopen-"+UUID.randomUUID(),source="source-"+UUID.randomUUID();
    UUID id=issue(run(tenant,source),source,"WARNING",OffsetDateTime.now().minusDays(2));
    resolve(id,OffsetDateTime.now().minusDays(1));
    var resolved=observation(id);
    sql.sql("update ouf_ingestion.runtime_issue set status='OPEN',resolved_at=null where issue_id=:id").param("id",id).update();
    var reopened=observation(id);
    assertThat(reopened).isAfterOrEqualTo(resolved);
    var rows=service.incidents("OPEN",source,OffsetDateTime.now().minusHours(1),100,actor(tenant));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("incident_id",id).containsEntry("lifecycle_state","OPEN");
  }

  private OffsetDateTime observation(UUID id) {
    return sql.sql("select last_updated_at from ouf_ingestion.runtime_issue where issue_id=:id").param("id",id).query(OffsetDateTime.class).single();
  }

  private UUID run(String tenant,String source) {
    UUID id=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.ing_run(run_id,tenant_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id) values(:id,:tenant,:source,'bundle-priority','1','sha256:priority','DELTA','RUNNING','corr-priority')")
      .param("id",id).param("tenant",tenant).param("source",source).update();
    return id;
  }
  private UUID issue(UUID run,String source,String severity,OffsetDateTime created) {
    UUID id=UUID.randomUUID();
    sql.sql("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,correlation_id,created_at) values(:id,:run,:source,'TEST',:severity,'ING_TEST_PRIORITY',:evidence,'corr-priority',:created)")
      .param("id",id).param("run",run).param("source",source).param("severity",severity).param("evidence","evidence://"+id).param("created",created).update();
    return id;
  }
  private void resolve(UUID id,OffsetDateTime at) {
    sql.sql("update ouf_ingestion.runtime_issue set status='RESOLVED',resolved_at=:at where issue_id=:id").param("at",at).param("id",id).update();
  }
  private static TrustedAuthorizationContext.Context actor(String tenant) {
    return new TrustedAuthorizationContext.Context("operator","HUMAN",tenant,Set.of("operations.incident.read"),"decision://priority");
  }
  private static String required(String name) {
    String value=System.getenv(name);if(value==null)throw new IllegalStateException(name+" required");return value;
  }
}
