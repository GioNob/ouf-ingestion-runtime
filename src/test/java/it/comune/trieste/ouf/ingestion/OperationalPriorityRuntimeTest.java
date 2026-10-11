package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;

@SpringBootTest
class OperationalPriorityRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r) {
    r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));
    r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));
    r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));
  }
  @Autowired JdbcClient sql;
  @Autowired OperationalAwarenessService service;

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
