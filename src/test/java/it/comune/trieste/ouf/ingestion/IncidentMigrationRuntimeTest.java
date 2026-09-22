package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import it.comune.trieste.ouf.authorization.*;
import it.comune.trieste.ouf.authorization.AuthorizationPolicy.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.DriverManager;
import java.time.*;
import java.util.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mock.web.MockHttpServletRequest;

class IncidentMigrationRuntimeTest {
 @Test void upgradePreservesHistoricalWindowAndResolvedDuration() throws Exception {
  String admin=System.getenv("OUF_ING_DB_URL"), user=System.getenv("OUF_ING_DB_USER"), password=System.getenv("OUF_ING_DB_PASSWORD");
  String database="incident_upgrade_"+UUID.randomUUID().toString().replace("-","");
  String url=admin.replaceFirst("/[^/?]+(\\?.*)?$","/"+database);
  try(var connection=DriverManager.getConnection(admin,user,password);var statement=connection.createStatement()){
   statement.execute("CREATE DATABASE "+database);
   try {
    Flyway.configure().dataSource(url,user,password).schemas("ouf_ingestion").defaultSchema("ouf_ingestion").target("14").load().migrate();
    var sql=JdbcClient.create(new DriverManagerDataSource(url,user,password));
    sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id,tenant_id) values('00000000-0000-4000-8000-000000000001','upgrade-source','b','1','hash','FULL_SNAPSHOT','FAILED','upgrade-correlation','tenant-a')").update();
    sql.sql("insert into ouf_ingestion.runtime_issue(issue_id,run_id,source_id,type_code,severity,issue_code,evidence_ref,status,correlation_id,created_at,resolved_at) values('00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000001','upgrade-source','T','ERROR','ING_SOURCE_UNREACHABLE','evidence','RESOLVED','upgrade-correlation',transaction_timestamp()-interval '2 days',transaction_timestamp()-interval '1 day')").update();
    Flyway.configure().dataSource(url,user,password).schemas("ouf_ingestion").defaultSchema("ouf_ingestion").load().migrate();
    var timeline=new IncidentTimeline(sql,new ObjectMapper().findAndRegisterModules(),new OperationalAwarenessService(sql));
    var now=OffsetDateTime.now(ZoneOffset.UTC);var actor=actor();
    var page=timeline.page(new IncidentTimeline.Query("upgrade-source",null,now.minusDays(3),10,now.minusHours(1),null,null,"ERROR"),actor);
    @SuppressWarnings("unchecked") var items=(List<Map<String,Object>>)page.get("items");
    assertThat(items).hasSize(1);
    assertThat(items.getFirst()).containsEntry("lifecycle_state","RESOLVED");
    assertThat(((Number)items.getFirst().get("duration_ms")).longValue()).isEqualTo(86400000L);
    assertThat(page).containsEntry("partial",false).containsEntry("hasMore",false);
    for(String severity:List.of("INFO","CRITICAL")) {
     var filtered=timeline.page(new IncidentTimeline.Query("upgrade-source",null,now.minusDays(3),10,now.minusHours(1),null,null,severity),actor);
     assertThat((List<?>)filtered.get("items")).isEmpty();
    }
   } finally {statement.execute("DROP DATABASE "+database+" WITH (FORCE)");}
  }
 }
 private TrustedAuthorizationContext.Context actor(){var request=new MockHttpServletRequest();TestAuthorization.bind(request,"reader","HUMAN",Set.of("operations.incident.read","operations.incident.explain"));var engine=(LocalAuthorization)request.getServletContext().getAttribute(ServletAuthorization.RUNTIME);var old=engine.currentSnapshot().bundle();
  var grants=old.grants().stream().map(g->new Grant(g.grantId(),g.capabilityId(),g.tenantId(),g.subjectId(),g.servicePrincipalId(),g.organizationId(),g.validFrom(),g.validUntil(),new GrantConstraints("ALLOW",null,"operational",null,Map.of("module","INGESTION"),Set.of(),Set.of("TENANT_OPERATIONAL"),null,Set.of(),null))).toList();
  try{TestAuthorization.install(engine,new PolicyBundle(old.bundleId(),2,old.publishedAt(),old.capabilities(),grants));}catch(Exception e){throw new IllegalStateException(e);}
  return new TrustedAuthorizationContext().owner(request,"operations.incident.read");}
}
