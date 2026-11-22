package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.*;

@SpringBootTest class ProtectedOperationalLogRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired ProtectedOperationalLogService logs;@Autowired JdbcClient sql;
  @Test void stateTriggersCreateSafeSearchableCorrelatableEventsAndAllowedAudit(){UUID run=UUID.randomUUID();sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id,phase) values(:r,'source-log','bundle','7','sha256:safe','FULL_SNAPSHOT','PREFLIGHT','corr-log','FULL_SNAPSHOT')").param("r",run).update();sql.sql("update ouf_ingestion.ing_run set state='FAILED',failure_code='ING_PREFLIGHT_FAILED' where run_id=:r").param("r",run).update();OffsetDateTime now=OffsetDateTime.now(ZoneOffset.UTC);var query=new ProtectedOperationalLogService.Search(now.minusMinutes(5),now.plusMinutes(1),null,"corr-log",run,null,100,"incident investigation");var actor=new TrustedAuthorizationContext.Context("human:ops","HUMAN_USER","tenant-a",Set.of("ingestion.log.read"),"decision://log");Map<String,Object> result=logs.search(query,actor,"request-corr");@SuppressWarnings("unchecked") List<Map<String,Object>> items=(List<Map<String,Object>>)result.get("items");assertThat(items).extracting(x->x.get("event_type")).contains("RUN_PREFLIGHT","RUN_FAILED");assertThat(items).allSatisfy(x->assertThat(x).doesNotContainKeys("actor_subject","payload_json","downstream_receipt_ref"));assertThat(sql.sql("select outcome||':'||tenant_id from ouf_ingestion.protected_log_access_audit where request_correlation_id='request-corr'").query(String.class).single()).isEqualTo("ALLOWED:tenant-a");assertThat(logs.aggregate(query,"event_type",actor,"aggregate-corr")).isNotEmpty();}
  @Test void deniedAccessAndInvalidBroadWindowAreAuditedOrRejected(){MockHttpServletRequest request=new MockHttpServletRequest();request.setUserPrincipal(()->"human:no-grant");request.setAttribute(TrustedAuthorizationContext.ACTOR_TYPE,"HUMAN_USER");request.setAttribute(TrustedAuthorizationContext.TENANT,"tenant-x");request.setAttribute(TrustedAuthorizationContext.DECISION,"decision://deny");logs.denied(request,"SEARCH","security review","deny-corr");assertThat(sql.sql("select outcome||':'||actor_subject from ouf_ingestion.protected_log_access_audit where request_correlation_id='deny-corr'").query(String.class).single()).isEqualTo("DENIED:human:no-grant");OffsetDateTime now=OffsetDateTime.now(ZoneOffset.UTC);var tooWide=new ProtectedOperationalLogService.Search(now.minusDays(2),now,null,null,null,null,10,"test");var actor=new TrustedAuthorizationContext.Context("h","HUMAN_USER","t",Set.of(),"d");assertThatThrownBy(()->logs.search(tooWide,actor,"x")).hasMessage("ING_LOG_WINDOW_INVALID");}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
