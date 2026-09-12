package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.*;

@SpringBootTest class QuarantineRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired QuarantineService quarantine;@Autowired TrustedAuthorizationContext authorization;@Autowired JdbcClient sql;
  @Test void payloadReferenceIsNotExposedAndReplayIsAuditedAgainstTrustedContext(){UUID run=run(),attempt=attempt(run);UUID q=quarantine.quarantine(run,attempt,"native-person-key","ING_MAPPING_NOT_APPLICABLE","object://secret-payload","evidence://safe","corr-q");Map<String,Object> view=quarantine.get(q);assertThat(view).doesNotContainKey("payload_ref").containsEntry("reason_code","ING_MAPPING_NOT_APPLICABLE").containsEntry("lifecycle_state","OPEN");var actor=authorization.require(request("HUMAN_USER",Set.of("ingestion.quarantine.replay")),"ingestion.quarantine.replay",true);Map<String,Object> preview=quarantine.preview(q,actor);assertThat(preview).doesNotContainKeys("payload_ref","source_object_id").containsEntry("reasonCode","ING_MAPPING_NOT_APPLICABLE");assertThat(String.valueOf(preview.get("sourceObjectFingerprint"))).hasSize(64);UUID replay=quarantine.requestReplay(q,"bundle://source/7",actor,"corr-r");assertThat(replay).isNotNull();assertThat(quarantine.get(q)).containsEntry("state","REPLAY_REQUESTED").containsEntry("lifecycle_state","REPROCESSING");assertThat(sql.sql("select tenant_id||':'||authorization_decision_ref from ouf_ingestion.audit_event where resource_id=:i").param("i",q.toString()).query(String.class).single()).isEqualTo("tenant-a:decision://42");}
  @Test void humanCanDismissOpenItemWithOptimisticLifecycleLock(){UUID run=run(),attempt=attempt(run),q=quarantine.quarantine(run,attempt,"object","ING_DATA_INVALID","object://secret","evidence://safe","corr");var actor=new TrustedAuthorizationContext.Context("human:operator","HUMAN_USER","tenant-a",Set.of(),"decision://42");quarantine.close(q,"DISMISSED","not actionable",null,0,actor,"corr-close");assertThat(quarantine.get(q)).containsEntry("lifecycle_state","DISMISSED").containsEntry("lifecycle_version",1L).containsEntry("resolution_reason","not actionable");assertThatThrownBy(()->quarantine.close(q,"DISMISSED","again",null,0,actor,"corr-close")).hasMessageContaining("CONFLICT");}
  @Test void actorHeadersCannotSubstituteServerEstablishedPrincipal(){MockHttpServletRequest r=new MockHttpServletRequest();r.addHeader("X-Actor-Subject","forged");r.addHeader("X-Capabilities","ingestion.quarantine.replay");assertThatThrownBy(()->authorization.resolve(r)).hasMessageContaining("403");}
  private UUID run(){UUID id=UUID.randomUUID();sql.sql("insert into ouf_ingestion.ing_run(run_id,source_id,bundle_id,bundle_version,bundle_checksum,mode,state,correlation_id,tenant_id) values(:i,:s,'b','1','sha256:x','REPLAY','RUNNING','corr','tenant-a')").param("i",id).param("s","source-"+id).update();return id;}
  private UUID attempt(UUID run){UUID id=UUID.randomUUID();sql.sql("insert into ouf_ingestion.processing_attempt(attempt_id,run_id,source_object_id,attempt_no,adapter_id,bundle_version,state) values(:i,:r,'native-person-key',1,'test','1','FAILED')").param("i",id).param("r",run).update();return id;}
  private MockHttpServletRequest request(String type,Set<String> capabilities){MockHttpServletRequest r=new MockHttpServletRequest();r.setUserPrincipal(()->"human:operator");r.setAttribute(TrustedAuthorizationContext.ACTOR_TYPE,type);r.setAttribute(TrustedAuthorizationContext.TENANT,"tenant-a");r.setAttribute(TrustedAuthorizationContext.DECISION,"decision://42");r.setAttribute(TrustedAuthorizationContext.CAPABILITIES,capabilities);return r;}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
