package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@SpringBootTest class RuntimeIssueGovernanceTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired RunStateRepository runs;@Autowired DurablePipelineRepository durable;@Autowired QuarantineService quarantine;@Autowired RuntimeIssueService issues;@Autowired OnboardingReviewRepository reviewQueue;@Autowired HistoricalContractService history;@Autowired JdbcClient sql;
  @Test void quarantineHasTenantScopedIssueDurableReviewHumanClosureAndHistoricalRefs(){
    ExecutionBundle bundle=bundle();UUID run=runs.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-a",bundle.sourceId(),300,"scheduler"),bundle,"corr-issue");runs.preflightSucceeded(run);UUID attempt=durable.recordFailure(run,"object-1",1,"test-adapter","1","ING_DATA_INVALID");UUID q=quarantine.quarantine(run,attempt,"object-1","ING_DATA_INVALID","lake:raw:1","evidence:1","corr-issue");UUID issue=sql.sql("select issue_id from ouf_ingestion.runtime_issue where quarantine_id=:q").param("q",q).query(UUID.class).single();
    var actor=actor("tenant-a");assertThat(issues.search("OPEN",null,null,run,50,actor)).extracting(x->x.get("issue_id")).contains(issue);assertThat(issues.search("OPEN",null,null,run,50,actor("tenant-b"))).isEmpty();
    UUID request=issues.requestOnboardingReview(issue,0,actor,"corr-review");AtomicBoolean remoteInsideTx=new AtomicBoolean(true);RuntimePorts.OnboardingReviewPort port=(i,s,e,c,k)->{remoteInsideTx.set(TransactionSynchronizationManager.isActualTransactionActive());return new RuntimePorts.OnboardingReviewPort.Receipt("onboarding-review:42",true);};new OnboardingReviewDispatcher(reviewQueue,port).dispatchOne("review-worker");assertThat(remoteInsideTx).isFalse();assertThat(sql.sql("select state||':'||onboarding_review_ref from ouf_ingestion.onboarding_review_outbox where request_id=:r").param("r",request).query(String.class).single()).isEqualTo("ACKED:onboarding-review:42");
    issues.close(issue,"RESOLVED","new ACTIVE bundle published",1,actor,"corr-close");assertThat(issues.get(issue,actor)).containsEntry("status","RESOLVED").containsEntry("resolution_reason","new ACTIVE bundle published");assertThat(sql.sql("select count(*) from ouf_ingestion.audit_event where resource_id=:i and tenant_id='tenant-a'").param("i",issue.toString()).query(Long.class).single()).isEqualTo(2);
    assertThat(history.runContracts(run,actor)).containsEntry("bundle_version","1").containsEntry("source_schema_ref","schema:places:1").containsEntry("semantic_publication_set_ref","sem:1");
    assertThatThrownBy(()->sql.sql("delete from ouf_ingestion.runtime_configuration_snapshot where run_id=:r").param("r",run).update()).hasMessageContaining("ingestion history is append-only");
  }
  private static ExecutionBundle bundle(){Map<String,Object> c=Map.of("sourceSchemaRef","schema:places:1","sourceSchemaVersion","1","semanticPublicationSetRef","sem:1","adapterProfileRef","adapter:test:1","mappingRefs",List.of("mapping:1"));return new ExecutionBundle("bundle","1","sha256:bundle","source-"+UUID.randomUUID(),"INTERNAL_MANAGED_CSV","managed:1",c);}
  private static TrustedAuthorizationContext.Context actor(String tenant){return new TrustedAuthorizationContext.Context("human:operator","HUMAN_USER",tenant,Set.of(),"decision:42");}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
