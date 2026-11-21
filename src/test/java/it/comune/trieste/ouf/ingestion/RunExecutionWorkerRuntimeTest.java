package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootTest(properties="ouf.ingestion.execution.initial-delay-ms=3600000") class RunExecutionWorkerRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired PlatformTransactionManager transactions;@Autowired RunStateRepository state;@Autowired RunExecutionRepository execution;@Autowired DurablePipelineRepository durable;@Autowired QuarantineService quarantine;@Autowired FrozenContractValidator validator;@Autowired SchemaSurveillanceService schemas;@Autowired PullSchemaGate gate;@Autowired ObjectMapper json;@Autowired JdbcClient sql;

  @BeforeEach void cleanRuns(){sql.sql("truncate table ouf_ingestion.ing_run cascade").update();}

  @Test void pinnedSnapshotRunsThroughAdapterPipelineDrainingAndAckCompletion(){
    String source="source-"+UUID.randomUUID();ExecutionBundle bundle=bundle(source);var claim=new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-1",source,300,"scheduler");UUID run=state.createPreflightRun(claim,bundle,"corr-worker");state.preflightSucceeded(run);sql.sql("update ouf_ingestion.ing_run set created_at='2000-01-01T00:00:00Z' where run_id=:r").param("r",run).update();
    RecordingLake lake=new RecordingLake();CanonicalRecordPipeline pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,lake,Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"),ZoneOffset.UTC));AdapterSpi adapter=new OneRecordAdapter();RunExecutionWorker worker=new RunExecutionWorker(execution,b->adapter,pipeline,schemas,state,gate);
    assertThat(worker.executeOne("worker-1")).contains(run);assertThat(state.run(run).get("state")).isEqualTo("DRAINING");assertThat(lake.zones).containsExactly(RuntimePorts.DataLakePort.Zone.RAW,RuntimePorts.DataLakePort.Zone.NORMALIZED,RuntimePorts.DataLakePort.Zone.CURATED);
    var handoff=durable.claim("dispatcher",Duration.ofMinutes(1)).orElseThrow();durable.acknowledge(handoff.handoffId(),"dispatcher","udp:receipt");worker.executeOne("worker-1");
    assertThat(state.run(run).get("state")).isEqualTo("SUCCEEDED");assertThat(sql.sql("select state from ouf_ingestion.ing_partition where run_id=:r").param("r",run).query(String.class).single()).isEqualTo("SUCCEEDED");assertThat(execution.bundle(run)).isEqualTo(bundle);
  }
  @Test void incompatibleShapeIsObservedIssuedLoggedQuarantinedAndPausesOnlyItsRun(){
    String source="source-"+UUID.randomUUID();ExecutionBundle bundle=bundle(source);UUID run=state.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-1",source,300,"scheduler"),bundle,"corr-drift");state.preflightSucceeded(run);sql.sql("update ouf_ingestion.ing_run set created_at='1999-01-01T00:00:00Z' where run_id=:r").param("r",run).update();
    RecordingLake lake=new RecordingLake();CanonicalRecordPipeline pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,lake,Clock.systemUTC());AdapterSpi adapter=new OneRecordAdapter(Map.of("unexpected","value"),Map.of("managedObjectRef","managed:object"));new RunExecutionWorker(execution,b->adapter,pipeline,schemas,state,gate).executeOne("worker-drift");
    assertThat(state.run(run).get("state")).isEqualTo("PAUSED");assertThat(sql.sql("select outcome from ouf_ingestion.schema_observation where run_id=:r").param("r",run).query(String.class).single()).isEqualTo("INCOMPATIBLE");assertThat(sql.sql("select count(*) from ouf_ingestion.runtime_issue where run_id=:r and issue_code='ING_SCHEMA_INCOMPATIBLE'").param("r",run).query(Long.class).single()).isOne();assertThat(sql.sql("select count(*) from ouf_ingestion.ing_quarantine where run_id=:r and reason_code='ING_SCHEMA_INCOMPATIBLE'").param("r",run).query(Long.class).single()).isOne();assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(Long.class).single()).isZero();assertThat(sql.sql("select count(*) from ouf_ingestion.operational_event where run_id=:r and event_type='SCHEMA_DRIFT_OPEN'").param("r",run).query(Long.class).single()).isOne();
  }
  @Test void gateway429FeedsTransientHealthAndHonorsRetryAfter(){
    String source="source-"+UUID.randomUUID();ExecutionBundle bundle=bundle(source);UUID run=state.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-1",source,300,"scheduler"),bundle,"corr-429");state.preflightSucceeded(run);sql.sql("update ouf_ingestion.ing_run set created_at='1900-01-01T00:00:00Z' where run_id=:r").param("r",run).update();
    RecordingLake lake=new RecordingLake();CanonicalRecordPipeline pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,lake,Clock.systemUTC());AdapterSpi adapter=new OneRecordAdapter(){@Override public RecordCursor open(ExecutionBundle b,Checkpoint c){return new RecordCursor(){public Optional<SourceRecord> next(){throw new AdapterException("ING_GATEWAY_429",ErrorClass.TRANSIENT_SOURCE,"ING_GATEWAY_429",Duration.ofMinutes(4));}public Checkpoint checkpoint(){return new Checkpoint(Map.of());}public void close(){}};}};
    new RunExecutionWorker(execution,b->adapter,pipeline,schemas,state,gate).executeOne("worker-429");
    Map<String,Object> health=sql.sql("select status,consecutive_failures,last_error_code,retry_not_before>transaction_timestamp()+interval '3 minutes' as retry_honored from ouf_ingestion.source_health where source_id=:s").param("s",source).query().singleRow();
    assertThat(health).containsEntry("status","DEGRADED").containsEntry("consecutive_failures",1).containsEntry("last_error_code","ING_GATEWAY_429").containsEntry("retry_honored",true);
  }
  @Test void explicitNonBlockingQuarantineCompletesWithWarningsOnlyAfterDrain(){
    String source="source-"+UUID.randomUUID();ExecutionBundle base=bundle(source);Map<String,Object> cfg=new LinkedHashMap<>(base.configuration());cfg.put("propertyMappings",List.of(Map.of("sourceField","missing","targetPropertyIri","https://example.test/name","transform","IDENTITY")));cfg.put("nonBlockingQuarantineReasonCodes",List.of("ING_MAPPING_SOURCE_FIELD_MISSING"));ExecutionBundle bundle=new ExecutionBundle(base.bundleId(),base.bundleVersion(),base.checksum(),source,base.acquisitionMode(),base.bindingRef(),cfg);UUID run=state.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-1",source,300,"scheduler"),bundle,"corr-warning");state.preflightSucceeded(run);sql.sql("update ouf_ingestion.ing_run set created_at='1980-01-01T00:00:00Z' where run_id=:r").param("r",run).update();CanonicalRecordPipeline pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,new RecordingLake(),Clock.systemUTC());RunExecutionWorker worker=new RunExecutionWorker(execution,b->new OneRecordAdapter(),pipeline,schemas,state,gate);worker.executeOne("worker-warning");assertThat(state.run(run).get("state")).isEqualTo("COMPLETED_WITH_WARNINGS");assertThat(sql.sql("select blocking from ouf_ingestion.ing_quarantine where run_id=:r").param("r",run).query(Boolean.class).single()).isFalse();assertThat(sql.sql("select severity from ouf_ingestion.runtime_issue where run_id=:r and quarantine_id is not null").param("r",run).query(String.class).single()).isEqualTo("WARNING");assertThat(sql.sql("select committed_watermark_json->>'ordinal' from ouf_ingestion.ing_partition where run_id=:r").param("r",run).query(String.class).single()).isEqualTo("1");assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(Long.class).single()).isZero();
  }
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings={"MATCH","COMPATIBLE_ADDITIVE","INCOMPATIBLE"})
  void legacyRestUsesRecordNamesWithoutSchemaFetch(String outcome) {
    var bundle=pullPublication("legacy-"+UUID.randomUUID(),"REST_JSON","1",1,Map.of()).execution();
    UUID run=start(bundle,"legacy","1700-01-01T00:00:00Z");
    String page=switch(outcome) {
      case "INCOMPATIBLE" -> """
          {"items":[{"id":"2"}]}
          """;
      case "COMPATIBLE_ADDITIVE" -> """
          {"items":[{"id":"2","name":"Beta","extra":1}]}
          """;
      default -> """
          {"items":[{"id":"2","name":"Beta"}]}
          """;
    };
    var requests=new ArrayList<Map<String,Object>>();
    var adapter=new GatewayJsonAdapter((binding,request,correlation)->{
      requests.add(request);
      return page.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    },json);
    var pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,new RecordingLake(),Clock.systemUTC());
    assertThat(new RunExecutionWorker(execution,b->adapter,pipeline,schemas,state,gate).executeOne("legacy")).contains(run);
    assertThat(requests).containsExactly(Map.of("pageSize",10));
    assertThat(sql.sql("select outcome from ouf_ingestion.schema_observation where run_id=:r and observed_schema_version is null")
        .param("r",run).query(String.class).single()).isEqualTo(outcome);
    assertThat(gate.hasOpen(run)).isFalse();
    if("INCOMPATIBLE".equals(outcome)) {
      assertThat(state.run(run)).containsEntry("state","PAUSED").containsEntry("failure_code","ING_SCHEMA_INCOMPATIBLE");
      assertThat(sql.sql("select count(*) from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(Long.class).single()).isZero();
    } else {
      ackAndComplete(run,"legacy");
    }
  }

  @Test void realGatewayRestAdapterRunsToUdpAckAndCommittedWatermark(){String source="rest-"+UUID.randomUUID();ExecutionBundle bundle=externalBundle(source,"REST_JSON");UUID run=start(bundle,"corr-rest","1700-01-01T00:00:00Z");RuntimePorts.GatewaySourcePort gateway=verticalGateway("1",BASE_FIELDS,"Town Hall");runVertical(run,new GatewayJsonAdapter(gateway,json),"rest-worker");assertThat(sql.sql("select payload_json#>>'{canonicalPayload,https://example.test/name}' from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(String.class).single()).isEqualTo("Town Hall");ackAndComplete(run,"rest-worker");}
  @Test void realGatewayWfsAdapterRunsToUdpAckWithCanonicalMapping(){String source="wfs-"+UUID.randomUUID();ExecutionBundle bundle=pullPublication(source,"WFS","1",1,Map.of()).execution();UUID run=start(bundle,"corr-wfs","1600-01-01T00:00:00Z");String xml="<wfs:FeatureCollection xmlns:wfs='http://www.opengis.net/wfs/2.0' xmlns:gml='http://www.opengis.net/gml/3.2' xmlns:x='urn:test'><wfs:member><x:place gml:id='p1'><x:id>p1</x:id><x:name>Town Hall</x:name><x:shape srsName='urn:ogc:def:crs:EPSG::4326'>1 2</x:shape></x:place></wfs:member></wfs:FeatureCollection>";runVertical(run,new GatewayWfsAdapter((r,q,c)->{assertThat(q).containsEntry("request","GetFeature");return xml.getBytes(java.nio.charset.StandardCharsets.UTF_8);}),"wfs-worker");assertThat(sql.sql("select payload_json#>>'{canonicalPayload,https://example.test/name}' from ouf_ingestion.handoff_outbox where run_id=:r").param("r",run).query(String.class).single()).isEqualTo("Town Hall");ackAndComplete(run,"wfs-worker");}
  @Test void governedRetryPreservesFailedAttemptAndResolvesOnlyAfterDurableAck(){
    ExecutionBundle bundle=bundle("retry-"+UUID.randomUUID());UUID run=start(bundle,"corr-retry","1400-01-01T00:00:00Z");
    var actor=new TrustedAuthorizationContext.Context("human:operator","HUMAN","tenant-1",Set.of("ouf.ingestion.quarantine.retry","ingestion.run.resume"),"decision://retry");
    RunLifecycleService controls=new RunLifecycleService(sql,json,refs->{},transactions);
    RuntimePorts.DataLakePort unavailable=(zone,r,o,bytes,hash,key)->{throw new IllegalStateException("ING_EXECUTION_GATEWAY_404");};
    RunExecutionWorker failing=new RunExecutionWorker(execution,b->new OneRecordAdapter(),new CanonicalRecordPipeline(json,validator,durable,quarantine,unavailable),schemas,state,gate);
    assertThat(failing.executeOne("retry-worker")).contains(run);
    UUID failed=sql.sql("select attempt_id from ouf_ingestion.processing_attempt where run_id=:r").param("r",run).query(UUID.class).single();
    UUID q=sql.sql("select quarantine_id from ouf_ingestion.ing_quarantine where run_id=:r").param("r",run).query(UUID.class).single();
    assertThatThrownBy(()->controls.resume(run,0,actor,"corr-unauthorized-retry")).hasMessage("ING_QUARANTINE_RETRY_REQUIRED");
    assertThat(controls.view(run,actor)).containsEntry("state","PAUSED").containsEntry("control_version",0L);
    quarantine.markRetryReady(q,0,actor,"corr-ready");controls.resume(run,0,actor,"corr-resume");
    RunExecutionWorker recovered=new RunExecutionWorker(execution,b->new OneRecordAdapter(),new CanonicalRecordPipeline(json,validator,durable,quarantine,new RecordingLake()),schemas,state,gate);
    assertThat(recovered.executeOne("retry-worker")).contains(run);
    assertThat(sql.sql("select attempt_no from ouf_ingestion.processing_attempt where run_id=:r order by attempt_no").param("r",run).query(Integer.class).list()).containsExactly(1,2);
    assertThat(sql.sql("select state from ouf_ingestion.processing_attempt where attempt_id=:a").param("a",failed).query(String.class).single()).isEqualTo("FAILED");
    assertThat(quarantine.get(q)).containsEntry("lifecycle_state","RETRY_READY");assertThat(state.run(run)).containsEntry("state","DRAINING");
    var delivery=durable.claim("retry-dispatch",Duration.ofMinutes(1)).orElseThrow();
    durable.retry(delivery.handoffId(),"retry-dispatch","ING_GATEWAY_503",Duration.ZERO,false);
    assertThat(quarantine.get(q)).containsEntry("lifecycle_state","RETRY_READY");
    delivery=durable.claim("retry-dispatch",Duration.ofMinutes(1)).orElseThrow();
    durable.acknowledge(delivery.handoffId(),"retry-dispatch","udp:durable-retry");durable.acknowledge(delivery.handoffId(),"duplicate","udp:duplicate");
    assertThat(quarantine.get(q)).containsEntry("lifecycle_state","RESOLVED").containsEntry("state","RELEASED").containsEntry("lifecycle_version",2L);
    assertThat(sql.sql("select status from ouf_ingestion.runtime_issue where quarantine_id=:q").param("q",q).query(String.class).single()).isEqualTo("RESOLVED");
    assertThat(sql.sql("select count(*) from ouf_ingestion.audit_event where event_type='QUARANTINE_RETRY_RESOLVED' and resource_id=:q").param("q",q.toString()).query(Long.class).single()).isOne();
    recovered.executeOne("retry-worker");assertThat(state.run(run)).containsEntry("state","SUCCEEDED");
  }

  @Test void nextAttemptNumberRequiresTheCurrentPartitionLease(){
    UUID run=start(bundle("lease-retry-"+UUID.randomUUID()),"corr-lease-retry","1300-01-01T00:00:00Z");
    var claim=execution.claim("lease-retry-worker",Duration.ofMinutes(1)).orElseThrow();
    assertThat(claim.runId()).isEqualTo(run);assertThat(execution.nextAttemptNumber(claim,"object-1")).isOne();
    durable.recordFailure(run,"object-1",1,"test-adapter","1","ING_EXECUTION_GATEWAY_404");
    assertThat(execution.nextAttemptNumber(claim,"object-1")).isEqualTo(2);
    execution.release(claim);
    assertThatThrownBy(()->execution.nextAttemptNumber(claim,"object-1")).hasMessage("ING_PARTITION_LEASE_LOST");
  }

  @Test void ackDoesNotResolveAnOpenQuarantineOrAnotherRecord(){
    UUID run=start(bundle("ack-scope-"+UUID.randomUUID()),"corr-ack-scope","1200-01-01T00:00:00Z");
    UUID failed=durable.recordFailure(run,"object-1",1,"test-adapter","1","ING_EXECUTION_GATEWAY_404");
    UUID open=quarantine.quarantine(run,failed,"object-1","ING_EXECUTION_GATEWAY_404","managed:ref","evidence:ref","corr-open");
    UUID otherAttempt=durable.recordFailure(run,"object-2",1,"test-adapter","1","ING_EXECUTION_GATEWAY_404");
    UUID other=quarantine.quarantine(run,otherAttempt,"object-2","ING_EXECUTION_GATEWAY_404","managed:other","evidence:other","corr-other");
    quarantine.markRetryReady(other,0,new TrustedAuthorizationContext.Context("human:operator","HUMAN","tenant-1",Set.of(),"decision://other"),"corr-ready-other");
    runVertical(run,new OneRecordAdapter(),"ack-scope-worker");
    var delivery=durable.claim("ack-scope-dispatch",Duration.ofMinutes(1)).orElseThrow();durable.acknowledge(delivery.handoffId(),"ack-scope-dispatch","udp:durable");
    assertThat(quarantine.get(open)).containsEntry("lifecycle_state","OPEN");
    assertThat(quarantine.get(other)).containsEntry("lifecycle_state","RETRY_READY");
  }

  @Autowired OnboardingReviewRepository reviews;
  @Autowired ActivationRepository activation;
  private static final List<Map<String,Object>> BASE_FIELDS=List.of(field("id","string"),field("name","string"));
  private static Map<String,Object> field(String name,String type){return Map.of("name",name,"type",type,"nullable",false);}
  private RuntimePorts.GatewaySourcePort verticalGateway(String version,List<Map<String,Object>> fields,String value) {
    return (binding,parameters,correlation)->{
      Map<String,Object> response=new LinkedHashMap<>();
      response.put("collection","places");response.put("schemaVersion",version);
      if(parameters.containsKey("resource")){
        assertThat(parameters).containsEntry("resource","schema").containsEntry("collection","places");
        response.put("fields",fields);response.put("geometry",null);response.put("key",List.of("id"));
      }else{
        // An unusable event proves a schema pause precedes even identity extraction.
        response.put("items",value==null?List.of(Map.of("op","invalid")):
            List.of(Map.of("id","p1","revision",7,"op","UPSERT","changedAt","2026-11-21T10:15:02+01:00","properties",Map.of("name",value))));
        response.put("mode","CURSOR");response.put("next","after-"+value);response.put("hasMore",false);
      }
      try{return json.writeValueAsBytes(response);}catch(Exception e){throw new AssertionError(e);}
    };
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings={"added","removed","type"})
  void schemaIncidentSurvivesWorkersAndResumesFromConfirmedCursor(String drift) {
    String source="drift-"+UUID.randomUUID();
    var approved=publishedVertical(source,"1",BASE_FIELDS);
    var changed=switch(drift){
      case "added"->List.of(field("name","string"),field("new","string"),field("id","string"));
      case "removed"->List.of(field("name","string"));
      default->List.of(field("id","integer"),field("name","string"));
    };
    UUID first=scheduledPoll(approved,0);
    runVertical(first,new GatewayJsonAdapter(verticalGateway("1",BASE_FIELDS,"before"),json),"before");
    ackAndComplete(first,"before");

    for(int poll=0;poll<2;poll++){
      UUID blocked=scheduledPoll(approved,poll+1);
      String checkpoint=checkpoint(blocked);
      assertThat(checkpoint).contains("after-before");
      var lake=new RecordingLake();
      var worker=new RunExecutionWorker(execution,b->new GatewayJsonAdapter((ref,params,c)->{
        if(!params.containsKey("resource"))assertThat(params).containsEntry("cursor","after-before");
        return verticalGateway("removed".equals(drift)?"2":"1",changed,null).fetch(ref,params,c);
      },json),new CanonicalRecordPipeline(json,validator,durable,quarantine,lake),schemas,state,gate);
      assertThat(worker.executeOne("worker-"+poll)).contains(blocked);
      assertThat(state.run(blocked)).containsEntry("state","PAUSED").containsEntry("failure_code","ING_SCHEMA_CHANGED");
      assertThat(checkpoint(blocked)).isEqualTo(checkpoint);
      assertThat(lake.zones).isEmpty();
      assertThat(sql.sql("select count(*) from ouf_ingestion.processing_attempt where run_id=:r").param("r",blocked).query(Long.class).single()).isZero();
      assertThat(sql.sql("select observation_polls from ouf_ingestion.pull_schema_notice where source_id=:s and tenant_id='tenant-1' and next_action='REVISE_ONBOARDING'")
          .param("s",source).query(Long.class).single()).isEqualTo(poll+1L);
    }
    assertThat(sql.sql("select count(*) from ouf_ingestion.onboarding_review_outbox q join ouf_ingestion.runtime_issue i using(issue_id) where i.source_id=:s")
        .param("s",source).query(Long.class).single()).isOne();
    if("added".equals(drift)) {
      var reservation=reviews.claim("lost-reply",Duration.ofMinutes(1),true).orElseThrow();
      assertThat(reservation.reconcile()).isFalse();
      sql.sql("update ouf_ingestion.onboarding_review_outbox set lease_until=clock_timestamp()-interval '1 second' where request_id=:r")
          .param("r",reservation.requestId()).update();
      RuntimePorts.OnboardingReviewPort remote=new RuntimePorts.OnboardingReviewPort(){
        public Receipt request(UUID id,String s,String e,String c,String k){throw new AssertionError("Second POST");}
        public Optional<Receipt> reconcile(UUID id,String s,String key){
          assertThat(key).isEqualTo(reservation.idempotencyKey());
          return Optional.of(new Receipt("onboarding:existing-review",true));
        }
      };
      assertThat(new OnboardingReviewDispatcher(reviews,remote).dispatchOne("restart",true)).contains(reservation.requestId());
      assertThatThrownBy(()->reviews.acknowledge(reservation,"stale")).hasMessage("ING_ONBOARDING_REVIEW_LEASE_LOST");
      assertThat(sql.sql("select state from ouf_ingestion.onboarding_review_outbox where request_id=:r").param("r",reservation.requestId()).query(String.class).single()).isEqualTo("ACKED");
    }
    var independent=externalBundle("other-"+UUID.randomUUID(),"REST_JSON");
    UUID other=start(independent,"independent","2000-01-01T00:00:00Z");
    runVertical(other,new GatewayJsonAdapter(verticalGateway("1",BASE_FIELDS,"independent"),json),"other");
    ackAndComplete(other,"other");

    boolean republish="type".equals(drift);
    var next=republish?publishedVertical(source,"1",changed,2):approved;
    UUID resumed=scheduledPoll(next,3);
    assertThat(checkpoint(resumed)).contains("after-before");
    RuntimePorts.GatewaySourcePort recovered=verticalGateway("1",republish?changed:BASE_FIELDS,"first-suspended");
    runVertical(resumed,new GatewayJsonAdapter((ref,params,c)->{
      if(!params.containsKey("resource"))assertThat(params).containsEntry("cursor","after-before");
      return recovered.fetch(ref,params,c);
    },json),"resumed");
    assertThat(sql.sql("select payload_json#>>'{canonicalPayload,https://example.test/name}' from ouf_ingestion.handoff_outbox where run_id=:r")
        .param("r",resumed).query(String.class).single()).isEqualTo("first-suspended");
    assertThat(sql.sql("select i.status from ouf_ingestion.pull_schema_incident g join ouf_ingestion.runtime_issue i using(issue_id) where g.resume_run=:r and g.resumed_at is not null")
        .param("r",resumed).query(String.class).single()).isEqualTo("RESOLVED");
    ackAndComplete(resumed,"resumed");
    sql.sql("update ouf_ingestion.ing_schedule set state='DISABLED' where source_id=:s").param("s",source).update();
  }
  private UUID scheduledPoll(ExecutionBundle bundle,int slot){
    // Make this fixture's slot due without waiting for four wall-clock intervals.
    sql.sql("""
        update ouf_ingestion.ing_schedule set next_run_at=timestamptz '1800-01-01 00:00:00Z'+(:slot * interval '1 minute'),
          workload_class='DELTA_ORDINARY',last_dispatched_at=null,discovery_verified_at=clock_timestamp() where source_id=:s
        """).param("slot",slot).param("s",bundle.sourceId()).update();
    var claim=state.claimDue("schema-scheduler",Duration.ofMinutes(2)).orElseThrow();
    assertThat(claim.sourceId()).isEqualTo(bundle.sourceId());
    assertThat(claim.publicationChecksum()).isEqualTo(bundle.checksum());
    UUID run=state.createPreflightRun(claim,bundle,"schema-poll-"+slot);
    state.completePreflight(claim,run);
    return run;
  }
  private String checkpoint(UUID run){
    return sql.sql("select checkpoint_json::text from ouf_ingestion.ing_partition where run_id=:r").param("r",run).query(String.class).single();
  }
  private ExecutionBundle publishedVertical(String source,String version,List<Map<String,Object>> fields){
    return publishedVertical(source,version,fields,1);
  }
  private ExecutionBundle publishedVertical(String source,String version,List<Map<String,Object>> fields,int generation){
    var profile=Map.of("readParameters",Map.of("api",Map.of("schemaVersion",version,"fieldsSha256",PullSchema.fingerprint(json.valueToTree(fields)))),
        "origin",Map.of("collection","places"),"keys",Map.of("declared",List.of(List.of("id"))));
    var publication=pullPublication(source,"REST_JSON",version,generation,profile);
    activation.reconcile(publication);
    return publication.execution();
  }

  private PublishedActivation pullPublication(String source,String mode,String version,int generation,Map<String,?> profile){
    var envelope=PublishedActivationTest.envelope(source,false,generation);
    var published=PublishedActivation.map(envelope,"bundle");
    var execution=new LinkedHashMap<>(externalBundle(source,mode).configuration());
    execution.remove("sourceSchema");
    execution.put("sourceSchemaVersion",version);
    execution.put("acquisitionMode",mode);
    var extraction=new LinkedHashMap<String,Object>();
    extraction.put("runtime",Map.of("execution",execution));
    extraction.put("projection",Map.of("place",execution.get("expectedFieldNames")));
    if(!profile.isEmpty()) extraction.put("sourceUnitProfile",profile);
    published.put("extractionProfile",extraction);
    published.put("dataAccessPolicies",List.of(Map.of("label","OPEN")));
    published.put("sourceObjectIdentityPolicy",Map.of("sourceFields",List.of("id")));
    published.put("semanticMapping",Map.of("sourceType",Map.of("typeCode","place","sourceId",source),"propertyMappings",execution.get("propertyMappings")));
    published.put("changeRepresentationProfile",Map.of("mode","FULL_SNAPSHOT"));
    PublishedActivationTest.hash(published);envelope.put("checksum",published.get("checksum"));
    envelope.put("tenantId","tenant-1");
    return PublishedActivation.decode(envelope,"tenant-1",json);
  }

  private void runVertical(UUID run,AdapterSpi adapter,String worker){CanonicalRecordPipeline pipeline=new CanonicalRecordPipeline(json,validator,durable,quarantine,new RecordingLake(),Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"),ZoneOffset.UTC));assertThat(new RunExecutionWorker(execution,b->adapter,pipeline,schemas,state,gate).executeOne(worker)).contains(run);assertThat(state.run(run).get("state")).isEqualTo("DRAINING");}
  private void ackAndComplete(UUID run,String worker){var handoff=durable.claim("udp",Duration.ofMinutes(1)).orElseThrow();assertThat(handoff.payload()).containsEntry("ingestionRunId",run.toString());durable.acknowledge(handoff.handoffId(),"udp","udp:receipt:"+run);new RunExecutionWorker(execution,b->new OneRecordAdapter(),new CanonicalRecordPipeline(json,validator,durable,quarantine,new RecordingLake(),Clock.systemUTC()),schemas,state,gate).executeOne(worker);assertThat(state.run(run).get("state")).isEqualTo("SUCCEEDED");assertThat(sql.sql("select count(*) from ouf_ingestion.ing_watermark where source_id=(select source_id from ouf_ingestion.ing_run where run_id=:r)").param("r",run).query(Long.class).single()).isOne();}
  private UUID start(ExecutionBundle bundle,String correlation,String created){UUID run=state.createPreflightRun(new RunStateRepository.ScheduleClaim(UUID.randomUUID(),"tenant-1",bundle.sourceId(),300,"scheduler"),bundle,correlation);state.preflightSucceeded(run);sql.sql("update ouf_ingestion.ing_run set created_at=cast(:c as timestamptz) where run_id=:r").param("c",created).param("r",run).update();return run;}
  private static ExecutionBundle externalBundle(String source,String mode){Map<String,Object> cfg=new LinkedHashMap<>(Map.ofEntries(Map.entry("sourceSchemaRef","schema:places:1"),Map.entry("sourceSchemaId","places"),Map.entry("sourceSchemaVersion","1"),Map.entry("expectedFieldNames",mode.equals("WFS")?List.of("id","name","shape"):List.of("id","name")),Map.entry("typeCode","place"),Map.entry("semanticPublicationSetRef","sem:1"),Map.entry("adapterProfileRef","adapter:"+mode.toLowerCase()+":1"),Map.entry("adapterId",mode.equals("WFS")?"gateway-wfs-v1":"gateway-rest-json-v1"),Map.entry("adapterRuntimeVersion","1.0.0"),Map.entry("observationPolicy","ACQUISITION_TIME"),Map.entry("propertyMappings",List.of(Map.of("sourceField","name","targetPropertyIri","https://example.test/name","transform","IDENTITY"))),Map.entry("mappingRefs",List.of("mapping:1")),Map.entry("pinnedReferences",List.of("schema:places:1","sem:1")),Map.entry("identityFields",List.of("id")),Map.entry("pageSize",10),Map.entry("typeName","x:place"),Map.entry("axisOrder","AUTHORITY")));if(mode.equals("REST_JSON")) cfg.put("sourceSchema",Map.of("fields",BASE_FIELDS));return new ExecutionBundle("bundle","1","sha256:bundle",source,mode,"gateway://binding/places",cfg);}
  private static ExecutionBundle bundle(String source){Map<String,Object> cfg=new LinkedHashMap<>(Map.ofEntries(Map.entry("sourceSchemaRef","schema:places:1"),Map.entry("sourceSchemaId","places"),Map.entry("sourceSchemaVersion","1"),Map.entry("expectedFieldNames",List.of("name")),Map.entry("typeCode","place"),Map.entry("semanticPublicationSetRef","sem:1"),Map.entry("adapterProfileRef","adapter:test:1"),Map.entry("adapterId","test-adapter"),Map.entry("adapterRuntimeVersion","1.0.0"),Map.entry("observationPolicy","ACQUISITION_TIME"),Map.entry("propertyMappings",List.of(Map.of("sourceField","name","targetPropertyIri","https://example.test/name","transform","IDENTITY"))),Map.entry("mappingRefs",List.of("mapping:1")),Map.entry("pinnedReferences",List.of("schema:places:1","sem:1"))));return new ExecutionBundle("bundle","1","sha256:bundle",source,"INTERNAL_MANAGED_CSV","managed:object",cfg);}
  private static class OneRecordAdapter implements AdapterSpi {private final Map<String,Object> payload,provenance;OneRecordAdapter(){this(Map.of("name","Town Hall"),Map.of("managedObjectRef","managed:object"));}OneRecordAdapter(Map<String,Object> payload,Map<String,Object> provenance){this.payload=payload;this.provenance=provenance;}public String adapterId(){return "test-adapter";}public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_CSV");}public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}public RecordCursor open(ExecutionBundle b,Checkpoint c){return new RecordCursor(){boolean sent;public Optional<SourceRecord> next(){if(sent)return Optional.empty();sent=true;return Optional.of(new SourceRecord("object-1",1,payload,provenance));}public Checkpoint checkpoint(){return new Checkpoint(Map.of("ordinal",sent?1:0));}public void close(){}};}}
  private static final class RecordingLake implements RuntimePorts.DataLakePort {final List<Zone> zones=new ArrayList<>();public Receipt persist(Zone zone,UUID run,String object,byte[] content,String hash,String key){zones.add(zone);return new Receipt("lake:"+zone+":"+key,true);}}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}

