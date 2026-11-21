package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;
import org.slf4j.*;

public final class RunExecutionWorker {
  private static final Logger LOG=LoggerFactory.getLogger(RunExecutionWorker.class);
  private final RunExecutionRepository runs;
  private final RuntimePorts.AdapterResolver adapters;
  private final CanonicalRecordPipeline pipeline;
  private final SchemaSurveillanceService schemas;
  private final RunStateRepository health;
  private final PullSchemaGate gate;

  public RunExecutionWorker(RunExecutionRepository runs,RuntimePorts.AdapterResolver adapters,CanonicalRecordPipeline pipeline,
                            SchemaSurveillanceService schemas,RunStateRepository health,PullSchemaGate gate) {
    this.runs=runs; this.adapters=adapters; this.pipeline=pipeline; this.schemas=schemas; this.health=health; this.gate=gate;
  }

  public Optional<UUID> executeOne(String worker) {
    runs.completeDrained();
    var claimed=runs.claim(worker,Duration.ofMinutes(2));
    if(claimed.isEmpty()) return Optional.empty();
    var claim=claimed.orElseThrow();
    var run=Optional.of(claim.runId());
    var context=runs.context(claim.runId());
    try {
      ExecutionBundle bundle=PublishedExecutionMapper.map(runs.bundle(claim.runId()));
      AdapterSpi adapter=adapters.resolve(bundle);
      verifyAdapter(adapter,bundle);
      boolean exhausted=false;
      long deadline=System.nanoTime()+Duration.ofSeconds(45).toNanos();
      runs.requireCurrent(claim);
      try(var cursor=open(adapter,bundle,claim,context.correlationId())) {
        for(int count=0; count<100 && System.nanoTime()<deadline; count++) {
          var next=cursor.next();
          if(next.isEmpty()) {exhausted=true;break;}
          var record=next.orElseThrow();
          var position=cursor.checkpoint().value();
          Map<String,Object> watermark=Map.of("ordinal",record.ordinal());
          var command=new CanonicalRecordPipeline.Command(claim.runId(),claim.partitionKey(),record.ordinal(),
              runs.nextAttemptNumber(claim,record.sourceObjectId()),adapter.adapterId(),bundle,record,
              context.correlationId(),position,watermark,claim);
          boolean pageSchemaChecked=adapter instanceof GatewayJsonAdapter && record.provenance().containsKey("schemaVersion");
          var outcome=pageSchemaChecked ? SchemaSurveillanceService.Outcome.MATCH :
              schemas.observe(claim.runId(),bundle,record,context.correlationId()).outcome();
          var result=outcome.blocking()?pipeline.reject(command,"ING_SCHEMA_"+outcome.name()):pipeline.process(command);
          if(result.succeeded()) continue;
          if(result.blockingFailure()) {
            String code=outcome.blocking()?"ING_SCHEMA_"+outcome.name():"ING_RECORD_QUARANTINED";
            health.recordFailure(context.sourceId(),code,AdapterSpi.ErrorClass.DATA,null);
            runs.pause(claim,code);
            log("INGESTION_RUN_PAUSED",claim,context,code);
            return run;
          }
          runs.nonBlockingQuarantined(claim,record.ordinal(),position,watermark);
        }
      }
      runs.requireCurrent(claim);
      if(exhausted) runs.inputExhausted(claim); else runs.release(claim);
      health.recordSuccess(context.sourceId());
      runs.completeDrained();
      log(exhausted?"INGESTION_INPUT_EXHAUSTED":"INGESTION_BATCH_COMPLETED",claim,context,null);
    } catch(ObservationOnly paused) {
      // The gate already committed the observation, incident, outbox and pause together.
    } catch(RuntimeException failure) {
      if("ING_PARTITION_LEASE_LOST".equals(failure.getMessage())) return run;
      var classified=RunCoordinator.classify(failure);
      health.recordFailure(context.sourceId(),classified.code(),classified.errorClass(),classified.retryAfter());
      runs.pause(claim,classified.code());
      log("INGESTION_RUN_PAUSED",claim,context,classified.code());
    }
    return run;
  }

  private AdapterSpi.RecordCursor open(AdapterSpi adapter,ExecutionBundle bundle,RunExecutionRepository.Claim claim,String correlation) {
    var checkpoint=new AdapterSpi.Checkpoint(claim.checkpoint());
    if(adapter instanceof GatewayJsonAdapter rest) {
      return rest.open(bundle,checkpoint,gate.hasOpen(claim.runId()),(schema,position)->{
        if(!gate.admits(claim,bundle,schema,position,correlation)) throw new ObservationOnly();
      });
    }
    if(adapter instanceof GatewayWfsAdapter wfs && PullSchema.declaresVerticalContract(bundle)) {
      if(!gate.admits(claim,bundle,wfs.observe(bundle),checkpoint.value(),correlation)) throw new ObservationOnly();
    } else if(gate.hasOpen(claim.runId())) {
      var missing=com.fasterxml.jackson.databind.node.MissingNode.getInstance();
      if(!gate.admits(claim,bundle,PullSchema.compare(bundle,missing,missing),checkpoint.value(),correlation))
        throw new ObservationOnly();
    }
    return adapter.open(bundle,checkpoint);
  }
  private static final class ObservationOnly extends RuntimeException {}
  private static void verifyAdapter(AdapterSpi adapter,ExecutionBundle bundle){String expected=required(bundle.configuration(),"adapterId");String version=required(bundle.configuration(),"adapterRuntimeVersion");if(!expected.equals(adapter.adapterId()))throw new IllegalStateException("ING_ADAPTER_ID_MISMATCH");AdapterSpi.Compatibility c=adapter.compatibility();if(compare(version,c.oldestReadableBundleVersion())<0||compare(version,c.current())>0)throw new IllegalStateException("ING_ADAPTER_VERSION_UNSUPPORTED");}
  private static int compare(String a,String b){int[] x=parse(a),y=parse(b);for(int i=0;i<3;i++){int c=Integer.compare(x[i],y[i]);if(c!=0)return c;}return 0;}
  private static int[] parse(String value){String[] p=value.split("\\.");if(p.length!=3)throw new IllegalStateException("ING_ADAPTER_VERSION_INVALID");try{return new int[]{Integer.parseInt(p[0]),Integer.parseInt(p[1]),Integer.parseInt(p[2])};}catch(NumberFormatException e){throw new IllegalStateException("ING_ADAPTER_VERSION_INVALID");}}
  private static String required(Map<String,Object> c,String key){Object v=c.get(key);if(v==null||String.valueOf(v).isBlank())throw new IllegalStateException("ING_ADAPTER_CONFIG_MISSING");return String.valueOf(v);}
  private static void log(String event,RunExecutionRepository.Claim c,RunExecutionRepository.RunContext x,String code){var l=code==null?LOG.atInfo():LOG.atWarn();l.addKeyValue("event",event).addKeyValue("runId",c.runId()).addKeyValue("sourceId",x.sourceId()).addKeyValue("correlationId",x.correlationId()).addKeyValue("errorCode",code).log("Ingestion execution state changed");}
}
