package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

public final class RunExecutionWorker {
  private static final Logger LOG=LoggerFactory.getLogger(RunExecutionWorker.class);
  private final RunExecutionRepository runs;private final RuntimePorts.AdapterResolver adapters;private final CanonicalRecordPipeline pipeline;private final SchemaSurveillanceService schemas;private final RunStateRepository health;
  private final java.util.function.Consumer<ExecutionBundle> verifyRetry;
  public RunExecutionWorker(RunExecutionRepository runs,RuntimePorts.AdapterResolver adapters,CanonicalRecordPipeline pipeline,
      SchemaSurveillanceService schemas,RunStateRepository health){
    this(runs,adapters,pipeline,schemas,health,b->{throw new IllegalStateException("ING_PUBLICATION_VERIFIER_REQUIRED");});
  }
  public RunExecutionWorker(RunExecutionRepository runs,RuntimePorts.AdapterResolver adapters,CanonicalRecordPipeline pipeline,
      SchemaSurveillanceService schemas,RunStateRepository health,java.util.function.Consumer<ExecutionBundle> verifyRetry){
    this.runs=runs;this.adapters=adapters;this.pipeline=pipeline;this.schemas=schemas;this.health=health;this.verifyRetry=verifyRetry;
  }
  public Optional<UUID> executeOne(String worker){
    runs.completeDrained();
    var claimed=runs.claim(worker,Duration.ofMinutes(2));
    if(claimed.isEmpty())return Optional.empty();
    var claim=claimed.orElseThrow();
    var context=runs.context(claim.runId());
    try{
      ExecutionBundle snapshot=runs.bundle(claim.runId());
      if(claim.publicationRetry())verifyRetry.accept(snapshot);
      ExecutionBundle bundle=PublishedExecutionMapper.map(snapshot);
      AdapterSpi adapter=adapters.resolve(bundle);
      verifyAdapter(adapter,bundle);
      boolean exhausted=false;
      long batchDeadline=System.nanoTime()+Duration.ofSeconds(45).toNanos();
      runs.requireCurrent(claim);
      var pending=pipeline.pendingEffects(claim);
      try(var cursor=adapter.open(bundle,new AdapterSpi.Checkpoint(claim.checkpoint()))){
        for(int count=0;count<100 && System.nanoTime()<batchDeadline;count++){
          var next=cursor.next();
          if(next.isEmpty()){pipeline.verifyPending(claim,pending,null,false);exhausted=true;break;}
          var record=next.orElseThrow();
          Map<String,Object> watermark=Map.of("ordinal",record.ordinal());
          var command=new CanonicalRecordPipeline.Command(claim.runId(),claim.partitionKey(),record.ordinal(),
            runs.nextAttemptNumber(claim,record.sourceObjectId()),adapter.adapterId(),bundle,record,context.correlationId(),
            cursor.checkpoint().value(),watermark,claim);
          var observation=schemas.observe(claim.runId(),bundle,record,context.correlationId());
          String schemaCode="ING_SCHEMA_"+observation.outcome().name();
          pipeline.verifyPending(claim,pending,record.ordinal(),observation.outcome().blocking());
          var result=observation.outcome().blocking()?pipeline.reject(command,schemaCode):pipeline.process(command);
          if(!pending.isEmpty()){
            pending=pipeline.pendingEffects(claim);
            if(!pending.isEmpty() && pending.getFirst().sequence()<=record.ordinal())pipeline.verifyPending(claim,pending,null,false);
          }
          if(!result.succeeded()){
            if(result.blockingFailure()){
              String code=observation.outcome().blocking()?schemaCode:"ING_RECORD_QUARANTINED";
              health.recordFailure(context.sourceId(),code,AdapterSpi.ErrorClass.DATA,null);
              runs.pause(claim,code);log("INGESTION_RUN_PAUSED",claim,context,code);
              return Optional.of(claim.runId());
            }
            runs.nonBlockingQuarantined(claim,record.ordinal(),cursor.checkpoint().value(),watermark);
          }
        }
      }
      runs.requireCurrent(claim);
      if(!pending.isEmpty()){
        throw new RuntimePorts.GatewayFailure("ING_LAKE_CURSOR_RETRY",AdapterSpi.ErrorClass.TRANSIENT_SOURCE);
      }
      health.recordSuccess(context.sourceId());
      if(exhausted)runs.inputExhausted(claim);else runs.release(claim);
      runs.completeDrained();log(exhausted?"INGESTION_INPUT_EXHAUSTED":"INGESTION_BATCH_COMPLETED",claim,context,null);
    }catch(RuntimeException failure){
      if("ING_PARTITION_LEASE_LOST".equals(failure.getMessage()))return Optional.of(claim.runId());
      var cause=RunCoordinator.classify(failure);
      if(cause.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE){
        boolean waiting=runs.defer(claim,context.sourceId(),cause,health);
        log(waiting?"INGESTION_RETRY_WAIT":"INGESTION_RUN_PAUSED",claim,context,cause.code());
      }else{
        health.recordFailure(context.sourceId(),cause.code(),cause.errorClass(),cause.retryAfter());
        runs.pause(claim,cause.code());log("INGESTION_RUN_PAUSED",claim,context,cause.code());
      }
    }
    return Optional.of(claim.runId());
  }
  private static void verifyAdapter(AdapterSpi adapter,ExecutionBundle bundle){String expected=required(bundle.configuration(),"adapterId");String version=required(bundle.configuration(),"adapterRuntimeVersion");if(!expected.equals(adapter.adapterId()))throw new IllegalStateException("ING_ADAPTER_ID_MISMATCH");AdapterSpi.Compatibility c=adapter.compatibility();if(compare(version,c.oldestReadableBundleVersion())<0||compare(version,c.current())>0)throw new IllegalStateException("ING_ADAPTER_VERSION_UNSUPPORTED");}
  private static int compare(String a,String b){int[] x=parse(a),y=parse(b);for(int i=0;i<3;i++){int c=Integer.compare(x[i],y[i]);if(c!=0)return c;}return 0;}
  private static int[] parse(String value){String[] p=value.split("\\.");if(p.length!=3)throw new IllegalStateException("ING_ADAPTER_VERSION_INVALID");try{return new int[]{Integer.parseInt(p[0]),Integer.parseInt(p[1]),Integer.parseInt(p[2])};}catch(NumberFormatException e){throw new IllegalStateException("ING_ADAPTER_VERSION_INVALID");}}
  private static String required(Map<String,Object> c,String key){Object v=c.get(key);if(v==null||String.valueOf(v).isBlank())throw new IllegalStateException("ING_ADAPTER_CONFIG_MISSING");return String.valueOf(v);}
  private static void log(String event,RunExecutionRepository.Claim c,RunExecutionRepository.RunContext x,String code){var l=code==null?LOG.atInfo():LOG.atWarn();l.addKeyValue("event",event).addKeyValue("runId",c.runId()).addKeyValue("sourceId",x.sourceId()).addKeyValue("correlationId",x.correlationId()).addKeyValue("errorCode",code).log("Ingestion execution state changed");}
}
