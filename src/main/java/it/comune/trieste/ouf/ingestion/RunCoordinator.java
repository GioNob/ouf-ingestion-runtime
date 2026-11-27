package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="ouf.ingestion.activation.enabled",havingValue="true")
public class RunCoordinator {
  private static final Logger LOG=LoggerFactory.getLogger(RunCoordinator.class);
  private final RunStateRepository state;private final RuntimePorts.ActiveBundlePort bundles;private final RuntimePorts.SemanticPort semantic;
  public RunCoordinator(RunStateRepository state,RuntimePorts.ActiveBundlePort bundles,RuntimePorts.SemanticPort semantic){this.state=state;this.bundles=bundles;this.semantic=semantic;}
  public Optional<UUID> dispatchOne(String worker){
    var claim=state.claimDue(worker,Duration.ofMinutes(2));if(claim.isEmpty())return Optional.empty();var c=claim.orElseThrow();UUID run=null;
    try{
      if(c.publicationId()!=null && c.intervalSeconds()==0){
        run=state.prepareManaged(c);if(run==null)return Optional.empty();
      }
      ExecutionBundle b=bundles.loadAndVerify(c);
      if(run==null)run=state.createPreflightRun(c,b,UUID.randomUUID().toString());
      else if(!c.publicationChecksum().equals(b.checksum()) || !c.sourceId().equals(b.sourceId()))throw new IllegalArgumentException("ING_ACTIVE_PUBLICATION_CHANGED");
      semantic.preflight(b);state.completePreflight(c,run);}
    catch(RuntimeException failure){
      Failure classified=classify(failure);
      try{if(c.publicationId()!=null)state.activationFailed(c,run,classified);else{if(run!=null)state.failPreflight(c,run,classified);state.recordFailure(c.sourceId(),classified.code(),classified.errorClass(),classified.retryAfter());state.releaseSchedule(c,false);}}catch(IllegalStateException lost){LOG.warn("ING_SCHEDULE_RECOVERY_REQUIRED");}
      LOG.atWarn().addKeyValue("event","INGESTION_PREFLIGHT_FAILED").addKeyValue("runId",run).addKeyValue("sourceId",c.sourceId()).addKeyValue("errorCode",classified.code()).log("Ingestion run preflight failed");return Optional.empty();
    }
    state.recordSuccess(c.sourceId());LOG.atInfo().addKeyValue("event","INGESTION_RUN_STARTED").addKeyValue("runId",run).addKeyValue("sourceId",c.sourceId()).log("Ingestion run passed preflight");return Optional.of(run);
  }
  static Failure classify(RuntimeException failure){
    if(failure instanceof ValuePins.BindingFailure pin)return new Failure(pin.getMessage(),AdapterSpi.ErrorClass.CONFIGURATION,null,pin);
    if(failure instanceof AdapterSpi.AdapterException a)return new Failure(a.code(),a.errorClass(),a.retryAfter().orElse(null));
    if(failure instanceof RuntimePorts.GatewayFailure g)return new Failure(g.safeCode(),g.errorClass(),g.retryAfter().orElse(null));
    String code=failure.getMessage()!=null&&failure.getMessage().matches("ING_[A-Z0-9_]{1,76}")?failure.getMessage():"ING_PREFLIGHT_FAILED";
    return new Failure(code,AdapterSpi.ErrorClass.CONFIGURATION,null);
  }
  record Failure(String code,AdapterSpi.ErrorClass errorClass,Duration retryAfter,ValuePins.BindingFailure reference){
    Failure(String code,AdapterSpi.ErrorClass type,Duration retryAfter){this(code,type,retryAfter,null);}
    Failure { if(code==null || !code.matches("ING_[A-Z0-9_]{1,76}"))code="ING_RUNTIME_FAILURE"; }
  }
}
