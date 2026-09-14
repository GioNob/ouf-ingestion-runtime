package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;
import org.slf4j.*;
import org.springframework.stereotype.Service;

@Service
public class RunCoordinator {
  private static final Logger LOG=LoggerFactory.getLogger(RunCoordinator.class);
  private final RunStateRepository state;private final RuntimePorts.ActiveBundlePort bundles;private final RuntimePorts.SemanticPort semantic;
  public RunCoordinator(RunStateRepository state,RuntimePorts.ActiveBundlePort bundles,RuntimePorts.SemanticPort semantic){this.state=state;this.bundles=bundles;this.semantic=semantic;}
  public Optional<UUID> dispatchOne(String worker){var claim=state.claimDue(worker,Duration.ofMinutes(2));if(claim.isEmpty())return Optional.empty();var c=claim.orElseThrow();UUID run=null;try{ExecutionBundle b=bundles.loadAndVerify(c.sourceId());run=state.createPreflightRun(c,b,UUID.randomUUID().toString());semantic.preflight(pinned(b));state.preflightSucceeded(run);state.recordSuccess(c.sourceId());state.releaseSchedule(c,true);LOG.atInfo().addKeyValue("event","INGESTION_RUN_STARTED").addKeyValue("runId",run).addKeyValue("sourceId",c.sourceId()).log("Ingestion run passed preflight");return Optional.of(run);}catch(RuntimeException failure){Failure classified=classify(failure);if(run!=null)state.preflightFailed(run,classified.code());state.recordFailure(c.sourceId(),classified.code(),classified.errorClass(),classified.retryAfter());state.releaseSchedule(c,false);LOG.atWarn().addKeyValue("event","INGESTION_PREFLIGHT_FAILED").addKeyValue("runId",run).addKeyValue("sourceId",c.sourceId()).addKeyValue("errorCode",classified.code()).addKeyValue("errorClass",classified.errorClass()).log("Ingestion run preflight failed");return Optional.empty();}}
  private static Collection<String> pinned(ExecutionBundle b){Object raw=b.configuration().get("pinnedReferences");if(!(raw instanceof Collection<?> refs)||refs.isEmpty())throw new IllegalArgumentException("ING_PREFLIGHT_REFS_REQUIRED");return refs.stream().map(String::valueOf).toList();}
  static Failure classify(RuntimeException failure){if(failure instanceof AdapterSpi.AdapterException a)return new Failure(a.code(),a.errorClass(),a.retryAfter().orElse(null));if(failure instanceof RuntimePorts.GatewayFailure g)return new Failure(g.safeCode(),g.errorClass(),g.retryAfter().orElse(null));String code=failure.getMessage()!=null&&failure.getMessage().matches("ING_[A-Z0-9_]{1,76}")?failure.getMessage():"ING_PREFLIGHT_FAILED";return new Failure(code,AdapterSpi.ErrorClass.CONFIGURATION,null);}
  record Failure(String code,AdapterSpi.ErrorClass errorClass,Duration retryAfter){}
}
