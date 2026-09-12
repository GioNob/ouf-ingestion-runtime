package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean({RuntimePorts.ActiveBundlePort.class,RuntimePorts.SemanticPort.class})
public class RunCoordinator {
  private static final Logger LOG=LoggerFactory.getLogger(RunCoordinator.class);
  private final RunStateRepository state;private final RuntimePorts.ActiveBundlePort bundles;private final RuntimePorts.SemanticPort semantic;
  public RunCoordinator(RunStateRepository state,RuntimePorts.ActiveBundlePort bundles,RuntimePorts.SemanticPort semantic){this.state=state;this.bundles=bundles;this.semantic=semantic;}
  public Optional<UUID> dispatchOne(String worker){var claim=state.claimDue(worker,Duration.ofMinutes(2));if(claim.isEmpty())return Optional.empty();var c=claim.orElseThrow();UUID run=null;try{ExecutionBundle b=bundles.loadAndVerify(c.sourceId());run=state.createPreflightRun(c,b,UUID.randomUUID().toString());semantic.preflight(pinned(b));state.preflightSucceeded(run);state.recordHealth(c.sourceId(),true,null);state.releaseSchedule(c,true);LOG.atInfo().addKeyValue("event","INGESTION_RUN_STARTED").addKeyValue("runId",run).addKeyValue("sourceId",c.sourceId()).log("Ingestion run passed preflight");return Optional.of(run);}catch(RuntimeException failure){if(run!=null)state.preflightFailed(run,"ING_PREFLIGHT_FAILED");state.recordHealth(c.sourceId(),false,"ING_PREFLIGHT_FAILED");state.releaseSchedule(c,false);LOG.atWarn().addKeyValue("event","INGESTION_PREFLIGHT_FAILED").addKeyValue("runId",run).addKeyValue("sourceId",c.sourceId()).addKeyValue("errorCode","ING_PREFLIGHT_FAILED").log("Ingestion run preflight failed");return Optional.empty();}}
  private static Collection<String> pinned(ExecutionBundle b){Object raw=b.configuration().get("pinnedReferences");if(!(raw instanceof Collection<?> refs)||refs.isEmpty())throw new IllegalArgumentException("ING_PREFLIGHT_REFS_REQUIRED");return refs.stream().map(String::valueOf).toList();}
}
