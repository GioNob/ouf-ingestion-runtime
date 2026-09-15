package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service @ConditionalOnBean(RuntimePorts.ReplayExecutionPort.class)
public class ReplayWorker {
  private static final Logger LOG=LoggerFactory.getLogger(ReplayWorker.class);
  private final ReplayRepository repository;private final RuntimePorts.ReplayExecutionPort executor;private final IngestionMetrics metrics;
  public ReplayWorker(ReplayRepository repository,RuntimePorts.ReplayExecutionPort executor,IngestionMetrics metrics){this.repository=repository;this.executor=executor;this.metrics=metrics;}
  public boolean executeOne(String worker){var found=repository.claim(worker,Duration.ofMinutes(5));if(found.isEmpty())return false;var c=found.orElseThrow();try{var receipt=executor.execute(new RuntimePorts.ReplayExecutionPort.ReplayPlan(c.replayId(),c.quarantineId(),c.replayMode(),c.originalRunId(),c.originalBundleRef(),c.targetBundleRef(),c.rawObjectRef(),c.parentAttemptId(),c.correlationId()));repository.succeeded(c,receipt);metrics.replay("succeeded");LOG.atInfo().addKeyValue("event","REPLAY_SUCCEEDED").addKeyValue("replayId",c.replayId()).addKeyValue("quarantineId",c.quarantineId()).addKeyValue("replayMode",c.replayMode()).addKeyValue("correlationId",c.correlationId()).log("Ingestion replay completed");}catch(RuntimeException failure){repository.failed(c,"ING_REPLAY_EXECUTION_FAILED",Duration.ofMinutes(1));metrics.replay("retry");LOG.atWarn().addKeyValue("event","REPLAY_RETRY").addKeyValue("replayId",c.replayId()).addKeyValue("quarantineId",c.quarantineId()).addKeyValue("replayMode",c.replayMode()).addKeyValue("correlationId",c.correlationId()).addKeyValue("errorCode","ING_REPLAY_EXECUTION_FAILED").log("Ingestion replay scheduled for retry");}return true;}
}
