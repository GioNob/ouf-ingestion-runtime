package it.comune.trieste.ouf.ingestion;

import java.util.UUID;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component @ConditionalOnProperty(name="ouf.ingestion.execution.enabled",havingValue="true")
public class ExecutionLoop {
  private static final Logger LOG=LoggerFactory.getLogger(ExecutionLoop.class);
  private final RunExecutionWorker worker;private final OutboxDispatcher outbox;private final String owner="execution-"+UUID.randomUUID();
  public ExecutionLoop(RunExecutionWorker worker,OutboxDispatcher outbox){this.worker=worker;this.outbox=outbox;}
  @Scheduled(fixedDelayString="${ouf.ingestion.execution.poll-delay-ms:1000}",initialDelayString="${ouf.ingestion.execution.initial-delay-ms:1000}")
  public void tick(){
    try{outbox.dispatchOne(owner+":delivery");}catch(RuntimeException failure){LOG.warn("ING_AUTOMATIC_DELIVERY_FAILED");}
    try{worker.executeOne(owner+":acquisition");}catch(RuntimeException failure){LOG.warn("ING_AUTOMATIC_EXECUTION_FAILED");}
  }
}
