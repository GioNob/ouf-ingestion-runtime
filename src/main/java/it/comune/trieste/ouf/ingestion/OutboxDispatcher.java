package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.slf4j.*;
import org.springframework.stereotype.Service;

public class OutboxDispatcher {
  private static final Logger LOG=LoggerFactory.getLogger(OutboxDispatcher.class);
  private final DurablePipelineRepository repository; private final RuntimePorts.DurableHandoffPort downstream;private final IngestionMetrics metrics;
  public OutboxDispatcher(DurablePipelineRepository repository,RuntimePorts.DurableHandoffPort downstream,IngestionMetrics metrics){this.repository=repository;this.downstream=downstream;this.metrics=metrics;}

  public boolean dispatchOne(String worker){
    // A process can reuse its worker name after expiration. Each lease needs its own fence.
    String owner=worker+":"+java.util.UUID.randomUUID();
    var claimed=repository.claim(owner,Duration.ofMinutes(2));if(claimed.isEmpty())return false;
    var h=claimed.orElseThrow();RuntimePorts.DurableHandoffPort.Receipt receipt;
    try {
      receipt=downstream.deliver(h.handoffId(),h.payload(),h.idempotencyKey());
      if(!receipt.durable() || receipt.reference()==null || receipt.reference().isBlank())
        throw new IllegalStateException("ING_ACK_NOT_DURABLE");
    } catch(RuntimeException failure) {
      var cause=RunCoordinator.classify(failure);
      boolean retry=cause.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE;
      String code=cause.code();
      repository.retry(h.handoffId(),owner,code,Duration.ofSeconds(RetryPolicy.delay(h.attempts(),0,cause.retryAfter())),!retry);
      metrics.handoff(retry?"retry":"blocked");safeLog(h.handoffId(),code);return true;
    }
    repository.acknowledge(h.handoffId(),owner,receipt.reference());metrics.handoff("acked");return true;
  }
  private static void safeLog(java.util.UUID id,String code){LOG.atWarn().addKeyValue("event","HANDOFF_UNCONFIRMED").addKeyValue("handoffId",id).addKeyValue("errorCode",code).log("Ingestion handoff lacks a durable acknowledgement");}
}
