package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.slf4j.*;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean(RuntimePorts.DurableHandoffPort.class)
public class OutboxDispatcher {
  private static final Logger LOG=LoggerFactory.getLogger(OutboxDispatcher.class);
  private final DurablePipelineRepository repository; private final RuntimePorts.DurableHandoffPort downstream;private final IngestionMetrics metrics;
  public OutboxDispatcher(DurablePipelineRepository repository,RuntimePorts.DurableHandoffPort downstream,IngestionMetrics metrics){this.repository=repository;this.downstream=downstream;this.metrics=metrics;}

  public boolean dispatchOne(String worker){
    var claimed=repository.claim(worker,Duration.ofMinutes(2)); if(claimed.isEmpty())return false;
    var h=claimed.orElseThrow();
    try{
      var receipt=downstream.deliver(h.handoffId(),h.payload(),h.idempotencyKey());
      if(!receipt.durable()){repository.retry(h.handoffId(),worker,"ING_ACK_NOT_DURABLE",Duration.ofSeconds(30),false);metrics.handoff("retry");safeLog(h.handoffId(),"ING_ACK_NOT_DURABLE");return true;}
      repository.acknowledge(h.handoffId(),worker,receipt.reference());metrics.handoff("acked");LOG.atInfo().addKeyValue("event","HANDOFF_ACKED").addKeyValue("handoffId",h.handoffId()).log("Ingestion handoff acknowledged");return true;
    }catch(RuntimeException failure){repository.retry(h.handoffId(),worker,"ING_DOWNSTREAM_UNAVAILABLE",Duration.ofSeconds(30),false);metrics.handoff("retry");safeLog(h.handoffId(),"ING_DOWNSTREAM_UNAVAILABLE");return true;}
  }
  private static void safeLog(java.util.UUID id,String code){LOG.atWarn().addKeyValue("event","HANDOFF_RETRY").addKeyValue("handoffId",id).addKeyValue("errorCode",code).log("Ingestion handoff scheduled for retry");}
}
