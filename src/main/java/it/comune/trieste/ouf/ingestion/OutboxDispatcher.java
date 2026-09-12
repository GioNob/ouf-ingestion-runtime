package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean(RuntimePorts.DurableHandoffPort.class)
public class OutboxDispatcher {
  private final DurablePipelineRepository repository; private final RuntimePorts.DurableHandoffPort downstream;
  public OutboxDispatcher(DurablePipelineRepository repository,RuntimePorts.DurableHandoffPort downstream){this.repository=repository;this.downstream=downstream;}

  public boolean dispatchOne(String worker){
    var claimed=repository.claim(worker,Duration.ofMinutes(2)); if(claimed.isEmpty())return false;
    var h=claimed.orElseThrow();
    try{
      var receipt=downstream.deliver(h.handoffId(),h.payload(),h.idempotencyKey());
      if(!receipt.durable()){repository.retry(h.handoffId(),worker,"ING_ACK_NOT_DURABLE",Duration.ofSeconds(30),false);return true;}
      repository.acknowledge(h.handoffId(),worker,receipt.reference());return true;
    }catch(RuntimeException failure){repository.retry(h.handoffId(),worker,"ING_DOWNSTREAM_UNAVAILABLE",Duration.ofSeconds(30),false);return true;}
  }
}
