package it.comune.trieste.ouf.ingestion;

import java.util.UUID;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Bounded automatic control loop. No data transfer or remote call holds a DB transaction. */
@Component
@ConditionalOnProperty(name="ouf.ingestion.activation.enabled",havingValue="true")
public class ActivationLoop {
 private static final Logger LOG=LoggerFactory.getLogger(ActivationLoop.class);
 private final PublicationGatewayClient gateway;private final ActivationRepository publications;private final RunCoordinator coordinator;
 private final String worker="activation-"+UUID.randomUUID();private String cursor="";
 public ActivationLoop(PublicationGatewayClient gateway,ActivationRepository publications,RunCoordinator coordinator){this.gateway=gateway;this.publications=publications;this.coordinator=coordinator;}
 @Scheduled(fixedDelayString="${ouf.ingestion.activation.poll-ms:10000}",initialDelayString="${ouf.ingestion.activation.initial-delay-ms:1000}")
 public void tick(){
  try{var page=gateway.discover(cursor);for(var item:page.items())try{publications.reconcile(gateway.decode(item));}catch(RuntimeException invalid){LOG.warn("ING_ACTIVATION_PUBLICATION_REJECTED");}cursor=page.nextAfter();}
  catch(RuntimeException unavailable){LOG.warn("ING_ACTIVATION_DISCOVERY_UNAVAILABLE");return;}
  try{coordinator.dispatchOne(worker);}catch(RuntimeException failure){LOG.warn("ING_ACTIVATION_DISPATCH_FAILED");}
 }
}
