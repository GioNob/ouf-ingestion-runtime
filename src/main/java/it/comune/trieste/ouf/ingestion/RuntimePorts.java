package it.comune.trieste.ouf.ingestion;
import java.util.*;
public final class RuntimePorts {private RuntimePorts(){}
  public interface ActiveBundlePort {ExecutionBundle loadAndVerify(String sourceId);}
  public interface GatewaySourcePort {byte[] fetch(String governedBindingRef,Map<String,Object> request,String correlationId);}
  public interface ManagedObjectPort {byte[] read(String objectRef,long expectedSize,String expectedHash);}
  public interface SemanticPort {void preflight(Collection<String> pinnedReferences);}
  public interface DurableHandoffPort {Receipt deliver(UUID handoffId,Map<String,Object> payload,String idempotencyKey);record Receipt(String reference,boolean durable){}}
}
