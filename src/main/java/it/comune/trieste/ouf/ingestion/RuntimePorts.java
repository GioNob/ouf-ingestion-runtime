package it.comune.trieste.ouf.ingestion;
import java.util.*;
public final class RuntimePorts {private RuntimePorts(){}
  public interface ActiveBundlePort {ExecutionBundle loadAndVerify(String sourceId);}
  public interface GatewaySourcePort {byte[] fetch(String governedBindingRef,Map<String,Object> request,String correlationId);}
  public static final class GatewayFailure extends RuntimeException {
    private final String safeCode;private final AdapterSpi.ErrorClass errorClass;
    public GatewayFailure(String safeCode,AdapterSpi.ErrorClass errorClass){super(safeCode);this.safeCode=safeCode;this.errorClass=errorClass;}
    public String safeCode(){return safeCode;}public AdapterSpi.ErrorClass errorClass(){return errorClass;}
  }
  public interface ManagedObjectPort {byte[] read(String objectRef,long expectedSize,String expectedHash);}
  public interface SemanticPort {void preflight(Collection<String> pinnedReferences);}
  public interface DurableHandoffPort {Receipt deliver(UUID handoffId,Map<String,Object> payload,String idempotencyKey);record Receipt(String reference,boolean durable){}}
  public interface ReplayExecutionPort {Receipt execute(UUID replayId,UUID quarantineId,String targetBundleRef,String correlationId);record Receipt(UUID processingAttemptId,String downstreamReceiptRef,boolean durable){}}
}
