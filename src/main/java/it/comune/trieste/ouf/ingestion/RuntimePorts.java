package it.comune.trieste.ouf.ingestion;
import java.time.Duration;
import java.util.*;
public final class RuntimePorts {private RuntimePorts(){}
  public interface ActiveBundlePort {ExecutionBundle loadAndVerify(String sourceId);}
  public interface AdapterResolver {AdapterSpi resolve(ExecutionBundle bundle);}
  public interface OnboardingReviewPort {Receipt request(UUID issueId,String sourceId,String evidenceRef,String correlationId,String idempotencyKey);record Receipt(String reviewRef,boolean durable){} }
  public interface GatewaySourcePort {byte[] fetch(String governedBindingRef,Map<String,Object> request,String correlationId);}
  public static final class GatewayFailure extends RuntimeException {
    private final String safeCode;private final AdapterSpi.ErrorClass errorClass;private final Duration retryAfter;
    public GatewayFailure(String safeCode,AdapterSpi.ErrorClass errorClass){this(safeCode,errorClass,null);}
    public GatewayFailure(String safeCode,AdapterSpi.ErrorClass errorClass,Duration retryAfter){super(safeCode);this.safeCode=safeCode;this.errorClass=errorClass;this.retryAfter=retryAfter;if(retryAfter!=null&&(retryAfter.isNegative()||retryAfter.isZero()))throw new IllegalArgumentException("ING_RETRY_AFTER_INVALID");}
    public String safeCode(){return safeCode;}public AdapterSpi.ErrorClass errorClass(){return errorClass;}public Optional<Duration> retryAfter(){return Optional.ofNullable(retryAfter);}
  }
  public interface ManagedObjectPort {byte[] read(String objectRef,long expectedSize,String expectedHash);}
  public interface DataLakePort {
    Receipt persist(Zone zone,UUID runId,String sourceObjectId,byte[] content,String contentHash,String idempotencyKey);
    enum Zone { RAW, NORMALIZED, CURATED }
    record Receipt(String objectRef,boolean durable){public Receipt{if(objectRef==null||objectRef.isBlank())throw new IllegalArgumentException("ING_DATALAKE_REF_REQUIRED");}}
  }
  public interface SemanticPort {void preflight(Collection<String> pinnedReferences);}
  public interface DurableHandoffPort {Receipt deliver(UUID handoffId,Map<String,Object> payload,String idempotencyKey);record Receipt(String reference,boolean durable){}}
  public interface ReplayExecutionPort {Receipt execute(UUID replayId,UUID quarantineId,String targetBundleRef,String correlationId);default Receipt execute(ReplayPlan plan){return execute(plan.replayId(),plan.quarantineId(),plan.targetBundleRef(),plan.correlationId());}record ReplayPlan(UUID replayId,UUID quarantineId,String mode,UUID originalRunId,String originalBundleRef,String targetBundleRef,String rawObjectRef,UUID parentAttemptId,String correlationId){}record Receipt(UUID processingAttemptId,String downstreamReceiptRef,boolean durable){}}
}
