package it.comune.trieste.ouf.ingestion;

import java.time.Duration;
import java.util.*;

public class OnboardingReviewDispatcher {
  private final OnboardingReviewRepository queue;
  private final RuntimePorts.OnboardingReviewPort onboarding;
  public OnboardingReviewDispatcher(OnboardingReviewRepository queue,RuntimePorts.OnboardingReviewPort onboarding){
    this.queue=queue;this.onboarding=onboarding;
  }
  public Optional<UUID> dispatchOne(String worker){return dispatchOne(worker,false);}
  Optional<UUID> dispatchOne(String worker,boolean schemaOnly){
    var claim=queue.claim(worker,Duration.ofMinutes(2),schemaOnly);
    if(claim.isEmpty()) return Optional.empty();
    var c=claim.get();
    boolean posted=false;
    try {
      Optional<RuntimePorts.OnboardingReviewPort.Receipt> found=Optional.empty();
      if(c.reconcile()) {
        queue.requireCurrent(c);
        found=onboarding.reconcile(c.issueId(),c.sourceId(),c.idempotencyKey());
        if(found.isEmpty() && !c.schemaNotice()) throw new IllegalStateException("ING_REVIEW_RECONCILIATION_REQUIRED");
      }
      RuntimePorts.OnboardingReviewPort.Receipt receipt;
      if (found.isPresent()) receipt=found.get();
      else {
        queue.requireCurrent(c); // Recheck the remaining lease and budget after a GET.
        posted=true;
        receipt=onboarding.request(c.issueId(),c.sourceId(),c.evidenceRef(),c.correlationId(),c.idempotencyKey());
      }
      if(!receipt.durable() || receipt.reviewRef()==null || receipt.reviewRef().isBlank())
        throw new IllegalStateException("ING_REVIEW_RECEIPT_INVALID");
      queue.acknowledge(c,receipt.reviewRef());
    } catch(RuntimeException failure) {
      var reason=RunCoordinator.classify(failure);
      // Even an invalid POST receipt can represent a durable effect. Reconcile it once.
      boolean retry=reason.errorClass()==AdapterSpi.ErrorClass.TRANSIENT_SOURCE ||
          (posted && "ING_REVIEW_RECEIPT_INVALID".equals(reason.code()));
      queue.fail(c,reason.code(),retry,reason.retryAfter());
    }
    return Optional.of(c.requestId());
  }
}
