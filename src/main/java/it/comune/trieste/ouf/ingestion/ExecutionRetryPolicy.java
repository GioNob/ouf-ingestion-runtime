package it.comune.trieste.ouf.ingestion;

import java.time.*;
import java.util.Map;

/** Retry budgets come only from the immutable publication snapshot. Missing policy fails closed. */
record ExecutionRetryPolicy(int attempts, long elapsedSeconds, long backoffSeconds) {
  static ExecutionRetryPolicy from(ExecutionBundle bundle) {
    Object value=bundle.configuration().get("syncProfile");
    if(!(value instanceof Map<?,?> sync) || !(sync.get("operationalPolicy") instanceof Map<?,?> policy))
      return new ExecutionRetryPolicy(0,0,0);
    return new ExecutionRetryPolicy((int)bounded(policy,"maxRetryAttempts",0,100),
        bounded(policy,"maxRetryElapsedSeconds",1,86400),bounded(sync,"retryBackoffSeconds",1,86400));
  }
  private static long bounded(Map<?,?> map,String key,long min,long max) {
    if(!(map.get(key) instanceof Number n)||n.doubleValue()!=n.longValue()||n.longValue()<min||n.longValue()>max)
      throw new IllegalStateException("ING_RETRY_POLICY_INVALID");
    return n.longValue();
  }
  long delay(Duration retryAfter) {
    if(retryAfter==null)return backoffSeconds;
    if(retryAfter.isNegative()||retryAfter.isZero())throw new IllegalArgumentException("ING_RETRY_AFTER_INVALID");
    return Math.max(backoffSeconds,Math.addExact(retryAfter.getSeconds(),retryAfter.getNano()==0?0:1));
  }
}
