package it.comune.trieste.ouf.ingestion;

import java.net.http.HttpHeaders;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** One limiter per route class in the process. No monitor is held during a wait or exchange. */
@Component
final class GatewayPacing {
  final int batchBytes, batchItems;
  final Duration fallbackTtl;
  final Lane lake, handoff, references;
  private final long capNanos, otherInterval;
  static final HttpHeaders NO_HEADERS = HttpHeaders.of(java.util.Map.of(), (k,v) -> true);

  @Autowired GatewayPacing(Environment env) { this(env::getProperty); }
  GatewayPacing(Function<String,String> properties) {
    batchBytes = positive(properties, "reference-batch-bytes", 262144);
    batchItems = positive(properties, "reference-batch-items", 1000);
    if (batchItems > 1000) throw new IllegalArgumentException("ING_REFERENCE_BATCH_LIMIT_INVALID");
    fallbackTtl = Duration.ofSeconds(positive(properties, "reference-fallback-seconds", 300));
    capNanos = Duration.ofSeconds(positive(properties, "server-wait-max-seconds", 86400)).toNanos();
    otherInterval = interval(positive(properties, "other-retry-per-minute", 60));
    lake = new Lane(positive(properties, "lake-per-minute", 60));
    handoff = new Lane(positive(properties, "handoff-per-minute", 60));
    references = new Lane(positive(properties, "references-per-minute", 60));
  }
  private static int positive(Function<String,String> properties, String name, int developmentDefault) {
    String value = properties.apply("ouf.ingestion.gateway." + name);
    int limit = value == null ? developmentDefault : Integer.parseInt(value);
    if (limit < 1) throw new IllegalArgumentException("ING_GATEWAY_LIMIT_INVALID");
    return limit;
  }

  private static long interval(int perMinute) { return Math.ceilDiv(Duration.ofMinutes(1).toNanos(), perMinute); }

  Duration retryDelay(Lane lane, HttpHeaders headers) {
    Duration wait = RetryPolicy.serverWait(headers, Instant.now(), lane == null ? otherInterval : lane.interval, capNanos);
    if (lane != null) lane.postpone(System.nanoTime(), wait.toNanos());
    return wait;
  }

  final class Lane {
    private final long interval;
    private long next = System.nanoTime(), reset = next, advertisedInterval;
    Lane(int perMinute) { interval = interval(perMinute); }

    void acquireForRead() { acquire(Duration.ofMinutes(1)); }
    void acquire() { acquire(Duration.ofSeconds(2)); }
    private void acquire(Duration maxWait) {
      long deadline = System.nanoTime() + maxWait.toNanos();
      while (true) {
        long wait;
        synchronized (this) {
          long now = System.nanoTime();
          wait = next - now;
          if (wait <= 0) {
            next = now + (reset - now > 0 ? Math.max(interval, advertisedInterval) : interval);
            return;
          }
        }
        // Hand longer waits to the durable scheduler, leaving time for claim fencing.
        if (wait > deadline - System.nanoTime())
          throw new RuntimePorts.GatewayFailure("ING_GATEWAY_PACING_WAIT", AdapterSpi.ErrorClass.TRANSIENT_SOURCE, Duration.ofNanos(wait));
        try { Thread.sleep(Duration.ofNanos(wait)); }
        catch (InterruptedException cancelled) {
          Thread.currentThread().interrupt();
          throw new RuntimePorts.GatewayFailure("ING_GATEWAY_PACING_INTERRUPTED", AdapterSpi.ErrorClass.CONFIGURATION);
        }
      }
    }

    synchronized void observeQuota(HttpHeaders headers) {
      long now = System.nanoTime();
      for (String prefix : new String[]{"X-RateLimit-", "RateLimit-"}) {
        BigDecimal remaining = RetryPolicy.decimal(headers, prefix + "Remaining");
        Long window = RetryPolicy.resetNanos(headers, prefix, Instant.now(), capNanos);
        if (remaining == null || window == null || window == 0) continue;
        reset = now + window;
        advertisedInterval = BigDecimal.valueOf(window).divide(remaining.max(BigDecimal.ONE), 0, RoundingMode.CEILING).longValueExact();
        postpone(now, advertisedInterval);
        return;
      }
    }
    private synchronized void postpone(long now, long wait) { if (next - now < wait) next = now + wait; }
  }
}
