package it.comune.trieste.ouf.ingestion;

import java.net.http.HttpHeaders;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/** OUF finite automatic retry profile v1. Durations include time spent doing I/O. */
final class RetryPolicy {
  static final int ATTEMPTS = 128, SECONDS = 86400;
  private static final long[] BACKOFF = {5, 15, 60, 300, 900};
  private RetryPolicy() {}

  static long delay(int attempt, long minimum, Duration retryAfter) {
    long base = Math.max(minimum, BACKOFF[Math.clamp(attempt - 1, 0, BACKOFF.length - 1)]);
    long delay = base + ThreadLocalRandom.current().nextLong(base / 5 + 1);
    if (retryAfter == null) return delay;
    return Math.max(delay, seconds(retryAfter));
  }

  static long afterProgress(Duration retryAfter) {
    return retryAfter == null ? 0 : seconds(retryAfter);
  }

  static long seconds(Duration wait) {
    if(wait.getSeconds()==Long.MAX_VALUE)return Long.MAX_VALUE;
    return wait.getSeconds()+(wait.getNano()==0?0:1);
  }

  // The caller uses this same sample for its lane and its durable retry.
  static Duration serverWait(HttpHeaders headers, Instant now, long fallbackNanos, long capNanos) {
    Long nanos = null;
    String value = headers.firstValue("Retry-After").orElse("").strip();
    if (value.matches("[0-9]+")) {
      nanos = boundedNanos(new BigDecimal(value), capNanos);
    } else if (!value.isEmpty()) {
      try {
        Instant at = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        Duration wait = Duration.between(now, at);
        nanos = wait.isNegative() ? 0L : wait.compareTo(Duration.ofNanos(capNanos)) >= 0 ? capNanos : wait.toNanos();
      } catch (java.time.format.DateTimeParseException invalid) { /* Try quota headers. */ }
    }
    if (nanos == null) nanos = resetNanos(headers, "X-RateLimit-", now, capNanos);
    if (nanos == null) nanos = resetNanos(headers, "RateLimit-", now, capNanos);
    long base = Math.clamp(nanos == null ? fallbackNanos : nanos, 1L, capNanos);
    long jitter = ThreadLocalRandom.current().nextLong(base / 5 + 1);
    return Duration.ofNanos(Math.min(base + jitter, capNanos));
  }

  static Long resetNanos(HttpHeaders headers, String prefix, Instant now, long capNanos) {
    BigDecimal seconds = decimal(headers, prefix + "Reset");
    if (seconds == null) return null;
    if (seconds.compareTo(BigDecimal.valueOf(1_000_000_000L)) >= 0)
      seconds = seconds.subtract(BigDecimal.valueOf(now.getEpochSecond())).subtract(BigDecimal.valueOf(now.getNano(), 9));
    return boundedNanos(seconds, capNanos);
  }

  static BigDecimal decimal(HttpHeaders headers, String name) {
    String value = headers.firstValue(name).orElse("").strip();
    return value.matches("[0-9]+(?:\\.[0-9]+)?") ? new BigDecimal(value) : null;
  }

  private static long boundedNanos(BigDecimal seconds, long capNanos) {
    return seconds.movePointRight(9).max(BigDecimal.ZERO).min(BigDecimal.valueOf(capNanos))
        .setScale(0, RoundingMode.CEILING).longValueExact();
  }
}
