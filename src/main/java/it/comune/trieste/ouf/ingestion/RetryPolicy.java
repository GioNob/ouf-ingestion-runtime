package it.comune.trieste.ouf.ingestion;

import java.net.http.HttpHeaders;
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

  static long seconds(Duration wait) {
    if(wait.getSeconds()==Long.MAX_VALUE)return Long.MAX_VALUE;
    return wait.getSeconds()+(wait.getNano()==0?0:1);
  }

  static Duration retryAfter(HttpHeaders headers) {
    String value = headers.firstValue("Retry-After").orElse("").strip();
    if (value.matches("[0-9]+")) {
      try { return Duration.ofSeconds(Math.max(1, Long.parseLong(value))); }
      catch (NumberFormatException overflow) { return Duration.ofSeconds(Long.MAX_VALUE); }
    }
    try {
      Duration wait = Duration.between(Instant.now(), ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
      return wait.isNegative() || wait.isZero() ? null : wait;
    } catch (java.time.format.DateTimeParseException invalid) { return null; }
  }
}
