package it.comune.trieste.ouf.ingestion;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Component;

@Component
public class IngestionMetrics {
  private final MeterRegistry registry;
  public IngestionMetrics(MeterRegistry registry){this.registry=registry;}
  public void handoff(String outcome){registry.counter("ouf.ingestion.handoff.total","outcome",outcome).increment();}
  public void quarantine(String reason){registry.counter("ouf.ingestion.quarantine.total","reason",safe(reason)).increment();}
  public void replay(String outcome){registry.counter("ouf.ingestion.replay.total","outcome",outcome).increment();}
  private static String safe(String value){return value!=null&&value.matches("[A-Z0-9_]{1,80}")?value:"OTHER";}
}
