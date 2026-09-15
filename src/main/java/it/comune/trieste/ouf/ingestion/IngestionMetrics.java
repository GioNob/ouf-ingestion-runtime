package it.comune.trieste.ouf.ingestion;

import io.micrometer.core.instrument.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

@Component
public class IngestionMetrics {
  private final MeterRegistry registry;private final JdbcClient sql;
  public IngestionMetrics(MeterRegistry registry,JdbcClient sql){this.registry=registry;this.sql=sql;gauge("ouf.ingestion.runs.active","select count(*) from ouf_ingestion.ing_run where state in ('READY','PREFLIGHT','RUNNING','DRAINING')");gauge("ouf.ingestion.schedule.due","select count(*) from ouf_ingestion.ing_schedule where state='ACTIVE' and next_run_at<=transaction_timestamp()");gauge("ouf.ingestion.outbox.backlog","select count(*) from ouf_ingestion.handoff_outbox where state<>'ACKED'");gauge("ouf.ingestion.quarantine.open","select count(*) from ouf_ingestion.ing_quarantine where state in ('OPEN','REPLAY_REQUESTED')");gauge("ouf.ingestion.replay.backlog","select count(*) from ouf_ingestion.replay_execution where state in ('QUEUED','RUNNING','RETRY_WAIT')");gauge("ouf.ingestion.sources.circuit_open","select count(*) from ouf_ingestion.source_health where status='PAUSED'");gauge("ouf.ingestion.retry.window.attempts","select coalesce(sum(retry_attempts_in_window),0) from ouf_ingestion.source_health");gauge("ouf.ingestion.pressure.level","select case state when 'NORMAL' then 0 when 'SOFT_PRESSURE' then 1 when 'RECOVERY' then 2 else 3 end from ouf_ingestion.runtime_pressure_policy where policy_key='GLOBAL'");gauge("ouf.ingestion.watermark.max_age_seconds","select coalesce(extract(epoch from transaction_timestamp()-min(committed_at)),0) from ouf_ingestion.ing_watermark");}
  public void handoff(String outcome){registry.counter("ouf.ingestion.handoff.total","outcome",outcome).increment();}
  public void quarantine(String reason){registry.counter("ouf.ingestion.quarantine.total","reason",safe(reason)).increment();}
  public void replay(String outcome){registry.counter("ouf.ingestion.replay.total","outcome",outcome).increment();}
  private void gauge(String name,String query){Gauge.builder(name,()->value(query)).register(registry);}
  private double value(String query){try{Number n=sql.sql(query).query(Number.class).single();return n.doubleValue();}catch(RuntimeException unavailable){return Double.NaN;}}
  private static String safe(String value){return value!=null&&value.matches("[A-Z0-9_]{1,80}")?value:"OTHER";}
}
