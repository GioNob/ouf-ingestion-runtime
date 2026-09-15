package it.comune.trieste.ouf.ingestion;

import org.springframework.boot.actuate.health.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

@Component("ingestionReadiness")
public final class IngestionReadinessHealthIndicator implements HealthIndicator {
  private final JdbcClient sql;
  public IngestionReadinessHealthIndicator(JdbcClient sql){this.sql=sql;}
  @Override public Health health(){try{MapBuilder details=sql.sql("select current_setting('server_version_num')::integer,(select max(installed_rank) from ouf_ingestion.flyway_schema_history where success)").query((rs,n)->new MapBuilder(rs.getInt(1),rs.getInt(2))).single();return Health.up().withDetail("postgresVersion",details.postgres()).withDetail("flywayRank",details.flyway()).build();}catch(RuntimeException unavailable){return Health.down().withDetail("reason","ING_DB_READINESS_FAILED").build();}}
  private record MapBuilder(int postgres,int flyway){}
}
