package it.comune.trieste.ouf.ingestion;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.flywaydb.core.api.MigrationVersion;
import org.springframework.boot.health.contributor.*;
import org.springframework.stereotype.Component;

/** Compatibility observation only: no migrate, repair, schema creation or application writes. */
@Component("ingestionReadiness")
public final class IngestionReadinessHealthIndicator implements HealthIndicator {
  record Migration(String version,String script,int checksum){}
  record HistoryRow(int rank, String version, String script, String type, Integer checksum, boolean success) {
    boolean isSchemaMarker() {
      return version == null && "SCHEMA".equals(type) && "\"ouf_ingestion\"".equals(script) && checksum == null && success;
    }
  }
  private static final int MAX_FUTURE_MIGRATIONS = 16;
  private static final Pattern NUMERIC_VERSION = Pattern.compile("[0-9]+(?:[._][0-9]+)*");
  // Release-owned SQL resource contract. CI compares every checksum with native Flyway.
  static final List<Migration> EXPECTED=List.of(
      new Migration("1","V1__ingestion_runtime_core.sql",28809713),
      new Migration("2","V2__durable_ack_and_replay.sql",-1190230229),
      new Migration("3","V3__trusted_operations.sql",1677862093),
      new Migration("4","V4__scheduler_and_source_health.sql",960152441),
      new Migration("5","V5__replay_execution_lifecycle.sql",-2033393121),
      new Migration("6","V6__protected_operational_log.sql",-2023917644),
      new Migration("7","V7__execution_worker_and_snapshot.sql",-848911848),
      new Migration("8","V8__schema_surveillance.sql",-61585214),
      new Migration("9","V9__runtime_issue_governance.sql",-1413097987),
      new Migration("10","V10__quarantine_lifecycle.sql",1627974653),
      new Migration("11","V11__classified_source_resilience.sql",-1835304128),
      new Migration("12","V12__governed_lifecycle_and_historical_replay.sql",-1198212964),
      new Migration("13","V13__pressure_admission_retry_budget.sql",1850262212),
      new Migration("14","V14__published_activation_schedule.sql",-1324686996),
      new Migration("15","V15__human_recovery_sessions.sql",-1837994869),
      new Migration("16","V16__incident_observation_time.sql",-405299983),
      new Migration("17","V17__incident_publication_and_cursor.sql",-1241321010),
      new Migration("18","V18__incident_publication_bookmarks.sql",1281376280),
      new Migration("21","V21__repeatable_managed_publication.sql",-1354813991));
  private final DataSource source;
  public IngestionReadinessHealthIndicator(DataSource source){this.source=source;}

  @Override public Health health(){
    try(Connection connection=source.getConnection()){
      connection.setReadOnly(true);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      connection.setAutoCommit(false);
      try(Statement sql=connection.createStatement()){
        sql.setQueryTimeout(5);
        sql.execute("SET LOCAL statement_timeout='5s'");
        sql.execute("SET LOCAL lock_timeout='1s'");
        sql.execute("SET LOCAL idle_in_transaction_session_timeout='5s'");
        int postgres;
        try(ResultSet facts=sql.executeQuery("SELECT current_setting('server_version_num')::integer,current_setting('server_encoding'),current_setting('transaction_read_only')")){
          if(!facts.next())return incompatible();
          postgres=facts.getInt(1);
          if(postgres<170000||postgres>=180000||!"UTF8".equals(facts.getString(2))||!"on".equals(facts.getString(3)))return incompatible();
        }
        // Allow for the optional SCHEMA marker and read one extra row to detect overflow.
        int limit = EXPECTED.size() + MAX_FUTURE_MIGRATIONS + 2;
        List<HistoryRow> history = new ArrayList<>();
        try(ResultSet rows=sql.executeQuery("SELECT installed_rank,version,script,type,checksum,success FROM ouf_ingestion.flyway_schema_history ORDER BY installed_rank LIMIT "+limit)){
          while(rows.next()) {
            history.add(new HistoryRow(rows.getInt(1), rows.getString(2), rows.getString(3),
                rows.getString(4), rows.getObject(5, Integer.class), rows.getBoolean(6)));
          }
        }
        return historyHealth(postgres, history);
      }finally{connection.rollback();}
    }catch(SQLException|RuntimeException unavailable){
      return Health.down().withDetail("reason","ING_DB_READINESS_FAILED").build();
    }
  }

  static Health historyHealth(int postgres, List<HistoryRow> history) {
    int start = !history.isEmpty() && history.getFirst().isSchemaMarker() ? 1 : 0;
    int sqlCount = history.size() - start;
    int futureCount = sqlCount - EXPECTED.size();
    if (futureCount < 0 || futureCount > MAX_FUTURE_MIGRATIONS) return incompatible();

    String schemaVersion = EXPECTED.getLast().version();
    MigrationVersion release = MigrationVersion.fromVersion(schemaVersion);
    int rank = 0;
    for (int i = 0; i < sqlCount; i++) {
      HistoryRow row = history.get(start + i);
      if (row.rank() <= rank || !row.success() || !"SQL".equals(row.type())) return incompatible();
      rank = row.rank();
      if (i < EXPECTED.size()) {
        Migration expected = EXPECTED.get(i);
        if (!expected.version().equals(row.version()) || !expected.script().equals(row.script())
            || !Objects.equals(expected.checksum(), row.checksum())) return incompatible();
      } else if (row.version() == null || !NUMERIC_VERSION.matcher(row.version()).matches()
          || MigrationVersion.fromVersion(row.version()).compareTo(release) <= 0) {
        return incompatible();
      }
    }
    return Health.up().withDetail("postgresVersion", postgres).withDetail("flywayRank", rank)
        .withDetail("schemaVersion", schemaVersion).withDetail("sqlMigrations", sqlCount)
        .withDetail("schemaCompatibility", futureCount == 0 ? "EXACT" : "FUTURE_MIGRATIONS_PRESENT")
        .withDetail("futureMigrations", futureCount).withDetail("transactionReadOnly", true).build();
  }
  private static Health incompatible(){return Health.down().withDetail("reason","ING_SCHEMA_COMPATIBILITY_FAILED").build();}
}
