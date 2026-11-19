package it.comune.trieste.ouf.ingestion;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import org.springframework.boot.health.contributor.*;
import org.springframework.stereotype.Component;

/** Compatibility observation only: no migrate, repair, schema creation or application writes. */
@Component("ingestionReadiness")
public final class IngestionReadinessHealthIndicator implements HealthIndicator {
  private record Migration(String version,String script,int checksum){}
  // Release-owned SQL resource contract. CI compares every checksum with native Flyway.
  private static final List<Migration> EXPECTED=List.of(
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
      new Migration("18","V18__incident_publication_bookmarks.sql",1281376280));
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
        int count=0,rank=0;
        try(ResultSet rows=sql.executeQuery("SELECT installed_rank,version,script,type,checksum,success FROM ouf_ingestion.flyway_schema_history ORDER BY installed_rank LIMIT 21")){
          boolean first=true;
          while(rows.next()){
            int installed=rows.getInt(1);
            String version=rows.getString(2),script=rows.getString(3),type=rows.getString(4);
            Integer checksum=(Integer)rows.getObject(5);
            boolean success=rows.getBoolean(6);
            if(first&&version==null&&"SCHEMA".equals(type)&&"\"ouf_ingestion\"".equals(script)&&checksum==null&&success){first=false;continue;}
            first=false;
            if(count>=EXPECTED.size()||!"SQL".equals(type)||!success||checksum==null)return incompatible();
            Migration expected=EXPECTED.get(count++);
            if(!expected.version().equals(version)||!expected.script().equals(script)||expected.checksum()!=checksum)return incompatible();
            if(installed<=rank)return incompatible();
            rank=installed;
          }
        }
        if(count!=EXPECTED.size())return incompatible();
        return Health.up().withDetail("postgresVersion",postgres).withDetail("flywayRank",rank)
            .withDetail("schemaVersion","18").withDetail("sqlMigrations",count)
            .withDetail("schemaCompatibility","EXACT_RELEASE_HISTORY")
            .withDetail("transactionReadOnly",true).build();
      }finally{connection.rollback();}
    }catch(SQLException|RuntimeException unavailable){
      return Health.down().withDetail("reason","ING_DB_READINESS_FAILED").build();
    }
  }
  private static Health incompatible(){return Health.down().withDetail("reason","ING_SCHEMA_COMPATIBILITY_FAILED").build();}
}
