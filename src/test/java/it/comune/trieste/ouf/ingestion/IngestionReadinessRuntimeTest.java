package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;

@SpringBootTest class IngestionReadinessRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired IngestionReadinessHealthIndicator readiness;
  @Autowired JdbcTemplate admin;

  @Test void readinessRequiresAuthenticatedPostgresAndSuccessfulFlyway() {
    var health = readiness.health();
    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("schemaVersion", "22").containsEntry("schemaCompatibility", "EXACT")
        .containsEntry("futureMigrations", 0).containsEntry("transactionReadOnly", true);
    assertThat((Integer) health.getDetails().get("postgresVersion")).isBetween(170000, 179999);
    assertThat((Integer) health.getDetails().get("flywayRank")).isGreaterThanOrEqualTo(13);
  }

  @Test void checksTheWholePrefixAndBoundedTailOnPostgres() {
    // The module suite shares a database. Only change history in this test's private database.
    String database = "readiness_" + UUID.randomUUID().toString().replace("-", "");
    admin.execute("CREATE DATABASE " + database);
    try {
      var source = new PGSimpleDataSource();
      source.setUrl(required("OUF_ING_DB_URL"));
      source.setDatabaseName(database);
      source.setUser(required("OUF_ING_DB_USER"));
      source.setPassword(required("OUF_ING_DB_PASSWORD"));
      Flyway.configure().dataSource(source).schemas("ouf_ingestion").load().migrate();
      var probe = new IngestionReadinessHealthIndicator(source);
      var sql = new JdbcTemplate(source);
      String history = "ouf_ingestion.flyway_schema_history";
      String insertFuture = """
          INSERT INTO ouf_ingestion.flyway_schema_history
            (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success)
          SELECT max(installed_rank) + 1, ?, 'CI probe', 'SQL', 'probe.sql', 42, current_user, 0, true
          FROM ouf_ingestion.flyway_schema_history
          """;
      String release = (String) probe.health().getDetails().get("schemaVersion");
      assertThat(release).isNotBlank();
      String future = release + ".1";
      sql.update(insertFuture, future);
      var health = probe.health();
      assertThat(health.getStatus()).isEqualTo(Status.UP);
      assertThat(health.getDetails()).containsEntry("schemaCompatibility", "FUTURE_MIGRATIONS_PRESENT")
          .containsEntry("futureMigrations", 1);

      sql.update("UPDATE " + history + " SET success = false WHERE version = ?", future);
      assertIncompatible(probe);
      sql.update("UPDATE " + history + " SET success = true WHERE version = ?", future);
      for (int i = 2; i <= 17; i++) sql.update(insertFuture, release + "." + i);
      assertIncompatible(probe);
      sql.update("DELETE FROM " + history + " WHERE version = ?", release + ".17");
      assertThat(probe.health().getDetails()).containsEntry("futureMigrations", 16);

      sql.update("UPDATE " + history + " SET checksum = checksum + 1 WHERE version = ?", release);
      assertIncompatible(probe);
      sql.update("DELETE FROM " + history + " WHERE version = ?", release);
      assertIncompatible(probe);
    } finally {
      admin.execute("DROP DATABASE " + database);
    }
  }

  private static void assertIncompatible(IngestionReadinessHealthIndicator probe) {
    var health = probe.health();
    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails()).containsEntry("reason", "ING_SCHEMA_COMPATIBILITY_FAILED");
  }
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
