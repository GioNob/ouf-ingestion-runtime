package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.*;

@SpringBootTest class IngestionReadinessRuntimeTest {
  @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",()->required("OUF_ING_DB_URL"));r.add("spring.datasource.username",()->required("OUF_ING_DB_USER"));r.add("spring.datasource.password",()->required("OUF_ING_DB_PASSWORD"));}
  @Autowired IngestionReadinessHealthIndicator readiness;
  @Test void readinessRequiresAuthenticatedPostgresAndSuccessfulFlyway(){var health=readiness.health();assertThat(health.getStatus()).isEqualTo(Status.UP);assertThat(health.getDetails()).containsKeys("postgresVersion","flywayRank");assertThat(((Number)health.getDetails().get("postgresVersion")).intValue()).isGreaterThanOrEqualTo(170000);assertThat(((Number)health.getDetails().get("flywayRank")).intValue()).isGreaterThanOrEqualTo(13);}
  private static String required(String n){String v=System.getenv(n);if(v==null)throw new IllegalStateException(n+" required");return v;}
}
