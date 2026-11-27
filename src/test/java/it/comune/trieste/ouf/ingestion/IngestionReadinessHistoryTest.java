package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.IngestionReadinessHealthIndicator.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.health.contributor.Status;

class IngestionReadinessHistoryTest {
  private final List<HistoryRow> rows = new ArrayList<>();

  IngestionReadinessHistoryTest() {
    for (Migration migration : EXPECTED) {
      rows.add(new HistoryRow(rows.size() + 1, migration.version(), migration.script(),
          "SQL", migration.checksum(), true));
    }
  }

  private void append(String version, String type, boolean success) {
    rows.add(new HistoryRow(rows.getLast().rank() + 1, version, "fixture.sql", type, 42, success));
  }

  private void assertReady(String state, int futureCount) {
    var health = historyHealth(170000, rows);
    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("schemaCompatibility", state)
        .containsEntry("futureMigrations", futureCount).containsEntry("sqlMigrations", EXPECTED.size() + futureCount);
  }

  private void assertIncompatible() {
    var health = historyHealth(170000, rows);
    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails()).containsOnlyKeys("reason").containsEntry("reason", "ING_SCHEMA_COMPATIBILITY_FAILED");
  }

  @Test void exactHistoryWithOrWithoutSchemaMarker() {
    assertReady("EXACT", 0);
    rows.addFirst(new HistoryRow(0, null, "\"ouf_ingestion\"", "SCHEMA", null, true));
    assertReady("EXACT", 0);
  }

  @Test void acceptsSixteenFutureVersionsAndRejectsOverflow() {
    for (int i = 1; i <= 16; i++) append(EXPECTED.getLast().version() + "." + i, "SQL", true);
    assertReady("FUTURE_MIGRATIONS_PRESENT", 16);
    append(EXPECTED.getLast().version() + ".17", "SQL", true);
    assertIncompatible();
  }

  @Test void failedFutureMigration() {
    append(EXPECTED.getLast().version() + ".1", "SQL", false);
    assertIncompatible();
  }

  @Test void rejectsNonSqlAndUnversionedTail() {
    append(EXPECTED.getLast().version() + ".1", "JDBC", true);
    assertIncompatible();
    rows.removeLast();
    append(null, "SQL", true);
    assertIncompatible();
  }

  @ParameterizedTest
  @ValueSource(strings = {"9", "latest", "21-beta"})
  void rejectsOlderOrNonNumericVersion(String version) {
    append(version, "SQL", true);
    assertIncompatible();
  }

  @Test void rejectsEquivalentVersion() {
    append(EXPECTED.getLast().version() + ".0", "SQL", true);
    assertIncompatible();
  }

  @Test void rejectsAlteredChecksumEvenWithFutureMigration() {
    HistoryRow row = rows.getFirst();
    rows.set(0, new HistoryRow(row.rank(), row.version(), row.script(), row.type(), row.checksum() + 1, true));
    append(EXPECTED.getLast().version() + ".1", "SQL", true);
    assertIncompatible();
  }

  @Test void prefixRequiresTheExpectedScriptTypeAndSuccess() {
    HistoryRow row = rows.getFirst();
    rows.set(0, new HistoryRow(row.rank(), row.version(), "renamed.sql", row.type(), row.checksum(), true));
    assertIncompatible();
    rows.set(0, new HistoryRow(row.rank(), row.version(), row.script(), "JDBC", row.checksum(), true));
    assertIncompatible();
    rows.set(0, new HistoryRow(row.rank(), row.version(), row.script(), row.type(), row.checksum(), false));
    assertIncompatible();
  }

  @Test void futureRowCannotReplaceMissingPrefixRow() {
    rows.removeLast();
    assertIncompatible();
    append(EXPECTED.getLast().version() + ".1", "SQL", true);
    assertIncompatible();
  }
}
