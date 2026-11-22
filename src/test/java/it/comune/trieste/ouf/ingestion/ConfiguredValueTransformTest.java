package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.ConfiguredValueTransform.*;
import static org.assertj.core.api.Assertions.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfiguredValueTransformTest {
  @Test void identityPreservesLexicalCellsAndHistoricalValues() {
    for (Object value : Arrays.asList(null, "", " 001,0 ", Map.of("nested", List.of(1))))
      assertThat(IDENTITY.apply(value, 1)).isSameAs(value);
  }

  @Test void decimalsPreserveExactDigitsAcrossBothSeparators() {
    for (String[] example : List.of(
        new String[]{"-12.3400", "-12.34"}, new String[]{"-0.00", "0"}, new String[]{"1000", "1000"},
        new String[]{"\u2003+00045.6500\t", "45.65"}, new String[]{"-000", "0"},
        new String[]{"0.0000000000000000010", "0.000000000000000001"},
        new String[]{"12345678901234567890.123456789012345678", "12345678901234567890.123456789012345678"},
        new String[]{"1" + "0".repeat(37), "1" + "0".repeat(37)},
        new String[]{"1." + "0".repeat(254), "1"})) {
      assertThat(DECIMAL_DOT_V1.apply(example[0], 1)).isEqualTo(example[1]);
      assertThat(DECIMAL_COMMA_V1.apply(example[0].replace('.', ','), 1)).isEqualTo(example[1]);
    }
  }

  @Test void decimalsRejectWrongSeparatorSyntaxAndOverflow() {
    for (String value : List.of("12,34", "12 .3", "+ 12", "1e3", "1,234.56", "NaN", "+-1.0",
        ".5", "3.", "1".repeat(39), "1" + "0".repeat(38), "0.0000000000000000001", "1." + "0".repeat(255))) {
      invalid(DECIMAL_DOT_V1, value);
      invalid(DECIMAL_COMMA_V1, value.replace(',', '#').replace('.', ',').replace('#', '.'));
    }
  }

  @Test void datesRequireExactCalendarDatesAndReturnIso() {
    assertThat(DATE_DMY_V1.apply(" 29/02/2024\t", 1)).isEqualTo("2024-02-29");
    for (String value : List.of("0001-01-01", "2024-02-29", "9999-12-31"))
      assertThat(DATE_ISO_V1.apply("\t" + value + " ", 1)).isEqualTo(value);
    for (String value : List.of("29/02/2023", "31/04/2024", "1/02/2024", "2024-02-29", "29/02/0000"))
      invalid(DATE_DMY_V1, value);
    for (String value : List.of("2023-02-29", "2024-04-31", "2024-2-29", "29/02/2024", "0000-02-29",
        "2024-02-29Z", "2024- 2-29"))
      invalid(DATE_ISO_V1, value);
  }

  @Test void typedRulesOmitBlankCellsAndRejectInvalidValuesWithoutDisclosingInput() {
    for (var rule : List.of(DECIMAL_DOT_V1, DECIMAL_COMMA_V1, DATE_ISO_V1, DATE_DMY_V1)) {
      for (Object value : Arrays.asList(null, "", " \t\u2003"))
        assertThat(rule.apply(value, 7)).isNull();
      for (Object value : List.of(1, true, "private-cell"))
        invalid(rule, value);
    }
  }

  @Test void unknownTransformHasADistinctSafeCode() {
    for (String rule : Arrays.asList(null, "", "GUESS", "decimal_dot_v1"))
      assertThatThrownBy(() -> ConfiguredValueTransform.resolve(rule))
          .isInstanceOf(IllegalArgumentException.class).hasMessage("ING_TRANSFORM_UNSUPPORTED").hasNoCause();
  }

  private static void invalid(ConfiguredValueTransform rule, Object value) {
    assertThatThrownBy(() -> rule.apply(value, 7))
        .isInstanceOf(InvalidValue.class).hasMessage("ING_TRANSFORM_VALUE_INVALID").hasNoCause();
  }
}
