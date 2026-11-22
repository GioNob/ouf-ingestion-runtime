package it.comune.trieste.ouf.ingestion;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.regex.Pattern;

/** Versioned conversions for explicit property mappings. */
enum ConfiguredValueTransform {
  IDENTITY, DECIMAL_DOT_V1, DECIMAL_COMMA_V1, DATE_ISO_V1, DATE_DMY_V1;

  private static final Pattern DOT = Pattern.compile("[+-]?[0-9]+(\\.[0-9]+)?");
  private static final Pattern COMMA = Pattern.compile("[+-]?[0-9]+(,[0-9]+)?");
  private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/uuuu", Locale.ROOT)
      .withResolverStyle(ResolverStyle.STRICT);

  static ConfiguredValueTransform resolve(String rule) {
    if (rule != null) {
      try {
        return valueOf(rule);
      } catch (IllegalArgumentException unknown) {
        // The configuration identifier may be untrusted.
      }
    }
    throw new IllegalArgumentException("ING_TRANSFORM_UNSUPPORTED");
  }

  Object apply(Object value, long rowOrdinal) {
    // IDENTITY also supports non-CSV adapters with historical non-string values.
    if (this == IDENTITY || value == null) return value;
    if (!(value instanceof String text)) throw new InvalidValue(rowOrdinal);
    String stripped = text.strip();
    if (stripped.isEmpty()) return null;
    return switch (this) {
      case DECIMAL_DOT_V1, DECIMAL_COMMA_V1 -> decimal(stripped, rowOrdinal);
      case DATE_ISO_V1, DATE_DMY_V1 -> date(stripped, rowOrdinal);
      case IDENTITY -> value;
    };
  }

  private String decimal(String text, long rowOrdinal) {
    Pattern lexical = this == DECIMAL_DOT_V1 ? DOT : COMMA;
    if (text.length() > 256 || !lexical.matcher(text).matches()) throw new InvalidValue(rowOrdinal);
    BigDecimal decimal = new BigDecimal(text.replace(',', '.')).stripTrailingZeros();
    if (decimal.precision() + Math.max(0, -decimal.scale()) > 38 || decimal.scale() > 18)
      throw new InvalidValue(rowOrdinal);
    return decimal.toPlainString();
  }

  private String date(String text, long rowOrdinal) {
    if (text.length() != 10) throw new InvalidValue(rowOrdinal);
    try {
      LocalDate date = LocalDate.parse(text, this == DATE_ISO_V1 ? DateTimeFormatter.ISO_LOCAL_DATE : DMY);
      if (date.getYear() < 1) throw new InvalidValue(rowOrdinal);
      return date.toString();
    } catch (DateTimeParseException invalidDate) {
      throw new InvalidValue(rowOrdinal);
    }
  }

  static final class InvalidValue extends IllegalArgumentException {
    private final long rowOrdinal;
    InvalidValue(long rowOrdinal) {
      super("ING_TRANSFORM_VALUE_INVALID");
      this.rowOrdinal = rowOrdinal;
    }
    long rowOrdinal() { return rowOrdinal; }
  }
}
