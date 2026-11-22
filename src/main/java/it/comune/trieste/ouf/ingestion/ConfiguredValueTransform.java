package it.comune.trieste.ouf.ingestion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Normalizer;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.regex.Pattern;

/** Versioned conversions for explicit property and component mappings. */
enum ConfiguredValueTransform {
  IDENTITY("string"), TEXT_NFC_TRIM_V1("string"), INTEGER_V1("integer"), INTEGER_GROUP_DOT_V1("integer"),
  DECIMAL_DOT_V1("decimal"), DECIMAL_COMMA_V1("decimal"), DECIMAL_GROUP_DOT_COMMA_V1("decimal"),
  DECIMAL_GROUP_COMMA_DOT_V1("decimal"), BOOLEAN_IT_V1("boolean"), DATE_ISO_V1("date"), DATE_DMY_V1("date"),
  DATE_DMY_DASH_V1("date"), DATETIME_ISO_OFFSET_V1("datetime"), DATETIME_LOCAL_ROME_V1("datetime"),
  GEOJSON_V1("geometry"), WKT_V1("geometry"), VALUE_MAP_V1("concept");

  final String output;
  ConfiguredValueTransform(String output) { this.output = output; }

  private static final Pattern DOT = Pattern.compile("[+-]?[0-9]+(\\.[0-9]+)?");
  private static final Pattern COMMA = Pattern.compile("[+-]?[0-9]+(,[0-9]+)?");
  private static final Pattern GROUP_DOT = Pattern.compile("[+-]?[0-9]{1,3}(?:\\.[0-9]{3})*+");
  private static final Pattern GROUP_IT = Pattern.compile(GROUP_DOT + "(?:,[0-9]+)?");
  private static final Pattern GROUP_EN = Pattern.compile("[+-]?[0-9]{1,3}(?:,[0-9]{3})*+(?:\\.[0-9]+)?");
  private static final Pattern INTEGER = Pattern.compile("[+-]?[0-9]+");
  private static final String LOCAL = "[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2})?";
  private static final Pattern ROME = Pattern.compile(LOCAL);
  private static final Pattern OFFSET = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\\.[0-9]{1,9})?)?(?:Z|[+-][0-9]{2}:[0-9]{2})");
  private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");
  private static final DateTimeFormatter DMY = formatter("dd/MM/uuuu"), DMY_DASH = formatter("dd-MM-uuuu");

  private static DateTimeFormatter formatter(String pattern) {
    return DateTimeFormatter.ofPattern(pattern, Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
  }

  static ConfiguredValueTransform resolve(String rule) {
    if (rule != null) {
      try { return valueOf(rule); } catch (IllegalArgumentException unknown) { /* Safe code below. */ }
    }
    throw new IllegalArgumentException("ING_TRANSFORM_UNSUPPORTED");
  }

  record Reader(ConfiguredValueTransform rule, String crs, ValuePins.ValueMap valueMap) {
    Object apply(Object value, long row) {
      if (rule == IDENTITY || value == null) return value;
      if (!(value instanceof String s)) throw new InvalidValue(row);
      if (rule == TEXT_NFC_TRIM_V1) s = Normalizer.normalize(s, Normalizer.Form.NFC);
      String text = CellWhitespace.trim(s);
      if (text.isEmpty()) return null;
      try {
        return switch (rule) {
          case IDENTITY -> value;
          case TEXT_NFC_TRIM_V1 -> text;
          case INTEGER_V1 -> new BigInteger(match(text, INTEGER, row)).toString();
          case INTEGER_GROUP_DOT_V1 -> new BigInteger(match(text, GROUP_DOT, row).replace(".", "")).toString();
          case DECIMAL_DOT_V1, DECIMAL_COMMA_V1 -> rule.legacyDecimal(text, row);
          case DECIMAL_GROUP_DOT_COMMA_V1 -> decimal(match(text, GROUP_IT, row).replace(".", "").replace(',', '.'));
          case DECIMAL_GROUP_COMMA_DOT_V1 -> decimal(match(text, GROUP_EN, row).replace(",", ""));
          case BOOLEAN_IT_V1 -> switch (text.toLowerCase(Locale.ROOT)) {
            case "sì", "si", "s", "vero", "v", "true", "1" -> true;
            case "no", "n", "falso", "f", "false", "0" -> false;
            default -> throw new InvalidValue(row);
          };
          case DATE_ISO_V1, DATE_DMY_V1, DATE_DMY_DASH_V1 -> {
            if (text.length() != 10) throw new InvalidValue(row);
            LocalDate date = LocalDate.parse(text, switch(rule) {
              case DATE_DMY_V1 -> DMY;
              case DATE_DMY_DASH_V1 -> DMY_DASH;
              default -> DateTimeFormatter.ISO_LOCAL_DATE;
            });
            if (date.getYear() < 1 && rule != DATE_DMY_DASH_V1) throw new InvalidValue(row);
            yield date.toString();
          }
          case DATETIME_ISO_OFFSET_V1 -> OffsetDateTime.parse(match(text, OFFSET, row)).toInstant().toString();
          case DATETIME_LOCAL_ROME_V1 -> {
            var local = LocalDateTime.parse(match(text, ROME, row).replace(' ', 'T'));
            var offsets = ROME_ZONE.getRules().getValidOffsets(local);
            if (offsets.size() != 1) throw new InvalidValue(row);
            yield local.toInstant(offsets.getFirst()).toString();
          }
          case GEOJSON_V1 -> ValueGeometry.geoJson(text, row);
          case WKT_V1 -> ValueGeometry.wkt(text, crs, row);
          case VALUE_MAP_V1 -> {
            String iri = valueMap.entries().get(text);
            if (iri == null) throw new InvalidValue(row);
            yield iri;
          }
        };
      } catch (DateTimeException | NumberFormatException invalid) { throw new InvalidValue(row); }
    }
  }

  Reader bind(Map<?,?> params, ValuePins pins) {
    if (this == WKT_V1 && params.keySet().equals(Set.of("crs"))
        && params.get("crs") instanceof String crs && crs.matches("EPSG:[0-9]+"))
      return new Reader(this, crs, null);
    if (this == VALUE_MAP_V1 && params.keySet().equals(Set.of("valueMapRef"))
        && params.get("valueMapRef") instanceof String ref)
      return new Reader(this, null, pins.valueMap(ref));
    if (!params.isEmpty() || this == VALUE_MAP_V1 || this == WKT_V1)
      throw new IllegalArgumentException("ING_TRANSFORM_PARAMS_INVALID");
    return new Reader(this, null, null);
  }

  Object apply(Object value, long row) { return bind(Map.of(), ValuePins.EMPTY).apply(value, row); }

  private static String match(String text, Pattern pattern, long row) {
    if (!pattern.matcher(text).matches()) throw new InvalidValue(row);
    return text;
  }
  private static String decimal(String text) { return new BigDecimal(text).stripTrailingZeros().toPlainString(); }
  private String legacyDecimal(String text, long row) {
    if (text.length() > 256) throw new InvalidValue(row);
    var value = new BigDecimal(match(text, this == DECIMAL_DOT_V1 ? DOT : COMMA, row).replace(',', '.')).stripTrailingZeros();
    if (value.precision() + Math.max(0, -value.scale()) > 38 || value.scale() > 18) throw new InvalidValue(row);
    return value.toPlainString();
  }

  static final class InvalidValue extends IllegalArgumentException {
    private final long rowOrdinal;
    InvalidValue(long rowOrdinal) { super("ING_TRANSFORM_VALUE_INVALID"); this.rowOrdinal = rowOrdinal; }
    long rowOrdinal() { return rowOrdinal; }
  }
}
