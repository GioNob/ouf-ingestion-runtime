package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;
import java.util.regex.*;

/** Parses geometry cells and emits canonical binary64 coordinates. */
final class ValueGeometry {
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final List<String> TYPES = List.of("Point","LineString","Polygon","MultiPoint",
      "MultiLineString","MultiPolygon","GeometryCollection");

  static Map<String,Object> point(BigDecimal lon, BigDecimal lat) {
    return wrap("EPSG:4326", geometry("Point", List.of(coordinate(lon), coordinate(lat))));
  }
  private static Map<String,Object> geometry(String type, Object data) {
    var result = new LinkedHashMap<String,Object>();
    result.put("type", type);
    result.put(type.equals("GeometryCollection") ? "geometries" : "coordinates", data);
    return result;
  }
  private static Map<String,Object> wrap(String crs, Object geo) {
    var result = new LinkedHashMap<String,Object>();
    result.put("crs", crs);
    result.put("geoJson", geo);
    return result;
  }

  static Object geoJson(String text, long row) {
    try {
      var geo = JSON.readValue(text, Map.class);
      return wrap("EPSG:4326", canonicalGeometry(geo, 0));
    } catch (JsonProcessingException | IllegalArgumentException invalid) {
      throw new ConfiguredValueTransform.InvalidValue(row);
    }
  }

  private static Map<String,Object> canonicalGeometry(Map<?,?> geo, int depth) {
    require(depth < 128 && geo != null && geo.get("type") instanceof String);
    String type = (String) geo.get("type");
    require(TYPES.contains(type) && !geo.containsKey("crs") && !geo.containsKey("features")
        && !geo.containsKey("geometry") && !geo.containsKey("properties"));
    boolean collection = type.equals("GeometryCollection");
    String dataKey = collection ? "geometries" : "coordinates";
    require(!geo.containsKey(collection ? "coordinates" : "geometries"));
    List<?> data = list(geo.get(dataKey));
    var canonical = new LinkedHashMap<String,Object>();
    geo.forEach((key,value) -> canonical.put((String) key, value));
    if (geo.containsKey("bbox")) {
      var bbox = list(geo.get("bbox"));
      require(bbox.size() >= 4 && bbox.size() % 2 == 0);
      canonical.put("bbox", bbox.stream().map(ValueGeometry::coordinate).toList());
    }
    if (collection) {
      var members = new ArrayList<>();
      for (Object member : data) {
        if (!(member instanceof Map<?,?> geometry)) throw new IllegalArgumentException();
        members.add(canonicalGeometry(geometry, depth + 1));
      }
      canonical.put(dataKey, members);
    } else if (!data.isEmpty()) {
      canonical.put(dataKey, coordinates(type, data));
    }
    return canonical;
  }
  private static List<?> coordinates(String type, List<?> data) {
    return switch (type) {
      case "Point" -> {
        require(data.size() >= 2);
        yield data.stream().map(ValueGeometry::coordinate).toList();
      }
      case "LineString", "MultiPoint" -> {
        if (type.equals("LineString")) require(data.size() >= 2);
        yield data.stream().map(p -> coordinates("Point", list(p))).toList();
      }
      case "Polygon" -> {
        yield data.stream().map(item -> {
          var ring = coordinates("LineString", list(item));
          require(ring.size() >= 4 && ring.getFirst().equals(ring.getLast()));
          return ring;
        }).toList();
      }
      case "MultiLineString", "MultiPolygon" ->
          data.stream().map(member -> coordinates(type.substring(5), list(member))).toList();
      default -> throw new IllegalArgumentException();
    };
  }
  private static List<?> list(Object value) {
    if (value instanceof List<?> values) return values;
    throw new IllegalArgumentException();
  }
  private static void require(boolean ok) { if (!ok) throw new IllegalArgumentException(); }

  private static Coordinate coordinate(Object source) {
    if (!(source instanceof Number number)) throw new IllegalArgumentException();
    double value = number.doubleValue();
    require(Double.isFinite(value));
    return new Coordinate(value == 0 ? 0 : value);
  }

  @JsonSerialize(using = CoordinateSerializer.class)
  private record Coordinate(double value) {}

  static final class CoordinateSerializer extends JsonSerializer<Coordinate> {
    @Override
    public void serialize(Coordinate coordinate, JsonGenerator generator, SerializerProvider provider) throws IOException {
      generator.writeNumber(shortest(coordinate.value()));
    }

    private static String shortest(double value) {
      var decimal = BigDecimal.valueOf(value).stripTrailingZeros();
      // Java 21 considers two digits even when one can round-trip. ECMAScript prefers one.
      if (decimal.precision() == 2) {
        var oneDigit = new BigDecimal(value).round(new MathContext(1, RoundingMode.HALF_EVEN));
        if (oneDigit.doubleValue() == value) decimal = oneDigit;
      }
      int exponent = decimal.precision() - decimal.scale() - 1;
      if (exponent >= -6 && exponent < 21) return decimal.toPlainString();
      return decimal.scaleByPowerOfTen(-exponent).toPlainString() + "e" + (exponent >= 0 ? "+" : "") + exponent;
    }
  }

  static Object wkt(String text, String crs, long row) {
    try {
      var parser = new Wkt(text);
      var result = parser.readGeometry(0, 0);
      require(parser.end());
      return wrap(crs, canonicalGeometry(result, 0));
    } catch (IllegalArgumentException invalid) { throw new ConfiguredValueTransform.InvalidValue(row); }
  }

  private static final class Wkt {
    private static final Pattern WORD = Pattern.compile("[a-zA-Z]+");
    private static final Pattern NUMBER = Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    private final String text;
    private int offset;
    Wkt(String text) { this.text = text; }
    private void space() { while (offset < text.length() && CellWhitespace.contains(text.charAt(offset))) offset++; }
    boolean end() { space(); return offset == text.length(); }
    private boolean take(char token) {
      space();
      if (offset == text.length() || text.charAt(offset) != token) return false;
      offset++; return true;
    }
    private String token(Pattern p) {
      space();
      Matcher matcher = p.matcher(text).region(offset, text.length());
      if (!matcher.lookingAt()) return "";
      offset = matcher.end(); return matcher.group();
    }
    Map<String,Object> readGeometry(int depth, int inheritedDimension) {
      require(depth < 128);
      String name = token(WORD);
      String type = TYPES.stream().filter(name::equalsIgnoreCase).findFirst().orElseThrow(IllegalArgumentException::new);
      String modifier = token(WORD).toUpperCase(Locale.ROOT);
      int dimension = inheritedDimension;
      // RFC 7946 defines the third ordinate as height. A measured WKT ordinate
      // cannot be relabelled as elevation or dropped by an implicit transformation.
      if (modifier.equals("Z")) {
        dimension = 3;
        require(inheritedDimension == 0 || inheritedDimension == dimension);
        modifier = token(WORD).toUpperCase(Locale.ROOT);
      }
      if (modifier.equals("EMPTY")) return geometry(type, List.of());
      require(modifier.isEmpty() && take('('));
      Object data;
      if (type.equals("GeometryCollection")) {
        var members = new ArrayList<>();
        do { members.add(readGeometry(depth + 1, dimension)); } while (take(','));
        data = members;
      } else if (type.equals("Point")) data = position(new int[]{dimension});
      else {
        int level = switch (type) { case "MultiPolygon" -> 3; case "Polygon","MultiLineString" -> 2; default -> 1; };
        data = sequence(level, type.equals("MultiPoint"), new int[]{dimension});
      }
      require(take(')'));
      return geometry(type, data);
    }
    private List<?> sequence(int level, boolean multiPoint, int[] dimension) {
      var values = new ArrayList<>();
      do {
        boolean brackets = level > 1 || multiPoint && take('(');
        if (level > 1) require(take('('));
        values.add(level == 1 ? position(dimension) : sequence(level - 1, false, dimension));
        if (brackets) require(take(')'));
      } while (take(','));
      return values;
    }
    private List<Double> position(int[] dimension) {
      var numbers = new ArrayList<Double>();
      do {
        String number = token(NUMBER);
        if (number.isEmpty()) break;
        numbers.add(Double.parseDouble(number));
      } while (offset < text.length() && CellWhitespace.contains(text.charAt(offset)));
      if (dimension[0] == 0) dimension[0] = numbers.size();
      require(numbers.size() >= 2 && numbers.size() <= 3 && numbers.size() == dimension[0]);
      return numbers;
    }
  }
}
