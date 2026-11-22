package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.*;

/** Parses geometry cells into JSON numbers without a floating point round trip. */
final class ValueGeometry {
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final List<String> TYPES = List.of("Point","LineString","Polygon","MultiPoint",
      "MultiLineString","MultiPolygon","GeometryCollection");

  static Map<String,Object> point(BigDecimal lon, BigDecimal lat) {
    return wrap("EPSG:4326", geometry("Point", List.of(lon, lat)));
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
      validate(geo, 0);
      return wrap("EPSG:4326", geo);
    } catch (JsonProcessingException | IllegalArgumentException invalid) {
      throw new ConfiguredValueTransform.InvalidValue(row);
    }
  }

  private static void validate(Map<?,?> geo, int depth) {
    require(depth < 128 && geo != null && geo.get("type") instanceof String);
    String type = (String) geo.get("type");
    require(TYPES.contains(type) && !geo.containsKey("crs") && !geo.containsKey("features")
        && !geo.containsKey("geometry") && !geo.containsKey("properties"));
    boolean collection = type.equals("GeometryCollection");
    require(!geo.containsKey(collection ? "coordinates" : "geometries"));
    List<?> data = list(geo.get(collection ? "geometries" : "coordinates"));
    if (geo.containsKey("bbox")) {
      var bbox = list(geo.get("bbox"));
      require(bbox.size() >= 4 && bbox.size() % 2 == 0 && bbox.stream().allMatch(Number.class::isInstance));
    }
    if (collection) {
      for (Object member : data) {
        if (!(member instanceof Map<?,?> geometry)) throw new IllegalArgumentException();
        validate(geometry, depth + 1);
      }
    } else if (!data.isEmpty()) coordinates(type, data);
  }
  private static void coordinates(String type, List<?> data) {
    switch (type) {
      case "Point" -> require(data.size() >= 2 && data.stream().allMatch(Number.class::isInstance));
      case "LineString", "MultiPoint" -> {
        if (type.equals("LineString")) require(data.size() >= 2);
        for (Object p : data) coordinates("Point", list(p));
      }
      case "Polygon" -> {
        for (Object item : data) {
          var ring = list(item);
          require(ring.size() >= 4);
          coordinates("LineString", ring);
          var first = list(ring.getFirst());
          var last = list(ring.getLast());
          require(first.size() == last.size());
          for (int i = 0; i < first.size(); i++)
            require(new BigDecimal(first.get(i).toString()).compareTo(new BigDecimal(last.get(i).toString())) == 0);
        }
      }
      case "MultiLineString", "MultiPolygon" -> {
        for (Object member : data) coordinates(type.substring(5), list(member));
      }
      default -> throw new IllegalArgumentException();
    }
  }
  private static List<?> list(Object value) {
    if (value instanceof List<?> values) return values;
    throw new IllegalArgumentException();
  }
  private static void require(boolean ok) { if (!ok) throw new IllegalArgumentException(); }

  static Object wkt(String text, String crs, long row) {
    try {
      var parser = new Wkt(text);
      var result = parser.readGeometry(0, 0);
      require(parser.end());
      validate(result, 0);
      return wrap(crs, result);
    } catch (IllegalArgumentException invalid) { throw new ConfiguredValueTransform.InvalidValue(row); }
  }

  private static final class Wkt {
    private static final Pattern WORD = Pattern.compile("[a-zA-Z]+");
    private static final Pattern NUMBER = Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    private final String text;
    private int offset;
    Wkt(String text) { this.text = text; }
    private void space() { while (offset < text.length() && Character.isWhitespace(text.charAt(offset))) offset++; }
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
    private List<BigDecimal> position(int[] dimension) {
      var numbers = new ArrayList<BigDecimal>();
      do {
        String number = token(NUMBER);
        if (number.isEmpty()) break;
        numbers.add(new BigDecimal(number));
      } while (offset < text.length() && Character.isWhitespace(text.charAt(offset)));
      if (dimension[0] == 0) dimension[0] = numbers.size();
      require(numbers.size() >= 2 && numbers.size() <= 3 && numbers.size() == dimension[0]);
      return numbers;
    }
  }
}
