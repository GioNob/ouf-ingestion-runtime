package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import java.math.*;
import java.net.URI;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;

/** Immutable evidence and component specifications for a single bundle. */
final class ValuePins {
  static final ValuePins EMPTY = new ValuePins(Map.of(), Map.of(), Map.of(), Map.of());
  static final Set<String> BASE = Set.of("string","integer","decimal","boolean","date","datetime","geometry","concept");
  private final Map<String,Map<String,Component>> definitions;
  private record MapPin(ValueMap contents) {}
  private final Map<String,MapPin> maps;
  private final Map<String,String> uniquePins;
  private final Map<String,Set<String>> concepts;
  record ValueMap(String vocabulary, Map<String,String> entries) {
    ValueMap {
      require(absolute(vocabulary) && entries.values().stream().allMatch(ValuePins::absolute));
      entries = Map.copyOf(entries);
    }
  }

  private ValuePins(Map<String,Map<String,Component>> definitions, Map<String,MapPin> maps, Map<String,String> uniquePins,
                    Map<String,Set<String>> concepts) {
    this.definitions = Map.copyOf(definitions);
    this.maps = Map.copyOf(maps);
    this.uniquePins = Map.copyOf(uniquePins);
    this.concepts = Map.copyOf(concepts);
    for (String ref : definitions.keySet()) checkClosure(ref, new HashSet<>(), new int[1]);
  }

  static ValuePins embedded(Object rawBindings) {
    return embedded(rawBindings, Map.of());
  }

  // Pin headers and the publisher's exact code table must describe the same publication.
  static ValuePins embedded(Object rawBindings, Map<String,ValueMap> publishedContents) {
    if (!(rawBindings instanceof List<?> bindings)) throw error();
    if (publishedContents.isEmpty() && bindings.stream().map(ValuePins::object)
        .noneMatch(p -> p.containsKey("definition") || p.containsKey("definitionSha256") || p.containsKey("valueMapRef"))) return EMPTY;
    Map<String,Map<String,Component>> schemas = new LinkedHashMap<>();
    Map<String,Set<String>> members = new HashMap<>();
    Map<String,MapPin> maps = new LinkedHashMap<>();
    var unique = new HashMap<String,String>();
    var coordinates = new HashSet<String>();
    var vocabularies = new HashMap<String,String>();
    for (Object raw : bindings) {
      var pin = object(raw);
      String id = string(pin,"semanticId");
      String ref = id + "@" + publication(pin);
      require(coordinates.add(ref));
      unique.merge(id,ref,(first,second) -> "");
      if (pin.containsKey("valueMapRef")) {
        require(ref.equals(pin.get("valueMapRef")) && absolute(string(pin,"vocabularyRef")));
        var contents = publishedContents.get(ref);
        require(contents == null || contents.vocabulary.equals(pin.get("vocabularyRef")));
        maps.put(ref,new MapPin(contents));
      }
      if (pin.containsKey("definitionSha256")) {
        var definition = object(pin.get("definition"));
        require(digest(definition).equals(pin.get("definitionSha256")));
        require(absolute(id));
        schemas.put(ref,components(definition));
      } else if (pin.containsKey("definition")) {
        var definition = object(pin.get("definition"));
        String vocabulary = string(definition,"vocabularyRef");
        require(absolute(id) && absolute(vocabulary));
        String previous = vocabularies.putIfAbsent(id,vocabulary);
        require(previous == null || previous.equals(vocabulary));
        members.computeIfAbsent(vocabulary, ignored -> new HashSet<>()).add(id);
      }
    }
    require(maps.keySet().containsAll(publishedContents.keySet()));
    return new ValuePins(schemas, maps, unique, members);
  }

  static void verifyBinding(Map<?,?> pin, Map<?,?> actual) {
    if ("DATATYPE".equals(actual.get("artifact_type"))) {
      var definition = object(pin.get("definition"));
      require(definition.equals(actual.get("definition")) && digest(definition).equals(pin.get("definitionSha256"))
          && pin.get("definitionSha256").equals(actual.get("definitionSha256")));
    } else if (pin.containsKey("definition") || pin.containsKey("definitionSha256")) {
      require("CONCEPT".equals(actual.get("artifact_type")) && !pin.containsKey("definitionSha256")
          && object(pin.get("definition")).equals(actual.get("definition")));
    }
  }
  private static final ObjectWriter WRITER = new ObjectMapper().writer(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .without(SerializationFeature.INDENT_OUTPUT);
  static String digest(Map<?,?> definition) {
    try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(WRITER.writeValueAsBytes(definition))); }
    catch (Exception invalid) { throw error(); }
  }
  private static String publication(Map<?,?> pin) {
    String set = string(pin,"publicationSetId");
    require(set.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"));
    return set;
  }
  private void checkClosure(String ref, Set<String> path, int[] leaves) {
    int split = ref.lastIndexOf('@');
    require(split > 0 && path.size() < 4 && path.add(ref.substring(0, split)));
    for (var spec : definition(ref).values()) {
      if (BASE.contains(spec.type)) require(++leaves[0] <= 256);
      else checkClosure(spec.type, path, leaves);
    }
    path.remove(ref.substring(0, split));
  }
  Map<String,Component> definition(String ref) {
    var def = definitions.get(ref);
    if (def == null) throw error();
    return def;
  }
  ValueMap valueMap(String ref) {
    var pin = maps.get(maps.containsKey(ref) ? ref : uniquePins.getOrDefault(ref,""));
    if (pin == null) throw new IllegalArgumentException("ING_VALUE_MAP_PIN_REQUIRED");
    if (pin.contents == null) throw new IllegalArgumentException("ING_VALUE_MAP_CONTENT_REQUIRED");
    return pin.contents;
  }
  boolean member(String iri, String vocabulary, ValueMap producer) {
    return producer != null && vocabulary.equals(producer.vocabulary)
        || concepts.getOrDefault(vocabulary,Set.of()).contains(iri);
  }

  record Component(String type, boolean many, boolean required, Map<?,?> constraints) {
    Component {
      Set<String> keys = switch(type) {
        case "integer","decimal","date","datetime" -> Set.of("minInclusive","maxInclusive");
        case "string" -> Set.of("maxLength");
        case "concept" -> Set.of("vocabularyRef");
        default -> Set.of();
      };
      require(keys.containsAll(constraints.keySet()));
      for (var entry : constraints.entrySet()) {
        switch ((String)entry.getKey()) {
          case "maxLength" -> {
            Object n = entry.getValue();
            require((n instanceof Integer || n instanceof Long || n instanceof BigInteger || n instanceof Short || n instanceof Byte)
                && new BigInteger(n.toString()).signum() >= 0);
          }
          case "vocabularyRef" -> require(entry.getValue() instanceof String s && absolute(s));
          default -> require(entry.getValue() instanceof String s && canonicalBound(type,s));
        }
      }
      if (constraints.containsKey("minInclusive") && constraints.containsKey("maxInclusive"))
        require(compare(type, string(constraints,"minInclusive"), string(constraints,"maxInclusive")) <= 0);
    }
    boolean accepts(Object value, ValueMap producer, ValuePins pins) {
      if (constraints.containsKey("maxLength")) {
        String text = (String)value;
        if (BigInteger.valueOf(text.codePointCount(0,text.length())).compareTo(new BigInteger(constraints.get("maxLength").toString())) > 0) return false;
      }
      return (!constraints.containsKey("minInclusive") || compare(type,(String)value,string(constraints,"minInclusive")) >= 0)
          && (!constraints.containsKey("maxInclusive") || compare(type,(String)value,string(constraints,"maxInclusive")) <= 0)
          && (!constraints.containsKey("vocabularyRef") || pins.member((String)value,string(constraints,"vocabularyRef"),producer));
    }
  }
  private static boolean canonicalBound(String type, String value) {
    try {
      return switch(type) {
        case "integer" -> value.matches("0|-?[1-9][0-9]*");
        case "decimal" -> value.matches("-?(?:0|[1-9][0-9]*)(?:\\.[0-9]*[1-9])?") && !value.equals("-0");
        case "date" -> value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}") && LocalDate.parse(value).toString().equals(value);
        case "datetime" -> value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?Z")
            && OffsetDateTime.parse(value) != null;
        default -> false;
      };
    } catch (DateTimeException invalid) { return false; }
  }
  private static int compare(String type, String a, String b) {
    return switch(type) {
      case "integer","decimal" -> new BigDecimal(a).compareTo(new BigDecimal(b));
      case "datetime" -> Instant.parse(a).compareTo(Instant.parse(b));
      default -> a.compareTo(b);
    };
  }
  private static Map<String,Component> components(Map<?,?> def) {
    require(def.keySet().equals(Set.of("components")));
    var items = list(def.get("components"));
    require(!items.isEmpty() && items.size() <= 32);
    var specs = new LinkedHashMap<String,Component>();
    for (Object raw : items) {
      var field = object(raw);
      require(Set.of("name","type","cardinality","required","labels","constraints").containsAll(field.keySet()));
      String name = string(field,"name"), type = string(field,"type"), cardinality = string(field,"cardinality");
      require(name.matches("[a-z][A-Za-z0-9]{0,63}") && Set.of("ONE","MANY").contains(cardinality)
          && field.get("required") instanceof Boolean);
      if (field.containsKey("labels")) require(object(field.get("labels")).values().stream().allMatch(String.class::isInstance));
      require(specs.put(name,new Component(type,cardinality.equals("MANY"),(boolean)field.get("required"),params(field,"constraints"))) == null);
    }
    return Collections.unmodifiableMap(specs);
  }
  static boolean absolute(String s) {
    try { return URI.create(s).isAbsolute(); } catch (IllegalArgumentException invalid) { return false; }
  }
  static Map<?,?> object(Object raw) { if (raw instanceof Map<?,?> m) return m; throw error(); }
  static List<?> list(Object raw) { if (raw instanceof List<?> l) return l; throw error(); }
  static String string(Map<?,?> m, String key) {
    if (m.get(key) instanceof String s && !s.isBlank()) return s;
    throw error();
  }
  static Map<?,?> params(Map<?,?> m,String key) { return m.containsKey(key) ? object(m.get(key)) : Map.of(); }
  static void require(boolean ok) { if (!ok) throw error(); }
  static IllegalArgumentException error() { return new IllegalArgumentException("ING_VALUE_DEFINITION_INVALID"); }
}
