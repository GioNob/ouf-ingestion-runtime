package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import java.math.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;

/** Immutable evidence and component specifications for a single bundle. */
final class ValuePins {
  private static final int MAX_VALUE_MAP_DEFINITION_BYTES = 786_432;
  private static final int MAX_VALUE_MAP_BUNDLE_BYTES = 1_048_576;
  static final ValuePins EMPTY = new ValuePins(Map.of(), Map.of(), Map.of(), Map.of());
  static final Set<String> BASE = Set.of("string","integer","decimal","boolean","date","datetime","geometry","concept");
  private final Map<String,Map<String,Component>> definitions;
  private final Map<String,ValueMap> maps;
  private final Map<String,String> uniquePins;
  private final Map<String,Set<String>> concepts;
  record ValueMap(String vocabulary, Map<String,String> entries) {
    ValueMap { entries = Map.copyOf(entries); }
  }

  private ValuePins(Map<String,Map<String,Component>> definitions, Map<String,ValueMap> maps, Map<String,String> uniquePins,
                    Map<String,Set<String>> concepts) {
    this.definitions = Map.copyOf(definitions);
    this.maps = Map.copyOf(maps);
    this.uniquePins = Map.copyOf(uniquePins);
    this.concepts = Map.copyOf(concepts);
    for (String ref : definitions.keySet()) checkClosure(ref, new HashSet<>(), new int[1]);
  }

  static ValuePins embedded(Object rawBindings) {
    if (!(rawBindings instanceof List<?> bindings)) throw error();
    if (bindings.stream().map(ValuePins::object)
        .noneMatch(p -> p.containsKey("definition") || p.containsKey("definitionSha256") || p.containsKey("valueMapRef"))) return EMPTY;
    Map<String,Map<String,Component>> schemas = new LinkedHashMap<>();
    Map<String,Set<String>> members = new HashMap<>();
    Map<String,ValueMap> maps = new LinkedHashMap<>();
    var unique = new HashMap<String,String>();
    var coordinates = new HashSet<String>();
    var vocabularies = new HashMap<String,String>();
    int valueMapBytes = 0;
    for (Object raw : bindings) {
      var pin = object(raw);
      String id = string(pin,"semanticId");
      String ref = id + "@" + publication(pin);
      require(coordinates.add(ref));
      unique.merge(id,ref,(first,second) -> "");
      if (pin.containsKey("valueMapRef")) {
        require(ref.equals(pin.get("valueMapRef")));
        requireMapContent(pin);
        var definition = object(pin.get("definition"));
        byte[] canonical = canonicalBytes(definition);
        if (canonical.length > MAX_VALUE_MAP_BUNDLE_BYTES - valueMapBytes)
          throw new IllegalArgumentException("ING_VALUE_MAP_LIMIT_EXCEEDED");
        valueMapBytes += canonical.length;
        require(canonical.length <= MAX_VALUE_MAP_DEFINITION_BYTES
            && digest(canonical).equals(pin.get("definitionSha256")));
        maps.put(ref,mapContents(pin, definition));
      } else if (pin.containsKey("definitionSha256")) {
        require(absolute(id));
        schemas.put(ref,components(checkedDefinition(pin)));
      } else if (pin.containsKey("definition")) {
        var definition = object(pin.get("definition"));
        String vocabulary = string(definition,"vocabularyRef");
        require(absolute(id) && absolute(vocabulary));
        String previous = vocabularies.putIfAbsent(id,vocabulary);
        require(previous == null || previous.equals(vocabulary));
        members.computeIfAbsent(vocabulary, ignored -> new HashSet<>()).add(id);
      }
    }
    return new ValuePins(schemas, maps, unique, members);
  }

  static void verifyBinding(Map<?,?> pin, Map<?,?> actual) {
    Object kind = actual.get("artifact_type");
    if (pin.containsKey("valueMapRef") || "VALUE_MAP".equals(kind)) {
      requireMapContent(pin);
      require("VALUE_MAP".equals(kind) && pin.containsKey("valueMapRef"));
      verifyDefinition(pin,actual);
    } else if ("DATATYPE".equals(kind)) {
      verifyDefinition(pin,actual);
    } else if (pin.containsKey("definition") || pin.containsKey("definitionSha256")) {
      require("CONCEPT".equals(kind) && !pin.containsKey("definitionSha256")
          && object(pin.get("definition")).equals(actual.get("definition")));
    }
  }

  private static void verifyDefinition(Map<?,?> pin, Map<?,?> actual) {
    require(checkedDefinition(pin).equals(actual.get("definition"))
        && pin.get("definitionSha256").equals(actual.get("definitionSha256")));
  }

  private static Map<?,?> checkedDefinition(Map<?,?> pin) {
    var definition = object(pin.get("definition"));
    require(digest(definition).equals(pin.get("definitionSha256")));
    return definition;
  }

  private static ValueMap mapContents(Map<?,?> pin, Map<?,?> definition) {
    require(Set.of("targetVocabulary","entries","labels").containsAll(definition.keySet()));
    String target = string(definition,"targetVocabulary");
    require(target.equals(pin.get("targetVocabulary")));
    int at = target.lastIndexOf('@');
    require(at > 0 && publicationId(target.substring(at + 1)));
    String vocabulary = target.substring(0,at);
    require(absolute(vocabulary));
    if (definition.containsKey("labels")) {
      var labels = object(definition.get("labels"));
      require(Set.of("it","en").containsAll(labels.keySet()) && labels.values().stream().allMatch(String.class::isInstance));
    }
    var items = list(definition.get("entries"));
    require(!items.isEmpty() && items.size() <= 2000);
    var entries = new HashMap<String,String>();
    String previous = null;
    for (Object item : items) {
      var entry = object(item);
      require(entry.keySet().equals(Set.of("code","concept")));
      String concept = string(entry,"concept");
      require(concept.codePointCount(0,concept.length()) <= 512 && absolute(concept));
      if (!(entry.get("code") instanceof String code)) throw error();
      require(!code.isEmpty() && code.codePointCount(0,code.length()) <= 128 && code.equals(CellWhitespace.trim(code))
          && code.chars().noneMatch(Character::isISOControl));
      require(previous == null || previous.compareTo(code) < 0);
      entries.put(code,concept);
      previous = code;
    }
    return new ValueMap(vocabulary,entries);
  }
  private static final ObjectWriter WRITER = new ObjectMapper().writer(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .without(SerializationFeature.INDENT_OUTPUT);
  private static byte[] canonicalBytes(Map<?,?> definition) {
    // Use the same character serialization and UTF-8 encoding as the Registry digest.
    try { return WRITER.writeValueAsString(definition).getBytes(StandardCharsets.UTF_8); }
    catch (com.fasterxml.jackson.core.JsonProcessingException invalid) { throw error(); }
  }
  static String digest(Map<?,?> definition) {
    return digest(canonicalBytes(definition));
  }
  private static String digest(byte[] canonical) {
    try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical)); }
    catch (java.security.NoSuchAlgorithmException invalid) { throw error(); }
  }
  private static String publication(Map<?,?> pin) {
    String set = string(pin,"publicationSetId");
    require(publicationId(set));
    return set;
  }
  private static boolean publicationId(String value) {
    return value.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}");
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
    return pin;
  }
  private static void requireMapContent(Map<?,?> pin) {
    if (pin.get("definition") == null) throw new IllegalArgumentException("ING_VALUE_MAP_CONTENT_REQUIRED");
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
      constraints = Map.copyOf(constraints);
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
