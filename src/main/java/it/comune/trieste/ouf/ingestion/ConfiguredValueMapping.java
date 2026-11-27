package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.ValuePins.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;

/** Compiles the published mapping once. The same expressions evaluate every row in probe and worker. */
final class ConfiguredValueMapping {
  private interface Evaluate { Object apply(Row row); }
  private record Expression(String type, boolean many, ValueMap producer, Evaluate evaluate) {}
  private record Target(String iri, Expression expression, boolean preserveNull) {}
  private record Row(Map<String,?> source, long ordinal) {
    Object cell(String field) {
      if (!source.containsKey(field)) throw new IllegalArgumentException("ING_MAPPING_SOURCE_FIELD_MISSING");
      return source.get(field);
    }
    RuntimeException invalid() { return new ConfiguredValueTransform.InvalidValue(ordinal); }
  }

  private final List<Target> targets = new ArrayList<>();
  private final Set<String> transformations = new LinkedHashSet<>();
  private final Collection<?> included;
  private final ValuePins pins;

  ConfiguredValueMapping(Map<String,Object> config, ValuePins pins) {
    this.pins = pins;
    included = list(config.getOrDefault("expectedFieldNames", List.of()));
    var seen = new HashSet<String>();
    for (Object raw : list(config.getOrDefault("propertyMappings", List.of()))) {
      var property = object(raw);
      String field = string(property,"sourceField");
      String rule = property.containsKey("transform") ? string(property,"transform") : "IDENTITY";
      Map<Object,Object> effective = new LinkedHashMap<>(params(property,"transformParams"));
      if (property.containsKey("valueMapRef") && "VALUE_MAP_V1".equals(rule)) {
        Object alias = property.get("valueMapRef");
        if (effective.containsKey("valueMapRef") && !Objects.equals(effective.get("valueMapRef"),alias))
          throw new IllegalArgumentException("ING_TRANSFORM_PARAMS_INVALID");
        effective.put("valueMapRef",alias);
      }
      add(seen,string(property,"targetPropertyIri"),field(field,rule,effective,false),rule.equals("IDENTITY"));
    }
    for (Object raw : list(config.getOrDefault("compositions",List.of()))) {
      var comp = object(raw);
      String target = string(comp,"targetPropertyIri");
      require(absolute(target));
      var expression = new LinkedHashMap<>(comp);
      expression.remove("targetPropertyIri"); expression.remove("dataAccessLabel"); expression.remove("kind");
      var value = switch (string(comp,"kind")) {
        case "STRUCTURED" -> structured(expression);
        case "CONSTRUCTOR" -> constructor(expression);
        default -> throw error();
      };
      add(seen,target,value,false);
    }
    if (targets.isEmpty()) throw new IllegalArgumentException("ING_PROPERTY_MAPPING_REQUIRED");
  }

  private void add(Set<String> seen, String target, Expression value, boolean preserveNull) {
    if (!seen.add(target)) throw new IllegalArgumentException("ING_MAPPING_TARGET_DUPLICATE");
    targets.add(new Target(target,value,preserveNull));
  }
  List<String> transformations() { return List.copyOf(transformations); }
  Map<String,Object> apply(Map<String,?> cells, long ordinal) {
    var row = new Row(cells,ordinal);
    var result = new LinkedHashMap<String,Object>();
    for (Target target : targets) {
      Object value = target.expression.evaluate.apply(row);
      if (value != null || target.preserveNull) result.put(target.iri,value);
    }
    return result;
  }

  private Expression field(String name, String id, Map<?,?> params, boolean composition) {
    if ((composition || !included.isEmpty()) && !included.contains(name))
      throw new IllegalArgumentException("ING_MAPPING_SOURCE_FIELD_MISSING");
    var reader = ConfiguredValueTransform.resolve(id).bind(params,pins);
    transformations.add(id);
    return new Expression(reader.rule().output,false,reader.valueMap(),row -> {
      Object value = reader.apply(row.cell(name), row.ordinal);
      if (composition && reader.rule() == ConfiguredValueTransform.IDENTITY && value != null && !(value instanceof String))
        throw row.invalid();
      return value;
    });
  }
  private Expression field(Map<?,?> input) {
    return field(string(input,"fieldName"),string(input,"reader"),params(input,"transformParams"),true);
  }
  private Expression constructor(Map<?,?> c) {
    require(Set.of("constructorId","inputs","params").containsAll(c.keySet()));
    String id = string(c,"constructorId");
    transformations.add(id);
    var parameters = params(c,"params");
    var inputs = list(c.get("inputs"));
    require(!inputs.isEmpty() && inputs.size() <= 8);
    var roles = new ArrayList<String>();
    var args = new ArrayList<Expression>();
    for (Object raw : inputs) {
      var input = object(raw);
      require(Set.of("role","fieldName","reader","transformParams").containsAll(input.keySet()));
      var argument = field(input);
      require(argument.type.equals(id.equals("POINT_FROM_LAT_LON_V1") ? "decimal" : "string"));
      roles.add(string(input,"role")); args.add(argument);
    }
    if (id.equals("POINT_FROM_LAT_LON_V1")) {
      require(parameters.isEmpty() && roles.size() == 2 && roles.containsAll(List.of("lat","lon")));
      var lat = args.get(roles.indexOf("lat"));
      var lon = args.get(roles.indexOf("lon"));
      return new Expression("geometry",false,null,row -> {
        String y = (String)lat.evaluate.apply(row), x = (String)lon.evaluate.apply(row);
        if (x == null && y == null) return null;
        if (x == null || y == null) throw row.invalid();
        var latitude = new BigDecimal(y); var longitude = new BigDecimal(x);
        if (latitude.abs().compareTo(BigDecimal.valueOf(90)) > 0 || longitude.abs().compareTo(BigDecimal.valueOf(180)) > 0)
          throw row.invalid();
        return ValueGeometry.point(longitude,latitude);
      });
    }
    if (!(parameters.get("separator") instanceof String separator)) throw error();
    int length = separator.codePointCount(0,separator.length());
    require(length >= 1 && length <= 8);
    if (id.equals("TEXT_JOIN_V1")) {
      require(parameters.size() == 1 && roles.size() >= 2 && roles.stream().allMatch("part"::equals));
      return new Expression("string",false,null,row -> {
        var parts = new ArrayList<String>();
        for (var arg : args) if (arg.evaluate.apply(row) instanceof String s) parts.add(s);
        return parts.isEmpty() ? null : String.join(separator,parts);
      });
    }
    require(id.equals("LIST_SPLIT_V1") && roles.equals(List.of("value"))
        && Set.of("itemReader","itemReaderParams","separator").containsAll(parameters.keySet()));
    var item = ConfiguredValueTransform.resolve(string(parameters,"itemReader")).bind(params(parameters,"itemReaderParams"),pins);
    transformations.add(item.rule().name());
    var split = Pattern.compile(Pattern.quote(separator));
    return new Expression(item.rule().output,true,item.valueMap(),row -> {
      var source = (String)args.getFirst().evaluate.apply(row);
      if (source == null) return null;
      var values = new ArrayList<>();
      for (var token : split.split(source,-1)) {
        Object value = item.apply(token,row.ordinal);
        if (value != null) values.add(value);
      }
      return values.isEmpty() ? null : values;
    });
  }

  private Expression structured(Map<?,?> s) {
    require(s.keySet().equals(Set.of("typeRef","components")));
    String ref = string(s,"typeRef");
    var definition = pins.definition(ref);
    var builders = new LinkedHashMap<String,Expression>();
    for (Object raw : list(s.get("components"))) {
      var component = object(raw);
      String name = string(component,"component");
      var spec = definition.get(name);
      require(spec != null && !builders.containsKey(name));
      var builder = new LinkedHashMap<>(component);
      builder.remove("component");
      Expression expression;
      if (builder.containsKey("items")) {
        require(spec.many() && builder.size() == 1);
        var items = new ArrayList<Expression>();
        for (Object item : list(builder.get("items"))) items.add(checked(object(item),spec,false));
        expression = new Expression(spec.type(),true,null,row -> {
          var values = new ArrayList<>();
          for (var item : items) {
            Object value = item.evaluate.apply(row);
            if (value != null) values.add(value);
          }
          return values.isEmpty() ? null : values;
        });
      } else expression = checked(builder,spec,spec.many());
      builders.put(name,expression);
    }
    return new Expression(ref,false,null,row -> {
      var values = new LinkedHashMap<String,Object>();
      for (var entry : definition.entrySet()) {
        var builder = builders.get(entry.getKey());
        Object value = builder == null ? null : builder.evaluate.apply(row);
        if (value != null) values.put(entry.getKey(),value);
        else if (entry.getValue().required()) throw row.invalid();
      }
      return values.isEmpty() ? null : values;
    });
  }
  private Expression checked(Map<?,?> b, Component spec, boolean many) {
    Expression expression;
    if (b.keySet().equals(Set.of("structured"))) {
      var nested = object(b.get("structured"));
      // Match the edge before descending. The pinned definition graph has already been bounded.
      require(spec.type().equals(string(nested,"typeRef")));
      expression = structured(nested);
    } else if (b.keySet().equals(Set.of("constructor"))) expression = constructor(object(b.get("constructor")));
    else {
      require(Set.of("fieldName","reader","transformParams").containsAll(b.keySet()));
      expression = field(b);
    }
    require(expression.type.equals(spec.type()) && expression.many == many);
    return new Expression(expression.type,many,expression.producer,row -> {
      Object value = expression.evaluate.apply(row);
      if (value != null && !spec.constraints().isEmpty()) {
        for (Object element : many ? list(value) : List.of(value))
          if (!spec.accepts(element,expression.producer,pins)) throw row.invalid();
      }
      return value;
    });
  }
}
