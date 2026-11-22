package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.OfflineAssertions.*;
import static it.comune.trieste.ouf.ingestion.ConfiguredValueTransform.*;
import java.math.BigDecimal;
import java.util.*;


class ConfiguredValueMappingTest {
  private static final String SET = "62aaefce-9199-4698-b899-30f0968cba56";
  private static final String OUT = "urn:target";
  private static final String A = "urn:type:a@" + SET, B = "urn:type:b@" + SET;
  private static final String MAP = "urn:map@" + SET, VOCAB = "urn:vocabulary";
  private static Map<String,Object> expr(String name, String reader) {
    return Map.of("fieldName", name, "reader", reader);
  }
  private static Map<String,Object> with(Map<String,Object> map, String key, Object value) {
    var copy = new LinkedHashMap<>(map); copy.put(key,value); return copy;
  }
  @SafeVarargs
  private static Map<String,Object> ctor(String id, Map<?,?> params, Map<String,Object>... inputs) {
    return Map.of("constructorId",id,"params",params,"inputs",Arrays.asList(inputs));
  }
  private static Map<String,Object> root(String kind, Map<String,Object> body) {
    return with(with(with(body,"kind",kind),"targetPropertyIri",OUT),"dataAccessLabel","OPEN");
  }
  private static Map<String,Object> schema(String name, String type, boolean required, String cardinality) {
    return Map.of("name",name,"type",type,"required",required,"cardinality",cardinality);
  }
  private static Map<String,Object> pin(String ref, List<?> components) {
    var definition = Map.of("components",components);
    return Map.of("semanticId",ref.substring(0,ref.lastIndexOf('@')),"publicationSetId",SET,
        "definition",definition,"definitionSha256",ValuePins.digest(definition));
  }
  private static ConfiguredValueMapping mapping(ValuePins pins, Map<String,Object> expression) {
    return new ConfiguredValueMapping(Map.of("compositions",List.of(expression),
        "expectedFieldNames",List.of("x","y","z")),pins);
  }
  private static void bad(Runnable action, String code) {
    var error = assertThrows(IllegalArgumentException.class,action::run);
    assertEquals(code,error.getMessage());
    assertNull(error.getCause());
  }

  void exactReadersAndSafeLexicalFailures() {
    assertEquals("é",TEXT_NFC_TRIM_V1.apply(" e\u0301 ",1));
    assertEquals("-987654321098765432109876543210",INTEGER_V1.apply(" -00987654321098765432109876543210 ",1));
    assertEquals("-987654321",INTEGER_GROUP_DOT_V1.apply("-987.654.321",1));
    assertEquals("1234567.89",DECIMAL_GROUP_DOT_COMMA_V1.apply("+1.234.567,890",1));
    assertEquals("1234567.89",DECIMAL_GROUP_COMMA_DOT_V1.apply("1,234,567.890",1));
    for (var t : List.of("SÌ"," SI ","S","Vero","v","true","1")) assertEquals(true,BOOLEAN_IT_V1.apply(t,1));
    for (var t : List.of("NO","N","Falso","F","false","0")) assertEquals(false,BOOLEAN_IT_V1.apply(t,1));
    assertEquals("2024-02-29",DATE_DMY_DASH_V1.apply("29-02-2024",1));
    assertEquals("2024-02-28T21:15:00Z",DATETIME_ISO_OFFSET_V1.apply("2024-02-28T22:15+01:00",1));
    assertEquals("2024-02-28T22:15:01.123456789Z",DATETIME_ISO_OFFSET_V1.apply("2024-02-28T22:15:01.123456789Z",1));
    assertEquals("2024-07-01T10:00:00Z",DATETIME_LOCAL_ROME_V1.apply("2024-07-01 12:00",1));
    for (var t : List.of("2024-03-31T02:30","2024-10-27T02:30","2024-07-01T12:00Z"))
      bad(()->DATETIME_LOCAL_ROME_V1.apply(t,1),"ING_TRANSFORM_VALUE_INVALID");
    for (var rule : List.of(INTEGER_V1,INTEGER_GROUP_DOT_V1,BOOLEAN_IT_V1,DATE_DMY_DASH_V1,DATETIME_ISO_OFFSET_V1,
        DECIMAL_GROUP_DOT_COMMA_V1,DECIMAL_GROUP_COMMA_DOT_V1)) {
      bad(()->rule.apply("private-cell",1),"ING_TRANSFORM_VALUE_INVALID");
      assertNull(rule.apply("  ",1));
    }
    bad(()->DECIMAL_GROUP_DOT_COMMA_V1.apply("1234,56",1),"ING_TRANSFORM_VALUE_INVALID");
    bad(()->INTEGER_GROUP_DOT_V1.apply("123.45",1),"ING_TRANSFORM_VALUE_INVALID");
  }

  void geometryReadersPreserveNumbersAndRejectMalformedGeometries() {
    var wkt = WKT_V1.bind(Map.of("crs","EPSG:3857"),ValuePins.EMPTY);
    var point = Map.of("type","Point","coordinates",List.of(new BigDecimal("100.0000000000000001"),new BigDecimal("200")));
    assertEquals(Map.of("crs","EPSG:3857","geoJson",point),wkt.apply("POINT(100.0000000000000001 200)",1));
    for (String w : List.of("MULTIPOINT ((1 2),(3 4))","MULTILINESTRING ((1 2,3 4))",
        "MULTIPOLYGON (((0 0,1 0,1 1,0 0)))","GEOMETRYCOLLECTION (POINT EMPTY,LINESTRING (0 0,1 1))"))
      assertNotNull(wkt.apply(w,1));
    assertEquals(Map.of("crs","EPSG:4326","geoJson",Map.of("type","Point","coordinates",List.of(1,2))),
        GEOJSON_V1.apply("{\"type\":\"Point\",\"coordinates\":[1,2]}",1));
    for (String w : List.of("POINT(1 2) trailing","POINT(1)","LINESTRING(1 2)","POLYGON ((0 0,1 0,1 1,0 1))","POINT M(1 2 3)"))
      bad(()->wkt.apply(w,1),"ING_TRANSFORM_VALUE_INVALID");
    for (String g : List.of("{\"type\":\"Feature\"}","{\"type\":\"LineString\",\"coordinates\":[[1,2]]}",
        "{\"type\":\"Point\",\"coordinates\":[1,true]}","{\"type\":\"Point\",\"coordinates\":[1,2]} {}"))
      bad(()->GEOJSON_V1.apply(g,1),"ING_TRANSFORM_VALUE_INVALID");
    bad(()->WKT_V1.bind(Map.of(),ValuePins.EMPTY),"ING_TRANSFORM_PARAMS_INVALID");
  }

  void pointJoinAndLiteralSplitUseDeclaredRolesAndAbsence() {
    var lon = with(expr("x","DECIMAL_COMMA_V1"),"role","lon");
    var lat = with(expr("y","DECIMAL_DOT_V1"),"role","lat");
    var point = mapping(ValuePins.EMPTY,root("CONSTRUCTOR",ctor("POINT_FROM_LAT_LON_V1",Map.of(),lon,lat)));
    assertEquals(Map.of(OUT,ValueGeometry.point(new BigDecimal("-180"),new BigDecimal("90"))),
        point.apply(Map.of("x","-180,0","y","90"),1));
    assertEquals(Map.of(),point.apply(Map.of("x"," ","y",""),1));
    for (var value : List.of("91","")) bad(()->point.apply(Map.of("x","1","y",value),1),"ING_TRANSFORM_VALUE_INVALID");
    var join = mapping(ValuePins.EMPTY,root("CONSTRUCTOR",ctor("TEXT_JOIN_V1",Map.of("separator"," / "),
        with(expr("y","TEXT_NFC_TRIM_V1"),"role","part"),with(expr("x","TEXT_NFC_TRIM_V1"),"role","part"))));
    assertEquals(Map.of(OUT,"b / a"),join.apply(Map.of("x","a","y","b"),1));
    assertEquals(Map.of(OUT,"a"),join.apply(Map.of("x","a","y"," "),1));
    assertEquals(Map.of(),join.apply(Map.of("x","","y"," "),1));
    var split = mapping(ValuePins.EMPTY,root("CONSTRUCTOR",split(Map.of("separator",".","itemReader","INTEGER_V1"))));
    assertEquals(Map.of(OUT,List.of("1","2")),split.apply(Map.of("x","01..+2."),1));
    assertEquals(Map.of(),split.apply(Map.of("x","..."),1));
    var geom = mapping(ValuePins.EMPTY,root("CONSTRUCTOR",split(Map.of("separator","|","itemReader","WKT_V1",
        "itemReaderParams",Map.of("crs","EPSG:3857")))));
    assertEquals(Map.of(OUT,List.of(WKT_V1.bind(Map.of("crs","EPSG:3857"),ValuePins.EMPTY).apply("POINT(1 2)",1))),
        geom.apply(Map.of("x","POINT(1 2)|"),1));
  }
  private static Map<String,Object> split(Map<?,?> params) {
    return ctor("LIST_SPLIT_V1",params,with(expr("x","IDENTITY"),"role","value"));
  }

  void genericNestedManyUsesOnlyPinnedDefinitions() {
    var parent = pin(A,List.of(schema("points",B,true,"MANY"),
        with(schema("ids","integer",false,"MANY"),"constraints",Map.of("maxInclusive","2"))));
    var child = pin(B,List.of(with(schema("name","string",false,"ONE"),"constraints",Map.of("maxLength",1))));
    var pins = ValuePins.embedded(List.of(child,parent));
    var nested = new ArrayList<Map<String,Object>>();
    for (String field : List.of("y","z")) nested.add(Map.of("structured",Map.of("typeRef",B,"components",
        List.of(with(expr(field,"TEXT_NFC_TRIM_V1"),"component","name")))));
    var structure = Map.<String,Object>of("typeRef",A,"components",List.of(
        Map.of("component","points","items",nested),Map.of("component","ids","constructor",split(Map.of("separator","|","itemReader","INTEGER_V1")))));
    var mapper = mapping(pins,root("STRUCTURED",structure));
    assertEquals(Map.of(OUT,Map.of("points",List.of(Map.of("name","😀")),"ids",List.of("1","2"))),
        mapper.apply(Map.of("x","+1|2","y","😀","z"," "),1));
    for (var row : List.of(Map.of("x","3","y","a","z",""),Map.of("x","","y","","z",""),
        Map.of("x","","y","ab","z",""))) bad(()->mapper.apply(row,4),"ING_TRANSFORM_VALUE_INVALID");
    bad(()->mapping(pins,root("STRUCTURED",with(structure,"components",List.of(with(expr("x","IDENTITY"),"component","unknown"))))),
        "ING_VALUE_DEFINITION_INVALID");
    bad(()->mapping(pins,root("STRUCTURED",with(structure,"components",List.of(with(expr("x","INTEGER_V1"),"component","ids"))))),
        "ING_VALUE_DEFINITION_INVALID");
    bad(()->ValuePins.embedded(List.of(parent)),"ING_VALUE_DEFINITION_INVALID");
    bad(()->ValuePins.embedded(List.of(pin(A,List.of(schema("loop",A,false,"ONE"))))),"ING_VALUE_DEFINITION_INVALID");
  }

  void registryAndEmbeddedDigestsMustBothMatch() {
    var first = pin(A,List.of(schema("x","string",true,"ONE")));
    var second = pin(A,List.of(schema("x","integer",true,"ONE")));
    var resolved = with(first,"artifact_type","DATATYPE");
    ValuePins.verifyBinding(first,resolved);
    for (var changed : List.of(second,with(first,"definitionSha256","0".repeat(64)))) {
      bad(()->ValuePins.verifyBinding(changed,resolved),"ING_VALUE_DEFINITION_INVALID");
      bad(()->ValuePins.verifyBinding(first,with(changed,"artifact_type","DATATYPE")),"ING_VALUE_DEFINITION_INVALID");
    }
    var corrupt = with(first,"definitionSha256","0".repeat(64));
    bad(()->ValuePins.verifyBinding(corrupt,with(corrupt,"artifact_type","DATATYPE")),"ING_VALUE_DEFINITION_INVALID");
    bad(()->ValuePins.embedded(List.of(corrupt)),"ING_VALUE_DEFINITION_INVALID");
    bad(()->ValuePins.verifyBinding(first,with(first,"artifact_type","PROPERTY")),"ING_VALUE_DEFINITION_INVALID");
    for (Object bound : List.of(1,"1.00","+1","1e1"))
      bad(()->ValuePins.embedded(List.of(pin(A,List.of(with(schema("x","decimal",false,"ONE"),"constraints",Map.of("minInclusive",bound)))))),
          "ING_VALUE_DEFINITION_INVALID");
  }

  void definitionLimitsAndBuildersFailBeforeReadingCells() {
    String c="urn:type:c@"+SET, d="urn:type:d@"+SET, e="urn:type:e@"+SET;
    var wide=new ArrayList<Map<String,Object>>();
    for(int i=0;i<32;i++) wide.add(schema("v"+i,"string",false,"ONE"));
    var branches=new ArrayList<Map<String,Object>>();
    for(int i=0;i<8;i++) branches.add(schema("b"+i,B,false,"ONE"));
    assertNotNull(ValuePins.embedded(List.of(pin(A,branches),pin(B,wide))));
    branches.add(schema("overflow",B,false,"ONE"));
    bad(()->ValuePins.embedded(List.of(pin(A,branches),pin(B,wide))),"ING_VALUE_DEFINITION_INVALID");
    wide.add(schema("extra","string",false,"ONE"));
    bad(()->ValuePins.embedded(List.of(pin(A,wide))),"ING_VALUE_DEFINITION_INVALID");
    var chain=List.of(pin(A,List.of(schema("b",B,false,"ONE"))),pin(B,List.of(schema("c",c,false,"ONE"))),
        pin(c,List.of(schema("d",d,false,"ONE"))),pin(d,List.of(schema("e",e,false,"ONE"))),
        pin(e,List.of(schema("v","string",false,"ONE"))));
    bad(()->ValuePins.embedded(chain),"ING_VALUE_DEFINITION_INVALID");
    bad(()->mapping(ValuePins.EMPTY,root("CONSTRUCTOR",ctor("TEXT_JOIN_V1",Map.of("separator"," "),
        with(expr("notIncluded","IDENTITY"),"role","part"),with(expr("x","IDENTITY"),"role","part")))),
        "ING_MAPPING_SOURCE_FIELD_MISSING");
    bad(()->mapping(ValuePins.EMPTY,root("CONSTRUCTOR",split(Map.of("separator","|","itemReader","WKT_V1")))),
        "ING_TRANSFORM_PARAMS_INVALID");
  }

  void valueMapMembershipFollowsTheItemReaderAndExactPin() {
    var spec = with(schema("codes","concept",true,"MANY"),"constraints",Map.of("vocabularyRef",VOCAB));
    var datatype = pin(A,List.of(spec));
    var header = Map.<String,Object>of("semanticId","urn:map","valueMapRef",MAP,"publicationSetId",SET,"vocabularyRef",VOCAB);
    var entries = Map.of("01","urn:c:one","02","urn:c:two");
    var map = new ValuePins.ValueMap(VOCAB,entries);
    var pins = ValuePins.embedded(List.of(datatype,header),Map.of(MAP,map));
    bad(()->ValuePins.embedded(List.of(header)),"ING_VALUE_MAP_CONTENT_REQUIRED");
    var params = Map.of("separator",";","itemReader","VALUE_MAP_V1","itemReaderParams",Map.of("valueMapRef",MAP));
    var expression = root("STRUCTURED",Map.of("typeRef",A,"components",List.of(Map.of("component","codes","constructor",split(params)))));
    assertEquals(Map.of(OUT,Map.of("codes",List.of("urn:c:two","urn:c:one"))),
        mapping(pins,expression).apply(Map.of("x"," 02; ;01"),2));
    var other = Map.of(MAP,new ValuePins.ValueMap("urn:other",entries));
    var concept = Map.<String,Object>of("semanticId","urn:c:one","publicationSetId",SET,"definition",Map.of("vocabularyRef",VOCAB));
    ValuePins.verifyBinding(concept,with(concept,"artifact_type","CONCEPT"));
    var fallback = ValuePins.embedded(List.of(datatype,with(header,"vocabularyRef","urn:other"),concept),other);
    assertEquals(Map.of(OUT,Map.of("codes",List.of("urn:c:one"))),mapping(fallback,expression).apply(Map.of("x","01"),2));
    bad(()->mapping(fallback,expression).apply(Map.of("x","02"),2),"ING_TRANSFORM_VALUE_INVALID");
    bad(()->mapping(pins,expression).apply(Map.of("x","private-cell"),2),"ING_TRANSFORM_VALUE_INVALID");
    bad(()->VALUE_MAP_V1.bind(Map.of("valueMapRef","urn:unbound"),pins),"ING_VALUE_MAP_PIN_REQUIRED");
    var property = Map.<String,Object>of("sourceField","x","targetPropertyIri",OUT,"transform","VALUE_MAP_V1","valueMapRef",MAP);
    assertEquals(Map.of(OUT,"urn:c:one"),new ConfiguredValueMapping(Map.of("propertyMappings",List.of(property)),pins)
        .apply(Map.of("x","01"),2));
    assertEquals(map,pins.valueMap("urn:map"));
    var ambiguous = ValuePins.embedded(List.of(header,Map.of("semanticId","urn:map","publicationSetId",new UUID(0,2).toString())),
        Map.of(MAP,map));
    bad(()->ambiguous.valueMap("urn:map"),"ING_VALUE_MAP_PIN_REQUIRED");
    bad(()->ValuePins.embedded(List.of(with(header,"valueMapRef","wrong")),Map.of(MAP,map)),"ING_VALUE_DEFINITION_INVALID");
    bad(()->ValuePins.embedded(List.of(with(header,"vocabularyRef","urn:other")),Map.of(MAP,map)),"ING_VALUE_DEFINITION_INVALID");
    bad(()->new ConfiguredValueMapping(Map.of("propertyMappings",List.of(with(property,"transformParams",Map.of("valueMapRef","other")))),pins),
        "ING_TRANSFORM_PARAMS_INVALID");
  }

  void canonicalBoundsApplyChronologicallyAndNumerically() {
    var bounds=Map.of("minInclusive","2026-01-01T00:00:00.1Z","maxInclusive","2026-01-01T00:00:00.2Z");
    var fields=List.of(with(schema("time","datetime",true,"ONE"),"constraints",bounds),
        with(schema("quantity","decimal",true,"ONE"),"constraints",Map.of("minInclusive","-10","maxInclusive","2")),
        with(schema("day","date",true,"ONE"),"constraints",Map.of("minInclusive","2026-01-01")));
    var pins=ValuePins.embedded(List.of(pin(A,fields)));
    var mapping=mapping(pins,root("STRUCTURED",Map.of("typeRef",A,"components",List.of(
        with(expr("x","DATETIME_ISO_OFFSET_V1"),"component","time"),with(expr("y","DECIMAL_DOT_V1"),"component","quantity"),
        with(expr("z","DATE_ISO_V1"),"component","day")))));
    var row=Map.<String,Object>of("x","2026-01-01T01:00:00.100+01:00","y","-010.00","z","2026-01-01");
    assertEquals(Map.of(OUT,Map.of("time","2026-01-01T00:00:00.100Z","quantity","-10","day","2026-01-01")),mapping.apply(row,2));
    for(var rejected:List.of(with(row,"x","2026-01-01T00:00:00.201Z"),with(row,"y","20"),with(row,"z","2025-12-31")))
      bad(()->mapping.apply(rejected,2),"ING_TRANSFORM_VALUE_INVALID");
  }
public static void main(String[] args){var subject=new ConfiguredValueMappingTest();subject.exactReadersAndSafeLexicalFailures();subject.geometryReadersPreserveNumbersAndRejectMalformedGeometries();subject.pointJoinAndLiteralSplitUseDeclaredRolesAndAbsence();subject.genericNestedManyUsesOnlyPinnedDefinitions();subject.registryAndEmbeddedDigestsMustBothMatch();subject.definitionLimitsAndBuildersFailBeforeReadingCells();subject.valueMapMembershipFollowsTheItemReaderAndExactPin();subject.canonicalBoundsApplyChronologicallyAndNumerically();System.out.println("PASS methods=8 checks="+checks);}}