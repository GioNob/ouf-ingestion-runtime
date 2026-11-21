package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PullSchemaTest {
  private final ObjectMapper json=new ObjectMapper();
  private final String fingerprint="sha256:0ef6406f365fdc5335736ce06130b03b3bc8aa77fa024bd0929ce8e54a45c0e8";
  private final ExecutionBundle bundle=new ExecutionBundle("test","1","hash","s","REST_JSON","gateway://places",
      Map.of("sourceSchemaVersion","legacy","identityFields",List.of("id"),"sourceSchemaRef","schema:places:1",
          "sourceUnitProfile",Map.of("origin",Map.of("collection","places"),"readParameters",
              Map.of("api",Map.of("schemaVersion","1","fieldsSha256",fingerprint)))));
  private ObjectNode declaration() throws Exception {
    return (ObjectNode)json.readTree("""
        {"schemaVersion":"1","collection":"places","fields":[{"type":"string","name":"name","nullable":false},
          {"nullable":false,"name":"id","type":"string"}],"key":["id"],"geometry":null}
        """);
  }
  private ExecutionBundle rest(Map<String,Object> extra) {
    var config=new HashMap<String,Object>(Map.of("sourceSchemaVersion","1","identityFields",List.of("id")));
    config.putAll(extra);
    return new ExecutionBundle("bundle","1","hash","r2b-pull","REST_JSON","gateway://r2b/pull",config);
  }

  @Test void flatPagesUseLegacyRequestsAndCheckpointsEvenWithCollectionAndRecordVersion() {
    var legacy=rest(Map.of("collection","places","publishedBundle",Map.of("bundleVersion","1"),
        "readParameters",Map.of("api",Map.of("pageSize",20))));
    var requests=new ArrayList<Map<String,Object>>();
    var adapter=new GatewayJsonAdapter((binding,request,correlation)->{
      requests.add(request);
      String response=request.containsKey("pageToken")
          ? """
            {"items":[]}
            """
          : """
            {"collection":"places","items":[{"id":"2","name":"Beta","schemaVersion":"column"}],"nextPageToken":"p2"}
            """;
      return response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    },json);
    try(var cursor=adapter.open(legacy,null,false,(schema,pos)->{throw new AssertionError("Unexpected schema check");})) {
      var record=cursor.next().orElseThrow();
      assertThat(record.payload()).isEqualTo(Map.of("id","2","name","Beta","schemaVersion","column"));
      assertThat(record.provenance()).isEqualTo(Map.of("bindingRef",legacy.bindingRef(),"page",1));
      assertThat(cursor.checkpoint().value()).isEqualTo(Map.of("recordIndex",1,"pageOffset",1));
      assertThat(cursor.next()).isEmpty();
    }
    assertThat(requests).containsExactly(Map.of("pageSize",100),Map.of("pageSize",100,"pageToken","p2"));
  }

  @Test void malformedFlatPageDoesNotReleaseItsValidPrefix() {
    var adapter=new GatewayJsonAdapter((binding,r,c)->"""
        {"items":[{"id":"1"},{"name":"missing id"}]}
        """.getBytes(java.nio.charset.StandardCharsets.UTF_8),json);
    try(var cursor=adapter.open(rest(Map.of()),null,false,(schema,pos)->{throw new AssertionError();})) {
      assertThatThrownBy(cursor::next).hasMessage("ING_SOURCE_IDENTITY_MISSING");
      assertThat(cursor.checkpoint().value()).isEqualTo(Map.of("recordIndex",0,"pageOffset",0));
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings={"binding","publication","profile"})
  void declaredVerticalWithMissingEnvelopeVersionStillChecksSchema(String marker) throws Exception {
    var config=new HashMap<String,Object>(Map.of("collection","places"));
    switch(marker) {
      case "binding" -> config.put("sourceContract","ouf.vertical-source.v1");
      case "publication" -> config.put("publishedBundle",Map.of("sourceContract","ouf.vertical-source.v1"));
      default -> config.put("sourceUnitProfile",bundle.configuration().get("sourceUnitProfile"));
    }
    var schema=declaration();
    config.put("sourceSchema",Map.of("fieldsSha256",fingerprint));
    var requests=new ArrayList<Map<String,Object>>();
    var adapter=new GatewayJsonAdapter((binding,request,correlation)->{
      requests.add(request);
      return (request.containsKey("resource") ? schema.toString() : "{\"collection\":\"places\",\"items\":[]}")
          .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    },json);
    try(var cursor=adapter.open(rest(config),null,false,(comparison,pos)->{
      assertThat(comparison.differences()).containsExactly("VERSION");
      throw new IllegalStateException("schema blocked");
    })) {assertThatThrownBy(cursor::next).hasMessage("schema blocked");}
    assertThat(requests).containsExactly(Map.of("limit",100,"collection","places"),Map.of("resource","schema","collection","places"));
  }

  @Test void versionInEnvelopeAloneRequiresFingerprintBeforeParsingEvents() throws Exception {
    var schema=declaration();
    var requests=new ArrayList<Map<String,Object>>();
    var adapter=new GatewayJsonAdapter((binding,request,correlation)->{
      requests.add(request);
      return (request.containsKey("resource") ? schema.toString() : """
          {"collection":"places","schemaVersion":"1","items":[{"op":"invalid"}]}
          """).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    },json);
    try(var cursor=adapter.open(rest(Map.of()),null,false,(comparison,pos)->{
      assertThat(comparison.differences()).containsExactly("APPROVED_FINGERPRINT_MISSING");
      throw new IllegalStateException("schema blocked");
    })) {assertThatThrownBy(cursor::next).hasMessage("schema blocked");}
    assertThat(requests).containsExactly(Map.of("pageSize",100),Map.of("resource","schema","collection","places"));
  }

  @Test void canonicalDigestIsIndependentOfFieldOrderAndObjectMemberOrder() throws Exception {
    var d=declaration();
    assertThat(PullSchema.fingerprint(d.get("fields"))).isEqualTo(fingerprint);
    assertThat(PullSchema.compare(bundle,d,d).differences()).isEmpty();
  }
  @Test void keyGeometryNullabilityVersionAndCollectionAreIndependentlyChecked() throws Exception {
    var d=declaration();
    var changed=d.deepCopy();
    ((ObjectNode)changed.get("fields").get(0)).put("nullable",true);
    changed.putArray("key").add("name");
    changed.putObject("geometry").put("field","name").put("type","Point").put("crs","EPSG:4326");
    changed.put("schemaVersion","2");
    changed.put("collection","other");
    assertThat(PullSchema.compare(bundle,d,changed).differences()).containsExactlyInAnyOrder("KEY","GEOMETRY","FIELDS","VERSION","COLLECTION");
  }
  @Test void emptyPageIsObservedAndDifferentVersionShortCircuitsInvalidEvents() throws Exception {
    var d=declaration();
    var page=d.deepCopy().put("schemaVersion","2").put("mode","CURSOR").put("hasMore",false);
    page.putArray("items");
    RuntimePorts.GatewaySourcePort port=(b,r,c)->(r.containsKey("resource")?d:page).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    var adapter=new GatewayJsonAdapter(port,json);
    try(var cursor=adapter.open(bundle,null,false,(comparison,pos)->{
      assertThat(comparison.differences()).containsExactly("VERSION");
      assertThat(pos).isEmpty();
      throw new IllegalStateException("observed");
    })){assertThatThrownBy(cursor::next).hasMessage("observed");}
  }
  @Test void envelopeDetectionGatesLaterPagesAndRestartsAtTheSameVerticalCursor() throws Exception {
    var approved=rest(Map.of("sourceSchema",Map.of("fieldsSha256",fingerprint)));
    var schema=declaration();
    var requests=new ArrayList<Map<String,Object>>();
    var adapter=new GatewayJsonAdapter((binding,request,correlation)->{
      requests.add(request);
      String response=request.containsKey("resource") ? schema.toString() : request.containsKey("cursor") ? """
          {"collection":"places","items":[],"mode":"CURSOR","hasMore":false}
          """ : """
          {"collection":"places","schemaVersion":"1","mode":"CURSOR","next":"p2","hasMore":true,
           "items":[{"id":"E-17","revision":4,"op":"DELETE"}]}
          """;
      return response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    },json);
    AdapterSpi.Checkpoint checkpoint;
    GatewayJsonAdapter.PageGate gate=(comparison,pos)->{
      if(!comparison.matches()) {
        assertThat(comparison.differences()).containsExactly("VERSION");
        throw new IllegalStateException("schema blocked");
      }
    };
    try(var cursor=adapter.open(approved,null,false,gate)) {
      assertThat(cursor.next().orElseThrow().provenance()).containsEntry("operation","DELETE");
      checkpoint=cursor.checkpoint();
      assertThatThrownBy(cursor::next).hasMessage("schema blocked");
    }
    try(var resumed=adapter.open(approved,checkpoint,false,gate)) {
      assertThatThrownBy(resumed::next).hasMessage("schema blocked");
    }
    var lookup=Map.of("resource","schema","collection","places");
    var next=Map.of("limit",100,"cursor","p2");
    assertThat(requests).isEqualTo(List.of(Map.of("pageSize",100),lookup,next,lookup,next,lookup));
  }

  @Test void openIncidentCannotBeClearedByAnUnversionedEmptyPage() {
    var adapter=new GatewayJsonAdapter((binding,request,correlation)->{
      assertThat(request).isEqualTo(Map.of("pageSize",100));
      return "{\"collection\":\"places\",\"items\":[]}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    },json);
    try(var cursor=adapter.open(rest(Map.of()),null,true,(comparison,pos)->{
      assertThat(comparison.differences()).contains("VERSION","APPROVED_FINGERPRINT_MISSING");
      throw new IllegalStateException("still blocked");
    })) {assertThatThrownBy(cursor::next).hasMessage("still blocked");}
  }

  @Test void tombstoneUsesEnvelopeIdentityAndRevisionWithoutProperties() throws Exception {
    var page=declaration().put("mode","CURSOR").put("hasMore",false);
    page.putArray("items").addObject().put("id","E-17").put("revision",4).put("op","DELETE");
    var adapter=new GatewayJsonAdapter((b,r,c)->page.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),json);
    try(var cursor=adapter.open(bundle,null)){
      var record=cursor.next().orElseThrow();
      assertThat(record.provenance()).containsEntry("sourceRevision","4").containsEntry("id","E-17")
          .containsEntry("operation","DELETE").containsEntry("schemaVersion","1");
      assertThat(record.payload()).isEmpty();
      assertThat(cursor.next()).isEmpty();
    }
  }
}
