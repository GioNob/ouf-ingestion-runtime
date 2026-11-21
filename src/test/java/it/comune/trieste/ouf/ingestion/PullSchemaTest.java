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
    try(var cursor=adapter.open(bundle,null,(comparison,pos)->{
      assertThat(comparison.differences()).containsExactly("VERSION");
      assertThat(pos).isEmpty();
      throw new IllegalStateException("observed");
    })){assertThatThrownBy(cursor::next).hasMessage("observed");}
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
