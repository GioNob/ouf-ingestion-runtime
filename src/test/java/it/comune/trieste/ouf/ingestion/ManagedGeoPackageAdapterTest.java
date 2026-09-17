package it.comune.trieste.ouf.ingestion;

import static org.assertj.core.api.Assertions.*;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;

class ManagedGeoPackageAdapterTest {
  @Test void stableFeatureKeysSurviveReloadAndResumeWhileLayersRemainSeparate()throws Exception{
    var first=read("cameras",Map.of());var reload=read("cameras-reloaded",Map.of());
    assertThat(first.get(0).sourceObjectId()).isEqualTo(reload.get(1).sourceObjectId());
    assertThat(first.get(1).sourceObjectId()).isEqualTo(reload.get(0).sourceObjectId());
    assertThat(first.get(0).sourceObjectId()).isNotEqualTo(first.get(1).sourceObjectId());
    assertThat(read("cameras",Map.of("ordinal",1))).hasSize(1).first().isEqualTo(first.get(1));
    assertThat(first.get(0).provenance()).containsEntry("layer","cameras").containsKey("contentHash");
    assertThat(ManagedGeoPackageAdapter.identity("src","cabinets",Map.of("code","CAM-1"),List.of("code"))).isNotEqualTo(first.get(0).sourceObjectId());
  }
  @Test void lengthEncodingPreventsCompositeDelimiterCollisions(){
    String a=ManagedGeoPackageAdapter.identity("s","l",Map.of("a","x\u001fb=y","b","z"),List.of("a","b"));
    String b=ManagedGeoPackageAdapter.identity("s","l",Map.of("a","x","b","y\u001fb=z"),List.of("a","b"));assertThat(a).isNotEqualTo(b);
  }
  @Test void failsClosedForMissingKeysUnknownLayersAndChangedCrsOrHash(){
    for(var change:List.of(Map.<String,Object>of("identityFields",List.of()),Map.<String,Object>of("identityFields",List.of("cabinet")),Map.<String,Object>of("layer","missing"),Map.<String,Object>of("sourceCrs","EPSG:6708"),Map.<String,Object>of("expectedHash","sha256:"+"0".repeat(64))))assertThatThrownBy(()->read("cameras",Map.of(),change)).isInstanceOf(AdapterSpi.AdapterException.class);
  }
  private List<AdapterSpi.SourceRecord> read(String fixture,Map<String,Object> checkpoint)throws Exception{
    return read(fixture,checkpoint,Map.of());
  }
  private List<AdapterSpi.SourceRecord> read(String fixture,Map<String,Object> checkpoint,Map<String,Object> changes)throws Exception{
    byte[] bytes=getClass().getResourceAsStream("/geopackage/"+fixture+".gpkg").readAllBytes();String hash="sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    var config=new LinkedHashMap<String,Object>(Map.<String,Object>of("expectedSize",bytes.length,"expectedHash",hash,"layer","cameras","sourceCrs","EPSG:4326","geometryColumn","geom","identityFields",List.of("code"),"maxRows",100,"maxCellChars",1000));config.putAll(changes);
    var adapter=new ManagedGeoPackageAdapter((ref,size,expected)->bytes);
    var bundle=new ExecutionBundle("b","1","checksum","src","INTERNAL_MANAGED_GEOPACKAGE","object://fixture",config);var rows=new ArrayList<AdapterSpi.SourceRecord>();
    try(var cursor=adapter.open(bundle,new AdapterSpi.Checkpoint(checkpoint))){for(var next=cursor.next();next.isPresent();next=cursor.next())rows.add(next.orElseThrow());}return rows;
  }
}
