package it.comune.trieste.ouf.ingestion;

import com.healthmarketscience.jackcess.*;
import it.comune.trieste.ouf.managed.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class ManagedAdditionalFormatsTest {
  @TempDir Path directory;
  @Test void accessCompositeIdentitySurvivesRowReorderingAndCursorResumes()throws Exception{
    for(var format:List.of(Database.FileFormat.V2000,Database.FileFormat.V2010)){
      byte[] first=access(format,false),second=access(format,true);
      var original=accessRows(first,Map.of(),Map.of());var reloaded=accessRows(second,Map.of(),Map.of());
      assertThat(original).hasSize(2);assertThat(original.get(0).sourceObjectId()).isEqualTo(reloaded.get(1).sourceObjectId());
      assertThat(original.get(1).sourceObjectId()).isEqualTo(reloaded.get(0).sourceObjectId());
      assertThat(accessRows(first,Map.of("ordinal",1),Map.of())).containsExactly(original.get(1));
      assertThat(original.get(0).payload()).doesNotContainKey("privateNote");
      for(var change:List.of(Map.<String,Object>of("maxRows",1),Map.<String,Object>of("expectedHash","sha256:"+"0".repeat(64)),Map.<String,Object>of("identityFields",List.of("privateNote"))))
        assertThatThrownBy(()->accessRows(first,Map.of(),change)).isInstanceOf(AdapterSpi.AdapterException.class);
    }
  }
  @Test void shapefileAdapterPinsEncodingCrsGeometryKeyAndApprovedLimits()throws Exception{
    byte[] bytes=ShapefileReaderTest.zip(ShapefileReaderTest.fixture());
    String geometry;try(var reader=new ShapefileReader(bytes)){geometry=reader.layers().getFirst().geometryColumn();}
    var configuration=base(bytes);configuration.putAll(Map.of("layer","assets","sourceCrs","EPSG:4326","geometryColumn",geometry,"encoding","UTF-8","identityFields",List.of("CODE")));
    var adapter=new ManagedShapefileAdapter((ref,size,hash)->bytes);
    var first=read(adapter,configuration,Map.of(),"INTERNAL_MANAGED_SHAPEFILE");
    assertThat(first).hasSize(1);assertThat(first.getFirst().payload()).containsEntry("CODE","001");
    assertThat(read(adapter,configuration,Map.of("ordinal",1),"INTERNAL_MANAGED_SHAPEFILE")).isEmpty();
    for(var change:List.of(Map.<String,Object>of("encoding","windows-1252"),Map.<String,Object>of("sourceCrs","EPSG:6708"),Map.<String,Object>of("identityFields",List.of(geometry)),Map.<String,Object>of("maxCellChars",2))){
      var changed=new LinkedHashMap<>(configuration);changed.putAll(change);
      assertThatThrownBy(()->read(adapter,changed,Map.of(),"INTERNAL_MANAGED_SHAPEFILE")).isInstanceOf(AdapterSpi.AdapterException.class);
    }
  }
  private byte[] access(Database.FileFormat format,boolean reverse)throws Exception{
    Path path=directory.resolve(format.name()+reverse+".db");
    try(var db=DatabaseBuilder.create(format,path.toFile())){
      var table=new TableBuilder("Assets").addColumn(new ColumnBuilder("zone",DataType.TEXT)).addColumn(new ColumnBuilder("id",DataType.LONG)).addColumn(new ColumnBuilder("privateNote",DataType.TEXT)).toTable(db);
      for(int id:reverse?new int[]{2,1}:new int[]{1,2})table.addRow("Z",id,"excluded");
    }return Files.readAllBytes(path);
  }
  private List<AdapterSpi.SourceRecord> accessRows(byte[] bytes,Map<String,Object> checkpoint,Map<String,Object> changes)throws Exception{
    var c=base(bytes);c.putAll(Map.of("layer","Assets","identityFields",List.of("zone","id"),"expectedFieldNames",List.of("zone","id")));c.putAll(changes);
    return read(new ManagedAccessAdapter((ref,size,hash)->bytes),c,checkpoint,"INTERNAL_MANAGED_ACCESS");
  }
  private Map<String,Object> base(byte[] bytes)throws Exception{return new LinkedHashMap<>(Map.of("expectedSize",bytes.length,"expectedHash","sha256:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)),"maxRows",100,"maxCellChars",1000));}
  private List<AdapterSpi.SourceRecord> read(AdapterSpi adapter,Map<String,Object> config,Map<String,Object> checkpoint,String mode){
    var bundle=new ExecutionBundle("b","1","hash","source",mode,"object://fixture",config);var rows=new ArrayList<AdapterSpi.SourceRecord>();
    try(var cursor=adapter.open(bundle,new AdapterSpi.Checkpoint(checkpoint))){for(var next=cursor.next();next.isPresent();next=cursor.next())rows.add(next.orElseThrow());}return rows;
  }
}
