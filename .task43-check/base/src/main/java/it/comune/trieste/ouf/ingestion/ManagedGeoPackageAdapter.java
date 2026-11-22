package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;
import it.comune.trieste.ouf.geopackage.GeoPackageReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Executes only the selected layer and approved key from an immutable publication. */
public final class ManagedGeoPackageAdapter implements AdapterSpi {
  private final RuntimePorts.ManagedObjectPort objects;
  public ManagedGeoPackageAdapter(RuntimePorts.ManagedObjectPort objects){this.objects=objects;}
  public String adapterId(){return "managed-geopackage-v1";}
  public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_GEOPACKAGE");}
  public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}
  public RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint){
    var c=bundle.configuration();String layerName=text(c,"layer"),hash=text(c,"expectedHash");
    long size=number(c,"expectedSize",1,10*1024*1024);
    int maxRows=(int)number(c,"maxRows",1,10000),maxCell=(int)number(c,"maxCellChars",1,1048576);
    if(!(c.get("identityFields") instanceof List<?> rawKeys)||rawKeys.isEmpty()||rawKeys.size()>256||rawKeys.stream().anyMatch(k->!(k instanceof String)))throw error("ING_GPKG_IDENTITY_REQUIRED",ErrorClass.CONFIGURATION);
    var keys=rawKeys.stream().map(String.class::cast).toList();
    if(new HashSet<>(keys).size()!=keys.size())throw error("ING_GPKG_IDENTITY_INVALID",ErrorClass.CONFIGURATION);
    byte[] bytes=objects.read(bundle.bindingRef(),size,hash);
    if(bytes.length!=size||!hash.equals("sha256:"+digest(bytes)))throw error("ING_MANAGED_INTEGRITY_MISMATCH",ErrorClass.DATA);
    List<SourceRecord> records=new ArrayList<>();
    try(var reader=new GeoPackageReader(bytes)){
      var layer=reader.layers().stream().filter(l->l.name().equals(layerName)).findFirst().orElseThrow(()->error("ING_GPKG_LAYER_MISSING",ErrorClass.CONFIGURATION));
      if(!layer.crs().equals(text(c,"sourceCrs"))||!layer.geometryColumn().equals(text(c,"geometryColumn")))throw error("ING_GPKG_PROFILE_MISMATCH",ErrorClass.CONFIGURATION);
      if(keys.contains(layer.geometryColumn())||!layer.columns().stream().map(GeoPackageReader.Column::name).toList().containsAll(keys))throw error("ING_GPKG_IDENTITY_INVALID",ErrorClass.CONFIGURATION);
      var ids=new HashSet<String>();
      for(var row:reader.features(layer,maxRows,maxCell)){
        String id=identity(bundle.sourceId(),layerName,row,keys);
        if(!ids.add(id))throw error("ING_MANAGED_DUPLICATE_IDENTITY",ErrorClass.DATA);
        records.add(new SourceRecord(id,records.size()+1,row,Map.of("managedObjectRef",bundle.bindingRef(),"contentHash",hash,"layer",layerName,"featureId",row.get(layer.primaryKey()),"geometryColumn",layer.geometryColumn(),"sourceCrs",layer.crs(),"srsId",layer.srsId())));
      }
    }catch(IllegalArgumentException e){throw error("ING_GPKG_INVALID_OR_UNSUPPORTED",ErrorClass.DATA);}
    long ordinal=checkpoint==null||checkpoint.value().isEmpty()?0:number(checkpoint.value(),"ordinal",0,records.size());
    return new RecordCursor(){int position=(int)ordinal;public Optional<SourceRecord> next(){return position<records.size()?Optional.of(records.get(position++)):Optional.empty();}public Checkpoint checkpoint(){return new Checkpoint(Map.of("ordinal",position));}public void close(){records.clear();}};
  }
  static String identity(String source,String layer,Map<String,Object> row,List<String> keys){
    var material=new StringBuilder("GPKG1");append(material,source);append(material,layer);
    for(String key:keys){Object value=row.get(key);if(value==null||String.valueOf(value).isBlank()||value instanceof Map<?,?>||value instanceof List<?>)throw error("ING_MANAGED_IDENTITY_MISSING",ErrorClass.DATA);append(material,key);append(material,String.valueOf(value).strip());}
    return "managed-gpkg:"+digest(material.toString().getBytes(StandardCharsets.UTF_8));
  }
  private static void append(StringBuilder b,String value){b.append(value.length()).append(':').append(value);}
  private static String digest(byte[] bytes){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}catch(Exception e){throw new IllegalStateException(e);}}
  private static String text(Map<String,Object> c,String key){if(!(c.get(key) instanceof String s)||s.isBlank())throw error("ING_GPKG_CONFIG_INVALID",ErrorClass.CONFIGURATION);return s;}
  private static long number(Map<String,Object> c,String key,long min,long max){if(!(c.get(key) instanceof Number n)||n.longValue()<min||n.longValue()>max||n.doubleValue()!=n.longValue())throw error("ING_GPKG_CONFIG_INVALID",ErrorClass.CONFIGURATION);return n.longValue();}
  private static AdapterException error(String code,ErrorClass cls){return new AdapterException(code,cls,code);}
}
